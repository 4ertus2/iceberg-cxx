#include "iceberg-tools/tools/compaction_streams.h"

#include <DataStreams/Iceberg/IcebergEqualityCompactionStream.h>
#include <DataStreams/Iceberg/IcebergPositionalCompactionStream.h>
#include <DataStreams/ParquetBlockOutputStream.h>
#include <YdbModes/switch_type.h>
#include <arrow/array/builder_binary.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>
#include <arrow_clickhouse_types.h>

#include <memory>
#include <stdexcept>

#include "DataStreams/MergingSortedInputStream.h"

namespace iceberg::tools {

static constexpr const char* file_index_column_name = "file_index";
static constexpr const char* additional_block_offset_column_name = "additional_block_offset";

namespace {

std::shared_ptr<AH::StubBlock> GetMetadataBlock(std::shared_ptr<const parquet::FileMetaData> metadata, int file_index) {
  arrow::FieldVector fields;
  fields.emplace_back(arrow::field(AH::Iceberg::IcebergEqualityCompactionStream::fieldsColumn, arrow::int32()));
  fields.emplace_back(arrow::field(AH::Iceberg::IcebergEqualityCompactionStream::layerIdColumn, arrow::int32()));
  fields.emplace_back(arrow::field(additional_block_offset_column_name, arrow::int32()));
  fields.emplace_back(arrow::field(AH::Iceberg::IcebergEqualityCompactionStream::partitionIdColumn, arrow::int32()));
  fields.emplace_back(arrow::field(file_index_column_name, arrow::int32()));
  auto schema = std::make_shared<arrow::Schema>(fields);

  arrow::Int32Builder builder_fields;
  arrow::Int32Builder builder_layer;
  arrow::Int32Builder builder_partition;
  arrow::Int32Builder builder_offset;
  arrow::Int32Builder builder_output_files;

  for (int i = 0; i < metadata->num_columns(); ++i) {
    AHY::ensure(builder_fields.Append(metadata->schema()->GetColumnRoot(i)->field_id()));
    AHY::ensure(builder_layer.Append(0));
    AHY::ensure(builder_offset.Append(0));
    AHY::ensure(builder_partition.Append(0));
    AHY::ensure(builder_output_files.Append(file_index));
  }

  auto data_fields = AHY::ensureResult(builder_fields.Finish());
  auto data_layers = AHY::ensureResult(builder_layer.Finish());
  auto data_offsets = AHY::ensureResult(builder_offset.Finish());
  auto data_partitions = AHY::ensureResult(builder_partition.Finish());
  auto data_output_files = AHY::ensureResult(builder_output_files.Finish());

  return std::static_pointer_cast<AH::StubBlock>(arrow::RecordBatch::Make(
      schema, data_fields->length(),
      std::vector{data_fields, data_layers, data_offsets, data_partitions, data_output_files}));
}

std::shared_ptr<AH::StubBlock> GetDeletesMetadataBlock() {
  arrow::FieldVector fields;
  fields.emplace_back(arrow::field(AH::Iceberg::IcebergEqualityCompactionStream::layerIdColumn, arrow::int32()));
  fields.emplace_back(arrow::field(AH::Iceberg::IcebergEqualityCompactionStream::partitionIdColumn, arrow::int32()));
  auto schema = std::make_shared<arrow::Schema>(fields);

  arrow::Int32Builder builder_layer;
  arrow::Int32Builder builder_partition;

  AHY::ensure(builder_layer.Append(0));
  AHY::ensure(builder_partition.Append(0));

  auto data_layers = AHY::ensureResult(builder_layer.Finish());
  auto data_partitions = AHY::ensureResult(builder_partition.Finish());

  return std::static_pointer_cast<AH::StubBlock>(
      arrow::RecordBatch::Make(schema, data_layers->length(), std::vector{data_layers, data_partitions}));
}

arrow::Result<std::shared_ptr<const parquet::FileMetaData>> GetFileMetadata(
    const arrow::fs::FileLocator& locator, parquet::ReaderProperties props = parquet::default_reader_properties(),
    parquet::ArrowReaderProperties arrow_props = parquet::default_arrow_reader_properties()) {
  parquet::arrow::FileReaderBuilder reader_builder;
  ARROW_ASSIGN_OR_RAISE(auto input_file, locator.filesystem->OpenInputFile(locator.path));
  ARROW_RETURN_NOT_OK(reader_builder.Open(input_file, props));
  reader_builder.memory_pool(arrow::default_memory_pool());
  reader_builder.properties(arrow_props);

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  ARROW_ASSIGN_OR_RAISE(arrow_reader, reader_builder.Build());

  std::shared_ptr<arrow::Schema> arrow_schema;
  ARROW_RETURN_NOT_OK(arrow_reader->GetSchema(&arrow_schema));
  auto metadata = arrow_reader->parquet_reader()->metadata();
  return metadata;
}

}  // namespace

S3InputFileStream::S3InputFileStream(
    const std::vector<std::pair<arrow::fs::FileLocator, arrow::fs::FileLocator>>& renames)
    : order_to_process_(renames) {}

AH::Clod S3InputFileStream::readImpl() {
  if (!data_input_stream_) {
    if (current_file_iterator_ >= order_to_process_.size()) {
      return {};
    }
    std::shared_ptr<const parquet::FileMetaData> metadata;
    if (auto status = GetFileMetadata(order_to_process_[current_file_iterator_].first); status.ok()) {
      metadata = status.ValueOrDie();
    } else {
      throw std::runtime_error("Can not get metadata from path " +
                               order_to_process_[current_file_iterator_].first.path);
    }

    metadata_block_ = GetMetadataBlock(metadata, current_file_iterator_);
    data_input_stream_ = std::make_shared<AH::ParquetBlockInputStream>(
        std::make_shared<arrow::fs::FileLocator>(order_to_process_[current_file_iterator_++].first));
  }
  AH::Clod clod = data_input_stream_->read();
  if (!clod) {
    data_input_stream_.reset();
    return readImpl();
  }
  clod.stub = metadata_block_;
  return clod;
}

AH::Header S3InputFileStream::getHeader() const { return nullptr; }

PositionalDeleteStream::PositionalDeleteStream(const std::vector<arrow::fs::FileLocator>& positional_delete_files,
                                               int chunk_size) {
  if (positional_delete_files.empty()) {
    return;
  }
  metadata_block_ = GetDeletesMetadataBlock();
  std::vector<AH::InputStreamPtr> data_streams;
  AH::Header input_data_stream_header;

  for (const auto& locator : positional_delete_files) {
    auto input_stream =
        std::make_shared<AH::ParquetBlockInputStream>(std::make_shared<arrow::fs::FileLocator>(locator));
    input_data_stream_header = input_stream->getHeader();
    data_streams.push_back(input_stream);
  }
  auto descr = std::make_shared<AHY::ReplaceSortDescription>(input_data_stream_header);
  merged_input_stream_ = std::make_shared<AHY::MergingSortedInputStream>(data_streams, descr, chunk_size, false);
}

AH::Clod PositionalDeleteStream::readImpl() {
  if (!merged_input_stream_) {
    return {};
  }
  AH::Clod clod = merged_input_stream_->read();
  clod.stub = metadata_block_;
  return clod;
}

EqualityDeleteStream::EqualityDeleteStream(const std::vector<arrow::fs::FileLocator>& equality_delete_files) {}

AH::Clod EqualityDeleteStream::readImpl() {
  if (!data_input_stream_) {
    if (current_file_iterator_ >= equality_delete_files_.size()) {
      return {};
    }
    metadata_block_ = GetDeletesMetadataBlock();
    data_input_stream_ = std::make_shared<AH::ParquetBlockInputStream>(
        std::make_shared<arrow::fs::FileLocator>(equality_delete_files_[current_file_iterator_++]));
  }
  AH::Clod clod = data_input_stream_->read();
  if (!clod) {
    data_input_stream_.reset();
    return readImpl();
  }
  clod.stub = metadata_block_;
  return clod;
}

CompactionOutputStream::CompactionOutputStream(
    const std::vector<std::pair<arrow::fs::FileLocator, arrow::fs::FileLocator>>& renames)
    : renames_(renames) {}

void CompactionOutputStream::write(const AH::Clod& clod) {
  auto out_file_index_scalar = AHY::ensureResult(
      std::static_pointer_cast<AH::StubBlock>(clod.stub)->GetColumnByName(file_index_column_name)->GetScalar(0));
  int out_file_index = std::dynamic_pointer_cast<arrow::Int32Scalar>(out_file_index_scalar)->value;

  if (!current_file_iterator_.has_value() || *current_file_iterator_ != out_file_index) {
    current_file_iterator_ = out_file_index;
    if (output_stream_) {
      output_stream_->flush();
    }
    output_stream_ = std::make_shared<AH::ParquetBlockOutputStream>(
        std::make_shared<arrow::fs::FileLocator>(renames_[out_file_index].second), clod.block->schema());
  }
  output_stream_->beforeWrite();
  output_stream_->write(clod);
}

std::pair<AH::InputStreamPtr, AH::OutputStreamPtr> BuildOnlineCompactionPipeline(
    const std::vector<std::pair<arrow::fs::FileLocator, arrow::fs::FileLocator>>& renames,
    const std::vector<arrow::fs::FileLocator>& positional_delete_files,
    const std::vector<arrow::fs::FileLocator>& equality_delete_files) {
  AH::OutputStreamPtr write_stream = std::make_shared<CompactionOutputStream>(renames);

  auto data_stream = std::make_shared<S3InputFileStream>(renames);
  auto positional_delete_data_stream = std::make_shared<PositionalDeleteStream>(positional_delete_files);
  auto equality_delete_data_stream = std::make_shared<EqualityDeleteStream>(positional_delete_files);

  auto compaction_positional_delete_stream =
      std::make_shared<AH::Iceberg::IcebergPositionalCompactionStream>(data_stream, positional_delete_data_stream);
  auto compaction_equality_delete_stream = std::make_shared<AH::Iceberg::IcebergEqualityCompactionStream>(
      compaction_positional_delete_stream, equality_delete_data_stream);
  return {compaction_equality_delete_stream, write_stream};
}

}  // namespace iceberg::tools
