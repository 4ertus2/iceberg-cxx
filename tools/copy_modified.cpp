#include <Common/SortDescription.h>
#include <DataStreams/CheckSortedBlockInputStream.h>
#include <DataStreams/DiffStream.h>
#include <DataStreams/ETL/IcebergConverter.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FileStubInputStream.h>
#include <DataStreams/FileStubOutputStream.h>
#include <DataStreams/FilterColumnsBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/MergingSortedInputStream.h>
#include <DataStreams/ParallelInputsSink.h>
#include <DataStreams/ParquetBlockInputStream.h>
#include <DataStreams/ParquetBlockOutputStream.h>
#include <DataStreams/ParquetStubInputStream.h>
#include <DataStreams/ParquetStubOutputStream.h>
#include <DataStreams/SortingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <YdbModes/SsaProgram.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/visit_data_inline.h>

#include <iostream>
#include <memory>

#include "iceberg-tools/tools/compaction_streams.h"
#include "iceberg-tools/tools/mod_opts.h"
#include "tools/common.h"
#include "tools/metadata_tree.h"
#include "tools/s3client.h"

namespace iceberg::udf {
void RegisterReplace(const std::string& func_name, const std::unordered_map<std::string, std::string>& replaces);
}

namespace {

using ModificationOptions = iceberg::tools::ModificationOptions;
using CopyMode = iceberg::tools::CopyMode;

static constexpr uint32_t MAX_MERGED_BLOCK_SIZE = 64 * 1024;

std::shared_ptr<AHY::Program> GetPosDeletesSSA(const std::unordered_map<std::string, std::string>& data_renames) {
  static const std::string func_name = "iceberg.replace";
  static const std::string column_name = "file_path";

  iceberg::udf::RegisterReplace(func_name, data_renames);

  auto step = std::make_shared<AHY::ProgramStep>();
  step->assignes = {AHY::Assign(column_name, func_name, {column_name})};

  std::vector<std::shared_ptr<AHY::ProgramStep>> steps = {step};
  return std::make_shared<AHY::Program>(std::move(steps));
}

AH::InputStreamPtr WrapStream(AH::InputStreamPtr istream, const ModificationOptions& opts) {
  if (!opts.sort_descr.empty()) {
    if (opts.sort_data) {
      istream = std::make_shared<AHY::SortingBlockInputStream>(istream, opts.sort_descr);
      istream = std::make_shared<AH::MergeSortingBlockInputStream>(istream, opts.sort_descr, MAX_MERGED_BLOCK_SIZE);
    } else {
      istream = std::make_shared<AHY::CheckSortedBlockInputStream>(istream, opts.sort_descr);
    }
  }
  if (opts.pos_delete_fix) {
    std::shared_ptr<AHY::Program> ssa = GetPosDeletesSSA(opts.data_renames);
    istream = std::make_shared<AHY::ExpressionBlockInputStream>(istream, ssa, true);
  }
  return istream;
}

using iceberg::tools::WrittenFileInfo;

template <CopyMode copy_mode>
void FileProjectionMT(const std::vector<arrow::fs::FileLocator>& input_files,
                      const std::vector<arrow::fs::FileLocator>& output_files, const ModificationOptions& opts,
                      std::unordered_map<std::string, WrittenFileInfo>& out_info, bool verbose) {
  if (input_files.empty()) {
    return;
  }
  if (input_files.size() != output_files.size()) {
    throw std::runtime_error(__FUNCTION__);
  }

  AH::InputStreams istreams;
  istreams.reserve(input_files.size());
  AH::OutputStreams ostreams;
  ostreams.reserve(output_files.size());

  [[maybe_unused]] AH::Header header;
  std::vector<std::string> projection;
  for (size_t i = 0; i < input_files.size(); ++i) {
    auto& input = input_files[i];
    auto& output = output_files[i];

    if (!i && copy_mode != CopyMode::kRawFile) {
      auto input_io_file = AHY::ensureResult(input.filesystem->OpenInputFile(input.path));
      auto parquet_meta = iceberg::ParquetMetadata(input_io_file);
      projection = opts.MakeFilterProjection(parquet_meta);
      if constexpr (copy_mode == CopyMode::kArrowParquet) {
        auto istream = std::make_shared<AH::ParquetBlockInputStream>(input_io_file, projection, false);
        header = istream->getHeader();
        if (verbose) {
          std::cerr << "schema (after projection): " << std::endl << header->ToString() << std::endl;
        }
      }
    }

    auto src_locator = std::make_shared<arrow::fs::FileLocator>(input);
    auto dst_locator = std::make_shared<arrow::fs::FileLocator>(output);

    if constexpr (copy_mode == CopyMode::kArrowParquet) {
      auto istream = std::make_shared<AH::ParquetBlockInputStream>(src_locator, projection);
      istreams.emplace_back(WrapStream(istream, opts));
      ostreams.emplace_back(std::make_shared<AH::ParquetBlockOutputStream>(dst_locator, header));
    } else if constexpr (copy_mode == CopyMode::kRawParquet) {
      auto istream = std::make_shared<AH::ParquetStubInputStream>(src_locator, projection);
      istreams.emplace_back(istream);
      uint32_t buffer_size = opts.chunk_size ? opts.chunk_size : AH::ParquetStubOutputStream::BUFFER_SIZE;
      auto ostream = std::make_shared<AH::ParquetStubOutputStream>(dst_locator, buffer_size);
      if (opts.calculate_crc) {
        ostream->initCRC();
      }
      ostreams.emplace_back(ostream);
    } else if constexpr (copy_mode == CopyMode::kRawFile) {
      auto istream = std::make_shared<AH::FileStubInputStream>(src_locator);
      istreams.emplace_back(istream);
      uint32_t buffer_size = opts.chunk_size ? opts.chunk_size : AH::FileStubOutputStream::BUFFER_SIZE;
      auto ostream = std::make_shared<AH::FileStubOutputStream>(dst_locator, buffer_size);
      if (opts.calculate_crc) {
        ostream->initCRC();
      }
      ostreams.emplace_back(ostream);
    }
  }
#if 0
  static std::mutex mtx;
  auto progress = [&](const AH::Clod&, unsigned thread_num) {
    std::lock_guard lock(mtx);
    std::cerr << "[copyNToN] " << thread_num << std::endl;
  };
#endif
  AH::TParallelInputsSink<(copy_mode != CopyMode::kArrowParquet)>::copyNToN(istreams, ostreams,
                                                                            opts.max_compute_threads /*, progress*/);

  if (output_files.size() != ostreams.size()) {
    throw std::runtime_error(__FUNCTION__);
  }

  out_info.clear();
  for (size_t i = 0; i < ostreams.size(); ++i) {
    auto& output = output_files[i];
    auto& info = out_info[output.path];
    if (opts.calculate_crc) {
      info.xxh128sum = iceberg::tools::ExtractHash(ostreams[i]);
    }

    auto meta_stream = ostreams[i]->getMetadataStream(AH::StreamMetadataType::CHUNK_SIZES);
    if (meta_stream) {
      AH::Block block = meta_stream->read();
      bool ok = block && (block->num_columns() == 1) && (block->column(0)->type_id() == arrow::Type::UINT64);
      if (ok) {
        auto& column = static_cast<const arrow::NumericArray<arrow::UInt64Type>&>(*block->column(0));
        for (int64_t chunk = 0; chunk < column.length(); ++chunk) {
          info.file_size += column.Value(chunk);
        }
      }
    }

    meta_stream = ostreams[i]->getMetadataStream(AH::StreamMetadataType::ROW_GROUP_OFFSETS);
    if (meta_stream) {
      AH::Block block = meta_stream->read();
      bool ok = block && (block->num_columns() == 1) && (block->column(0)->type_id() == arrow::Type::UINT64);
      if (ok) {
        auto& column = static_cast<const arrow::NumericArray<arrow::UInt64Type>&>(*block->column(0));
        info.split_offsets.reserve(column.length());
        for (int64_t chunk = 0; chunk < column.length(); ++chunk) {
          info.split_offsets.emplace_back(column.Value(chunk));
        }
      }
    }
  }
}

using iceberg::tools::ManifestEntryHelper;
using iceberg::tools::MetadataTree;

}  // namespace

namespace iceberg::tools {

void CopyFilesMT(std::shared_ptr<S3Client> s3client, const std::unordered_map<std::string, std::string>& src_dst,
                 unsigned max_threads) {
  if (src_dst.empty()) {
    return;
  }
  if (!max_threads) {
    max_threads = 1;
  }

  AH::InputStreams istreams;
  istreams.reserve(src_dst.size());
  AH::OutputStreams ostreams;
  ostreams.reserve(src_dst.size());

  for (auto& [src, dst] : src_dst) {
    auto src_locator = std::make_shared<arrow::fs::FileLocator>(s3client->SrcFileLocator(src));
    auto dst_locator = std::make_shared<arrow::fs::FileLocator>(s3client->DstFileLocator(dst));

    istreams.emplace_back(std::make_shared<AH::FileStubInputStream>(src_locator));
    ostreams.emplace_back(std::make_shared<AH::FileStubOutputStream>(dst_locator));
  }

  AH::StubParallelInputsSink::copyNToN(istreams, ostreams, max_threads);
}

std::vector<std::string> ModificationOptions::MakeFilterProjection(
    std::shared_ptr<parquet::FileMetaData> metadata) const {
  if (columns_to_skip.empty()) return {};

  auto parquet_schema = metadata->schema();
  int num_columns = parquet_schema->num_columns();

  std::vector<std::string> ret;
  ret.reserve(num_columns);
  for (int i = 0; i < num_columns; ++i) {
    const parquet::ColumnDescriptor* col = parquet_schema->Column(i);
    if (col && !columns_to_skip.contains(col->name())) {
      ret.push_back(col->name());
    }
  }

  return ret;
}

static bool CreateParentDirs(const arrow::fs::FileLocator& locator) {
  if (dynamic_cast<arrow::fs::LocalFileSystem*>(locator.filesystem.get())) {
    auto dir = std::filesystem::path(locator.path).parent_path();
    if (!std::filesystem::exists(dir) && !std::filesystem::create_directories(dir)) {
      std::cerr << "cannot create dir " << dir << std::endl;
      return false;
    }
  }
  return true;
}

bool CopyModifiedFiles(std::shared_ptr<S3Client> s3client, const std::unordered_map<std::string, std::string>& renames,
                       const ModificationOptions& mod_opts, const std::string& tmp_dir,
                       std::unordered_map<std::string, WrittenFileInfo>& out_file_infos, bool verbose) {
  std::unordered_map<std::string, std::string> others;
  std::unordered_map<std::string, std::string> data;
  std::unordered_map<std::string, std::string> pos_deletes;
  for (auto& [src, dst] : renames) {
    // Do not convert meta and deletes
    bool is_data = mod_opts.data_files.contains(dst);
    bool is_pos_delete = mod_opts.pos_delete_files.contains(dst);
    if (is_data) {
      data.emplace(src, dst);
    } else if (is_pos_delete && s3client) {
      pos_deletes.emplace(src, dst);
    } else {
      others.emplace(src, dst);
    }
  }

  if (verbose) {
    std::cerr << "[mt modify] data: " << data.size() << " pos del: " << pos_deletes.size()
              << " others: " << others.size() << std::endl;
  }

  {
    std::vector<arrow::fs::FileLocator> data_inputs;
    std::vector<arrow::fs::FileLocator> data_outputs;

    for (auto& [src, dst] : data) {
      data_inputs.emplace_back(s3client->SrcFileLocator(src));
      data_outputs.emplace_back(s3client->DstFileLocator(dst));

      if (verbose) {
        std::cerr << "[mt modify] " << data_inputs.back().path << " " << data_outputs.back().path << std::endl;
      }
    }

    std::vector<arrow::fs::FileLocator> pos_del_inputs;
    std::vector<arrow::fs::FileLocator> pos_del_outputs;

    for (auto& [src, dst] : pos_deletes) {
      pos_del_inputs.emplace_back(s3client->SrcFileLocator(src));
      pos_del_outputs.emplace_back(s3client->DstFileLocator(dst));

      if (verbose) {
        std::cerr << "[pos del] " << pos_del_inputs.back().path << " " << pos_del_outputs.back().path << std::endl;
      }
    }

    {
      for (auto& dst : pos_del_outputs) {
        if (!CreateParentDirs(dst)) {
          return false;
        }
      }

      ModificationOptions opts = {.data_renames = mod_opts.data_renames,
                                  .pos_delete_fix = true,
                                  .max_compute_threads = mod_opts.max_compute_threads,
                                  .max_io_threads = mod_opts.max_io_threads,
                                  .chunk_size = mod_opts.chunk_size};
      FileProjectionMT<CopyMode::kArrowParquet>(pos_del_inputs, pos_del_outputs, opts, out_file_infos, verbose);
    }

    for (auto& dst : data_outputs) {
      if (!CreateParentDirs(dst)) {
        return false;
      }
    }

    switch (mod_opts.copy_mode) {
      case CopyMode::kRawFile:
        FileProjectionMT<CopyMode::kRawFile>(data_inputs, data_outputs, mod_opts, out_file_infos, verbose);
        break;
      case CopyMode::kRawParquet:
        FileProjectionMT<CopyMode::kRawParquet>(data_inputs, data_outputs, mod_opts, out_file_infos, verbose);
        break;
      case CopyMode::kArrowParquet:
        FileProjectionMT<CopyMode::kArrowParquet>(data_inputs, data_outputs, mod_opts, out_file_infos, verbose);
        break;
    }
  }

  if (others.size()) {
    return CopyFiles(s3client, others, CopyOptions());
  }
  return true;
}

bool CopyFilesWithApplyingDeleteFiles(std::shared_ptr<S3Client> s3client,
                                      const std::unordered_map<std::string, std::string>& renames,
                                      const std::vector<std::string>& positional_delete_files,
                                      const std::vector<std::string>& equality_delete_files, bool verbose) {
  std::vector<std::pair<arrow::fs::FileLocator, arrow::fs::FileLocator>> renames_locators;
  std::vector<arrow::fs::FileLocator> positional_delete_locators;
  std::vector<arrow::fs::FileLocator> equality_delete_locators;

  for (auto& [src, dst] : renames) {
    renames_locators.emplace_back(s3client->SrcFileLocator(src), s3client->DstFileLocator(dst));

    if (verbose) {
      std::cerr << "[copmaction] " << renames_locators.back().first.path << " " << renames_locators.back().second.path
                << std::endl;
    }
  }

  for (const auto& file_name : positional_delete_files) {
    positional_delete_locators.emplace_back(s3client->SrcFileLocator(file_name));
  }

  for (const auto& file_name : equality_delete_files) {
    equality_delete_locators.emplace_back(s3client->SrcFileLocator(file_name));
  }

  auto [input_stream, output_stream] =
      BuildOnlineCompactionPipeline(renames_locators, positional_delete_locators, equality_delete_locators);
  output_stream->beforeWrite();
  for (auto clod = input_stream->read(); clod;) {
    output_stream->write(clod);
  }
  output_stream->afterWrite();
  return true;
}

void ConvertDiffFile(std::shared_ptr<S3Client> client, const std::vector<std::string>& data_files,
                     const std::vector<std::string>& diff_files, const std::string& result_data_file,
                     const std::string& result_diff_file, const std::vector<std::string>& primary_key, int num_threads,
                     int chunk_size, bool verbose) {
  std::vector<std::shared_ptr<arrow::fs::FileLocator>> data_locators;
  for (const auto& data_file : data_files) {
    data_locators.push_back(std::make_shared<arrow::fs::FileLocator>(client->SrcFileLocator(data_file)));
  }

  std::vector<std::shared_ptr<arrow::fs::FileLocator>> diff_locators;
  for (const auto& diff_file : diff_files) {
    diff_locators.push_back(std::make_shared<arrow::fs::FileLocator>(client->SrcFileLocator(diff_file)));
  }

  auto tmp_file_writer = std::make_shared<AH::Iceberg::TmpFilesWriter>(std::make_shared<arrow::fs::LocalFileSystem>());
  std::cout << "[convert to iceberg] " << result_data_file << ' ' << result_diff_file << std::endl;
  AH::ETL::IcebergConverter converter(data_locators, diff_locators, tmp_file_writer, primary_key, num_threads,
                                      chunk_size);

  converter.convertToDiffFileToIceberg(AH::ETL::IcebergRepresentation{
      .update_file = std::make_shared<arrow::fs::FileLocator>(client->DstFileLocator(result_data_file)),
      .delete_file = std::make_shared<arrow::fs::FileLocator>(client->DstFileLocator(result_diff_file)),
  });
}

}  // namespace iceberg::tools
