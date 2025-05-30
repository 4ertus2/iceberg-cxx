#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <DataStreams/ParquetBlockInputStream.h>
#include <arrow_clickhouse_types.h>

#include <memory>
#include <utility>
#include <vector>

#include "DataStreams/IBlockInputStream.h"
#include "iceberg-tools/tools/mod_opts.h"
#include "tools/common.h"
#include "tools/metadata_tree.h"
#include "tools/s3client.h"

namespace iceberg::tools {

class S3InputFileStream : public AH::IBlockInputStream {
 public:
  explicit S3InputFileStream(const std::vector<std::pair<arrow::fs::FileLocator, arrow::fs::FileLocator>>& renames);

  AH::Header getHeader() const override;

 protected:
  AH::Clod readImpl() override;

 private:
  std::vector<std::pair<arrow::fs::FileLocator, arrow::fs::FileLocator>> order_to_process_;
  size_t current_file_iterator_ = 0;
  std::shared_ptr<AH::ParquetBlockInputStream> data_input_stream_;
  std::shared_ptr<AH::StubBlock> metadata_block_;
};

class PositionalDeleteStream : public AH::IInputStream {
 public:
  explicit PositionalDeleteStream(const std::vector<arrow::fs::FileLocator>& positional_delete_files,
                                  int chunk_size = 1000000);

 protected:
  AH::Clod readImpl() override;

 private:
  AH::InputStreamPtr merged_input_stream_;
  std::shared_ptr<AH::StubBlock> metadata_block_;
};

class EqualityDeleteStream : public AH::IInputStream {
 public:
  explicit EqualityDeleteStream(const std::vector<arrow::fs::FileLocator>& equality_delete_files);

 protected:
  AH::Clod readImpl() override;

 private:
  std::vector<arrow::fs::FileLocator> equality_delete_files_;
  size_t current_file_iterator_ = 0;
  std::shared_ptr<AH::ParquetBlockInputStream> data_input_stream_;
  std::shared_ptr<AH::StubBlock> metadata_block_;
};

class CompactionOutputStream : public AH::IOutputStream {
 public:
  explicit CompactionOutputStream(
      const std::vector<std::pair<arrow::fs::FileLocator, arrow::fs::FileLocator>>& renames);

 protected:
  void write(const AH::Clod& clod) override;

 private:
  std::vector<std::pair<arrow::fs::FileLocator, arrow::fs::FileLocator>> renames_;
  std::shared_ptr<AH::IOutputStream> output_stream_;
  std::optional<int> current_file_iterator_;
};

std::pair<AH::InputStreamPtr, AH::OutputStreamPtr> BuildOnlineCompactionPipeline(
    const std::vector<std::pair<arrow::fs::FileLocator, arrow::fs::FileLocator>>& renames,
    const std::vector<arrow::fs::FileLocator>& positional_delete_files,
    const std::vector<arrow::fs::FileLocator>& equality_delete_files);

}  // namespace iceberg::tools
