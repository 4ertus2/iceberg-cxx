#pragma once
#include <Common/SortDescription.h>
#include <DataStreams/IBlockStream_fwd.h>

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/manifest_entry.h"
#include "tools/common.h"

namespace iceberg::tools {

struct DataFileInfo {
  iceberg::ManifestEntry::Status status;
  iceberg::ContentFile::FileContent content;
};

enum class CopyMode : uint32_t {
  kRawFile = 0,
  kRawParquet,
  kArrowParquet,
};

struct ModificationOptions {
  std::unordered_map<std::string, std::string>& data_renames;
  std::unordered_map<std::string, DataFileInfo> data_files;
  std::unordered_map<std::string, DataFileInfo> pos_delete_files;
  std::unordered_map<std::string, DataFileInfo> eq_delete_files;
  const std::unordered_set<std::string> columns_to_skip;
  const AH::SortDescription sort_descr;
  bool sort_data = false;  // sort or check sort_descr
  bool pos_delete_fix = false;
  bool calculate_crc = false;
  CopyMode copy_mode = CopyMode::kArrowParquet;
  uint32_t max_compute_threads = 1;
  uint32_t max_io_threads = 0;  // 0 means no special limit for io threads
  uint32_t chunk_size = 0;

  std::vector<std::string> MakeFilterProjection(std::shared_ptr<parquet::FileMetaData> metadata) const;
};

struct WrittenFileInfo {
  size_t file_size;
  std::vector<int64_t> split_offsets;
  std::string xxh128sum;
};

class S3Client;

bool FileExists(const arrow::fs::FileLocator& loc);

bool CopyModifiedFiles(std::shared_ptr<S3Client> s3client, const std::unordered_map<std::string, std::string>& renames,
                       const ModificationOptions& mod_opts, const std::string& tmp_dir,
                       std::unordered_map<std::string, WrittenFileInfo>& out_file_sizes, bool verbose);

bool CopyFilesWithApplyingDeleteFiles(std::shared_ptr<S3Client> s3client,
                                      const std::unordered_map<std::string, std::string>& renames,
                                      const std::vector<std::string>& positional_delete_files,
                                      const std::vector<std::string>& equality_delete_files, bool verbose);

std::string ExtractHash(const AH::OutputStreamPtr& ostream);
std::map<std::string, std::string> FindHashSums(std::shared_ptr<S3Client> s3client,
                                                const std::vector<arrow::fs::FileLocator>& sum_files,
                                                const std::filesystem::path& local_dir, const std::string& suffix = {});
std::map<std::string, std::string> ReadSumsFile(const std::string& sum_file);
void CopyFilesMT(std::shared_ptr<S3Client> s3client, const std::unordered_map<std::string, std::string>& src_dst,
                 unsigned max_threads);
std::map<std::string, std::string> CalcHashSums(
    std::shared_ptr<S3Client> s3client, const std::unordered_map<std::string, iceberg::tools::DataFileInfo>& files,
    unsigned max_compute_threads, bool verbose = false);
void WriteHashSums(const std::string& sum_file, const std::map<std::string, std::string>& sums, bool verbose);
bool CheckHashSums(const std::string& sum_file, const std::map<std::string, std::string>& sums, bool verbose);

void ConvertDiffFile(std::shared_ptr<S3Client> client, const std::vector<std::string>& data_files,
                     const std::vector<std::string>& diff_files, const std::string& result_data_file,
                     const std::string& result_diff_file, const std::vector<std::string>& primary_key, int num_threads,
                     int chunk_size, bool verbose);

}  // namespace iceberg::tools
