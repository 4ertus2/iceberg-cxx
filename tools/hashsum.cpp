#include <DataStreams/FileStubInputStream.h>
#include <DataStreams/FileStubOutputStream.h>
#include <DataStreams/ParallelInputsSink.h>

#include <fstream>
#include <iostream>
#include <map>

#include "iceberg-tools/tools/mod_opts.h"
#include "tools/common.h"
#include "tools/metadata_tree.h"
#include "tools/s3client.h"

namespace iceberg::tools {
namespace {

// returns map for stable, ordered output
std::map<std::string, std::string> HashSumMT(const std::vector<arrow::fs::FileLocator>& input_files,
                                             const arrow::fs::FileLocator& dev_null, unsigned max_compute_threads) {
  if (input_files.empty()) {
    return {};
  }

  AH::InputStreams istreams;
  istreams.reserve(input_files.size());
  AH::OutputStreams ostreams;
  ostreams.reserve(input_files.size());

  for (size_t i = 0; i < input_files.size(); ++i) {
    auto src_locator = std::make_shared<arrow::fs::FileLocator>(input_files[i]);
    auto dst_locator = std::make_shared<arrow::fs::FileLocator>(dev_null);

    istreams.emplace_back(std::make_shared<AH::FileStubInputStream>(src_locator));
    auto out = std::make_shared<AH::FileStubOutputStream>(dst_locator);
    out->initCRC();
    ostreams.emplace_back(out);
  }
#if 0
  static std::mutex mtx;
  auto progress = [&](const AH::Clod&, unsigned thread_num) {
    std::lock_guard lock(mtx);
    std::cerr << "[copyNToN] " << thread_num << std::endl;
  };
#endif
  AH::StubParallelInputsSink::copyNToN(istreams, ostreams, max_compute_threads /*, progress*/);

  std::map<std::string, std::string> path_to_hash;
  for (size_t i = 0; i < ostreams.size(); ++i) {
    auto& path = input_files[i].path;
    auto hash = ExtractHash(ostreams[i]);
    if (hash.empty()) {
      throw std::runtime_error("cannot extract hash from stream for " + path);
    }
    path_to_hash.emplace(path, std::move(hash));
  }
  return path_to_hash;
}

std::vector<arrow::fs::FileLocator> GetSrcLocations(
    std::shared_ptr<iceberg::tools::S3Client> s3client,
    const std::unordered_map<std::string, iceberg::tools::DataFileInfo>& files, bool verbose) {
  if (!s3client) {
    throw std::runtime_error(__FUNCTION__ + std::string() + " requires S3 client");
  }

  std::vector<arrow::fs::FileLocator> inputs;
  inputs.reserve(files.size());
  for (auto& [src, _] : files) {
    inputs.emplace_back(s3client->SrcFileLocator(src));

    if (verbose) {
      std::cerr << "[mt crc] " << inputs.back().path << std::endl;
    }
  }
  return inputs;
}

}  // namespace

std::string ExtractHash(const AH::OutputStreamPtr& ostream) {
  auto meta_stream = ostream->getMetadataStream(AH::StreamMetadataType::TOTALS);
  if (!meta_stream) {
    return {};
  }

  AH::Block block = meta_stream->read();
  if (!block || block->num_rows() != 1) {
    return {};
  }

  static constexpr const char* col_name_crc = "crc";
  auto column = block->GetColumnByName(col_name_crc);
  if (!column || column->type_id() != arrow::Type::STRING) {
    return {};
  }

  auto& str_column = static_cast<const arrow::StringArray&>(*column);
  return std::string(str_column.GetView(0));
}

// returns map for stable, ordered output
std::map<std::string, std::string> ReadSumsFile(const std::string& sum_file) {
  std::map<std::string, std::string> sums;
  std::ifstream ifs(sum_file);
  std::string line;
  while (std::getline(ifs, line)) {
    if (line.empty()) {
      break;
    }

    std::string path;
    std::string crc;
    std::stringstream ss;
    ss << line;
    ss >> crc >> path;
    if (crc.size() != 32 || path.empty()) {
      throw std::runtime_error("unexpected CRC " + crc + " or path " + path);
    }
    sums.emplace(std::move(path), std::move(crc));
  }
  return sums;
}

std::map<std::string, std::string> CalcHashSums(
    std::shared_ptr<S3Client> s3client, const std::unordered_map<std::string, iceberg::tools::DataFileInfo>& files,
    unsigned max_compute_threads, bool verbose) {
  std::vector<arrow::fs::FileLocator> inputs = GetSrcLocations(s3client, files, verbose);
  arrow::fs::FileLocator dev_null = s3client->SrcFileLocator("/dev/null");
  return HashSumMT(inputs, dev_null, max_compute_threads);
}

void WriteHashSums(const std::string& sum_file, const std::map<std::string, std::string>& sums, bool verbose) {
  if (sums.empty()) {
    return;
  }
  std::ofstream ofs(sum_file);
  for (auto& [path, crc] : sums) {
    if (crc.size() != 32 || path.empty()) {
      throw std::runtime_error("unexpected CRC " + crc + " or path " + path);
    }
    ofs << crc << "  " << path << std::endl;
    if (verbose) {
      std::cerr << crc << "  " << path << std::endl;
    }
  }
  std::cerr << "CRC file written: " << sum_file << std::endl;
}

bool CheckHashSums(const std::string& sum_file, const std::map<std::string, std::string>& sums, bool verbose) {
  bool ok = true;
  auto known_sums = ReadSumsFile(sum_file);
  for (auto& [path, sum] : sums) {
    auto it = known_sums.find(path);
    if (it == known_sums.end()) {
      std::cerr << path << ": FAILED, no CRC" << std::endl;
      ok = false;
    } else if (it->second != sum) {
      std::cerr << path << ": FAILED, expected: " << it->second << " actual: " << sum << std::endl;
      ok = false;
    }
    if (verbose) {
      std::cerr << path << ": OK" << std::endl;
    }
  }
  return ok;
}

bool FileExists(const arrow::fs::FileLocator& loc) {
  auto res = loc.filesystem->GetFileInfo(loc.path);
  return res.ok() && (*res).IsFile();
}

std::map<std::string, std::string> FindHashSums(std::shared_ptr<S3Client> s3client,
                                                const std::vector<arrow::fs::FileLocator>& sum_files,
                                                const std::filesystem::path& local_dir, const std::string& suffix) {
  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
  std::map<std::string, std::string> out;
  for (auto& loc : sum_files) {
    std::string tmp_path = local_dir / std::filesystem::path(loc.path).filename();
    tmp_path += suffix;

    if (FileExists(loc)) {
      try {
        arrow::fs::FileLocator tmp_loc{.filesystem = fs, .path = tmp_path};
        s3client->CopyFiles({loc}, {tmp_loc}, false);
      } catch (const std::exception& ex) {
        std::cerr << ex.what() << std::endl;
      }
    }

    auto known_sums = ReadSumsFile(tmp_path);
    for (auto&& [path, sum] : known_sums) {
      out.emplace(std::move(path), std::move(sum));
    }
  }
  return out;
}

}  // namespace iceberg::tools
