#include <Common/SortDescription.h>
#include <ThriftHiveMetastore.h>
#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <arrow/filesystem/filesystem.h>
#include <hive_metastore_types.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

#include "iceberg-tools/tools/mod_opts.h"
#include "iceberg/table_metadata.h"
#include "tools/common.h"
#include "tools/metadata_tree.h"
#include "tools/metastore_client.h"
#include "tools/s3client.h"

namespace hive = Apache::Hadoop::Hive;
namespace thrift = apache::thrift;

using iceberg::tools::CopyFiles;
using iceberg::tools::CopyFilesOrThrow;
using iceberg::tools::CopyOptions;
using iceberg::tools::MetadataTree;
using iceberg::tools::S3Client;
using iceberg::tools::S3Init;
using iceberg::tools::SnapshotMaker;
using iceberg::tools::StringFix;

static constexpr uint16_t HMS_PORT = 9083;

namespace {

void LogAction(const std::string& action, const std::string& table, const std::string& src, const std::string& dst) {
  std::cerr << action << " for table '" << table << "' from " << src << " to " << dst << std::endl;
}

std::string DbTableName(const std::string& db, const std::string& table_name) {
  return db.empty() ? table_name : (db + "." + table_name);
}

std::string CropPrefix(const std::string& src, const std::string& prefix = "://") {
  auto pos = src.find(prefix);
  if (pos != std::string::npos) {
    return src.substr(pos + prefix.size());
  }
  return src;
}

std::map<std::string, std::string> CropPrefix(std::map<std::string, std::string>&& src, const std::string& prefix) {
  if (prefix.empty()) {
    return src;
  }
  std::map<std::string, std::string> out;
  for (auto [k, v] : src) {
    out.emplace(CropPrefix(k, prefix), v);
  }
  return out;
}

std::string CropTrailing(std::string&& s, char value = '/') {
  while (s.size() && s.back() == value) {
    s.pop_back();
  }
  return s;
}

std::string PreserveURL(std::string&& s) {
  if (s.size() && s[s.size() - 1] == ':') {
    return s + '/';
  }
  return s;
}

std::string GetParentLocation(const std::string& path, uint32_t grand_count = 0) {
  auto parent = std::filesystem::path(path).parent_path();
  for (uint32_t i = 0; i < grand_count; ++i) {
    parent = parent.parent_path();
  }
  return CropTrailing(parent);
}

std::vector<std::string> SplitKeys(std::string&& s, char delim = ',') {
  std::vector<std::string> result = {""};
  for (auto symb : s) {
    if (symb == delim) {
      if (!result.empty() && !result.back().empty()) {
        result.push_back("");
      }
    } else {
      result.back().push_back(symb);
    }
  }
  while (!result.empty() && result.back().empty()) {
    result.pop_back();
  }
  return result;
}

void BackupLocalDir(const std::filesystem::path& src_path, const std::filesystem::path& bak_path) {
  if (src_path.empty()) {
    return;
  }
  if (bak_path.empty()) {
    std::filesystem::remove_all(src_path);
  } else {
    std::filesystem::remove_all(bak_path);
    std::filesystem::rename(src_path, bak_path);
  }
  std::filesystem::create_directories(src_path);
}

#if 0
void CopyDir(std::shared_ptr<S3Client> s3client, const std::string& src, const std::string& dst) {
  if (!s3client) {
    throw std::runtime_error("no S3 client");
  }
  s3client->CopyDir(src, dst, false);
}
#endif

template <typename T>
std::set<std::string> CopyFilesToDir(std::shared_ptr<S3Client> s3client, const T& srcs, const std::string& dst_dir,
                                     unsigned max_threads) {
  std::unordered_map<std::string, std::string> src_dst;
  std::set<std::string> out;
  for (auto& src : srcs) {
    std::string dst_path = dst_dir / std::filesystem::path(src).filename();
    src_dst[src] = dst_path;
    out.emplace(std::move(dst_path));
  }
  CopyFilesMT(s3client, src_dst, max_threads);
  return out;
}

struct HmsTableInfo {
  static constexpr const char* kMetadataLocation = "metadata_location";
  static constexpr const char* kPreviousMetadataLocation = "previous_metadata_location";

  std::string host;
  uint16_t port = 0;
  std::string db;
  std::string tablename;
  std::filesystem::path path;
  std::optional<hive::Table> table;

  void CopyNotSet(const HmsTableInfo& other) {
    if (host.empty()) {
      host = other.host;
    }
    if (!port) {
      port = other.port;
    }
    if (db.empty()) {
      db = other.db;
    }
    if (tablename.empty()) {
      tablename = other.tablename;
    }
  }

  bool IsSame(const HmsTableInfo& other) const {
    return host == other.host && port == other.port && db == other.db && tablename == other.tablename;
  }

  std::filesystem::path MetaJsonParentPath() const { return GetParentLocation(std::filesystem::path(MetadataJson())); }

  const std::string& SdLocation() const {
    if (!table) {
      throw std::runtime_error(std::string("table is not loaded '") + DbTableName(db, tablename) + "'");
    }
    return table->sd.location;
  }

  std::string MetadataJson() const {
    if (!table) {
      throw std::runtime_error(std::string("table is not loaded '") + DbTableName(db, tablename) + "'");
    }
    auto it = table->parameters.find(kMetadataLocation);
    if (it == table->parameters.end()) {
      throw std::runtime_error(std::string("no 'metadata_location' parameter for table '") +
                               DbTableName(db, tablename) + "'");
    }
    return it->second;
  }

  void Load(bool verbose) {
    table = hive::Table();
    ice_tea::MetastoreClient client(host, port);
    client.Get().get_table(*table, db, tablename);
    MetadataJson();

    if (verbose) {
      std::cerr << "Loaded table: ";
      table->printTo(std::cerr);
      std::cerr << std::endl;
    }
  }

  void Update(const std::optional<hive::Table>& t, const std::string& sd_location, const std::string& meta_json) {
    if (t) {
      table = *t;
    }
    if (!table) {
      table = hive::Table();
    }
    table->dbName = db;
    table->tableName = tablename;

    // TODO(a.v.zuykov): update 'current-snapshot-id' and 'current-snapshot-summary' properties if changed
    // TODO(a.v.zuykov): update table 'owner', 'retention' if needed
    // TODO(a.v.zuykov): update sd 'bucketCols', 'sortCols', 'parameters', 'skewedInfo' if changed
    table->sd.location = sd_location;
    if (table->parameters.contains(kMetadataLocation)) {
      table->parameters[kPreviousMetadataLocation] = table->parameters[kMetadataLocation];
    }
    table->parameters[kMetadataLocation] = meta_json;
  }

  bool Store(bool verbose) {
    ice_tea::MetastoreClient client(host, port);

    bool need_create_table = false;
    try {
      hive::Table tmp_table;
      client.Get().get_table(tmp_table, db, tablename);
    } catch (const hive::NoSuchObjectException&) {
      need_create_table = true;
    }

    if (verbose) {
      std::cerr << "Store table: ";
      table->printTo(std::cerr);
      std::cerr << std::endl;
    }

    try {
      if (need_create_table) {
        client.Get().create_table(*table);
      } else {
        client.Get().alter_table(db, tablename, *table);
      }
    } catch (const thrift::TException& ex) {
      std::cerr << ex.what() << std::endl;
      return false;
    }

    return true;
  }
};

void CropSameSuffix(std::string& logical_location, std::string& actual_location) {
  std::filesystem::path left(logical_location);
  std::filesystem::path right(actual_location);
  while (!left.empty() && !right.empty()) {
    if (left.filename() != right.filename()) {
      left = PreserveURL(left.string());
      right = PreserveURL(right.string());
      break;
    }
    left = GetParentLocation(left);
    right = GetParentLocation(right);
  }
  logical_location = left;
  actual_location = right;
}

bool ReadTSV(const std::string& file, std::function<void(std::ifstream& input)> callback) {
  if (!std::filesystem::is_regular_file(file)) {
    return false;
  }
  std::ifstream input(file);
  while (!input.eof() && !input.bad()) {
    callback(input);
  }
  return true;
}

bool IsTableAllowed(const std::string& allow_list, const std::string& db, const std::string& table) {
  if (allow_list.empty()) {
    return true;
  }
  std::unordered_set<std::string> allowed;
  ReadTSV(allow_list, [&allowed](std::ifstream& input) {
    std::string line;
    std::getline(input, line);
    if (!line.empty()) {
      allowed.insert(line);
    }
  });
  return allowed.contains(db + "." + table);
}

std::unordered_set<std::string> GetColumnFilter(const std::string& full_table_name,
                                                const std::string& deny_columns_list) {
  std::unordered_set<std::string> columns_to_skip;
  if (!deny_columns_list.empty()) {
    bool ok = ReadTSV(deny_columns_list, [&columns_to_skip, full_table_name](std::ifstream& input) {
      std::string line;
      std::getline(input, line);
      if (!line.empty()) {
        std::stringstream ss(line);
        std::string table_name;
        std::string col_name;
        ss >> table_name >> col_name;
        if (table_name == full_table_name) {
          columns_to_skip.emplace(std::move(col_name));
        }
      }
    });
    if (!ok) {
      throw std::runtime_error("cannot read deny_colum_list '" + deny_columns_list + "'");
    }
  }
  return columns_to_skip;
}

std::vector<std::string> SplitString(const std::string& src, char delimiter = ',') {
  std::vector<std::string> out;
  std::stringstream ss(src);
  while (ss.good()) {
    std::string value;
    std::getline(ss, value, delimiter);
    value.erase(std::remove(value.begin(), value.end(), ' '), value.end());
    if (value.size()) {
      out.emplace_back(std::move(value));
    }
  }
  return out;
}

std::shared_ptr<iceberg::SortOrder> MakeSortOrder(const std::string& columns_line, const std::string& directions_line,
                                                  int32_t order_id = 0) {
  auto columns = SplitString(columns_line);
  if (columns.empty()) {
    return {};
  }

  std::vector<iceberg::SortDirection> directions;
  {
    auto directions_str = SplitString(directions_line);
    directions.reserve(columns.size());
    std::size_t pos{};
    for (auto& s : directions_str) {
      directions.emplace_back((std::stoi(s, &pos) > 0) ? iceberg::SortDirection::kAsc : iceberg::SortDirection::kDesc);
    }
  }
  directions.resize(columns.size(), iceberg::SortDirection::kAsc);

  auto sort_order = std::make_shared<iceberg::SortOrder>();
  sort_order->fields.reserve(columns.size());
  for (size_t i = 0; i < columns.size(); ++i) {
    iceberg::SortField field{.transform = columns[i],  // we need tmp place for name
                             .source_id = 0,           // we do not know ids here
                             .direction = directions[i],
                             .null_order = iceberg::NullOrder::kNullsLast};
    sort_order->fields.emplace_back(std::move(field));
  }
  return sort_order;
}

void FixSortOrderFields(iceberg::SortOrder& sort_order, std::shared_ptr<iceberg::Schema> current_schema) {
  std::unordered_map<std::string, int32_t> names;
  for (const iceberg::types::NestedField& col : current_schema->Columns()) {
    names[col.name] = col.field_id;
  }

  for (auto& field : sort_order.fields) {
    auto it = names.find(field.transform);
    if (it == names.end()) {
      throw std::runtime_error("no column in schema: " + field.transform);
    }
    field.source_id = it->second;
    field.transform = {};
  }
}

AH::SortDescription MakeSortDescription(std::shared_ptr<iceberg::SortOrder> sort_order,
                                        std::shared_ptr<iceberg::Schema> schema) {
  if (!sort_order || !schema) {
    return {};
  }

  std::unordered_map<int32_t, std::string> id2name;
  for (auto& col : schema->Columns()) {
    id2name[col.field_id] = col.name;
  }

  std::vector<AH::SortColumnDescription> sort_descr;
  sort_descr.reserve(sort_order->fields.size());

  for (size_t i = 0; i < sort_order->fields.size(); ++i) {
    auto& field = sort_order->fields[i];
    if (!id2name.contains(field.source_id)) {
      throw std::runtime_error("no column with field_id " + std::to_string(field.source_id));
    }
    AH::SortColumnDescription dir{.column_name = id2name[field.source_id],
                                  .direction = ((field.direction == iceberg::SortDirection::kAsc) ? 1 : -1),
                                  .nulls_direction = ((field.null_order == iceberg::NullOrder::kNullsFirst) ? -1 : 1)};
    sort_descr.emplace_back(std::move(dir));
  }

  return sort_descr;
}

#if 0
void EnsureSameSchemaAndSortOrder(const MetadataTree& src, const MetadataTree& dst) {
  auto src_table_metadata = src.GetMetadataFile().table_metadata;
  auto dst_table_metadata = dst.GetMetadataFile().table_metadata;

  iceberg::tools::EnsureSameSchema(src_table_metadata->GetCurrentSchema(), dst_table_metadata->GetCurrentSchema());
  iceberg::tools::EnsureSameSortOrder(src_table_metadata->GetSortOrder(), dst_table_metadata->GetSortOrder());
}
#endif

enum class CopyMode : uint32_t {
  kUnknown = 0,
  kCopyActual,    // copy met data and converted meta
  kModifyActual,  // kCopyActual with modifications
  kCopyMeta,      // copy converted metadata only
  kDryRun,        // convert meta to tmp dir, copy nothing
  kCRC,           // calculate hashsum file for actual files
  kCheckCRC,      // check hashsums from file
  kCopyOnWrite,   // copy file and apply delete files
  kDiffTable,     // convert diff table into iceberg updates
};

CopyMode StringToMode(const std::string& str_mode, bool& fix_location) {
  if (str_mode == "actual") {
    return CopyMode::kCopyActual;
  } else if (str_mode == "meta") {
    return CopyMode::kCopyMeta;
  } else if (str_mode == "fixlocation") {
    fix_location = true;
    return CopyMode::kCopyMeta;
  } else if (str_mode == "crc") {
    return CopyMode::kCRC;
  } else if (str_mode == "checkcrc") {
    return CopyMode::kCheckCRC;
  } else if (str_mode == "dryrun") {
    return CopyMode::kDryRun;
  } else if (str_mode == "copy_on_write") {
    return CopyMode::kCopyOnWrite;
  } else if (str_mode == "diff_table") {
    return CopyMode::kDiffTable;
  }
  return CopyMode::kUnknown;
}

CopyMode DataModifyMode(CopyMode mode) {
  switch (mode) {
    case CopyMode::kCopyActual:
      return CopyMode::kModifyActual;
    default:
      break;
  }
  return mode;
}

struct MetaModification {
  using DataFileInfo = iceberg::tools::DataFileInfo;
  using ManifestEntry = iceberg::ManifestEntry;

  std::filesystem::path meta_tmpdir_json;
  MetadataTree meta_tree;
  std::vector<MetadataTree> prev_meta;
  std::unordered_map<std::string, std::string> renames_data;
  std::unordered_map<std::string, std::string> renames_meta;
  std::unordered_map<std::string, std::string> rename_locations;

  MetaModification(const std::filesystem::path& meta_json, const std::filesystem::path& meta_tmpdir, bool ignore_snaps)
      : meta_tmpdir_json(meta_tmpdir / meta_json.filename()), meta_tree(meta_tmpdir_json, ignore_snaps) {}

  MetaModification(const std::filesystem::path& meta_json, const std::filesystem::path& meta_tmpdir, int64_t snapshot)
      : meta_tmpdir_json(meta_tmpdir / meta_json.filename()), meta_tree(meta_tmpdir_json, snapshot) {}

  MetaModification(const std::filesystem::path& meta_json, const std::filesystem::path& meta_tmpdir, std::string ref)
      : meta_tmpdir_json(meta_tmpdir / meta_json.filename()), meta_tree(meta_tmpdir_json, ref) {}

  std::string TableMetadataLocation() const { return meta_tree.GetMetadataFile().table_metadata->location; }

  std::optional<int64_t> CurrentSnapshotId() const {
    return meta_tree.GetMetadataFile().table_metadata->current_snapshot_id;
  }

  void FixLocation(const std::filesystem::path& logical_location, const std::filesystem::path& dst_path,
                   bool ignore_missing_snaps) {
    iceberg::tools::LoadTree(meta_tree, meta_tmpdir_json, prev_meta, renames_data, renames_meta, rename_locations,
                             ignore_missing_snaps);
    StringFix fix_paths{logical_location.string(), dst_path.string()};
    if (fix_paths.NeedFix()) {
      FixLocation(fix_paths);
    }
  }

  void FixLocation(const StringFix& fix_paths) {
    for (auto& prev_tree : prev_meta) {
      prev_tree.FixLocation(fix_paths, renames_data, renames_meta, rename_locations);
    }
    meta_tree.FixLocation(fix_paths, renames_data, renames_meta, rename_locations);
  }

  // Do not get deleted files by default
  std::unordered_map<std::string, DataFileInfo> GetDataFiles(
      std::optional<iceberg::ContentFile::FileContent> content_type = {},
      const std::unordered_set<ManifestEntry::Status>& with_status = {ManifestEntry::Status::kExisting,
                                                                      ManifestEntry::Status::kAdded}) const {
    std::unordered_map<std::string, DataFileInfo> out;
    for (auto& [_, man] : meta_tree.GetManifests()) {
      for (auto& file : man->entries) {
        bool need_content = !content_type || *content_type == file.data_file.content;
        bool need_status = with_status.contains(file.status);
        if (need_content && need_status) {
          out.emplace(file.data_file.file_path, DataFileInfo{.status = file.status, .content = file.data_file.content});
        }
      }
    }
    return out;
  }

  void VisitTrees(std::function<void(MetadataTree&)> visit) {
    for (auto& prev_tree : prev_meta) {
      visit(prev_tree);
    }
    visit(meta_tree);
  }

  void FilterSchemaColumns(const std::unordered_set<std::string>& columns_to_skip) {
    if (columns_to_skip.empty()) {
      return;
    }
    meta_tree.GetMetadataFile().table_metadata->FilterSchemaColumns(columns_to_skip);
    for (MetadataTree& meta : prev_meta) {
      meta.GetMetadataFile().table_metadata->FilterSchemaColumns(columns_to_skip);
    }
  }

  void ClearColumnsStats() {
    VisitTrees([&](MetadataTree& tree) {
      for (auto& [_, man] : tree.GetManifests()) {
        for (auto& entry : man->entries) {
          auto& data_file = entry.data_file;
          data_file.column_sizes = {};
          data_file.value_counts = {};
          data_file.null_value_counts = {};
          data_file.nan_value_counts = {};
          data_file.distinct_counts = {};
          data_file.lower_bounds = {};
          data_file.upper_bounds = {};
          data_file.key_metadata = {};
          data_file.split_offsets = {};
          data_file.equality_ids = {};
          data_file.sort_order_id = {};
        }
      }
    });
  }

  void ChangeFilesMeta(const std::unordered_map<std::string, iceberg::tools::WrittenFileInfo>& out_file_infos) {
    VisitTrees([&](MetadataTree& tree) {
      for (auto& [_, man] : tree.GetManifests()) {
        for (auto& entry : man->entries) {
          auto& data_file = entry.data_file;
          std::string file_path = CropPrefix(data_file.file_path);
          if (auto it = out_file_infos.find(file_path); it != out_file_infos.end()) {
            data_file.file_size_in_bytes = it->second.file_size;
            data_file.split_offsets = std::move(it->second.split_offsets);
          }
        }
      }
    });
  }

  void SetSortOrder(std::shared_ptr<iceberg::SortOrder> sort_order) {
    if (!sort_order) {
      return;
    }
    auto& table_metadata = meta_tree.GetMetadataFile().table_metadata;
    table_metadata->SetSortOrder(sort_order);
  }
};

class RequiredSnap {
 public:
  RequiredSnap(int64_t snapshot_id, const std::string& snapshot_ref) : id_(snapshot_id), ref_(snapshot_ref) {}

  std::optional<int64_t> GetRequiredSnap(const iceberg::tools::MetadataTree::MetadataFile& meta_file) const {
    if (id_) {
      return id_;
    }
    if (!ref_.empty()) {
      auto& refs = meta_file.Refs();
      auto it = refs.find(ref_);
      if (it == refs.end()) {
        throw std::runtime_error("no ref '" + ref_ + "' in meta");
      }
      return it->second.snapshot_id;
    }
    return {};
  }

 private:
  int64_t id_;
  std::string ref_;
};

std::set<std::string> FetchActualMeta(std::shared_ptr<S3Client> s3client, const std::string& meta_json,
                                      const std::string& dst_dir, const RequiredSnap& required, unsigned max_threads) {
  auto out = CopyFilesToDir(s3client, std::vector<std::string>{meta_json}, dst_dir, 1);
  if (out.empty()) {
    throw std::runtime_error("cannot copy " + meta_json);
  }
  auto& tmp_meta_json = *out.begin();

  bool copy_others = false;
  auto metadata_file = MetadataTree::ReadMetadataFile(tmp_meta_json);
  std::optional<int64_t> required_snap = required.GetRequiredSnap(metadata_file);
  if (!required_snap) {
    required_snap = metadata_file.table_metadata->current_snapshot_id;
    copy_others = true;
  }

  std::set<std::string> src_snap_files;
  std::set<std::string> dst_snap_files;

  for (auto& snap : metadata_file.Snapshots()) {
    if (!snap) {
      continue;
    }

    if (copy_others || (required_snap && snap->snapshot_id == *required_snap)) {
      src_snap_files.emplace(snap->manifest_list_location);
    }
  }

  bool fallback = false;
  try {
    auto dst_files = CopyFilesToDir(s3client, src_snap_files, dst_dir, max_threads);
    dst_snap_files.insert(dst_files.begin(), dst_files.end());
  } catch (const std::exception& ex) {
    fallback = true;
    std::cerr << "[copy snap failed] " << ex.what() << std::endl;
  }

  if (fallback) {
    for (auto& src : src_snap_files) {
      bool broken = false;
      try {
        auto dst_files = CopyFilesToDir(s3client, std::vector<std::string>{src}, dst_dir, 1);
        dst_snap_files.insert(dst_files.begin(), dst_files.end());
      } catch (const std::exception& ex) {
        std::cerr << "[missing snap] " << src << std::endl;
        broken = true;
      }
      if (broken) {
        std::string broken_path = dst_dir / std::filesystem::path(src).filename();
        auto locator = s3client->DstFileLocator(broken_path);
        locator.filesystem->DeleteFile(locator.path).ok();
      }
    }
  }

  std::set<std::string> src_man_files;
  for (auto& snap_file : dst_snap_files) {
    std::ifstream list_input(snap_file);
    std::vector<iceberg::ManifestFile> list = iceberg::ice_tea::ReadManifestList(list_input);

    for (auto& man_file : list) {
      src_man_files.emplace(man_file.path);
    }
  }
  auto dst_man_files = CopyFilesToDir(s3client, src_man_files, dst_dir, max_threads);

  out.insert(dst_snap_files.begin(), dst_snap_files.end());
  out.insert(dst_man_files.begin(), dst_man_files.end());
  return out;
}

void ChangeLocations(MetaModification& src_meta_mod, HmsTableInfo& src, HmsTableInfo& dst,
                     const std::string& src_metadata_path, std::string& meta_subdir, bool is_fix_location,
                     uint16_t fix_items, bool ignore_missign_snapshots, bool& backup_metadata) {
  std::string logical_location = CropTrailing(src_meta_mod.TableMetadataLocation());
  std::string actual_location = GetParentLocation(src_metadata_path);
  bool fix_paths = false;
  if (logical_location != actual_location) {
    if (!is_fix_location) {
      throw std::runtime_error("broken location. Logical location '" + logical_location + "', actual location: '" +
                               actual_location + "'. Fix source metadata first (--mode fixlocation)");
    } else {
      std::cerr << "[s] logical_location: " << logical_location << std::endl;
      std::cerr << "[s] actual_location: " << actual_location << std::endl;
    }
    dst.path = actual_location;
    CropSameSuffix(logical_location, actual_location);
    backup_metadata = true;
    fix_paths = !logical_location.empty();
  } else {
    if (is_fix_location) {
      throw std::runtime_error("--mode fixlocation, nothing to fix. Logical location '" + logical_location +
                               "', actual location: '" + actual_location + "'");
    }

    if (fix_items) {
      logical_location = GetParentLocation(src_metadata_path, fix_items);
      fix_paths = !dst.path.empty();
      src.path = logical_location;
      actual_location = CropTrailing(dst.path);
      std::filesystem::path tmp_path = src_metadata_path;
      for (unsigned i = 0; i < fix_items; ++i) {
        tmp_path = tmp_path.parent_path();
        meta_subdir = tmp_path.filename().string() + "/" + meta_subdir;
      }
    } else if (!dst.path.empty() && dst.path != logical_location) {
      fix_paths = true;
      src.path = actual_location;
      actual_location = CropTrailing(dst.path);
    } else {
      src.path = actual_location;
      dst.path = actual_location;
    }
  }

  std::cerr << "[x] src_metadata_path: " << src_metadata_path << std::endl;
  std::cerr << "[x] logical_location: " << logical_location << std::endl;
  std::cerr << "[x] src sd.location: " << src.SdLocation() << std::endl;
  std::cerr << "[x] src_path: " << src.path << std::endl;
  std::cerr << "[x] dst_path: " << dst.path << std::endl;
  std::cerr << "[x] actual_location: " << actual_location << std::endl;

  if (fix_paths) {
    if (actual_location.empty()) {
      throw std::runtime_error("unexpected empty actual_location to fix location to");
    }
    src_meta_mod.FixLocation(logical_location, actual_location, ignore_missign_snapshots);
  }
}

std::map<std::string, std::string> FilterByCRC(std::shared_ptr<S3Client> s3client, const std::string& src_path,
                                               const std::string& dst_path,
                                               const std::map<std::string, std::string>& src_known_sums,
                                               const std::map<std::string, std::string> dst_known_sums, bool verbose,
                                               std::unordered_map<std::string, std::string>& src_data_renames) {
  std::vector<std::string> filter;
  filter.reserve(src_data_renames.size());
  std::map<std::string, std::string> out;

  auto src_prefix = CropTrailing(CropPrefix(src_path)) + "/";
  auto dst_prefix = CropTrailing(CropPrefix(dst_path)) + "/";
  for (auto& [src, dst] : src_data_renames) {
    auto src_relative = CropPrefix(src, src_prefix);
    auto dst_relative = CropPrefix(dst, dst_prefix);

    if (src_known_sums.contains(src_relative) && dst_known_sums.contains(dst_relative)) {
      auto& src_hash = src_known_sums.find(src_relative)->second;
      if (src_hash == dst_known_sums.find(dst_relative)->second) {
        if (iceberg::tools::FileExists(s3client->DstFileLocator(dst))) {
          if (verbose) {
            std::cerr << "[crc keep existing] " << dst << std::endl;
          }
          filter.push_back(src);
          out.emplace(src_relative, src_hash);
        }
      }
    }
  }
  for (auto& src : filter) {
    src_data_renames.erase(src);
  }
  return out;
}

void MakeNewSnapshot(const std::vector<std::string>& data_files, const std::vector<std::string>& delete_files,
                     std::shared_ptr<arrow::fs::FileSystem> fs, const std::string& parent_metadata_path,
                     const std::string& target_metadata_path, const std::string& target_data_path) {
  auto metadata_tree = MetadataTree(parent_metadata_path);
  const auto current_point = std::chrono::system_clock::now();
  auto point_value = std::chrono::duration_cast<std::chrono::seconds>(current_point.time_since_epoch()).count();

  auto snap_maker = SnapshotMaker(fs, metadata_tree.GetMetadataFile().table_metadata, point_value);
  snap_maker.MakeMetadataFiles(target_metadata_path, target_data_path, target_metadata_path, target_data_path, {},
                               data_files, delete_files, 0);
}

}  // namespace

ABSL_FLAG(std::string, src_host, "localhost", "src HMS host");
ABSL_FLAG(std::string, src_diff_host, "localhost", "src diff HMS host");
ABSL_FLAG(std::string, dst_host, "", "dst HMS host (default: same as src_host)");
ABSL_FLAG(uint16_t, src_port, HMS_PORT, "src HMS port");
ABSL_FLAG(uint16_t, src_diff_port, HMS_PORT, "src diff HMS port");
ABSL_FLAG(uint16_t, dst_port, 0, "dst HMS port (default: same as src_port)");
ABSL_FLAG(std::string, src_db, "", "src database name");
ABSL_FLAG(std::string, src_diff_db, "", "src diff database name");
ABSL_FLAG(std::string, dst_db, "", "dst database name (default: same as src_db)");
ABSL_FLAG(std::string, src_table, "", "src table name");
ABSL_FLAG(std::string, src_diff_table, "", "src diff table name");
ABSL_FLAG(std::string, dst_table, "", "dst table name (default: same as src_table)");
ABSL_FLAG(std::string, dst_path, "", "destination path");
ABSL_FLAG(std::string, tmpdir, "/tmp/syncice", "path to tmp directory");
ABSL_FLAG(std::string, mode, "dryrun", "one of: dryrun, fixlocation, actual, meta, crc, checkcrc");
ABSL_FLAG(std::string, crc_file, "",
          "output file in crc mode, input file in checkcrc mode (default: <snapshot_id>.xxh128 in tmpdir/.../_crc)");
ABSL_FLAG(uint64_t, snapshot_id, 0, "copy specified snapshot_id only");
ABSL_FLAG(uint64_t, diff_snapshot_id, 0, "copy specified diff_snapshot_id only");
ABSL_FLAG(std::string, ref, "", "copy snapshot tagged by specified reference (tag or branch) only");
ABSL_FLAG(std::string, allow_list, "tables.txt", "list of allowed db.table pairs");
ABSL_FLAG(std::string, deny_columns_list, "", "list of denied columns");
ABSL_FLAG(std::string, sort_order_columns, "", "sorting key columns (comma separated names list)");
ABSL_FLAG(std::string, sort_order_directions, "", "sorting key directions (comma separated 1/-1 list)");
ABSL_FLAG(std::string, key_attributes, "", "key attributes for diff table (used only for from_diff_table mode)");
// ABSL_FLAG(std::string, sort_order_null_directions, "", ""); // TODO(a.v.zuykov)
ABSL_FLAG(uint16_t, intermediate_path_items, 0, "number of elements in path between root location and metadata folder");
ABSL_FLAG(bool, sort_data, true, "sort data if sort_order_columns set (check order otherwise)");
ABSL_FLAG(bool, update_hms, false, "create/alter dst_db.dst_table in dst_host:dst_port HMS");
ABSL_FLAG(bool, crc_on_copy, true, "calculate crc for copying files if possible");
ABSL_FLAG(bool, backup, false, "backup original metadata");
ABSL_FLAG(bool, fix_pos_deletes, false, "fix paths in positional deletes (if set it breaks restore-by-copy backups)");
ABSL_FLAG(bool, ignore_missign_snapshots, true, "ignore snapshots with load errors");
ABSL_FLAG(bool, force, false, "disable rewrite protection");
ABSL_FLAG(bool, overwrite_files, false, "force overwrite existing dst files");
ABSL_FLAG(bool, raw_parquet_rw, false, "use low level parquet reader insted of arrow primitives");
ABSL_FLAG(uint32_t, chunk_size_mb, 32, "batch size in mb to copy");
ABSL_FLAG(uint32_t, compute_threads, 1, "max compute threads (in data modifications)");
ABSL_FLAG(uint32_t, io_threads, 0, "max I/O threads (in data modifications)");
ABSL_FLAG(bool, verbose, false, "print more debug information");
ABSL_FLAG(std::string, loglevel, "", "S3 SDK loglevel, one of: off, fatal, error, warn, info, debug, trace");

int main(int argc, char** argv) {
  try {
    absl::ParseCommandLine(argc, argv);

    HmsTableInfo src;
    HmsTableInfo src_diff;
    HmsTableInfo dst;

    src.host = absl::GetFlag(FLAGS_src_host);
    src_diff.host = absl::GetFlag(FLAGS_src_diff_host);
    dst.host = absl::GetFlag(FLAGS_dst_host);
    src.port = absl::GetFlag(FLAGS_src_port);
    src_diff.port = absl::GetFlag(FLAGS_src_diff_port);
    dst.port = absl::GetFlag(FLAGS_dst_port);
    src.db = absl::GetFlag(FLAGS_src_db);
    src_diff.db = absl::GetFlag(FLAGS_src_diff_db);
    dst.db = absl::GetFlag(FLAGS_dst_db);
    src.tablename = absl::GetFlag(FLAGS_src_table);
    src_diff.tablename = absl::GetFlag(FLAGS_src_diff_table);
    dst.tablename = absl::GetFlag(FLAGS_dst_table);
    dst.path = absl::GetFlag(FLAGS_dst_path);
    const uint16_t fix_items = absl::GetFlag(FLAGS_intermediate_path_items);
    const std::filesystem::path tmpdir = absl::GetFlag(FLAGS_tmpdir);
    const std::string mode_str = absl::GetFlag(FLAGS_mode);
    const std::string crc_file = absl::GetFlag(FLAGS_crc_file);
    int64_t snapshot_id = absl::GetFlag(FLAGS_snapshot_id);
    int64_t diff_snapshot_id = absl::GetFlag(FLAGS_diff_snapshot_id);
    const std::string snapshot_ref = absl::GetFlag(FLAGS_ref);
    const std::string allow_list = absl::GetFlag(FLAGS_allow_list);
    const std::string deny_columns_list = absl::GetFlag(FLAGS_deny_columns_list);
    const std::string sort_order_columns = absl::GetFlag(FLAGS_sort_order_columns);
    const std::string sort_order_directions = absl::GetFlag(FLAGS_sort_order_directions);
    const std::vector<std::string> key_attributes = SplitKeys(absl::GetFlag(FLAGS_key_attributes));
    const bool sort_data = absl::GetFlag(FLAGS_sort_data);
    bool update_hms = absl::GetFlag(FLAGS_update_hms);
    const bool crc_on_copy = absl::GetFlag(FLAGS_crc_on_copy);
    bool backup_metadata = absl::GetFlag(FLAGS_backup);
    const bool fix_pos_deletes = absl::GetFlag(FLAGS_fix_pos_deletes);
    const bool ignore_missign_snapshots = absl::GetFlag(FLAGS_ignore_missign_snapshots);
    bool force = absl::GetFlag(FLAGS_force);
    const bool overwrite_files = absl::GetFlag(FLAGS_overwrite_files);
    const bool raw_parquet_rw = absl::GetFlag(FLAGS_raw_parquet_rw);
    const uint32_t chunk_size = absl::GetFlag(FLAGS_chunk_size_mb) * 1024 * 1024;
    const uint32_t max_compute_threads = absl::GetFlag(FLAGS_compute_threads);
    const uint32_t max_io_threads = absl::GetFlag(FLAGS_io_threads);

    const bool verbose = absl::GetFlag(FLAGS_verbose);
    const std::string loglevel = absl::GetFlag(FLAGS_loglevel);

    std::string meta_subdir = "metadata";
    const std::string data_subdir = "data";
    const std::string crc_subdir = "_crc";
    const std::string crc_ext = ".xxh128";

    bool is_fix_location = false;
    CopyMode copy_mode = StringToMode(mode_str, is_fix_location);

    if (copy_mode == CopyMode::kUnknown || src.db.empty() || src.tablename.empty()) {
      throw std::runtime_error("Require args: mode, src_db, src_tablename");
    }
    if (tmpdir.empty()) {
      throw std::runtime_error("Wrong args: tmpdir should not be empty");
    }
    dst.CopyNotSet(src);
    if (is_fix_location) {
      force = true;
      if (!dst.IsSame(src)) {
        throw std::runtime_error("--mode fixlocation requires equal src and dst");
      }
    }
    if (snapshot_id && !snapshot_ref.empty()) {
      throw std::runtime_error("use either snapshot or ref");
    }

    std::string full_table_name = src.db + "." + src.tablename;
    if (!IsTableAllowed(allow_list, src.db, src.tablename)) {
      throw std::runtime_error(full_table_name + " is not in allow list '" + allow_list + "'");
    }

    std::unordered_set<std::string> columns_to_skip = GetColumnFilter(full_table_name, deny_columns_list);
    if (!columns_to_skip.empty() || raw_parquet_rw) {
      copy_mode = DataModifyMode(copy_mode);
    }

    auto sort_order = MakeSortOrder(sort_order_columns, sort_order_directions);
    if (sort_order) {
      if (raw_parquet_rw) {
        throw std::runtime_error("raw_parquet_rw incompatible with sorting");
      }

      copy_mode = DataModifyMode(copy_mode);

      for (auto& field : sort_order->fields) {
        if (columns_to_skip.contains(field.transform)) {
          throw std::runtime_error("both sort and filter for column " + field.transform);
        }
      }
    }
    if (fix_pos_deletes) {
      copy_mode = DataModifyMode(copy_mode);
    }

    src.Load(verbose);
    std::string src_metadata_path = src.MetaJsonParentPath();
    if (src_metadata_path.empty()) {
      throw std::runtime_error("MetaJsonParentPath is empty");
    }

    const std::filesystem::path table_tmpdir = tmpdir / src.tablename;
    const std::filesystem::path meta_tmpdir = table_tmpdir / meta_subdir;
    const std::filesystem::path diff_table_tmpdir = tmpdir / src_diff.tablename;
    const std::filesystem::path diff_meta_tmpdir = diff_table_tmpdir / meta_subdir;
    const std::filesystem::path meta_tmpdir_orig = meta_tmpdir.string() + ".orig";
    const std::filesystem::path data_tmpdir = table_tmpdir / data_subdir;
    std::filesystem::remove_all(meta_tmpdir);
    std::filesystem::create_directories(meta_tmpdir);
    std::filesystem::remove_all(diff_meta_tmpdir);
    std::filesystem::create_directories(diff_meta_tmpdir);
    std::filesystem::remove_all(meta_tmpdir_orig);
    std::filesystem::remove_all(data_tmpdir);
    std::filesystem::create_directories(data_tmpdir);

    // set threads number for arrow::io::default_io_context() used in s3client
    if (max_io_threads && !arrow::io::SetIOThreadPoolCapacity(max_io_threads).ok()) {
      throw std::runtime_error("cannot set io_threads");
    }
    std::shared_ptr<S3Client> s3client = std::make_shared<S3Client>(false, S3Init::LogLevel(loglevel), chunk_size);

    LogAction("[copy tmp meta]", src.tablename, src.MetadataJson(), meta_tmpdir);
    auto meta_tmp_files =
        FetchActualMeta(s3client, src.MetadataJson(), meta_tmpdir, {snapshot_id, snapshot_ref}, max_io_threads);

    LogAction("[load meta]", src.tablename, meta_tmpdir, "memory");
    MetaModification src_meta_mod =
        snapshot_id
            ? MetaModification(src.MetadataJson(), meta_tmpdir, snapshot_id)
            : (snapshot_ref.empty() ? MetaModification(src.MetadataJson(), meta_tmpdir, ignore_missign_snapshots)
                                    : MetaModification(src.MetadataJson(), meta_tmpdir, snapshot_ref));
    {
      auto current_snapshot_id = src_meta_mod.CurrentSnapshotId();
      if (!current_snapshot_id || (snapshot_id && *current_snapshot_id != snapshot_id)) {
        throw std::runtime_error("unexpected current snapshot id: " +
                                 (current_snapshot_id ? std::to_string(*current_snapshot_id) : std::string("<no>")));
      }
      snapshot_id = *current_snapshot_id;
    }

    auto src_deleted_files = src_meta_mod.GetDataFiles({}, {iceberg::ManifestEntry::Status::kDeleted});

    // src meta modification
    {
      ChangeLocations(src_meta_mod, src, dst, src_metadata_path, meta_subdir, is_fix_location, fix_items,
                      ignore_missign_snapshots, backup_metadata);

      if (sort_order) {
        auto current_schema = src_meta_mod.meta_tree.GetMetadataFile().table_metadata->GetCurrentSchema();
        FixSortOrderFields(*sort_order, current_schema);
      }
      src_meta_mod.SetSortOrder(sort_order);
      src_meta_mod.FilterSchemaColumns(columns_to_skip);

      bool stats_valid = columns_to_skip.empty() && sort_order.get() && !raw_parquet_rw;
      if (!stats_valid) {
        src_meta_mod.ClearColumnsStats();
      }

      if (verbose) {
        src_meta_mod.VisitTrees([](MetadataTree& tree) { std::cout << tree << std::endl; });
      }
    }

    BackupLocalDir(meta_tmpdir, meta_tmpdir_orig);
    std::set<std::string> meta_orig_files;
    for (const auto& name : meta_tmp_files) {
      std::string orig_name = meta_tmpdir_orig / std::filesystem::path(name).filename();
      meta_orig_files.emplace(std::move(orig_name));
    }

    bool meta_after_data = (copy_mode == CopyMode::kModifyActual) || raw_parquet_rw;
    if (!meta_after_data) {
      src_meta_mod.VisitTrees([&meta_tmpdir](MetadataTree& tree) { tree.WriteFiles(meta_tmpdir); });
    }

    const MetadataTree& src_meta_tree = src_meta_mod.meta_tree;
    std::unordered_map<std::string, std::string>& src_data_renames = src_meta_mod.renames_data;
    std::unordered_map<std::string, iceberg::tools::DataFileInfo> data_files =
        src_meta_mod.GetDataFiles(iceberg::DataFile::FileContent::kData);
    std::unordered_map<std::string, iceberg::tools::DataFileInfo> pos_delete_files;
    std::unordered_map<std::string, iceberg::tools::DataFileInfo> eq_delete_files;
    if (fix_pos_deletes) {
      pos_delete_files = src_meta_mod.GetDataFiles(iceberg::DataFile::FileContent::kPositionDeletes);
    }
    if (copy_mode == CopyMode::kCopyOnWrite) {
      pos_delete_files = src_meta_mod.GetDataFiles(iceberg::DataFile::FileContent::kPositionDeletes);
      eq_delete_files = src_meta_mod.GetDataFiles(iceberg::DataFile::FileContent::kEqualityDeletes);
    }
    std::unordered_map<std::string, iceberg::tools::WrittenFileInfo> out_file_infos;

    auto current_schema = src_meta_tree.GetMetadataFile().table_metadata->GetCurrentSchema();

    if (!src_deleted_files.empty()) {
      for (auto& [src, _] : src_deleted_files) {
        if (verbose) {
          std::cerr << "[filter deleted] " << src << std::endl;
        }
        if (!src_data_renames.erase(src)) {
          throw std::runtime_error("no src path: " + src);
        }
      }
    }

    std::map<std::string, std::string> data_crcs;
    if (s3client && !overwrite_files && (copy_mode == CopyMode::kCopyActual || copy_mode == CopyMode::kModifyActual)) {
      std::vector<arrow::fs::FileLocator> src_sum_files;
      std::vector<arrow::fs::FileLocator> dst_sum_files;
      src_meta_mod.VisitTrees([&](MetadataTree& tree) {
        auto& snap_id = tree.GetMetadataFile().table_metadata->current_snapshot_id;
        if (snap_id && *snap_id >= 0) {
          std::string relative_path = (std::filesystem::path(crc_subdir) / (std::to_string(*snap_id) + crc_ext));
          src_sum_files.emplace_back(s3client->SrcFileLocator(std::filesystem::path(src.path) / relative_path));
          dst_sum_files.emplace_back(s3client->DstFileLocator(std::filesystem::path(dst.path) / relative_path));
        }
      });

      auto src_sums = iceberg::tools::FindHashSums(s3client, src_sum_files, table_tmpdir, ".src");
      if (!crc_file.empty()) {
        for (auto&& [path, sum] : iceberg::tools::ReadSumsFile(crc_file)) {
          src_sums.emplace(std::move(path), std::move(sum));
        }
      }
      auto dst_sums = iceberg::tools::FindHashSums(s3client, dst_sum_files, table_tmpdir, ".dst");
      data_crcs = FilterByCRC(s3client, src.path, dst.path, src_sums, dst_sums, verbose, src_data_renames);

      std::cerr << "[crc] src sums: " << src_sums.size() << " dst sums: " << dst_sums.size()
                << " filtered: " << data_crcs.size() << std::endl;
    }

    iceberg::tools::ModificationOptions mod_opts{
        .data_renames = src_data_renames,
        .data_files = std::move(data_files),
        .pos_delete_files = std::move(pos_delete_files),
        .eq_delete_files = std::move(pos_delete_files),
        .columns_to_skip = std::move(columns_to_skip),
        .sort_descr = MakeSortDescription(sort_order, current_schema),
        .sort_data = sort_data,
        .calculate_crc = crc_on_copy,
        .copy_mode = (raw_parquet_rw ? iceberg::tools::CopyMode::kRawParquet : iceberg::tools::CopyMode::kArrowParquet),
        .max_compute_threads = max_compute_threads,
        .max_io_threads = max_io_threads,
        .chunk_size = chunk_size};

    bool copy_meta = true;
    switch (copy_mode) {
      case CopyMode::kCopyActual: {
        LogAction("[copy actual]", src.tablename, src.path, dst.path);
        if (max_compute_threads < 2) {
          if (verbose) {
            for (auto& [from, to] : src_data_renames) {
              std::cerr << "[copy] " << from << " " << to << std::endl;
            }
          }
          CopyOptions copy_opts{.use_threads = false, .no_check_dest = overwrite_files};
          CopyFilesOrThrow(s3client, src_data_renames, copy_opts);
        } else {
          mod_opts.copy_mode = iceberg::tools::CopyMode::kRawFile;
          if (!CopyModifiedFiles(s3client, src_data_renames, mod_opts, data_tmpdir, out_file_infos, verbose)) {
            if (verbose) {
              for (auto& [from, to] : src_data_renames) {
                std::cerr << "[copy failed] " << from << " " << to << std::endl;
              }
            }
            throw std::runtime_error("cannot copy files");
          }
        }
        break;
      }
      case CopyMode::kModifyActual: {
        LogAction("[copy modified]", src.tablename, src.path, dst.path);
        if (!CopyModifiedFiles(s3client, src_data_renames, mod_opts, data_tmpdir, out_file_infos, verbose)) {
          if (verbose) {
            for (auto& [from, to] : src_data_renames) {
              std::cerr << "[copy failed] " << from << " " << to << std::endl;
            }
          }
          throw std::runtime_error("cannot modify files");
        }
        break;
      }
      case CopyMode::kCRC: {
        auto files = src_meta_mod.GetDataFiles();
        auto sums = iceberg::tools::CalcHashSums(s3client, files, max_compute_threads, verbose);
        auto src_prefix = CropTrailing(CropPrefix(src.path)) + "/";
        sums = CropPrefix(std::move(sums), src_prefix);
        iceberg::tools::WriteHashSums(crc_file, sums, verbose);
        return 0;
      }
      case CopyMode::kCheckCRC: {
        auto files = src_meta_mod.GetDataFiles();
        auto sums = iceberg::tools::CalcHashSums(s3client, files, max_compute_threads, verbose);
        auto src_prefix = CropTrailing(CropPrefix(src.path)) + "/";
        sums = CropPrefix(std::move(sums), src_prefix);
        bool ok = iceberg::tools::CheckHashSums(crc_file, sums, verbose);
        return ok ? 0 : 1;
      }
      case CopyMode::kDryRun:
        backup_metadata = false;
        copy_meta = false;
        update_hms = false;
        break;
      case CopyMode::kCopyMeta:
        break;
      case CopyMode::kCopyOnWrite: {
        LogAction("[copy with delete files]", src.tablename, src.path, dst.path);

        std::vector<std::string> positional_delete_files;
        std::vector<std::string> equality_delete_files;
        std::unordered_map<std::string, std::string> inverted_renames;
        for (const auto& [src, dst] : mod_opts.data_renames) {
          inverted_renames[dst] = src;
        }

        for (const auto& file : mod_opts.pos_delete_files) {
          positional_delete_files.push_back(inverted_renames[file.first]);
          mod_opts.data_renames.erase(inverted_renames[file.first]);
        }

        for (const auto& file : mod_opts.eq_delete_files) {
          equality_delete_files.push_back(inverted_renames[file.first]);
          mod_opts.data_renames.erase(inverted_renames[file.first]);
        }
        CopyFilesWithApplyingDeleteFiles(s3client, mod_opts.data_renames, positional_delete_files,
                                         equality_delete_files, verbose);
        break;
      }
      case CopyMode::kDiffTable: {
        src_diff.Load(verbose);
        auto diff_meta_tmp_files = FetchActualMeta(s3client, src_diff.MetadataJson(), diff_meta_tmpdir,
                                                   {diff_snapshot_id, snapshot_ref}, max_io_threads);

        MetaModification src_diff_meta_mod =
            diff_snapshot_id
                ? MetaModification(src_diff.MetadataJson(), diff_meta_tmpdir, diff_snapshot_id)
                : (snapshot_ref.empty()
                       ? MetaModification(src_diff.MetadataJson(), diff_meta_tmpdir, ignore_missign_snapshots)
                       : MetaModification(src_diff.MetadataJson(), diff_meta_tmpdir, snapshot_ref));

        std::unordered_map<std::string, iceberg::tools::DataFileInfo> diff_files =
            src_diff_meta_mod.GetDataFiles(iceberg::DataFile::FileContent::kData);

        std::vector<std::string> data_filenames;
        std::vector<std::string> diff_filenames;

        data_files.reserve(mod_opts.data_files.size());
        for (const auto& [src, _] : mod_opts.data_files) {
          data_filenames.push_back(src);
        }

        diff_filenames.reserve(diff_files.size());
        for (const auto& [src, _] : diff_files) {
          diff_filenames.push_back(src);
        }

        std::string result_data_file = dst.path / "data/data_table.parquet";
        std::string result_delete_file = dst.path / "data/delete_table.parquet";

        iceberg::tools::ConvertDiffFile(s3client, data_filenames, diff_filenames, result_data_file, result_delete_file,
                                        key_attributes, max_io_threads, chunk_size, verbose);
        LogAction("[convert into iceberg sucessfull]", result_data_file, result_delete_file, "");
        MakeNewSnapshot({CropPrefix(result_data_file)}, {CropPrefix(result_delete_file)}, s3client->GetDstFileSystem(),
                        src.MetadataJson(), CropPrefix(dst.path / "metadata"), CropPrefix(dst.path / "data"));
        return 0;
      }
      case CopyMode::kUnknown:
        throw std::runtime_error("unknown mode");
    }

    std::string tmp_crc_file = table_tmpdir / (std::to_string(snapshot_id) + crc_ext);
    if (crc_on_copy && (out_file_infos.size() || data_crcs.size())) {
      auto dst_prefix = CropTrailing(CropPrefix(dst.path)) + "/";
      for (auto& [path, info] : out_file_infos) {
        if (info.xxh128sum.size()) {
          data_crcs.emplace(CropPrefix(path, dst_prefix), info.xxh128sum);
        }
      }
      if (data_crcs.size()) {
        iceberg::tools::WriteHashSums(tmp_crc_file, data_crcs, false);
      }
    }

    if (meta_after_data) {
      src_meta_mod.ChangeFilesMeta(out_file_infos);
      src_meta_mod.VisitTrees([&meta_tmpdir](MetadataTree& tree) { tree.WriteFiles(meta_tmpdir); });
    }

    if (backup_metadata) {
      std::filesystem::path bak_metadata_path = dst.path / (meta_subdir + ".bak");
      LogAction("[backup meta]", src.tablename, meta_tmpdir_orig, bak_metadata_path);

#if 0
      CopyDir(s3client, meta_tmpdir_orig, bak_metadata_path);
#else
      CopyFilesToDir(s3client, meta_orig_files, bak_metadata_path, max_io_threads);
#endif
    }

    std::filesystem::path dst_metadata_path = dst.path / meta_subdir;
    if (copy_meta) {
      LogAction("[copy meta]", dst.tablename, meta_tmpdir, dst_metadata_path);

      if (!force && (dst_metadata_path == src_metadata_path)) {
        throw std::runtime_error("do not allow rewrite metadata without --force");
      }

#if 0
      CopyDir(s3client, meta_tmpdir, dst_metadata_path);
#else
      CopyFilesToDir(s3client, meta_tmp_files, dst_metadata_path, max_io_threads);
#endif

      if (std::filesystem::exists(tmp_crc_file)) {
        LogAction("[copy crc]", dst.tablename, tmp_crc_file, (dst.path / crc_subdir));

        CopyFilesToDir(s3client, std::vector<std::string>{tmp_crc_file}, dst.path / crc_subdir, 1);
      }
    }

    if (update_hms) {
      if (!force && dst.IsSame(src)) {
        throw std::runtime_error("do not allow rewrite HMS table without --force");
      }

      auto dst_meta_json = dst_metadata_path / std::filesystem::path(src.MetadataJson()).filename();
      dst.Update(src.table, CropTrailing(dst.path), dst_meta_json);
      if (!dst.Store(verbose)) {
        throw std::runtime_error("cannot write dst table in HMS");
      }
    }
  } catch (std::exception& ex) {
    std::cerr << ex.what() << std::endl;
    return 1;
  }

  return 0;
}
