//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/table_properties.h"

#include <cstddef>
#include <cstdlib>
#include <utility>

#include "db/seqno_to_time_mapping.h"
#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/unique_id.h"
#include "table/table_properties_internal.h"
#include "table/unique_id_impl.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

const uint32_t TablePropertiesCollectorFactory::Context::kUnknownColumnFamily =
    std::numeric_limits<int32_t>::max();

namespace {
void AppendProperty(std::string& props, const std::string& key,
                    const std::string& value, const std::string& prop_delim,
                    const std::string& kv_delim) {
  props.append(key);
  props.append(kv_delim);
  props.append(value);
  props.append(prop_delim);
}

template <class TValue>
void AppendProperty(std::string& props, const std::string& key,
                    const TValue& value, const std::string& prop_delim,
                    const std::string& kv_delim) {
  AppendProperty(props, key, std::to_string(value), prop_delim, kv_delim);
}
}  // namespace

void TableProperties::DoubleCheck(TransferService* node) const {
  LOG_CERR("TableProperties::DoubleCheck::orig_file_number");
  node->send(&orig_file_number, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::data_size");
  node->send(&data_size, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::index_size");
  node->send(&index_size, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::index_key_is_user_key");
  node->send(&index_key_is_user_key, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::index_value_is_delta_encoded");
  node->send(&index_value_is_delta_encoded, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::filter_size");
  node->send(&filter_size, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::raw_key_size");
  node->send(&raw_key_size, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::raw_value_size");
  node->send(&raw_value_size, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::num_data_blocks");
  node->send(&num_data_blocks, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::num_entries");
  node->send(&num_entries, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::num_filter_entries");
  node->send(&num_filter_entries, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::num_deletions");
  node->send(&num_deletions, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::num_merge_operands");
  node->send(&num_merge_operands, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::num_range_deletions");
  node->send(&num_range_deletions, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::format_version");
  node->send(&format_version, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::fixed_key_len");
  node->send(&fixed_key_len, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::column_family_id");
  node->send(&column_family_id, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::creation_time");
  node->send(&creation_time, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::oldest_key_time");
  node->send(&oldest_key_time, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::file_creation_time");
  node->send(&file_creation_time, sizeof(uint64_t));
  LOG_CERR(
      "TableProperties::DoubleCheck::slow_compression_estimated_data_size");
  node->send(&slow_compression_estimated_data_size, sizeof(uint64_t));
  LOG_CERR(
      "TableProperties::DoubleCheck::fast_compression_estimated_data_size");
  node->send(&fast_compression_estimated_data_size, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::seqno_to_time_mapping");
  node->send(&external_sst_file_global_seqno_offset, sizeof(uint64_t));
  LOG_CERR("TableProperties::DoubleCheck::db_id");
  size_t db_id_len = db_id.size();
  node->send(&db_id_len, sizeof(size_t));
  if (db_id_len) node->send(db_id.c_str(), db_id_len);
  LOG_CERR("TableProperties::DoubleCheck::db_session_id");
  size_t db_session_id_len = db_session_id.size();
  node->send(&db_session_id_len, sizeof(size_t));
  if (db_session_id_len) node->send(db_session_id.c_str(), db_session_id_len);
  LOG_CERR("TableProperties::DoubleCheck::db_host_id");
  size_t db_host_id_len = db_host_id.size();
  node->send(&db_host_id_len, sizeof(size_t));
  if (db_host_id_len) node->send(db_host_id.c_str(), db_host_id_len);
  LOG_CERR("TableProperties::DoubleCheck::column_family_name");
  size_t column_family_name_len = column_family_name.size();
  node->send(&column_family_name_len, sizeof(size_t));
  if (column_family_name_len)
    node->send(column_family_name.c_str(), column_family_name_len);
  LOG_CERR("TableProperties::DoubleCheck::filter_policy_name");
  size_t filter_policy_name_len = filter_policy_name.size();
  node->send(&filter_policy_name_len, sizeof(size_t));
  if (filter_policy_name_len)
    node->send(filter_policy_name.c_str(), filter_policy_name_len);
  LOG_CERR("TableProperties::DoubleCheck::comparator_name");
  size_t comparator_name_len = comparator_name.size();
  node->send(&comparator_name_len, sizeof(size_t));
  if (comparator_name_len)
    node->send(comparator_name.c_str(), comparator_name_len);
  LOG_CERR("TableProperties::DoubleCheck::merge_operator_name");
  size_t merge_operator_name_len = merge_operator_name.size();
  node->send(&merge_operator_name_len, sizeof(size_t));
  if (merge_operator_name_len)
    node->send(merge_operator_name.c_str(), merge_operator_name_len);
  LOG_CERR("TableProperties::DoubleCheck::prefix_extractor_name");
  size_t prefix_extractor_name_len = prefix_extractor_name.size();
  node->send(&prefix_extractor_name_len, sizeof(size_t));
  if (prefix_extractor_name_len)
    node->send(prefix_extractor_name.c_str(), prefix_extractor_name_len);
  LOG_CERR("TableProperties::DoubleCheck::property_collectors_names");
  size_t property_collectors_names_len = property_collectors_names.size();
  node->send(&property_collectors_names_len, sizeof(size_t));
  if (property_collectors_names_len)
    node->send(property_collectors_names.c_str(),
               property_collectors_names_len);
  LOG_CERR("TableProperties::DoubleCheck::compression_name");
  size_t compression_name_len = compression_name.size();
  node->send(&compression_name_len, sizeof(size_t));
  if (compression_name_len)
    node->send(compression_name.c_str(), compression_name_len);
  LOG_CERR("TableProperties::DoubleCheck::compression_options");
  size_t compression_options_len = compression_options.size();
  node->send(&compression_options_len, sizeof(size_t));
  if (compression_options_len)
    node->send(compression_options.c_str(), compression_options_len);
  LOG_CERR("TableProperties::DoubleCheck::seqno_to_time_mapping");
  size_t seqno_to_time_mapping_len = seqno_to_time_mapping.size();
  node->send(&seqno_to_time_mapping_len, sizeof(size_t));
  if (seqno_to_time_mapping_len)
    node->send(seqno_to_time_mapping.c_str(), seqno_to_time_mapping_len);
}

void TableProperties::PackRemote(TransferService* node) const {
  LOG("TableProperties::PackRemote");
  node->send(reinterpret_cast<const void*>(this), sizeof(TableProperties));
  size_t db_id_len = db_id.size();
  node->send(&db_id_len, sizeof(size_t));
  if (db_id_len) node->send(db_id.c_str(), db_id_len);

  size_t db_session_id_len = db_session_id.size();
  node->send(&db_session_id_len, sizeof(size_t));
  if (db_session_id_len) node->send(db_session_id.c_str(), db_session_id_len);
  size_t db_host_id_len = db_host_id.size();
  node->send(&db_host_id_len, sizeof(size_t));
  if (db_host_id_len) node->send(db_host_id.c_str(), db_host_id_len);
  size_t column_family_name_len = column_family_name.size();
  node->send(&column_family_name_len, sizeof(size_t));
  if (column_family_name_len)
    node->send(column_family_name.c_str(), column_family_name_len);
  size_t filter_policy_name_len = filter_policy_name.size();
  node->send(&filter_policy_name_len, sizeof(size_t));
  if (filter_policy_name_len)
    node->send(filter_policy_name.c_str(), filter_policy_name_len);
  size_t comparator_name_len = comparator_name.size();
  node->send(&comparator_name_len, sizeof(size_t));
  if (comparator_name_len)
    node->send(comparator_name.c_str(), comparator_name_len);

  size_t merge_operator_name_len = merge_operator_name.size();
  node->send(&merge_operator_name_len, sizeof(size_t));
  if (merge_operator_name_len)
    node->send(merge_operator_name.c_str(), merge_operator_name_len);
  size_t prefix_extractor_name_len = prefix_extractor_name.size();
  node->send(&prefix_extractor_name_len, sizeof(size_t));
  if (prefix_extractor_name_len)
    node->send(prefix_extractor_name.c_str(), prefix_extractor_name_len);
  size_t property_collectors_names_len = property_collectors_names.size();
  node->send(&property_collectors_names_len, sizeof(size_t));
  if (property_collectors_names_len)
    node->send(property_collectors_names.c_str(),
               property_collectors_names_len);
  size_t compression_name_len = compression_name.size();
  node->send(&compression_name_len, sizeof(size_t));
  if (compression_name_len)
    node->send(compression_name.c_str(), compression_name_len);
  size_t compression_options_len = compression_options.size();
  node->send(&compression_options_len, sizeof(size_t));
  if (compression_options_len)
    node->send(compression_options.c_str(), compression_options_len);

  size_t seqno_to_time_mapping_len = seqno_to_time_mapping.size();
  node->send(&seqno_to_time_mapping_len, sizeof(size_t));
  if (seqno_to_time_mapping_len)
    node->send(seqno_to_time_mapping.c_str(), seqno_to_time_mapping_len);

  size_t user_collected_properties_len = user_collected_properties.size();
  node->send(&user_collected_properties_len, sizeof(size_t));
  for (auto iter = user_collected_properties.begin();
       iter != user_collected_properties.end(); ++iter) {
    size_t key_len = iter->first.size();
    node->send(&key_len, sizeof(size_t));
    node->send(iter->first.c_str(), key_len);
    size_t value_len = iter->second.size();
    node->send(&value_len, sizeof(size_t));
    node->send(iter->second.c_str(), value_len);
  }

  size_t readable_properties_len = readable_properties.size();
  node->send(&readable_properties_len, sizeof(size_t));
  for (auto iter = readable_properties.begin();
       iter != readable_properties.end(); ++iter) {
    size_t key_len = iter->first.size();
    node->send(&key_len, sizeof(size_t));
    node->send(iter->first.c_str(), key_len);
    size_t value_len = iter->second.size();
    node->send(&value_len, sizeof(size_t));
    node->send(iter->second.c_str(), value_len);
  }
}

void* TableProperties::UnPackRemote(TransferService* node) {
  auto* ret_ptr = new TableProperties();
  void* mem = reinterpret_cast<void*>(ret_ptr);

  node->receive(mem, sizeof(TableProperties));
  size_t db_id_len = 0;
  node->receive(&db_id_len, sizeof(size_t));
  if (db_id_len > 0) {
    char* db_id = new char[db_id_len];
    node->receive(db_id, db_id_len);
    new (&ret_ptr->db_id) std::string(db_id, db_id_len);
    delete[] db_id;
  } else {
    new (&ret_ptr->db_id) std::string();
  }
  size_t db_session_id_len = 0;
  node->receive(&db_session_id_len, sizeof(size_t));
  if (db_session_id_len > 0) {
    char* db_session_id = new char[db_session_id_len];
    node->receive(db_session_id, db_session_id_len);
    new (&ret_ptr->db_session_id) std::string(db_session_id, db_session_id_len);
    delete[] db_session_id;
  } else {
    new (&ret_ptr->db_session_id) std::string();
  }
  size_t db_host_id_len = 0;
  node->receive(&db_host_id_len, sizeof(size_t));
  if (db_host_id_len > 0) {
    char* db_host_id = new char[db_host_id_len];
    node->receive(db_host_id, db_host_id_len);
    new (&ret_ptr->db_host_id) std::string(db_host_id, db_host_id_len);
    delete[] db_host_id;
  } else {
    new (&ret_ptr->db_host_id) std::string();
  }
  size_t column_family_name_len = 0;
  node->receive(&column_family_name_len, sizeof(size_t));
  if (column_family_name_len > 0) {
    char* column_family_name = new char[column_family_name_len];
    node->receive(column_family_name, column_family_name_len);
    new (&ret_ptr->column_family_name)
        std::string(column_family_name, column_family_name_len);
    delete[] column_family_name;
  } else {
    new (&ret_ptr->column_family_name) std::string();
  }
  size_t filter_policy_name_len = 0;
  node->receive(&filter_policy_name_len, sizeof(size_t));
  if (filter_policy_name_len > 0) {
    char* filter_policy_name = new char[filter_policy_name_len];
    node->receive(filter_policy_name, filter_policy_name_len);
    new (&ret_ptr->filter_policy_name)
        std::string(filter_policy_name, filter_policy_name_len);
    delete[] filter_policy_name;
  } else {
    new (&ret_ptr->filter_policy_name) std::string();
  }
  size_t comparator_name_len = 0;
  node->receive(&comparator_name_len, sizeof(size_t));
  if (comparator_name_len > 0) {
    char* comparator_name = new char[comparator_name_len];
    node->receive(comparator_name, comparator_name_len);
    new (&ret_ptr->comparator_name)
        std::string(comparator_name, comparator_name_len);
    delete[] comparator_name;
  } else {
    new (&ret_ptr->comparator_name) std::string();
  }
  size_t merge_operator_name_len = 0;
  node->receive(&merge_operator_name_len, sizeof(size_t));
  if (merge_operator_name_len > 0) {
    char* merge_operator_name = new char[merge_operator_name_len];
    node->receive(merge_operator_name, merge_operator_name_len);
    new (&ret_ptr->merge_operator_name)
        std::string(merge_operator_name, merge_operator_name_len);
    delete[] merge_operator_name;
  } else {
    new (&ret_ptr->merge_operator_name) std::string();
  }
  size_t prefix_extractor_name_len = 0;
  node->receive(&prefix_extractor_name_len, sizeof(size_t));
  if (prefix_extractor_name_len > 0) {
    char* prefix_extractor_name = new char[prefix_extractor_name_len];
    node->receive(prefix_extractor_name, prefix_extractor_name_len);
    new (&ret_ptr->prefix_extractor_name)
        std::string(prefix_extractor_name, prefix_extractor_name_len);
    delete[] prefix_extractor_name;
  } else {
    new (&ret_ptr->prefix_extractor_name) std::string();
  }
  size_t property_collectors_names_len = 0;
  node->receive(&property_collectors_names_len, sizeof(size_t));
  if (property_collectors_names_len > 0) {
    char* property_collectors_names = new char[property_collectors_names_len];
    node->receive(property_collectors_names, property_collectors_names_len);
    new (&ret_ptr->property_collectors_names)
        std::string(property_collectors_names, property_collectors_names_len);
    delete[] property_collectors_names;
  } else {
    new (&ret_ptr->property_collectors_names) std::string();
  }
  size_t compression_name_len = 0;
  node->receive(&compression_name_len, sizeof(size_t));
  if (compression_name_len > 0) {
    char* compression_name = new char[compression_name_len];
    node->receive(compression_name, compression_name_len);
    new (&ret_ptr->compression_name)
        std::string(compression_name, compression_name_len);
    delete[] compression_name;
  } else {
    new (&ret_ptr->compression_name) std::string();
  }
  size_t compression_options_len = 0;
  node->receive(&compression_options_len, sizeof(size_t));
  if (compression_options_len > 0) {
    char* compression_options = new char[compression_options_len];
    node->receive(compression_options, compression_options_len);
    new (&ret_ptr->compression_options)
        std::string(compression_options, compression_options_len);
    delete[] compression_options;
  } else {
    new (&ret_ptr->compression_options) std::string();
  }
  size_t seqno_to_time_mapping_len = 0;
  node->receive(&seqno_to_time_mapping_len, sizeof(size_t));
  if (seqno_to_time_mapping_len > 0) {
    char* seqno_to_time_mapping = new char[seqno_to_time_mapping_len];
    node->receive(seqno_to_time_mapping, seqno_to_time_mapping_len);
    new (&ret_ptr->seqno_to_time_mapping)
        std::string(seqno_to_time_mapping, seqno_to_time_mapping_len);
    delete[] seqno_to_time_mapping;
  } else {
    new (&ret_ptr->seqno_to_time_mapping) std::string();
  }
  size_t user_collected_properties_len = 0;
  node->receive(&user_collected_properties_len, sizeof(size_t));
  new (&ret_ptr->user_collected_properties)
      std::map<std::string, std::string>();
  new (&ret_ptr->readable_properties) std::map<std::string, std::string>();
  LOG("TableProperties::UnPackRemote");
  for (size_t i = 0; i < user_collected_properties_len; i++) {
    std::string key = "";
    std::string value = "";
    size_t key_len = 0;
    node->receive(&key_len, sizeof(size_t));
    char* key_cp = new char[key_len];
    node->receive(key_cp, key_len);
    key = std::string(key_cp, key_len);
    delete[] key_cp;
    size_t value_len = 0;
    node->receive(&value_len, sizeof(size_t));
    char* value_cp = new char[value_len];
    node->receive(value_cp, value_len);
    value = std::string(value_cp, value_len);
    delete[] value_cp;
    ret_ptr->user_collected_properties[key] = value;
  }
  size_t readable_properties_len = 0;
  node->receive(&readable_properties_len, sizeof(size_t));
  for (size_t i = 0; i < readable_properties_len; i++) {
    std::string key = "";
    std::string value = "";
    size_t key_len = 0;
    node->receive(&key_len, sizeof(size_t));
    char* key_cp = new char[key_len];
    node->receive(key_cp, key_len);
    key = std::string(key_cp, key_len);
    delete[] key_cp;
    size_t value_len = 0;
    node->receive(&value_len, sizeof(size_t));
    char* value_cp = new char[value_len];
    node->receive(value_cp, value_len);
    value = std::string(value_cp, value_len);
    delete[] value_cp;
    ret_ptr->readable_properties[key] = value;
  }
  LOG("TableProperties::UnPackRemote");
  return mem;
}

std::string TableProperties::ToString(const std::string& prop_delim,
                                      const std::string& kv_delim) const {
  std::string result;
  result.reserve(1024);

  // Basic Info
  AppendProperty(result, "# data blocks", num_data_blocks, prop_delim,
                 kv_delim);
  AppendProperty(result, "# entries", num_entries, prop_delim, kv_delim);
  AppendProperty(result, "# deletions", num_deletions, prop_delim, kv_delim);
  AppendProperty(result, "# merge operands", num_merge_operands, prop_delim,
                 kv_delim);
  AppendProperty(result, "# range deletions", num_range_deletions, prop_delim,
                 kv_delim);

  AppendProperty(result, "raw key size", raw_key_size, prop_delim, kv_delim);
  AppendProperty(result, "raw average key size",
                 num_entries != 0 ? 1.0 * raw_key_size / num_entries : 0.0,
                 prop_delim, kv_delim);
  AppendProperty(result, "raw value size", raw_value_size, prop_delim,
                 kv_delim);
  AppendProperty(result, "raw average value size",
                 num_entries != 0 ? 1.0 * raw_value_size / num_entries : 0.0,
                 prop_delim, kv_delim);

  AppendProperty(result, "data block size", data_size, prop_delim, kv_delim);
  char index_block_size_str[80];
  snprintf(index_block_size_str, sizeof(index_block_size_str),
           "index block size (user-key? %d, delta-value? %d)",
           static_cast<int>(index_key_is_user_key),
           static_cast<int>(index_value_is_delta_encoded));
  AppendProperty(result, index_block_size_str, index_size, prop_delim,
                 kv_delim);
  if (index_partitions != 0) {
    AppendProperty(result, "# index partitions", index_partitions, prop_delim,
                   kv_delim);
    AppendProperty(result, "top-level index size", top_level_index_size,
                   prop_delim, kv_delim);
  }
  AppendProperty(result, "filter block size", filter_size, prop_delim,
                 kv_delim);
  AppendProperty(result, "# entries for filter", num_filter_entries, prop_delim,
                 kv_delim);
  AppendProperty(result, "(estimated) table size",
                 data_size + index_size + filter_size, prop_delim, kv_delim);

  AppendProperty(
      result, "filter policy name",
      filter_policy_name.empty() ? std::string("N/A") : filter_policy_name,
      prop_delim, kv_delim);

  AppendProperty(result, "prefix extractor name",
                 prefix_extractor_name.empty() ? std::string("N/A")
                                               : prefix_extractor_name,
                 prop_delim, kv_delim);

  AppendProperty(result, "column family ID",
                 column_family_id ==
                         ROCKSDB_NAMESPACE::TablePropertiesCollectorFactory::
                             Context::kUnknownColumnFamily
                     ? std::string("N/A")
                     : std::to_string(column_family_id),
                 prop_delim, kv_delim);
  AppendProperty(
      result, "column family name",
      column_family_name.empty() ? std::string("N/A") : column_family_name,
      prop_delim, kv_delim);

  AppendProperty(result, "comparator name",
                 comparator_name.empty() ? std::string("N/A") : comparator_name,
                 prop_delim, kv_delim);

  AppendProperty(
      result, "merge operator name",
      merge_operator_name.empty() ? std::string("N/A") : merge_operator_name,
      prop_delim, kv_delim);

  AppendProperty(result, "property collectors names",
                 property_collectors_names.empty() ? std::string("N/A")
                                                   : property_collectors_names,
                 prop_delim, kv_delim);

  AppendProperty(
      result, "SST file compression algo",
      compression_name.empty() ? std::string("N/A") : compression_name,
      prop_delim, kv_delim);

  AppendProperty(
      result, "SST file compression options",
      compression_options.empty() ? std::string("N/A") : compression_options,
      prop_delim, kv_delim);

  AppendProperty(result, "creation time", creation_time, prop_delim, kv_delim);

  AppendProperty(result, "time stamp of earliest key", oldest_key_time,
                 prop_delim, kv_delim);

  AppendProperty(result, "file creation time", file_creation_time, prop_delim,
                 kv_delim);

  AppendProperty(result, "slow compression estimated data size",
                 slow_compression_estimated_data_size, prop_delim, kv_delim);
  AppendProperty(result, "fast compression estimated data size",
                 fast_compression_estimated_data_size, prop_delim, kv_delim);

  // DB identity and DB session ID
  AppendProperty(result, "DB identity", db_id, prop_delim, kv_delim);
  AppendProperty(result, "DB session identity", db_session_id, prop_delim,
                 kv_delim);
  AppendProperty(result, "DB host id", db_host_id, prop_delim, kv_delim);
  AppendProperty(result, "original file number", orig_file_number, prop_delim,
                 kv_delim);

  // Unique ID, when available
  std::string id;
  Status s = GetUniqueIdFromTableProperties(*this, &id);
  AppendProperty(result, "unique ID",
                 s.ok() ? UniqueIdToHumanString(id) : "N/A", prop_delim,
                 kv_delim);

  SeqnoToTimeMapping seq_time_mapping;
  s = seq_time_mapping.Add(seqno_to_time_mapping);
  AppendProperty(result, "Sequence number to time mapping",
                 s.ok() ? seq_time_mapping.ToHumanString() : "N/A", prop_delim,
                 kv_delim);

  return result;
}

void TableProperties::Add(const TableProperties& tp) {
  data_size += tp.data_size;
  index_size += tp.index_size;
  index_partitions += tp.index_partitions;
  top_level_index_size += tp.top_level_index_size;
  index_key_is_user_key += tp.index_key_is_user_key;
  index_value_is_delta_encoded += tp.index_value_is_delta_encoded;
  filter_size += tp.filter_size;
  raw_key_size += tp.raw_key_size;
  raw_value_size += tp.raw_value_size;
  num_data_blocks += tp.num_data_blocks;
  num_entries += tp.num_entries;
  num_filter_entries += tp.num_filter_entries;
  num_deletions += tp.num_deletions;
  num_merge_operands += tp.num_merge_operands;
  num_range_deletions += tp.num_range_deletions;
  slow_compression_estimated_data_size +=
      tp.slow_compression_estimated_data_size;
  fast_compression_estimated_data_size +=
      tp.fast_compression_estimated_data_size;
}

std::map<std::string, uint64_t>
TableProperties::GetAggregatablePropertiesAsMap() const {
  std::map<std::string, uint64_t> rv;
  rv["data_size"] = data_size;
  rv["index_size"] = index_size;
  rv["index_partitions"] = index_partitions;
  rv["top_level_index_size"] = top_level_index_size;
  rv["filter_size"] = filter_size;
  rv["raw_key_size"] = raw_key_size;
  rv["raw_value_size"] = raw_value_size;
  rv["num_data_blocks"] = num_data_blocks;
  rv["num_entries"] = num_entries;
  rv["num_filter_entries"] = num_filter_entries;
  rv["num_deletions"] = num_deletions;
  rv["num_merge_operands"] = num_merge_operands;
  rv["num_range_deletions"] = num_range_deletions;
  rv["slow_compression_estimated_data_size"] =
      slow_compression_estimated_data_size;
  rv["fast_compression_estimated_data_size"] =
      fast_compression_estimated_data_size;
  return rv;
}

// WARNING: manual update to this function is needed
// whenever a new string property is added to TableProperties
// to reduce approximation error.
//
// TODO: eliminate the need of manually updating this function
// for new string properties
std::size_t TableProperties::ApproximateMemoryUsage() const {
  std::size_t usage = 0;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  usage += malloc_usable_size((void*)this);
#else
  usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE

  std::size_t string_props_mem_usage =
      db_id.size() + db_session_id.size() + db_host_id.size() +
      column_family_name.size() + filter_policy_name.size() +
      comparator_name.size() + merge_operator_name.size() +
      prefix_extractor_name.size() + property_collectors_names.size() +
      compression_name.size() + compression_options.size();
  usage += string_props_mem_usage;

  for (auto iter = user_collected_properties.begin();
       iter != user_collected_properties.end(); ++iter) {
    usage += (iter->first.size() + iter->second.size());
  }

  return usage;
}

const std::string TablePropertiesNames::kDbId = "rocksdb.creating.db.identity";
const std::string TablePropertiesNames::kDbSessionId =
    "rocksdb.creating.session.identity";
const std::string TablePropertiesNames::kDbHostId =
    "rocksdb.creating.host.identity";
const std::string TablePropertiesNames::kOriginalFileNumber =
    "rocksdb.original.file.number";
const std::string TablePropertiesNames::kDataSize = "rocksdb.data.size";
const std::string TablePropertiesNames::kIndexSize = "rocksdb.index.size";
const std::string TablePropertiesNames::kIndexPartitions =
    "rocksdb.index.partitions";
const std::string TablePropertiesNames::kTopLevelIndexSize =
    "rocksdb.top-level.index.size";
const std::string TablePropertiesNames::kIndexKeyIsUserKey =
    "rocksdb.index.key.is.user.key";
const std::string TablePropertiesNames::kIndexValueIsDeltaEncoded =
    "rocksdb.index.value.is.delta.encoded";
const std::string TablePropertiesNames::kFilterSize = "rocksdb.filter.size";
const std::string TablePropertiesNames::kRawKeySize = "rocksdb.raw.key.size";
const std::string TablePropertiesNames::kRawValueSize =
    "rocksdb.raw.value.size";
const std::string TablePropertiesNames::kNumDataBlocks =
    "rocksdb.num.data.blocks";
const std::string TablePropertiesNames::kNumEntries = "rocksdb.num.entries";
const std::string TablePropertiesNames::kNumFilterEntries =
    "rocksdb.num.filter_entries";
const std::string TablePropertiesNames::kDeletedKeys = "rocksdb.deleted.keys";
const std::string TablePropertiesNames::kMergeOperands =
    "rocksdb.merge.operands";
const std::string TablePropertiesNames::kNumRangeDeletions =
    "rocksdb.num.range-deletions";
const std::string TablePropertiesNames::kFilterPolicy = "rocksdb.filter.policy";
const std::string TablePropertiesNames::kFormatVersion =
    "rocksdb.format.version";
const std::string TablePropertiesNames::kFixedKeyLen =
    "rocksdb.fixed.key.length";
const std::string TablePropertiesNames::kColumnFamilyId =
    "rocksdb.column.family.id";
const std::string TablePropertiesNames::kColumnFamilyName =
    "rocksdb.column.family.name";
const std::string TablePropertiesNames::kComparator = "rocksdb.comparator";
const std::string TablePropertiesNames::kMergeOperator =
    "rocksdb.merge.operator";
const std::string TablePropertiesNames::kPrefixExtractorName =
    "rocksdb.prefix.extractor.name";
const std::string TablePropertiesNames::kPropertyCollectors =
    "rocksdb.property.collectors";
const std::string TablePropertiesNames::kCompression = "rocksdb.compression";
const std::string TablePropertiesNames::kCompressionOptions =
    "rocksdb.compression_options";
const std::string TablePropertiesNames::kCreationTime = "rocksdb.creation.time";
const std::string TablePropertiesNames::kOldestKeyTime =
    "rocksdb.oldest.key.time";
const std::string TablePropertiesNames::kFileCreationTime =
    "rocksdb.file.creation.time";
const std::string TablePropertiesNames::kSlowCompressionEstimatedDataSize =
    "rocksdb.sample_for_compression.slow.data.size";
const std::string TablePropertiesNames::kFastCompressionEstimatedDataSize =
    "rocksdb.sample_for_compression.fast.data.size";
const std::string TablePropertiesNames::kSequenceNumberTimeMapping =
    "rocksdb.seqno.time.map";

#ifndef NDEBUG
// WARNING: TEST_SetRandomTableProperties assumes the following layout of
// TableProperties
//
// struct TableProperties {
//    int64_t orig_file_number = 0;
//    ...
//    ... int64_t properties only
//    ...
//    std::string db_id;
//    ...
//    ... std::string properties only
//    ...
//    std::string compression_options;
//    UserCollectedProperties user_collected_properties;
//    ...
//    ... Other extra properties: non-int64_t/non-std::string properties only
//    ...
// }
void TEST_SetRandomTableProperties(TableProperties* props) {
  Random* r = Random::GetTLSInstance();
  uint64_t* pu = &props->orig_file_number;
  assert(static_cast<void*>(pu) == static_cast<void*>(props));
  std::string* ps = &props->db_id;
  const uint64_t* const pu_end = reinterpret_cast<const uint64_t*>(ps);
  // Use the last string property's address instead of
  // the first extra property (e.g `user_collected_properties`)'s address
  // in the for-loop to avoid advancing pointer to pointing to
  // potential non-zero padding bytes between these two addresses due to
  // user_collected_properties's alignment requirement
  const std::string* const ps_end_inclusive = &props->compression_options;

  for (; pu < pu_end; ++pu) {
    *pu = r->Next64();
  }
  assert(static_cast<void*>(pu) == static_cast<void*>(ps));
  for (; ps <= ps_end_inclusive; ++ps) {
    *ps = r->RandomBinaryString(13);
  }
}
#endif

}  // namespace ROCKSDB_NAMESPACE
