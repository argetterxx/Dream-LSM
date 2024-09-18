//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <utility>

#include "db/blob/blob_index.h"
#include "db/dbformat.h"
#include "db/version_set.h"
#include "logging/event_logger.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/remote_transfer_service.h"
#include "rocksdb/slice.h"
#include "rocksdb/types.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {}  // anonymous namespace

uint64_t PackFileNumberAndPathId(uint64_t number, uint64_t path_id) {
  assert(number <= kFileNumberMask);
  return number | (path_id * (kFileNumberMask + 1));
}

std::string FileMetaData::DebugString() const {
  std::string r;
  r.append("\n FileMetaData #");
  r.append("\n fd.GetNumber packed_number_and_path_id" +
           std::to_string(fd.GetNumber()));
  r.append("\n fd.file_size " + std::to_string(fd.file_size));
  r.append("\n fd.smallest_seqno " + std::to_string(fd.smallest_seqno));
  r.append("\n fd.largest_seqno " + std::to_string(fd.largest_seqno));
  if (fd.table_reader == nullptr) {
    r.append("\n fd.table_reader == nullptr");
  } else {
    r.append("\n fd.table_reader != nullptr :" +
             fd.table_reader->GetTableProperties()->column_family_name);
  }
  r.append("\n smallest: " + smallest.DebugString(false));
  r.append("\n largest: " + largest.DebugString(false));
  if (table_reader_handle == nullptr) {
    r.append("\n table_reader_handle==nullptr");
  } else {
    r.append("\n table_reader_handle!=nullptr :" +
             std::to_string(reinterpret_cast<size_t>(table_reader_handle)));
  }
  r.append("\n stats: " + std::to_string(stats.num_reads_sampled.load()));
  r.append("\n compensated_file_size: " +
           std::to_string(compensated_file_size));
  r.append("\n num_entries: " + std::to_string(num_entries));
  r.append("\n num_deletions: " + std::to_string(num_deletions));
  r.append("\n raw_key_size: " + std::to_string(raw_key_size));
  r.append("\n raw_value_size: " + std::to_string(raw_value_size));
  r.append("\n num_range_deletions: " + std::to_string(num_range_deletions));
  r.append("\n compensated_range_deletion_size: " +
           std::to_string(compensated_range_deletion_size));
  r.append("\n refs: " + std::to_string(refs));
  if (being_compacted) {
    r.append("\n being_compacted: true");
  } else {
    r.append("\n being_compacted: false");
  }
  if (init_stats_from_file) {
    r.append("\n init_stats_from_file: true");
  } else {
    r.append("\n init_stats_from_file: false");
  }
  if (marked_for_compaction) {
    r.append("\n marked_for_compaction: true");
  } else {
    r.append("\n marked_for_compaction: false");
  }
  r.append("\n temperature: " + std::to_string((size_t)temperature));
  r.append("\n oldest_blob_file_number: " +
           std::to_string(oldest_blob_file_number));

  r.append("\n oldest_ancester_time : " + std::to_string(oldest_ancester_time));
  r.append("\n file_creation_time : " + std::to_string(file_creation_time));
  r.append("\n epoch_number : " + std::to_string(epoch_number));
  r.append("\n file_checksum : " + file_checksum);
  r.append("\n file_checksum_func_name : " + file_checksum_func_name);
  r.append("\n unique_id : " + std::to_string(unique_id.at(0)) + " " +
           std::to_string(unique_id.at(1)));
  r.append("\n");
  return r;
}

void FileMetaData::PackRemote(TransferService* node) const {
  LOG("FileMetaData::PackRemote");
  assert(fd.table_reader == nullptr);
  fd.PackRemote(node);
  assert(table_reader_handle == nullptr);

  size_t file_checksum_len = file_checksum.size();
  node->send(&file_checksum_len, sizeof(size_t));
  if (file_checksum_len > 0)
    node->send(file_checksum.c_str(), file_checksum_len);

  size_t file_checksum_func_name_len = file_checksum_func_name.size();
  node->send(&file_checksum_func_name_len, sizeof(size_t));
  if (file_checksum_func_name_len > 0) {
    node->send(file_checksum_func_name.c_str(), file_checksum_func_name_len);
  }

  size_t ret_val = smallest.size();
  node->send(&ret_val, sizeof(size_t));
  if (ret_val > 0) {
    ret_val = 0;
    node->send(smallest.get_rep()->data(), smallest.size());
  }
  ret_val = 0;

  ret_val = largest.size();
  node->send(&ret_val, sizeof(size_t));
  LOG("FileMetaData::PackRemote largest_len ", ret_val);
  if (ret_val > 0) {
    ret_val = 0;
    node->send(largest.get_rep()->data(), largest.size());
    LOG("FileMetaData::PackRemote largest: ",
        largest.get_rep()->substr(0, largest.size()));
  }

  uint64_t uid[2] = {unique_id.at(0), unique_id.at(1)};
  node->send(uid, sizeof(uint64_t) * 2);
  node->send(reinterpret_cast<const char*>(this), sizeof(FileMetaData));
  LOG("FileMetaData::PackRemote done.");
}

void* FileMetaData::UnPackRemote(TransferService* node) {
  LOG("server FileMetaData::UnPackRemote");
  void* mem = malloc(sizeof(FileMetaData));
  auto ptr = reinterpret_cast<FileMetaData*>(mem);
  void* local_fd = FileDescriptor::UnPackRemote(node);

  std::string remote_file_checksum;
  size_t file_checksum_len = 0;
  node->receive(&file_checksum_len, sizeof(size_t));
  if (file_checksum_len > 0) {
    remote_file_checksum.resize(file_checksum_len);
    node->receive(remote_file_checksum.data(), file_checksum_len);
  }

  std::string remote_file_checksum_func_name;
  size_t file_checksum_func_name_len = 0;
  node->receive(&file_checksum_func_name_len, sizeof(size_t));
  if (file_checksum_func_name_len > 0) {
    remote_file_checksum_func_name.resize(file_checksum_func_name_len);
    node->receive(remote_file_checksum_func_name.data(),
                  file_checksum_func_name_len);
  }

  void* local_smallest = nullptr;
  size_t smallest_len = 0;
  node->receive(&smallest_len, sizeof(size_t));
  if (smallest_len > 0) {
    local_smallest = malloc(smallest_len);
    node->receive(local_smallest, smallest_len);
  }

  void* local_largest = nullptr;
  size_t largest_len = 0;
  node->receive(&largest_len, sizeof(size_t));
  if (largest_len > 0) {
    local_largest = malloc(largest_len);
    node->receive(local_largest, largest_len);
  }

  void* local_uid = malloc(2 * sizeof(uint64_t));
  node->receive(local_uid, 2 * sizeof(uint64_t));

  node->receive(mem, sizeof(FileMetaData));
  new (&ptr->file_checksum) std::string(remote_file_checksum);
  new (&ptr->file_checksum_func_name)
      std::string(remote_file_checksum_func_name);
  new (ptr->smallest.get_rep()) std::string();
  ptr->smallest.get_rep()->resize(smallest_len);
  memcpy(const_cast<char*>(ptr->smallest.get_rep()->data()),
         reinterpret_cast<char*>(local_smallest), smallest_len);
  new (ptr->largest.get_rep()) std::string();
  ptr->largest.get_rep()->resize(largest_len);
  memcpy(const_cast<char*>(ptr->largest.get_rep()->data()),
         reinterpret_cast<char*>(local_largest), largest_len);
  free(local_largest);
  free(local_smallest);

  new (&ptr->unique_id) std::array<uint64_t, 2>();
  ptr->unique_id.at(0) = reinterpret_cast<uint64_t*>(local_uid)[0];
  ptr->unique_id.at(1) = reinterpret_cast<uint64_t*>(local_uid)[1];
  ptr->fd = *reinterpret_cast<FileDescriptor*>(local_fd);
  free(local_fd);
  free(local_uid);

  LOG("server FileMetaData::UnPackRemote done.");
  return mem;
}

void FileMetaData::DoubleCheck(TransferService* node) const {
  LOG_CERR("CheckDouble:: FileMetaData::fd:", fd.file_size, ' ',
           fd.smallest_seqno, ' ', fd.largest_seqno, ' ',
           fd.packed_number_and_path_id);
  node->send(&(fd.file_size), sizeof(uint64_t));
  node->send(&(fd.smallest_seqno), sizeof(SequenceNumber));
  node->send(&(fd.largest_seqno), sizeof(SequenceNumber));
  node->send(&(fd.packed_number_and_path_id), sizeof(uint64_t));
  LOG_CERR("CheckDouble:: FileMetaData::smallest", smallest.DebugString(true));
  size_t ret_val = smallest.size();
  node->send(&ret_val, sizeof(size_t));
  node->send(smallest.get_rep()->data(), smallest.size());
  LOG_CERR("CheckDouble:: FileMetaData::largest", largest.DebugString(true));
  ret_val = largest.size();
  node->send(&ret_val, sizeof(size_t));
  node->send(largest.get_rep()->data(), largest.size());
  LOG_CERR("CheckDouble:: FileMetaData::compensated_file_size",
           compensated_file_size, ' ', num_entries, ' ', num_deletions, ' ',
           raw_key_size, ' ', raw_value_size, ' ', num_range_deletions, ' ',
           compensated_range_deletion_size, ' ', refs, ' ', file_creation_time,
           ' ', epoch_number, ' ', file_checksum, ' ', unique_id.at(0), ' ',
           unique_id.at(1));
  node->send(&compensated_file_size, sizeof(uint64_t));
  LOG_CERR("CheckDouble:: FileMetaData::num_entries");
  node->send(&num_entries, sizeof(uint64_t));
  LOG_CERR("CheckDouble:: FileMetaData::num_deletions");
  node->send(&num_deletions, sizeof(uint64_t));
  LOG_CERR("CheckDouble:: FileMetaData::raw_key_size:", raw_key_size);
  node->send(&raw_key_size, sizeof(uint64_t));
  LOG_CERR("CheckDouble:: FileMetaData::raw_value_size:", raw_value_size);
  node->send(&raw_value_size, sizeof(uint64_t));
  LOG_CERR("CheckDouble:: FileMetaData::num_range_deletions");
  node->send(&num_range_deletions, sizeof(uint64_t));
  LOG_CERR("CheckDouble:: FileMetaData::compensated_range_deletion_size");
  node->send(&compensated_range_deletion_size, sizeof(uint64_t));
  LOG_CERR("CheckDouble:: FileMetaData::refs");
  node->send(&refs, sizeof(uint64_t));
  LOG_CERR("CheckDouble:: FileMetaData::file_creation_time");
  node->send(&file_creation_time, sizeof(uint64_t));
  LOG_CERR("CheckDouble:: FileMetaData::epoch_number");
  node->send(&epoch_number, sizeof(uint64_t));
  LOG_CERR("CheckDouble:: FileMetaData::file_checksum");
  size_t file_checksum_len = file_checksum.size();
  node->send(&file_checksum_len, sizeof(size_t));
  node->send(file_checksum.c_str(), file_checksum_len);
  LOG_CERR("CheckDouble:: FileMetaData::unique_id");
  uint64_t uid[2] = {unique_id.at(0), unique_id.at(1)};
  node->send(uid, sizeof(uint64_t) * 2);
}

void FileMetaData::PackLocal(TransferService* node) const {
  LOG("server FileMetaData::PackLocal");
  fd.PackLocal(node);
  LOG("server FileMetaData::PackLocal fd");
  assert(table_reader_handle == nullptr);
  assert(file_checksum == kUnknownFileChecksum);
  assert(file_checksum_func_name == kUnknownFileChecksumFuncName);

  size_t ret_val = smallest.size();
  node->send(&ret_val, sizeof(size_t));
  if (ret_val > 0) node->send(smallest.get_rep()->data(), smallest.size());

  ret_val = largest.size();
  node->send(&ret_val, sizeof(size_t));
  if (ret_val > 0) node->send(largest.get_rep()->data(), largest.size());

  uint64_t uid[2] = {unique_id.at(0), unique_id.at(1)};
  node->send(uid, sizeof(uint64_t) * 2);
  node->send(reinterpret_cast<const char*>(this), sizeof(FileMetaData));
}

void* FileMetaData::UnPackLocal(TransferService* node) {
  LOG("client FileMetaData::UnPackLocal");
  void* local_fd = FileDescriptor::UnPackLocal(node);
  size_t len = 0, len2 = 0;
  void *data = nullptr, *data2 = nullptr;
  node->receive(&len, sizeof(size_t));
  if (len > 0) {
    data = malloc(len);
    node->receive(data, len);
  }

  node->receive(&len2, sizeof(size_t));
  if (len2 > 0) {
    data2 = malloc(len2);
    node->receive(data2, len2);
  }

  void* local_uid = malloc(2 * sizeof(uint64_t));
  node->receive(local_uid, 2 * sizeof(uint64_t));

  void* mem = malloc(sizeof(FileMetaData));
  node->receive(mem, sizeof(FileMetaData));
  auto ptr = reinterpret_cast<FileMetaData*>(mem);
  ptr->fd = *reinterpret_cast<FileDescriptor*>(local_fd);
  free(local_fd);
  new (&ptr->file_checksum) std::string(kUnknownFileChecksum);
  new (&ptr->file_checksum_func_name) std::string(kUnknownFileChecksumFuncName);
  // new (&ptr->smallest.SharedSet(const std::string &now)) InternalKey();
  if (len > 0) {
    std::string* sptr = ptr->smallest.get_rep();
    new (sptr) std::string(reinterpret_cast<const char*>(data), len);
  } else {
    std::string* sptr = ptr->smallest.get_rep();
    new (sptr) std::string();
  }
  if (len2 > 0) {
    std::string* sptr = ptr->largest.get_rep();
    new (sptr) std::string(reinterpret_cast<const char*>(data2), len2);
  } else {
    std::string* sptr = ptr->largest.get_rep();
    new (sptr) std::string();
  }

  LOG("client FileMetaData::UnPackLocal unique_id");
  new (&ptr->unique_id) std::array<uint64_t, 2>();
  ptr->unique_id.at(0) = reinterpret_cast<uint64_t*>(local_uid)[0];
  ptr->unique_id.at(1) = reinterpret_cast<uint64_t*>(local_uid)[1];
  free(local_uid);
  LOG("client FileMetaData::UnPackLocal unique_id");
  free(data);
  free(data2);
  return mem;
}

Status FileMetaData::UpdateBoundaries(const Slice& key, const Slice& value,
                                      SequenceNumber seqno,
                                      ValueType value_type) {
  if (value_type == kTypeBlobIndex) {
    BlobIndex blob_index;
    const Status s = blob_index.DecodeFrom(value);
    if (!s.ok()) {
      return s;
    }

    if (!blob_index.IsInlined() && !blob_index.HasTTL()) {
      if (blob_index.file_number() == kInvalidBlobFileNumber) {
        return Status::Corruption("Invalid blob file number");
      }
      if (oldest_blob_file_number == kInvalidBlobFileNumber ||
          oldest_blob_file_number > blob_index.file_number()) {
        oldest_blob_file_number = blob_index.file_number();
      }
    }
  }
  if (smallest.size() == 0) {
    smallest.DecodeFrom(key);
  }
  largest.DecodeFrom(key);
  fd.smallest_seqno = std::min(fd.smallest_seqno, seqno);
  fd.largest_seqno = std::max(fd.largest_seqno, seqno);

  return Status::OK();
}

void* VersionEdit::UnPackLocal(TransferService* node,
                               VersionEdit* version_edit) {
  void* mem = nullptr;
  if (version_edit == nullptr)
    mem = malloc(sizeof(VersionEdit));
  else
    mem = reinterpret_cast<void*>(version_edit);
  node->receive(mem, sizeof(VersionEdit));
  auto ret_version_edit_ = reinterpret_cast<VersionEdit*>(mem);
  size_t db_id_len_ = 0;
  node->receive(&db_id_len_, sizeof(size_t));
  if (db_id_len_ > 0) {
    char* db_id_ = new char[db_id_len_];
    node->receive(db_id_, db_id_len_);
    new (&ret_version_edit_->db_id_) std::string(db_id_, db_id_len_);
    delete[] db_id_;
  } else {
    new (&ret_version_edit_->db_id_) std::string();
  }

  size_t comparator_len_ = 0;
  node->receive(&comparator_len_, sizeof(size_t));
  if (comparator_len_ > 0) {
    char* comparator_ = new char[comparator_len_];
    node->receive(comparator_, comparator_len_);
    new (&ret_version_edit_->comparator_)
        std::string(comparator_, comparator_len_);
    delete[] comparator_;
  } else {
    new (&ret_version_edit_->comparator_) std::string();
  }

  new (&ret_version_edit_->compact_cursors_)
      std::vector<std::pair<int, InternalKey>>();
  size_t compact_cursors_size_ = 0;
  node->receive(&compact_cursors_size_, sizeof(size_t));
  for (size_t i = 0; i < compact_cursors_size_; i++) {
    int level = 0;
    node->receive(&level, sizeof(int));
    size_t str_len_ = 0;
    node->receive(&str_len_, sizeof(size_t));
    char* str_ = new char[str_len_];
    node->receive(str_, str_len_);
    InternalKey key;
    key.DecodeFrom(Slice(str_, str_len_));
    ret_version_edit_->compact_cursors_.emplace_back(level, key);
    delete[] str_;
  }

  LOG_CERR("UnpackLocal:: DeletedFiles")
  new (&ret_version_edit_->deleted_files_) DeletedFiles();
  ret_version_edit_->deleted_files_.clear();
  size_t deleted_files_size_ = 0;
  node->receive(&deleted_files_size_, sizeof(size_t));
  LOG_CERR("UnpackLocal:: DeletedFiles:: ", deleted_files_size_);
  for (size_t i = 0; i < deleted_files_size_; i++) {
    int level = 0;
    node->receive(&level, sizeof(int));
    uint64_t file_number = 0;
    node->receive(&file_number, sizeof(uint64_t));
    LOG_CERR("UnpackLocal:: DeletedFiles:: ", level, ' ', file_number);
    ret_version_edit_->deleted_files_.insert(
        std::make_pair(level, file_number));
  }
  LOG_CERR("UnpackLocal:: DeletedFiles::End ", deleted_files_size_);

  new (&ret_version_edit_->new_files_)
      std::vector<std::pair<int, FileMetaData>>();
  size_t new_files_size_ = 0;
  node->receive(&new_files_size_, sizeof(size_t));
  for (size_t i = 0; i < new_files_size_; i++) {
    int level = 0;
    node->receive(&level, sizeof(int));
    auto local_file_meta_data =
        reinterpret_cast<FileMetaData*>(FileMetaData::UnPackLocal(node));
    ret_version_edit_->new_files_.emplace_back(level, *local_file_meta_data);
    free(local_file_meta_data);
  }

  new (&ret_version_edit_->blob_file_additions_)
      std::vector<BlobFileAddition>();
  new (&ret_version_edit_->blob_file_garbages_) std::vector<BlobFileGarbage>();

  new (&ret_version_edit_->wal_additions_) std::vector<WalAddition>();
  new (&ret_version_edit_->wal_deletion_) WalDeletion();

  size_t column_family_name_len = 0;
  node->receive(&column_family_name_len, sizeof(size_t));
  if (column_family_name_len > 0) {
    char* column_family_name_ = new char[column_family_name_len];
    node->receive(column_family_name_, column_family_name_len);
    new (&ret_version_edit_->column_family_name_)
        std::string(column_family_name_, column_family_name_len);
    delete[] column_family_name_;
  } else {
    new (&ret_version_edit_->column_family_name_) std::string();
  }

  size_t full_history_ts_low_size = 0;
  node->receive(&full_history_ts_low_size, sizeof(size_t));
  if (full_history_ts_low_size > 0) {
    char* full_history_ts_low_ = new char[full_history_ts_low_size];
    node->receive(full_history_ts_low_, full_history_ts_low_size);
    new (&ret_version_edit_->full_history_ts_low_)
        std::string(full_history_ts_low_, full_history_ts_low_size);
    delete[] full_history_ts_low_;
  } else {
    new (&ret_version_edit_->full_history_ts_low_) std::string();
  }
  return mem;
}

void VersionEdit::DoubleCheck(TransferService* node) const {
  LOG_CERR("CheckDouble:: VersionEdit::max_level_");
  node->send(&max_level_, sizeof(int));
  LOG_CERR("CheckDouble:: VersionEdit::log_number_");
  node->send(&log_number_, sizeof(uint64_t));
  LOG_CERR("CheckDouble:: VersionEdit::prev_log_number_");
  node->send(&prev_log_number_, sizeof(uint64_t));
  LOG_CERR("CheckDouble:: VersionEdit::next_file_number_");
  node->send(&next_file_number_, sizeof(uint64_t));
  LOG_CERR("CheckDouble:: VersionEdit::max_column_family_");
  node->send(&max_column_family_, sizeof(int));
  LOG_CERR("CheckDouble:: VersionEdit::min_log_number_to_keep_");
  node->send(&min_log_number_to_keep_, sizeof(uint64_t));
  LOG_CERR("CheckDouble:: VersionEdit::last_sequence_");
  node->send(&last_sequence_, sizeof(SequenceNumber));
  LOG_CERR("CheckDouble:: VersionEdit::has_log_number_");
  node->send(&has_log_number_, sizeof(bool));
  LOG_CERR("CheckDouble:: VersionEdit::has_prev_log_number_");
  node->send(&has_prev_log_number_, sizeof(bool));
  LOG_CERR("CheckDouble:: VersionEdit::has_next_file_number_");
  node->send(&has_next_file_number_, sizeof(bool));
  LOG_CERR("CheckDouble:: VersionEdit::has_max_column_family_");
  node->send(&has_max_column_family_, sizeof(bool));
  LOG_CERR("CheckDouble:: VersionEdit::has_min_log_number_to_keep_");
  node->send(&has_min_log_number_to_keep_, sizeof(bool));
  LOG_CERR("CheckDouble:: VersionEdit::has_last_sequence_");
  node->send(&has_last_sequence_, sizeof(bool));
  LOG_CERR("CheckDouble:: VersionEdit::CompactCursors");
  size_t compact_cursors_size_ = compact_cursors_.size();
  node->send(&compact_cursors_size_, sizeof(size_t));
  for (auto pr : compact_cursors_) {
    node->send(&pr.first, sizeof(int));
    auto string_ptr = pr.second.get_rep();
    size_t str_len_ = string_ptr->length();
    node->send(&str_len_, sizeof(size_t));
    node->send(string_ptr->c_str(), str_len_);
  }
  // LOG_CERR("CheckDouble:: VersionEdit::DeletedFiles");
  size_t deleted_files_size_ = deleted_files_.size();
  node->send(&deleted_files_size_, sizeof(size_t));
  for (auto it = deleted_files_.begin(); it != deleted_files_.end(); it++) {
    node->send(&(it->first), sizeof(int));
    node->send(&(it->second), sizeof(uint64_t));
  }
  LOG_CERR("CheckDouble:: VersionEdit::NewFiles");
  size_t new_files_size_ = new_files_.size();
  node->send(&new_files_size_, sizeof(size_t));
  // for (auto pr : new_files_) {
  //   node->send(&pr.first, sizeof(int));
  //   pr.second.PackLocal(node);
  // }
  // LOG_CERR("CheckDouble:: VersionEdit::remaining_entries_");
  node->send(&remaining_entries_, sizeof(uint32_t));
  // LOG_CERR("CheckDouble:: VersionEdit::full_history_ts_low_");
  size_t full_history_ts_low_size = full_history_ts_low_.size();
  node->send(&full_history_ts_low_size, sizeof(size_t));
  if (full_history_ts_low_size > 0) {
    node->send(full_history_ts_low_.c_str(), full_history_ts_low_size);
  }
}

void VersionEdit::PackLocal(TransferService* node) const {
  node->send(reinterpret_cast<const void*>(this), sizeof(VersionEdit));
  size_t db_id_len_ = db_id_.size();
  node->send(&db_id_len_, sizeof(size_t));
  if (db_id_len_ > 0) node->send(db_id_.c_str(), db_id_len_);

  size_t comparator_len_ = comparator_.size();
  node->send(&comparator_len_, sizeof(size_t));
  if (comparator_.size() > 0)
    node->send(comparator_.c_str(), comparator_.size());

  size_t compact_cursors_size_ = compact_cursors_.size();
  node->send(&compact_cursors_size_, sizeof(size_t));
  for (auto pr : compact_cursors_) {
    node->send(&pr.first, sizeof(int));
    auto string_ptr = pr.second.get_rep();
    size_t str_len_ = string_ptr->length();
    node->send(&str_len_, sizeof(size_t));
    node->send(string_ptr->c_str(), str_len_);
  }

  size_t deleted_files_size_ = deleted_files_.size();
  node->send(&deleted_files_size_, sizeof(size_t));
  for (auto pr : deleted_files_) {
    int level = pr.first;
    uint64_t file_number = pr.second;
    node->send(&level, sizeof(int));
    node->send(&file_number, sizeof(uint64_t));
  }

  size_t new_files_size_ = new_files_.size();
  node->send(&new_files_size_, sizeof(size_t));
  for (auto pr : new_files_) {
    node->send(&pr.first, sizeof(int));
    pr.second.PackLocal(node);
  }

  size_t column_family_name_len = column_family_name_.size();
  node->send(&column_family_name_len, sizeof(size_t));
  if (column_family_name_len > 0) {
    node->send(column_family_name_.c_str(), column_family_name_len);
  }

  size_t full_history_ts_low_size = full_history_ts_low_.size();
  node->send(&full_history_ts_low_size, sizeof(size_t));
  if (full_history_ts_low_size > 0) {
    node->send(full_history_ts_low_.c_str(), full_history_ts_low_size);
  }
}

void VersionEdit::free_remote() {
  new_files_.~NewFiles();
  db_id_.~basic_string();
  comparator_.~basic_string();
  column_family_name_.~basic_string();
  full_history_ts_low_.~basic_string();
  compact_cursors_.~CompactCursors();
  wal_additions_.~WalAdditions();
  deleted_files_.~DeletedFiles();
}

void VersionEdit::PackRemote(TransferService* node) const {
  LOG("VersionEdit::PackRemote");
  size_t new_file_size_ = new_files_.size();
  node->send(&new_file_size_, sizeof(size_t));
  for (size_t i = 0; i < new_file_size_; i++) {
    node->send(&new_files_[i].first, sizeof(int));
    new_files_[i].second.PackRemote(node);
  }
  node->send(&has_last_sequence_, sizeof(bool));
  node->send(&last_sequence_, sizeof(SequenceNumber));
  LOG("VersionEdit::PackRemote done.");
}

void VersionEdit::UnPackRemote(TransferService* node) {
  LOG("VersionEdit::UnPackRemote");
  size_t new_file_size_ = 0;
  node->receive(&new_file_size_, sizeof(size_t));
  for (size_t i = 0; i < new_file_size_; i++) {
    int level = 0;
    node->receive(&level, sizeof(int));
    auto local_file_meta_data =
        reinterpret_cast<FileMetaData*>(FileMetaData::UnPackRemote(node));
    new_files_.push_back(std::make_pair(level, *local_file_meta_data));
    free(local_file_meta_data);
  }
  node->receive(&has_last_sequence_, sizeof(bool));
  node->receive(&last_sequence_, sizeof(SequenceNumber));
  LOG("VersionEdit::UnPackRemote done.");
}

void VersionEdit::Clear() {
  max_level_ = 0;
  db_id_.clear();
  comparator_.clear();
  log_number_ = 0;
  prev_log_number_ = 0;
  next_file_number_ = 0;
  max_column_family_ = 0;
  min_log_number_to_keep_ = 0;
  last_sequence_ = 0;
  has_db_id_ = false;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_max_column_family_ = false;
  has_min_log_number_to_keep_ = false;
  has_last_sequence_ = false;
  compact_cursors_.clear();
  deleted_files_.clear();
  new_files_.clear();
  blob_file_additions_.clear();
  blob_file_garbages_.clear();
  wal_additions_.clear();
  wal_deletion_.Reset();
  column_family_ = 0;
  is_column_family_add_ = false;
  is_column_family_drop_ = false;
  column_family_name_.clear();
  is_in_atomic_group_ = false;
  remaining_entries_ = 0;
  full_history_ts_low_.clear();
}

bool VersionEdit::EncodeTo(std::string* dst) const {
  if (has_db_id_) {
    PutVarint32(dst, kDbId);
    PutLengthPrefixedSlice(dst, db_id_);
  }
  if (has_comparator_) {
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
    PutVarint32Varint64(dst, kLogNumber, log_number_);
  }
  if (has_prev_log_number_) {
    PutVarint32Varint64(dst, kPrevLogNumber, prev_log_number_);
  }
  if (has_next_file_number_) {
    PutVarint32Varint64(dst, kNextFileNumber, next_file_number_);
  }
  if (has_max_column_family_) {
    PutVarint32Varint32(dst, kMaxColumnFamily, max_column_family_);
  }
  if (has_min_log_number_to_keep_) {
    PutVarint32Varint64(dst, kMinLogNumberToKeep, min_log_number_to_keep_);
  }
  if (has_last_sequence_) {
    PutVarint32Varint64(dst, kLastSequence, last_sequence_);
  }
  for (size_t i = 0; i < compact_cursors_.size(); i++) {
    if (compact_cursors_[i].second.Valid()) {
      PutVarint32(dst, kCompactCursor);
      PutVarint32(dst, compact_cursors_[i].first);  // level
      PutLengthPrefixedSlice(dst, compact_cursors_[i].second.Encode());
    }
  }
  for (const auto& deleted : deleted_files_) {
    PutVarint32Varint32Varint64(dst, kDeletedFile, deleted.first /* level */,
                                deleted.second /* file number */);
  }

  bool min_log_num_written = false;
  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    if (!f.smallest.Valid() || !f.largest.Valid() ||
        f.epoch_number == kUnknownEpochNumber) {
      return false;
    }

    PutVarint32(dst, kNewFile4);
    PutVarint32Varint64(dst, new_files_[i].first /* level */, f.fd.GetNumber());
    PutVarint64(dst, f.fd.GetFileSize());
    PutLengthPrefixedSlice(dst, f.smallest.Encode());
    PutLengthPrefixedSlice(dst, f.largest.Encode());
    PutVarint64Varint64(dst, f.fd.smallest_seqno, f.fd.largest_seqno);
    // Customized fields' format:
    // +-----------------------------+
    // | 1st field's tag (varint32)  |
    // +-----------------------------+
    // | 1st field's size (varint32) |
    // +-----------------------------+
    // |    bytes for 1st field      |
    // |  (based on size decoded)    |
    // +-----------------------------+
    // |                             |
    // |          ......             |
    // |                             |
    // +-----------------------------+
    // | last field's size (varint32)|
    // +-----------------------------+
    // |    bytes for last field     |
    // |  (based on size decoded)    |
    // +-----------------------------+
    // | terminating tag (varint32)  |
    // +-----------------------------+
    //
    // Customized encoding for fields:
    //   tag kPathId: 1 byte as path_id
    //   tag kNeedCompaction:
    //        now only can take one char value 1 indicating need-compaction
    //
    PutVarint32(dst, NewFileCustomTag::kOldestAncesterTime);
    std::string varint_oldest_ancester_time;
    PutVarint64(&varint_oldest_ancester_time, f.oldest_ancester_time);
    TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:VarintOldestAncesterTime",
                             &varint_oldest_ancester_time);
    PutLengthPrefixedSlice(dst, Slice(varint_oldest_ancester_time));

    PutVarint32(dst, NewFileCustomTag::kFileCreationTime);
    std::string varint_file_creation_time;
    PutVarint64(&varint_file_creation_time, f.file_creation_time);
    TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:VarintFileCreationTime",
                             &varint_file_creation_time);
    PutLengthPrefixedSlice(dst, Slice(varint_file_creation_time));

    PutVarint32(dst, NewFileCustomTag::kEpochNumber);
    std::string varint_epoch_number;
    PutVarint64(&varint_epoch_number, f.epoch_number);
    PutLengthPrefixedSlice(dst, Slice(varint_epoch_number));

    PutVarint32(dst, NewFileCustomTag::kFileChecksum);
    PutLengthPrefixedSlice(dst, Slice(f.file_checksum));

    PutVarint32(dst, NewFileCustomTag::kFileChecksumFuncName);
    PutLengthPrefixedSlice(dst, Slice(f.file_checksum_func_name));

    if (f.fd.GetPathId() != 0) {
      PutVarint32(dst, NewFileCustomTag::kPathId);
      char p = static_cast<char>(f.fd.GetPathId());
      PutLengthPrefixedSlice(dst, Slice(&p, 1));
    }
    if (f.temperature != Temperature::kUnknown) {
      PutVarint32(dst, NewFileCustomTag::kTemperature);
      char p = static_cast<char>(f.temperature);
      PutLengthPrefixedSlice(dst, Slice(&p, 1));
    }
    if (f.marked_for_compaction) {
      PutVarint32(dst, NewFileCustomTag::kNeedCompaction);
      char p = static_cast<char>(1);
      PutLengthPrefixedSlice(dst, Slice(&p, 1));
    }
    if (has_min_log_number_to_keep_ && !min_log_num_written) {
      PutVarint32(dst, NewFileCustomTag::kMinLogNumberToKeepHack);
      std::string varint_log_number;
      PutFixed64(&varint_log_number, min_log_number_to_keep_);
      PutLengthPrefixedSlice(dst, Slice(varint_log_number));
      min_log_num_written = true;
    }
    if (f.oldest_blob_file_number != kInvalidBlobFileNumber) {
      PutVarint32(dst, NewFileCustomTag::kOldestBlobFileNumber);
      std::string oldest_blob_file_number;
      PutVarint64(&oldest_blob_file_number, f.oldest_blob_file_number);
      PutLengthPrefixedSlice(dst, Slice(oldest_blob_file_number));
    }
    UniqueId64x2 unique_id = f.unique_id;
    TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:UniqueId", &unique_id);
    if (unique_id != kNullUniqueId64x2) {
      PutVarint32(dst, NewFileCustomTag::kUniqueId);
      std::string unique_id_str = EncodeUniqueIdBytes(&unique_id);
      PutLengthPrefixedSlice(dst, Slice(unique_id_str));
    }

    if (f.compensated_range_deletion_size) {
      PutVarint32(dst, kCompensatedRangeDeletionSize);
      std::string compensated_range_deletion_size;
      PutVarint64(&compensated_range_deletion_size,
                  f.compensated_range_deletion_size);
      PutLengthPrefixedSlice(dst, Slice(compensated_range_deletion_size));
    }

    TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:NewFile4:CustomizeFields",
                             dst);

    PutVarint32(dst, NewFileCustomTag::kTerminate);
  }

  for (const auto& blob_file_addition : blob_file_additions_) {
    PutVarint32(dst, kBlobFileAddition);
    blob_file_addition.EncodeTo(dst);
  }

  for (const auto& blob_file_garbage : blob_file_garbages_) {
    PutVarint32(dst, kBlobFileGarbage);
    blob_file_garbage.EncodeTo(dst);
  }

  for (const auto& wal_addition : wal_additions_) {
    PutVarint32(dst, kWalAddition2);
    std::string encoded;
    wal_addition.EncodeTo(&encoded);
    PutLengthPrefixedSlice(dst, encoded);
  }

  if (!wal_deletion_.IsEmpty()) {
    PutVarint32(dst, kWalDeletion2);
    std::string encoded;
    wal_deletion_.EncodeTo(&encoded);
    PutLengthPrefixedSlice(dst, encoded);
  }

  // 0 is default and does not need to be explicitly written
  if (column_family_ != 0) {
    PutVarint32Varint32(dst, kColumnFamily, column_family_);
  }

  if (is_column_family_add_) {
    PutVarint32(dst, kColumnFamilyAdd);
    PutLengthPrefixedSlice(dst, Slice(column_family_name_));
  }

  if (is_column_family_drop_) {
    PutVarint32(dst, kColumnFamilyDrop);
  }

  if (is_in_atomic_group_) {
    PutVarint32(dst, kInAtomicGroup);
    PutVarint32(dst, remaining_entries_);
  }

  if (HasFullHistoryTsLow()) {
    PutVarint32(dst, kFullHistoryTsLow);
    PutLengthPrefixedSlice(dst, full_history_ts_low_);
  }
  return true;
}

static bool GetInternalKey(Slice* input, InternalKey* dst) {
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    dst->DecodeFrom(str);
    return dst->Valid();
  } else {
    return false;
  }
}

bool VersionEdit::GetLevel(Slice* input, int* level, const char** /*msg*/) {
  uint32_t v = 0;
  if (GetVarint32(input, &v)) {
    *level = v;
    if (max_level_ < *level) {
      max_level_ = *level;
    }
    return true;
  } else {
    return false;
  }
}

const char* VersionEdit::DecodeNewFile4From(Slice* input) {
  const char* msg = nullptr;
  int level = 0;
  FileMetaData f;
  uint64_t number = 0;
  uint32_t path_id = 0;
  uint64_t file_size = 0;
  SequenceNumber smallest_seqno = 0;
  SequenceNumber largest_seqno = kMaxSequenceNumber;
  if (GetLevel(input, &level, &msg) && GetVarint64(input, &number) &&
      GetVarint64(input, &file_size) && GetInternalKey(input, &f.smallest) &&
      GetInternalKey(input, &f.largest) &&
      GetVarint64(input, &smallest_seqno) &&
      GetVarint64(input, &largest_seqno)) {
    // See comments in VersionEdit::EncodeTo() for format of customized fields
    while (true) {
      uint32_t custom_tag = 0;
      Slice field;
      if (!GetVarint32(input, &custom_tag)) {
        return "new-file4 custom field";
      }
      if (custom_tag == kTerminate) {
        break;
      }
      if (!GetLengthPrefixedSlice(input, &field)) {
        return "new-file4 custom field length prefixed slice error";
      }
      switch (custom_tag) {
        case kPathId:
          if (field.size() != 1) {
            return "path_id field wrong size";
          }
          path_id = field[0];
          if (path_id > 3) {
            return "path_id wrong vaue";
          }
          break;
        case kOldestAncesterTime:
          if (!GetVarint64(&field, &f.oldest_ancester_time)) {
            return "invalid oldest ancester time";
          }
          break;
        case kFileCreationTime:
          if (!GetVarint64(&field, &f.file_creation_time)) {
            return "invalid file creation time";
          }
          break;
        case kEpochNumber:
          if (!GetVarint64(&field, &f.epoch_number)) {
            return "invalid epoch number";
          }
          break;
        case kFileChecksum:
          f.file_checksum = field.ToString();
          break;
        case kFileChecksumFuncName:
          f.file_checksum_func_name = field.ToString();
          break;
        case kNeedCompaction:
          if (field.size() != 1) {
            return "need_compaction field wrong size";
          }
          f.marked_for_compaction = (field[0] == 1);
          break;
        case kMinLogNumberToKeepHack:
          // This is a hack to encode kMinLogNumberToKeep in a
          // forward-compatible fashion.
          if (!GetFixed64(&field, &min_log_number_to_keep_)) {
            return "deleted log number malformatted";
          }
          has_min_log_number_to_keep_ = true;
          break;
        case kOldestBlobFileNumber:
          if (!GetVarint64(&field, &f.oldest_blob_file_number)) {
            return "invalid oldest blob file number";
          }
          break;
        case kTemperature:
          if (field.size() != 1) {
            return "temperature field wrong size";
          } else {
            Temperature casted_field = static_cast<Temperature>(field[0]);
            if (casted_field <= Temperature::kCold) {
              f.temperature = casted_field;
            }
          }
          break;
        case kUniqueId:
          if (!DecodeUniqueIdBytes(field.ToString(), &f.unique_id).ok()) {
            f.unique_id = kNullUniqueId64x2;
            return "invalid unique id";
          }
          break;
        case kCompensatedRangeDeletionSize:
          if (!GetVarint64(&field, &f.compensated_range_deletion_size)) {
            return "Invalid compensated range deletion size";
          }
          break;
        default:
          if ((custom_tag & kCustomTagNonSafeIgnoreMask) != 0) {
            // Should not proceed if cannot understand it
            return "new-file4 custom field not supported";
          }
          break;
      }
    }
  } else {
    return "new-file4 entry";
  }
  f.fd =
      FileDescriptor(number, path_id, file_size, smallest_seqno, largest_seqno);
  new_files_.push_back(std::make_pair(level, f));
  return nullptr;
}

Status VersionEdit::DecodeFrom(const Slice& src) {
  Clear();
#ifndef NDEBUG
  bool ignore_ignorable_tags = false;
  TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:IgnoreIgnorableTags",
                           &ignore_ignorable_tags);
#endif
  Slice input = src;
  const char* msg = nullptr;
  uint32_t tag = 0;

  // Temporary storage for parsing
  int level = 0;
  FileMetaData f;
  Slice str;
  InternalKey key;
  while (msg == nullptr && GetVarint32(&input, &tag)) {
#ifndef NDEBUG
    if (ignore_ignorable_tags && tag > kTagSafeIgnoreMask) {
      tag = kTagSafeIgnoreMask;
    }
#endif
    switch (tag) {
      case kDbId:
        if (GetLengthPrefixedSlice(&input, &str)) {
          db_id_ = str.ToString();
          has_db_id_ = true;
        } else {
          msg = "db id";
        }
        break;
      case kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kLogNumber:
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:
        if (GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;

      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kMaxColumnFamily:
        if (GetVarint32(&input, &max_column_family_)) {
          has_max_column_family_ = true;
        } else {
          msg = "max column family";
        }
        break;

      case kMinLogNumberToKeep:
        if (GetVarint64(&input, &min_log_number_to_keep_)) {
          has_min_log_number_to_keep_ = true;
        } else {
          msg = "min log number to kee";
        }
        break;

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kCompactCursor:
        if (GetLevel(&input, &level, &msg) && GetInternalKey(&input, &key)) {
          // Here we re-use the output format of compact pointer in LevelDB
          // to persist compact_cursors_
          compact_cursors_.push_back(std::make_pair(level, key));
        } else {
          if (!msg) {
            msg = "compaction cursor";
          }
        }
        break;

      case kDeletedFile: {
        uint64_t number = 0;
        if (GetLevel(&input, &level, &msg) && GetVarint64(&input, &number)) {
          deleted_files_.insert(std::make_pair(level, number));
        } else {
          if (!msg) {
            msg = "deleted file";
          }
        }
        break;
      }

      case kNewFile: {
        uint64_t number = 0;
        uint64_t file_size = 0;
        if (GetLevel(&input, &level, &msg) && GetVarint64(&input, &number) &&
            GetVarint64(&input, &file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest)) {
          f.fd = FileDescriptor(number, 0, file_size);
          new_files_.push_back(std::make_pair(level, f));
        } else {
          if (!msg) {
            msg = "new-file entry";
          }
        }
        break;
      }
      case kNewFile2: {
        uint64_t number = 0;
        uint64_t file_size = 0;
        SequenceNumber smallest_seqno = 0;
        SequenceNumber largest_seqno = kMaxSequenceNumber;
        if (GetLevel(&input, &level, &msg) && GetVarint64(&input, &number) &&
            GetVarint64(&input, &file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest) &&
            GetVarint64(&input, &smallest_seqno) &&
            GetVarint64(&input, &largest_seqno)) {
          f.fd = FileDescriptor(number, 0, file_size, smallest_seqno,
                                largest_seqno);
          new_files_.push_back(std::make_pair(level, f));
        } else {
          if (!msg) {
            msg = "new-file2 entry";
          }
        }
        break;
      }

      case kNewFile3: {
        uint64_t number = 0;
        uint32_t path_id = 0;
        uint64_t file_size = 0;
        SequenceNumber smallest_seqno = 0;
        SequenceNumber largest_seqno = kMaxSequenceNumber;
        if (GetLevel(&input, &level, &msg) && GetVarint64(&input, &number) &&
            GetVarint32(&input, &path_id) && GetVarint64(&input, &file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest) &&
            GetVarint64(&input, &smallest_seqno) &&
            GetVarint64(&input, &largest_seqno)) {
          f.fd = FileDescriptor(number, path_id, file_size, smallest_seqno,
                                largest_seqno);
          new_files_.push_back(std::make_pair(level, f));
        } else {
          if (!msg) {
            msg = "new-file3 entry";
          }
        }
        break;
      }

      case kNewFile4: {
        msg = DecodeNewFile4From(&input);
        break;
      }

      case kBlobFileAddition:
      case kBlobFileAddition_DEPRECATED: {
        BlobFileAddition blob_file_addition;
        const Status s = blob_file_addition.DecodeFrom(&input);
        if (!s.ok()) {
          return s;
        }

        AddBlobFile(std::move(blob_file_addition));
        break;
      }

      case kBlobFileGarbage:
      case kBlobFileGarbage_DEPRECATED: {
        BlobFileGarbage blob_file_garbage;
        const Status s = blob_file_garbage.DecodeFrom(&input);
        if (!s.ok()) {
          return s;
        }

        AddBlobFileGarbage(std::move(blob_file_garbage));
        break;
      }

      case kWalAddition: {
        WalAddition wal_addition;
        const Status s = wal_addition.DecodeFrom(&input);
        if (!s.ok()) {
          return s;
        }

        wal_additions_.emplace_back(std::move(wal_addition));
        break;
      }

      case kWalAddition2: {
        Slice encoded;
        if (!GetLengthPrefixedSlice(&input, &encoded)) {
          msg = "WalAddition not prefixed by length";
          break;
        }

        WalAddition wal_addition;
        const Status s = wal_addition.DecodeFrom(&encoded);
        if (!s.ok()) {
          return s;
        }

        wal_additions_.emplace_back(std::move(wal_addition));
        break;
      }

      case kWalDeletion: {
        WalDeletion wal_deletion;
        const Status s = wal_deletion.DecodeFrom(&input);
        if (!s.ok()) {
          return s;
        }

        wal_deletion_ = std::move(wal_deletion);
        break;
      }

      case kWalDeletion2: {
        Slice encoded;
        if (!GetLengthPrefixedSlice(&input, &encoded)) {
          msg = "WalDeletion not prefixed by length";
          break;
        }

        WalDeletion wal_deletion;
        const Status s = wal_deletion.DecodeFrom(&encoded);
        if (!s.ok()) {
          return s;
        }

        wal_deletion_ = std::move(wal_deletion);
        break;
      }

      case kColumnFamily:
        if (!GetVarint32(&input, &column_family_)) {
          if (!msg) {
            msg = "set column family id";
          }
        }
        break;

      case kColumnFamilyAdd:
        if (GetLengthPrefixedSlice(&input, &str)) {
          is_column_family_add_ = true;
          column_family_name_ = str.ToString();
        } else {
          if (!msg) {
            msg = "column family add";
          }
        }
        break;

      case kColumnFamilyDrop:
        is_column_family_drop_ = true;
        break;

      case kInAtomicGroup:
        is_in_atomic_group_ = true;
        if (!GetVarint32(&input, &remaining_entries_)) {
          if (!msg) {
            msg = "remaining entries";
          }
        }
        break;

      case kFullHistoryTsLow:
        if (!GetLengthPrefixedSlice(&input, &str)) {
          msg = "full_history_ts_low";
        } else if (str.empty()) {
          msg = "full_history_ts_low: empty";
        } else {
          full_history_ts_low_.assign(str.data(), str.size());
        }
        break;

      default:
        if (tag & kTagSafeIgnoreMask) {
          // Tag from future which can be safely ignored.
          // The next field must be the length of the entry.
          uint32_t field_len;
          if (!GetVarint32(&input, &field_len) ||
              static_cast<size_t>(field_len) > input.size()) {
            if (!msg) {
              msg = "safely ignoreable tag length error";
            }
          } else {
            input.remove_prefix(static_cast<size_t>(field_len));
          }
        } else {
          msg = "unknown tag";
        }
        break;
    }
  }

  if (msg == nullptr && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != nullptr) {
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

std::string VersionEdit::DebugString(bool hex_key) const {
  std::string r;
  r.append("VersionEdit {");
  if (has_db_id_) {
    r.append("\n  DB ID: ");
    r.append(db_id_);
  }
  if (has_comparator_) {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  if (has_log_number_) {
    r.append("\n  LogNumber: ");
    AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_) {
    r.append("\n  PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_) {
    r.append("\n  NextFileNumber: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_max_column_family_) {
    r.append("\n  MaxColumnFamily: ");
    AppendNumberTo(&r, max_column_family_);
  }
  if (has_min_log_number_to_keep_) {
    r.append("\n  MinLogNumberToKeep: ");
    AppendNumberTo(&r, min_log_number_to_keep_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  for (const auto& level_and_compact_cursor : compact_cursors_) {
    r.append("\n  CompactCursor: ");
    AppendNumberTo(&r, level_and_compact_cursor.first);
    r.append(" ");
    r.append(level_and_compact_cursor.second.DebugString(hex_key));
  }
  for (const auto& deleted_file : deleted_files_) {
    r.append("\n  DeleteFile: ");
    AppendNumberTo(&r, deleted_file.first);
    r.append(" ");
    AppendNumberTo(&r, deleted_file.second);
  }
  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    r.append("\n  AddFile: ");
    AppendNumberTo(&r, new_files_[i].first);
    r.append(" ");
    AppendNumberTo(&r, f.fd.GetNumber());
    r.append(" ");
    AppendNumberTo(&r, f.fd.GetFileSize());
    r.append(" ");
    r.append(f.smallest.DebugString(hex_key));
    r.append(" .. ");
    r.append(f.largest.DebugString(hex_key));
    if (f.oldest_blob_file_number != kInvalidBlobFileNumber) {
      r.append(" blob_file:");
      AppendNumberTo(&r, f.oldest_blob_file_number);
    }
    r.append(" oldest_ancester_time:");
    AppendNumberTo(&r, f.oldest_ancester_time);
    r.append(" file_creation_time:");
    AppendNumberTo(&r, f.file_creation_time);
    r.append(" epoch_number:");
    AppendNumberTo(&r, f.epoch_number);
    r.append(" file_checksum:");
    r.append(Slice(f.file_checksum).ToString(true));
    r.append(" file_checksum_func_name: ");
    r.append(f.file_checksum_func_name);
    if (f.temperature != Temperature::kUnknown) {
      r.append(" temperature: ");
      // Maybe change to human readable format whenthe feature becomes
      // permanent
      r.append(std::to_string(static_cast<int>(f.temperature)));
    }
    if (f.unique_id != kNullUniqueId64x2) {
      r.append(" unique_id(internal): ");
      UniqueId64x2 id = f.unique_id;
      r.append(InternalUniqueIdToHumanString(&id));
      r.append(" public_unique_id: ");
      InternalUniqueIdToExternal(&id);
      r.append(UniqueIdToHumanString(EncodeUniqueIdBytes(&id)));
    }
  }

  for (const auto& blob_file_addition : blob_file_additions_) {
    r.append("\n  BlobFileAddition: ");
    r.append(blob_file_addition.DebugString());
  }

  for (const auto& blob_file_garbage : blob_file_garbages_) {
    r.append("\n  BlobFileGarbage: ");
    r.append(blob_file_garbage.DebugString());
  }

  for (const auto& wal_addition : wal_additions_) {
    r.append("\n  WalAddition: ");
    r.append(wal_addition.DebugString());
  }

  if (!wal_deletion_.IsEmpty()) {
    r.append("\n  WalDeletion: ");
    r.append(wal_deletion_.DebugString());
  }

  r.append("\n  ColumnFamily: ");
  AppendNumberTo(&r, column_family_);
  if (is_column_family_add_) {
    r.append("\n  ColumnFamilyAdd: ");
    r.append(column_family_name_);
  }
  if (is_column_family_drop_) {
    r.append("\n  ColumnFamilyDrop");
  }
  if (is_in_atomic_group_) {
    r.append("\n  AtomicGroup: ");
    AppendNumberTo(&r, remaining_entries_);
    r.append(" entries remains");
  }
  if (HasFullHistoryTsLow()) {
    r.append("\n FullHistoryTsLow: ");
    r.append(Slice(full_history_ts_low_).ToString(hex_key));
  }
  r.append("\n}\n");
  return r;
}

std::string VersionEdit::DebugJSON(int edit_num, bool hex_key) const {
  JSONWriter jw;
  jw << "EditNumber" << edit_num;

  if (has_db_id_) {
    jw << "DB ID" << db_id_;
  }
  if (has_comparator_) {
    jw << "Comparator" << comparator_;
  }
  if (has_log_number_) {
    jw << "LogNumber" << log_number_;
  }
  if (has_prev_log_number_) {
    jw << "PrevLogNumber" << prev_log_number_;
  }
  if (has_next_file_number_) {
    jw << "NextFileNumber" << next_file_number_;
  }
  if (has_max_column_family_) {
    jw << "MaxColumnFamily" << max_column_family_;
  }
  if (has_min_log_number_to_keep_) {
    jw << "MinLogNumberToKeep" << min_log_number_to_keep_;
  }
  if (has_last_sequence_) {
    jw << "LastSeq" << last_sequence_;
  }

  if (!deleted_files_.empty()) {
    jw << "DeletedFiles";
    jw.StartArray();

    for (const auto& deleted_file : deleted_files_) {
      jw.StartArrayedObject();
      jw << "Level" << deleted_file.first;
      jw << "FileNumber" << deleted_file.second;
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!new_files_.empty()) {
    jw << "AddedFiles";
    jw.StartArray();

    for (size_t i = 0; i < new_files_.size(); i++) {
      jw.StartArrayedObject();
      jw << "Level" << new_files_[i].first;
      const FileMetaData& f = new_files_[i].second;
      jw << "FileNumber" << f.fd.GetNumber();
      jw << "FileSize" << f.fd.GetFileSize();
      jw << "SmallestIKey" << f.smallest.DebugString(hex_key);
      jw << "LargestIKey" << f.largest.DebugString(hex_key);
      jw << "OldestAncesterTime" << f.oldest_ancester_time;
      jw << "FileCreationTime" << f.file_creation_time;
      jw << "EpochNumber" << f.epoch_number;
      jw << "FileChecksum" << Slice(f.file_checksum).ToString(true);
      jw << "FileChecksumFuncName" << f.file_checksum_func_name;
      if (f.temperature != Temperature::kUnknown) {
        jw << "temperature" << std::to_string(static_cast<int>(f.temperature));
      }
      if (f.oldest_blob_file_number != kInvalidBlobFileNumber) {
        jw << "OldestBlobFile" << f.oldest_blob_file_number;
      }
      if (f.temperature != Temperature::kUnknown) {
        // Maybe change to human readable format whenthe feature becomes
        // permanent
        jw << "Temperature" << static_cast<int>(f.temperature);
      }
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!blob_file_additions_.empty()) {
    jw << "BlobFileAdditions";

    jw.StartArray();

    for (const auto& blob_file_addition : blob_file_additions_) {
      jw.StartArrayedObject();
      jw << blob_file_addition;
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!blob_file_garbages_.empty()) {
    jw << "BlobFileGarbages";

    jw.StartArray();

    for (const auto& blob_file_garbage : blob_file_garbages_) {
      jw.StartArrayedObject();
      jw << blob_file_garbage;
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!wal_additions_.empty()) {
    jw << "WalAdditions";

    jw.StartArray();

    for (const auto& wal_addition : wal_additions_) {
      jw.StartArrayedObject();
      jw << wal_addition;
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!wal_deletion_.IsEmpty()) {
    jw << "WalDeletion";
    jw.StartObject();
    jw << wal_deletion_;
    jw.EndObject();
  }

  jw << "ColumnFamily" << column_family_;

  if (is_column_family_add_) {
    jw << "ColumnFamilyAdd" << column_family_name_;
  }
  if (is_column_family_drop_) {
    jw << "ColumnFamilyDrop" << column_family_name_;
  }
  if (is_in_atomic_group_) {
    jw << "AtomicGroup" << remaining_entries_;
  }

  if (HasFullHistoryTsLow()) {
    jw << "FullHistoryTsLow" << Slice(full_history_ts_low_).ToString(hex_key);
  }

  jw.EndObject();

  return jw.Get();
}

}  // namespace ROCKSDB_NAMESPACE
