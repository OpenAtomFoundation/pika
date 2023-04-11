// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// A checkpoint is an openable snapshot of a database at a point in time.

#ifndef ROCKSDB_LITE

#  include <vector>
#  include "rocksdb/status.h"
#  include "rocksdb/transaction_log.h"

namespace rocksdb {

class DB;

class DBCheckpoint {
 public:
  // Creates a Checkpoint object to be used for creating openable sbapshots
  static Status Create(DB* db, DBCheckpoint** checkpoint_ptr);

  // Builds an openable snapshot of RocksDB on the same disk, which
  // accepts an output directory on the same disk, and under the directory
  // (1) hard-linked SST files pointing to existing live SST files
  // SST files will be copied if output directory is on a different filesystem
  // (2) a copied manifest files and other files
  // The directory should not already exist and will be created by this API.
  // The directory will be an absolute path
  virtual Status CreateCheckpoint(const std::string& checkpoint_dir) = 0;

  virtual Status GetCheckpointFiles(std::vector<std::string>& live_files, VectorLogPtr& live_wal_files,
                                    uint64_t& manifest_file_size, uint64_t& sequence_number) = 0;

  virtual Status CreateCheckpointWithFiles(const std::string& checkpoint_dir, std::vector<std::string>& live_files,
                                           VectorLogPtr& live_wal_files, uint64_t manifest_file_size,
                                           uint64_t sequence_number) = 0;

  virtual ~DBCheckpoint() {}
};

}  // namespace rocksdb
#endif  // !ROCKSDB_LITE
