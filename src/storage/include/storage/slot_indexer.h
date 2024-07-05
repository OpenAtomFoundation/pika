//  Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef __SLOT_INDEXER_H__
#define __SLOT_INDEXER_H__

#include <stdint.h>
#include <vector>

namespace storage {
// Manage slots to rocksdb indexes
// TODO(wangshaoyi): temporarily mock return
class SlotIndexer {
public:
  SlotIndexer() = delete;
  SlotIndexer(uint32_t inst_num) : inst_num_(inst_num) {}
  ~SlotIndexer() {}
  uint32_t GetInstanceID(uint32_t slot_id) {return slot_id % inst_num_; }
  void ReshardSlots(const std::vector<uint32_t>& slots) {}

private:
  uint32_t inst_num_ = 3;
};
} // namespace storage end

#endif
