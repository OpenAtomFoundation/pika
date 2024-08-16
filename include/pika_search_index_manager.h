#pragma once

#include "storage/src/coding.h"
#include "pika_search_index_info.h"
#include "pika_search_indexer.h"
#include "storage/storage.h"

namespace search {

struct IndexManager {
  IndexMap index_map;
  GlobalIndexer *indexer;

  IndexManager(GlobalIndexer *indexer) : indexer(indexer){}

};

}  // namespace redis
