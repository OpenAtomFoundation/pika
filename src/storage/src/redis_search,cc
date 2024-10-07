#include <cstdint>
#include <vector>
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "src/base_key_format.h"
#include "src/base_value_format.h"
#include "src/batch.h"
#include "src/coding.h"
#include "src/redis.h"
#include "src/search_format.h"
#include "storage/storage_define.h"
namespace storage {

Status Redis::PutHnswIndexMetaData(const rocksdb::Slice& index_key, HnswMetaValue& meta_value) {
  auto meta_value_str = meta_value.Encode();
  Status s = db_->Put(default_write_options_, handles_[kMetaCF], index_key, meta_value_str);
  return s;
}

Status Redis::GetHnswIndexMetaData(const rocksdb::Slice& index_key, HnswMetaValue& meta_value) {
  rocksdb::ReadOptions read_options;
  std::string meta_value_str;

  Status s = db_->Get(read_options, handles_[kMetaCF], index_key, &meta_value_str);
  if (s.ok()) {
    meta_value.Decode(meta_value_str);
    return Status::OK();
  }

  return s;
}

Status Redis::PutHnswNodeMetaData(std::string& node_key, uint16_t level, HnswNodeMetaData& node_meta) {
  std::string meta_str = node_meta.Encode();

  auto key = ConstructHnswNode(level, node_key);
  Status s = db_->Put(default_write_options_, handles_[kSearchDataCF], key, meta_str);
  return s;
}

Status Redis::GetHnswNodeMetaData(std::string& node_key, uint16_t level, HnswNodeMetaData& node_meta) {
  std::string value;
  auto key = ConstructHnswNode(level, node_key);
  rocksdb::ReadOptions read_options;
  Status s = db_->Get(read_options, handles_[kSearchDataCF], key, &value);

  if (s.ok()) {
    return node_meta.Decode(value);
  }

  return s;
}

Status Redis::AddHnswEdge(std::string& edge_key) {
  Status s = db_->Put(default_write_options_, handles_[kSearchDataCF], edge_key, Slice());
  return s;
}

Status Redis::RemoveHnswEdge(std::string& edge_key) {
  Status s = db_->Delete(default_write_options_, handles_[kSearchDataCF], edge_key);
  return s;
}

Status Redis::HnswNodeDecodeNeighbours(std::string& node_key, uint16_t level, std::vector<std::string>& neighbours) {
  neighbours.clear();
  auto edge_prefix = ConstructHnswEdgeWithSingleEnd(level, node_key);
  auto iter = db_->NewIterator(default_read_options_, handles_[kSearchDataCF]);
  for (iter->Seek(edge_prefix); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(edge_prefix)) {
      break;
    }
    auto neightbour_edge = iter->key();
    neightbour_edge.remove_prefix(edge_prefix.size());
    std::string neighbour;
    DecodeSizedString(&neightbour_edge, &neighbour);
    neighbours.push_back(neighbour);
  }
  delete iter;
  return Status::OK();
}

Status Redis::HnswNodeAddNeighbour(std::string& node_key, uint16_t level, std::string& neighbour_key) {
  auto edge_index_key = ConstructHnswEdge(level, node_key, neighbour_key);
  Status s = db_->Put(default_write_options_, handles_[kSearchDataCF], edge_index_key, Slice());
  if (s.ok()) {
    HnswNodeMetaData metadata;
    GetHnswNodeMetaData(node_key, level, metadata);
    metadata.num_neighbours++;
    PutHnswNodeMetaData(node_key, level, metadata);
    return Status::OK();
  }
  return s;
}

Status Redis::HnswNodeRemoveNeighbour(std::string& node_key, uint16_t level, std::string& neighbour_key) {
  auto edge_index_key = ConstructHnswEdge(level, node_key, neighbour_key);
  Status s = db_->Delete(default_write_options_, handles_[kSearchDataCF], edge_index_key);
  if (s.ok()) {
    HnswNodeMetaData metadata;
    GetHnswNodeMetaData(node_key, level, metadata);
    metadata.num_neighbours--;
    PutHnswNodeMetaData(node_key, level, metadata);
    return Status::OK();
  }

  return Status::OK();
}

}  // namespace storage