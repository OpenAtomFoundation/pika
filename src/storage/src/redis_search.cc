#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "src/base_data_key_format.h"
#include "src/base_data_value_format.h"
#include "src/base_key_format.h"
#include "src/base_value_format.h"
#include "src/redis.h"
#include "src/search_meta_value_format.h"
#include "storage/storage.h"
#include "storage/storage_define.h"

namespace storage {

Status Redis::PutHnswIndexMetaData(const rocksdb::Slice& index_key, SearchMetaValue& meta_value) {
  BaseMetaKey meta_key(index_key);
  Status s = db_->Put(default_write_options_, meta_key.Encode(), meta_value.Encode());
  return s;
}

// node_inde_key should be constructed from pika search part
Status Redis::GetHnswIndexMetaData(const rocksdb::Slice& index_key, SearchMetaValue& meta_value) {
  rocksdb::ReadOptions read_options;
  std::string meta_value_str;

  BaseMetaKey meta_key(index_key);
  Status s = db_->Get(read_options, handles_[kMetaCF], meta_key.Encode(), &meta_value_str);
  if (s.ok() && !ExpectedMetaValue(DataType::kSearch, meta_value_str)) {
    return Status::NotFound();
  }
  if (s.ok()) {
    meta_value.Decode(meta_value_str);
    return Status::OK();
  }

  return s;
}

Status Redis::PutHnswNode(std::string& node_key, search::HnswNodeFieldMetadata& node_meta) {
  SearchDataKey key(node_key, version, field);
  std::string meta_str;
  node_meta.Encode(&meta_str);
  BaseDataValue value(meta_str);
  Status s = db_->Put(default_write_options_, handles_[kSearchDataCF], node_key, value.Encode());
  return s;
}


Status Redis::GetHnswNode(std::string& node_key, search::HnswNodeFieldMetadata& node_meta) {
  rocksdb::PinnableSlice value;
  Status s = db_->Get(default_read_options_, handles_[kSearchDataCF], node_key, &value);

  if (s.ok()) {
    return node_meta.Decode(&value);
  }

  return s;
}

Status Redis::AddHnswEdge(std::string& edge_key) {
  Status s = db_->Put(default_write_options_, edge_key, Slice());
  return s;
}

Status Redis::RemoveHnswEdge(std::string& edge_key) {
  Status s = db_->Put(default_write_options_, edge_key, Slice());
  return s;
}

}  // namespace storage