#pragma once

#include <memory>
#include <string_view>
#include <utility>

#include "include/pika_db.h"
#include "include/pika_search_encoding.h"
#include "include/pika_search_index_info.h"
#include "include/pika_search_value.h"

#include "pstd/include/pstd_status.h"
#include "src/search_meta_value_format.h"
#include "tsl/htrie_map.h"

namespace search {

struct GlobalIndexer;

class FieldValueRetriver {
 private:
  // shared_ptr<DB>->storage()->Hget()...
  // DB from every Cmd

  // doesn't need to use these two data type to store a storage handle
  // just use a enum to specify which type
  // class HashData {
  //  public:
  //   std::shared_ptr<DB> db_;
  //   // HashMetadata metadata;
  //   std::string_view key_;

  //   HashData(std::shared_ptr<DB> db, std::string_view key) : db_(std::move(db)), key_(key) {}
  // };

  // class JsonData {};

  std::shared_ptr<DB> db_;
  std::string_view key_;
  IndexOnDataType type_;
  // metadata

 public:
  FieldValueRetriver(IndexOnDataType type, std::string_view key, std::shared_ptr<DB> db)
      : type_(type), key_(key), db_(std::move(db)) {}

  pstd::Status Retrieve(std::string_view field, const IndexFieldMetadata &field_meta, Value *value);

  pstd::Status ParseFromHash(const std::string &input, const IndexFieldMetadata &field_meta, Value *value);
};

struct IndexUpdater {
  using FieldValues = std::map<std::string, Value>;

  const IndexInfo *info = nullptr;
  GlobalIndexer *indexer = nullptr;

  explicit IndexUpdater(const IndexInfo *info) : info(info) {}

  pstd::Status Record(std::string_view key, FieldValues *field_values) const;
  pstd::Status UpdateIndex(const std::string &field, std::string_view key, const Value &original,
                           const Value &current) const;
  pstd::Status Update(const FieldValues &original, std::string_view key) const;

  pstd::Status Build() const;

  // pstd::Status UpdateTagIndex(std::string_view key, const Value &original, const Value &current,
  //                             const SearchKey &search_key, const TagFieldMetadata *tag) const;
  // pstd::Status UpdateNumericIndex(std::string_view key, const Value &original, const Value &current,
  //                                 const SearchKey &search_key, const NumericFieldMetadata *num) const;
  pstd::Status UpdateHnswVectorIndex(std::string_view key, const Value &original, const Value &current,
                                     const std::string &search_key, storage::SearchMetaValue *vector) const;
};

struct GlobalIndexer {
  using FieldValues = IndexUpdater::FieldValues;
  struct RecordResult {
    IndexUpdater updater;
    std::string key;
    FieldValues fields;
  };

  tsl::htrie_map<char, IndexUpdater> prefix_map;
  std::vector<IndexUpdater> updater_list;

  storage::Storage *storage = nullptr;

  // storage->GetDBByID
  // storage->GetInstancebyKeyString
  explicit GlobalIndexer(storage::Storage *storage) : storage(storage) {}

  void Add(IndexUpdater updater);
  void Remove(const IndexInfo *index);

  pstd::Status Record(std::string_view key, const std::string &ns, RecordResult *result);
  static pstd::Status Update(const RecordResult &original);
};
}  // namespace search