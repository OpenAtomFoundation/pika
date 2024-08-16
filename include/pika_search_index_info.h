#pragma once

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "pika_search_encoding.h"

namespace search {

struct IndexInfo;

struct FieldInfo {
  std::string name;
  IndexInfo *index = nullptr;
  std::unique_ptr<IndexFieldMetadata> metadata;

  FieldInfo(std::string name, std::unique_ptr<IndexFieldMetadata> &&metadata)
      : name(std::move(name)), metadata(std::move(metadata)) {}

  bool IsSortable() const { return metadata->IsSortable(); }
  bool HasIndex() const { return !metadata->noindex; }

  template <typename T>
  const T *MetadataAs() const {
    return dynamic_cast<const T *>(metadata.get());
  }
};

struct IndexInfo {
  using FieldMap = std::map<std::string, FieldInfo>;

  std::string name;
  FieldMap fields;
  std::vector<std::string> prefixes;

  IndexInfo(std::string name)
      : name(std::move(name)) {}

  void Add(FieldInfo &&field) {
    const auto &name = field.name;
    field.index = this;
    fields.emplace(name, std::move(field));
  }
};

struct IndexMap : std::map<std::string, std::unique_ptr<IndexInfo>> {
  auto Insert(std::unique_ptr<IndexInfo> index_info) {
    // TODO: pika seems not support namespace yet
    // auto key = ComposeNamespaceKey(index_info->ns, index_info->name, false);
    return emplace(index_info->name, std::move(index_info));
  }

  auto Find(const std::string &index) const { return find(index); }
};

}  // namespace search
