#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

#include "storage/src/coding.h"

namespace search {

enum class IndexOnDataType : uint8_t { HASH, JSON };

inline constexpr auto kErrorInsufficientLength = "insufficient length while decoding metadata";
inline constexpr auto kErrorIncorrectLength = "length is too short or too long to be parsed as a vector";

enum class IndexFieldType : uint8_t {
  TAG = 1,

  NUMERIC = 2,

  VECTOR = 3,
};

struct IndexFieldMetadata {
  bool noindex = false;
  IndexFieldType type;

  explicit IndexFieldMetadata(IndexFieldType type) : type(type) {}

  // flag: <noindex: 1 bit> <type: 4 bit> <reserved: 3 bit>
  uint8_t MakeFlag() const { return noindex | (uint8_t)type << 1; }

  void DecodeFlag(uint8_t flag) {
    noindex = flag & 1;
    type = DecodeType(flag);
  }

  static IndexFieldType DecodeType(uint8_t flag) { return IndexFieldType(flag >> 1); }

  virtual ~IndexFieldMetadata() = default;

  std::string_view Type() const {
    switch (type) {
      case IndexFieldType::TAG:
        return "tag";
      case IndexFieldType::NUMERIC:
        return "numeric";
      case IndexFieldType::VECTOR:
        return "vector";
      default:
        return "unknown";
    }
  }

  virtual void Encode(std::string *dst) const { storage::PutFixed8(dst, MakeFlag()); }

  virtual rocksdb::Status Decode(rocksdb::Slice *input) {
    uint8_t flag = 0;
    if (!storage::GetFixed8(input, &flag)) {
      return rocksdb::Status::Corruption(kErrorInsufficientLength);
    }

    DecodeFlag(flag);
    return rocksdb::Status::OK();
  }

  virtual bool IsSortable() const { return false; }

  static inline rocksdb::Status Decode(rocksdb::Slice *input, std::unique_ptr<IndexFieldMetadata> &ptr);
};

enum class SearchSubkeyType : uint8_t {
  INDEX_META = 0,

  PREFIXES = 1,

  // field metadata
  FIELD_META = 2,

  // field indexing data
  FIELD = 3,

  // field alias
  FIELD_ALIAS = 4,
};

enum class VectorType : uint8_t {
  FLOAT64 = 1,
};

enum class DistanceMetric : uint8_t {
  L2 = 0,
  IP = 1,
  COSINE = 2,
};

enum class HnswLevelType : uint8_t {
  NODE = 1,
  EDGE = 2,
};

static void PutHnswLevelType(std::string *dst, HnswLevelType type) { storage::PutFixed8(dst, uint8_t(type)); }

void PutHnswLevelPrefix(std::string *dst, uint16_t level) { storage::PutFixed16(dst, level); }

void PutHnswLevelNodePrefix(std::string *dst, uint16_t level) {
  PutHnswLevelPrefix(dst, level);
  PutHnswLevelType(dst, HnswLevelType::NODE);
}

void PutHnswLevelEdgePrefix(std::string *dst, uint16_t level) {
  PutHnswLevelPrefix(dst, level);
  PutHnswLevelType(dst, HnswLevelType::EDGE);
}

std::string ConstructHnswLevelNodePrefix(uint16_t level) {
  std::string dst;
  PutHnswLevelNodePrefix(&dst, level);
  return dst;
}

std::string ConstructHnswNode(uint16_t level, std::string_view key) {
  std::string dst;
  PutHnswLevelNodePrefix(&dst, level);
  storage::PutSizedString(&dst, key);
  return dst;
}

std::string ConstructHnswEdgeWithSingleEnd(uint16_t level, std::string_view key) {
  std::string dst;
  PutHnswLevelEdgePrefix(&dst, level);
  storage::PutSizedString(&dst, key);
  return dst;
}

std::string ConstructHnswEdge(uint16_t level, std::string_view key1, std::string_view key2) {
  std::string dst;
  PutHnswLevelEdgePrefix(&dst, level);
  storage::PutSizedString(&dst, key1);
  storage::PutSizedString(&dst, key2);
  return dst;
}

struct HnswNodeFieldMetadata {
  uint16_t num_neighbours;
  std::vector<double> vector;

  HnswNodeFieldMetadata() = default;
  HnswNodeFieldMetadata(uint16_t num_neighbours, uint16_t dim, std::vector<double> vector)
      : num_neighbours(num_neighbours), vector(std::move(vector)) {}

  void Encode(std::string *dst) const {
    storage::PutFixed16(dst, num_neighbours);
    storage::PutFixed16(dst, static_cast<uint16_t>(vector.size()));
    for (double element : vector) {
      storage::PutDouble(dst, element);
    }
  }

  rocksdb::Status Decode(rocksdb::Slice *input) {
    if (input->size() < 2 + 2) {
      return rocksdb::Status::Corruption(kErrorInsufficientLength);
    }
    storage::GetFixed16(input, (uint16_t *)(&num_neighbours));

    uint16_t dim = 0;
    storage::GetFixed16(input, (uint16_t *)(&dim));

    if (input->size() != dim * sizeof(double)) {
      return rocksdb::Status::Corruption(kErrorIncorrectLength);
    }
    vector.resize(dim);

    for (auto i = 0; i < dim; ++i) {
      storage::GetDouble(input, &vector[i]);
    }
    return rocksdb::Status::OK();
  }
};

}  // namespace search