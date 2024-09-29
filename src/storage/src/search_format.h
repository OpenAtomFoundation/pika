#ifndef SRC_SERACH_FORMAT_H_
#define SRC_SERACH_FORMAT_H_

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <vector>
#include "rocksdb/slice.h"
#include "src/base_value_format.h"
#include "src/coding.h"

namespace storage {

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

static const uint64_t kDefaultHnswMetaValueLength = sizeof(DataType) + sizeof(VectorType) + sizeof(DataType) +
                                                    sizeof(uint16_t) + sizeof(DistanceMetric) + 3 * sizeof(uint32_t) +
                                                    sizeof(double) + sizeof(uint16_t);

// clang-format off
/*
 *| type | vector_type | stored_data_type | dim | distance | capacity | ef_construction |  ef_runtime | epislon | *num_level |
 *| 1B   |  1B         |            1B    |  2B |     1B   |      4B  |   4B            |    4B       |   8B    |2B
 */
// clang-format on

// meta kv key
// just index name + field name

// meta kv value
class HnswMetaValue {
 public:
  explicit HnswMetaValue() : type_(DataType::kSearch) {}
  explicit HnswMetaValue(DataType type, VectorType vector_type, DataType stored_data_type, uint16_t dim,
                         DistanceMetric distance_metric, uint32_t capacity, uint32_t ef_construction,
                         uint32_t ef_runtime, double epislon, uint16_t num_level)
      : type_(type),
        vector_type_(vector_type),
        stored_data_type_(stored_data_type),
        dim_(dim),
        distance_metric_(distance_metric),
        capacity_(capacity),
        ef_construction_(ef_construction),
        ef_runtime_(ef_runtime),
        epislon_(epislon),
        num_level_(num_level) {}

  std::string Encode() {
    std::string str;
    str.resize(kDefaultHnswMetaValueLength);
    char* dst = str.data();

    memcpy(dst, &type_, sizeof(type_));
    dst += sizeof(type_);
    EncodeFixed8(dst, static_cast<uint8_t>(vector_type_));
    dst += 1;
    EncodeFixed8(dst, static_cast<uint8_t>(stored_data_type_));
    dst += 1;
    EncodeFixed16(dst, dim_);
    dst += 2;
    EncodeFixed8(dst, static_cast<uint8_t>(distance_metric_));
    dst += 1;
    EncodeFixed32(dst, capacity_);
    dst += 4;
    EncodeFixed32(dst, ef_construction_);
    dst += 4;
    EncodeFixed32(dst, ef_runtime_);
    dst += 4;
    EncodeFixed64(dst, EncodeDoubleToUInt64(epislon_));
    dst += 8;
    EncodeFixed16(dst, num_level_);
    return str;
  }

  void Decode(std::string& value) {
    assert(value.size() >= kDefaultHnswMetaValueLength);

    size_t offset = 0;
    type_ = static_cast<DataType>(static_cast<uint8_t>(value[0]));
    offset += 1;
    vector_type_ = static_cast<VectorType>(DecodeFixed8(value.data() + offset));
    offset += 1;
    stored_data_type_ = static_cast<DataType>(DecodeFixed8(value.data() + offset));
    offset += 1;
    dim_ = DecodeFixed16(value.data() + offset);
    offset += 2;
    distance_metric_ = static_cast<DistanceMetric>(DecodeFixed8(value.data() + offset));
    offset += 1;
    capacity_ = DecodeFixed32(value.data() + offset);
    offset += 4;
    ef_construction_ = DecodeFixed32(value.data() + offset);
    offset += 4;
    ef_runtime_ = DecodeFixed32(value.data() + offset);
    offset += 4;
    epislon_ = DecodeDoubleFromUInt64(DecodeFixed64(value.data() + offset));
    offset += 8;
    num_level_ = DecodeFixed16(value.data() + offset);
  }

  VectorType GetVectorType() { return vector_type_; }
  DataType GetStoredDataType() { return stored_data_type_; }
  uint16_t GetDim() { return dim_; }
  DistanceMetric GetDistanceMetric() { return distance_metric_; }
  uint32_t GetCapacity() { return capacity_; }
  uint32_t GetEfConstruction() { return ef_construction_; }
  uint32_t GetEfRuntime() { return ef_runtime_; }
  double GetEpislon() { return epislon_; }
  uint16_t GetNumLevel() { return num_level_; }

  void SetVectorType(VectorType type) { vector_type_ = type; }
  void SetDim(uint16_t dim) { dim_ = dim; }
  void SetDistanceMetric(DistanceMetric distance_metric) { distance_metric_ = distance_metric; }
  void SetCapacity(uint32_t cap) { capacity_ = cap; }
  void SetEfConstruction(uint32_t efc) { ef_construction_ = efc; }
  void SetEfRuntime(uint32_t efr) { ef_runtime_ = efr; }
  void SetEpislon(double epislon) { epislon_ = epislon; }
  void SetNumLevel(uint16_t num_level) { num_level_ = num_level; }

 private:
  DataType type_;
  VectorType vector_type_;
  DataType stored_data_type_;
  uint16_t dim_;
  DistanceMetric distance_metric_;
  uint32_t capacity_ = 500000;
  uint32_t ef_construction_ = 200;
  uint32_t ef_runtime_ = 10;
  double epislon_ = 0.01;
  uint16_t num_level_ = 0;
};

// only used for reading
// TODO: seems no need for a parsed class
class ParsedHnswMetaValue {
 public:
  ParsedHnswMetaValue(std::string* value) {
    assert(value->size() >= kDefaultHnswMetaValueLength);
    value_ = value;

    size_t offset = 0;
    type_ = static_cast<DataType>(DecodeFixed8(value_->data() + offset));
    offset += 1;
    vector_type_ = static_cast<VectorType>(DecodeFixed8(value_->data() + offset));
    offset += 1;
    stored_data_type_ = static_cast<DataType>(DecodeFixed8(value_->data() + offset));
    offset += 1;
    dim_ = DecodeFixed16(value_->data() + offset);
    offset += 2;
    capacity_ = DecodeFixed32(value_->data() + offset);
    offset += 4;
    ef_construction_ = DecodeFixed32(value_->data() + offset);
    offset += 4;
    ef_runtime_ = DecodeFixed32(value_->data() + offset);
    offset += 4;
    epislon_ = DecodeDoubleFromUInt64(DecodeFixed64(value_->data() + offset));
    offset += 4;
    num_level_ = DecodeFixed16(value_->data() + offset);
  }

  // getters
  VectorType GetVectorType() { return vector_type_; }
  DataType GetStoredDataType() { return stored_data_type_; }
  uint16_t GetDim() { return dim_; }
  DistanceMetric GetDistanceMetric() { return distance_metric_; }
  uint32_t GetCapacity() { return capacity_; }
  uint32_t GetEfConstruction() { return ef_construction_; }
  uint32_t GetEfRuntime() { return ef_runtime_; }
  double GetEpislon() { return epislon_; }
  uint16_t GetNumLevel() { return num_level_; }

  void SetVectorType(VectorType type) {
    vector_type_ = type;
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + 1;
      EncodeFixed8(dst, (uint8_t)vector_type_);
    }
  }

  void SetDim(uint16_t dim) {
    dim_ = dim;
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + 3;
      EncodeFixed16(dst, (uint8_t)dim_);
    }
  }

  void SetDistanceMetric(DistanceMetric distance_metric) {
    distance_metric_ = distance_metric;
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + 5;
      EncodeFixed8(dst, (uint8_t)distance_metric_);
    }
  }

  void SetCapacity(uint32_t cap) {
    capacity_ = cap;
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + 6;
      EncodeFixed32(dst, capacity_);
    }
  }

  void SetEfConstruction(uint32_t efc) {
    ef_construction_ = efc;
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + 10;
      EncodeFixed32(dst, ef_construction_);
    }
  }

  void SetEfRuntime(uint32_t efr) {
    ef_runtime_ = efr;
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + 14;
      EncodeFixed32(dst, ef_runtime_);
    }
  }

  void SetEpislon(double epislon) {
    epislon_ = epislon;
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + 18;
      EncodeFixed64(dst, EncodeDoubleToUInt64(epislon_));
    }
  }

  void SetNumLevel(uint16_t num_level) {
    num_level_ = num_level;
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + 26;
      EncodeFixed16(dst, num_level_);
    }
  }

 private:
  DataType type_;
  VectorType vector_type_;
  DataType stored_data_type_;
  uint16_t dim_;
  DistanceMetric distance_metric_;
  uint32_t capacity_ = 500000;
  uint32_t ef_construction_ = 200;
  uint32_t ef_runtime_ = 10;
  double epislon_ = 0.01;
  uint16_t num_level_ = 0;
  std::string* value_ = nullptr;
};

// data kv value
class HnswNodeMetaData {
 public:
  HnswNodeMetaData() = default;
  HnswNodeMetaData(uint16_t num_neighbours, std::vector<double> vector)
      : num_neighbours(num_neighbours), vector(std::move(vector)) {}

  std::string Encode() const {
    std::string str;
    str.resize(2 + 2 + vector.size() * 8);
    char* dst = str.data();
    EncodeFixed16(dst, num_neighbours);
    dst += 2;
    EncodeFixed16(dst, static_cast<uint16_t>(vector.size()));
    dst += 2;
    for (double element : vector) {
      EncodeFixed64(dst, EncodeDoubleToUInt64(element));
      dst += 8;
    }
    return str;
  }

  rocksdb::Status Decode(std::string& input) {
    char* dst = input.data();
    num_neighbours = DecodeFixed16(dst);
    dst += 2;

    uint16_t dim = 0;
    dim = DecodeFixed16(dst);
    dst += 2;

    vector.resize(dim);

    for (auto i = 0; i < dim; ++i) {
      vector[i] = DecodeDoubleFromUInt64(DecodeFixed64(dst));
      dst += 8;
    }
    return rocksdb::Status::OK();
  }

  uint16_t num_neighbours;
  std::vector<double> vector;
};

inline std::string ConstructHnswLevelNodePrefix(uint16_t level) {
  std::string dst;
  dst.resize(2 + 1);
  EncodeFixed16(dst.data(), level);
  EncodeFixed8(dst.data() + 2, (uint8_t)HnswLevelType::NODE);
  return dst;
}

// data kv key
inline std::string ConstructHnswNode(uint16_t level, std::string& key) {
  std::string dst;
  dst.resize(2 + 1 + 4 + key.size());
  char* offset = dst.data();
  EncodeFixed16(offset, level);
  offset += 2;
  EncodeFixed8(offset, (uint8_t)HnswLevelType::NODE);
  offset += 1;
  EncodeFixed32(offset, key.size());
  offset += 4;
  memcpy(offset, key.data(), key.size());
  return dst;
}

inline std::string ConstructHnswEdgeWithSingleEnd(uint16_t level, std::string& key) {
  std::string dst;
  dst.resize(2 + 1 + 4 + key.size());
  char* offset = dst.data();
  EncodeFixed16(offset, level);
  offset += 2;
  EncodeFixed8(offset, (uint8_t)HnswLevelType::EDGE);
  offset += 1;
  EncodeFixed32(offset, key.size());
  offset += 4;
  memcpy(offset, key.data(), key.size());
  return dst;
}

inline std::string ConstructHnswEdge(uint16_t level, std::string& key1, std::string& key2) {
  std::string dst;
  dst.resize(2 + 1 + 4 + key1.size() + 4 + key2.size());
  char* offset = dst.data();
  EncodeFixed16(offset, level);
  offset += 2;
  EncodeFixed8(offset, (uint8_t)HnswLevelType::EDGE);
  offset += 1;
  EncodeFixed32(offset, key1.size());
  offset += 4;
  memcpy(offset, key1.data(), key1.size());
  offset += key1.size();
  EncodeFixed32(offset, key2.size());
  offset += 4;
  memcpy(offset, key2.data(), key2.size());
  return dst;
}

}  // namespace storage

#endif