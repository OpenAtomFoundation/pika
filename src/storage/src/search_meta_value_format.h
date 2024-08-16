#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "include/pika_search_encoding.h"
#include "rocksdb/slice.h"
#include "src/base_value_format.h"
#include "src/coding.h"
#include "storage/storage_define.h"

namespace storage {

static const uint64_t kDefaultSearchMetaValueLength = sizeof(DataType) + sizeof(search::VectorType) + sizeof(DataType) +
                                                      sizeof(uint16_t) + sizeof(search::DistanceMetric) +
                                                      3 * sizeof(uint32_t) + sizeof(double) + sizeof(uint16_t);

/*
 *| vector_type | stored_data_type | dim | distance | capacity | ef_construction |  ef_runtime | epislon | num_level
 *|  1B  |     1B    |    2B   |     1B     |      4B     |   4B   |    4B  |     8B    | 2B
 */
class SearchMetaValue {
 public:
  explicit SearchMetaValue() : type_(DataType::kSearch) {}
  explicit SearchMetaValue(DataType type, search::VectorType vector_type, DataType stored_data_type, uint16_t dim,
                           search::DistanceMetric distance_metric, uint32_t capacity, uint32_t ef_construction,
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

  rocksdb::Slice Encode() {
    value_.resize(kDefaultSearchMetaValueLength);
    char* dst = &value_[0];

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
    dst += 4;
    EncodeFixed16(dst, num_level_);
    dst += 2;
    return Slice(value_);
  }

  void Decode(std::string& value) {
    value_ = std::move(value);
    assert(value_.size() == kDefaultSearchMetaValueLength);
    if (value_.size() != kDefaultSearchMetaValueLength) {
      LOG(ERROR) << "Invalid search meta value length: " << value_.size()
                 << " expected: " << kDefaultSearchMetaValueLength;
      return;
    }

    size_t offset = 0;
    type_ = static_cast<DataType>(static_cast<uint8_t>(value_[0]));
    offset += kTypeLength;
    vector_type_ = static_cast<search::VectorType>(DecodeFixed8(value_.data() + offset));
    offset += 1;
    stored_data_type_ = static_cast<DataType>(DecodeFixed8(value_.data() + offset));
    offset += 1;
    dim_ = DecodeFixed16(value_.data() + offset);
    offset += 2;
    capacity_ = DecodeFixed32(value_.data() + offset);
    offset += 4;
    ef_construction_ = DecodeFixed32(value_.data() + offset);
    offset += 4;
    ef_runtime_ = DecodeFixed32(value_.data() + offset);
    offset += 4;
    epislon_ = DecodeDoubleFromUInt64(DecodeFixed64(value_.data() + offset));
    offset += 4;
    num_level_ = DecodeFixed16(value_.data() + offset);
  }

  uint16_t Dim() const { return dim_; }

  search::DistanceMetric DistanceMetric() const { return distance_metric_; }

  uint32_t Capacity() const { return capacity_; }

  uint32_t EFConstruction() const { return ef_construction_; }

  uint32_t EFRuntime() const { return ef_runtime_; }

  double Epislon() const { return epislon_; }

  uint16_t NumLevel() const { return num_level_; }

  void SetVectorType(search::VectorType type) {
    assert(value_.size == kDefaultSearchMetaValueLength);
    vector_type_ = type;
    char* dst = const_cast<char*>(value_.data() + sizeof(DataType));
    EncodeFixed8(dst, static_cast<uint8_t>(vector_type_));
  }

  void SetStoredDataType(DataType type) {
    assert(value_.size == kDefaultSearchMetaValueLength);
    stored_data_type_ = type;
    char* dst = const_cast<char*>(value_.data() + sizeof(DataType) + sizeof(DataType));
    EncodeFixed8(dst, static_cast<uint8_t>(stored_data_type_));
  }

  void SetDim(uint16_t dim) {
    assert(value_.size == kDefaultSearchMetaValueLength);
    dim_ = dim;
    char* dst = const_cast<char*>(value_.data() + sizeof(DataType) + sizeof(DataType));
    EncodeFixed16(dst, dim_);
  }

  void SetDistanceMetric(search::DistanceMetric metric) {
    assert(value_.size == kDefaultSearchMetaValueLength);
    distance_metric_ = metric;
    char* dst = const_cast<char*>(value_.data() + sizeof(DataType) + sizeof(DataType) + sizeof(uint16_t));
    EncodeFixed8(dst, static_cast<uint8_t>(distance_metric_));
  }

  void SetCapacity(uint32_t capacity) {
    assert(value_.size == kDefaultSearchMetaValueLength);
    capacity_ = capacity;
    char* dst = const_cast<char*>(value_.data() + sizeof(DataType) + sizeof(DataType) + sizeof(uint16_t) +
                                  sizeof(search::DistanceMetric));
    EncodeFixed32(dst, capacity_);
  }

  void SetEfConstruction(uint32_t ef_construction) {
    assert(value_.size == kDefaultSearchMetaValueLength);
    ef_construction_ = ef_construction;
    char* dst = const_cast<char*>(value_.data() + sizeof(DataType) + sizeof(DataType) + sizeof(uint16_t) +
                                  sizeof(search::DistanceMetric));
    EncodeFixed32(dst, ef_construction_);
  }

  void SetEfRuntime(uint32_t ef_runtime) {
    assert(value_.size == kDefaultSearchMetaValueLength);
    ef_runtime_ = ef_runtime;
    char* dst = const_cast<char*>(value_.data() + sizeof(DataType) + sizeof(DataType) + sizeof(uint16_t) +
                                  sizeof(search::DistanceMetric) + sizeof(uint32_t));
    EncodeFixed32(dst, ef_runtime_);
  }

  void SetEfEpislon(double epislon) {
    assert(value_.size == kDefaultSearchMetaValueLength);
    epislon_ = epislon;
    char* dst = const_cast<char*>(value_.data() + sizeof(DataType) + sizeof(DataType) + sizeof(uint16_t) +
                                  sizeof(search::DistanceMetric) + 2 * sizeof(uint32_t));
    EncodeFixed64(dst, EncodeDoubleToUInt64(epislon));
  }

  void SetNumLevel(uint16_t num_level) {
    assert(value_.size == kDefaultSearchMetaValueLength);
    num_level_ = num_level;
    char* dst = const_cast<char*>(value_.data() + sizeof(DataType) + sizeof(DataType) + sizeof(uint16_t) +
                                  sizeof(search::DistanceMetric) + 2 * sizeof(uint32_t) + sizeof(uint64_t));
    EncodeFixed64(dst, num_level_);
  }

 private:
  DataType type_;
  search::VectorType vector_type_;
  DataType stored_data_type_;
  uint16_t dim_;
  search::DistanceMetric distance_metric_;
  uint32_t capacity_ = 500000;
  uint32_t ef_construction_ = 200;
  uint32_t ef_runtime_ = 10;
  double epislon_ = 0.01;
  uint16_t num_level_ = 0;
  uint64_t version_ = 0;
  std::string value_;
};

class ParsedSearchMetaValue {
 public:
  explicit ParsedSearchMetaValue(std::string* internal_value_str) {
    assert(internal_value_str->size() >= kSearchMetaValueSuffixLength);
    if (internal_value_str->size() >= kSearchMetaValueSuffixLength) {
      size_t offset = 0;
      offset += kTypeLength;
      vector_type_ = static_cast<search::VectorType>(DecodeFixed8(internal_value_str->data() + offset));
      offset += 1;
      stored_data_type_ = static_cast<DataType>(DecodeFixed8(internal_value_str->data() + offset));
      offset += 1;
      dim_ = DecodeFixed16(internal_value_str->data() + offset);
      offset += 2;
      capacity_ = DecodeFixed32(internal_value_str->data() + offset);
      offset += 4;
      ef_construction_ = DecodeFixed32(internal_value_str->data() + offset);
      offset += 4;
      ef_runtime_ = DecodeFixed32(internal_value_str->data() + offset);
      offset += 4;
      epislon_ = DecodeDoubleFromUInt64(DecodeFixed64(internal_value_str->data() + offset));
      offset += 4;
      num_level_ = DecodeFixed16(internal_value_str->data() + offset);
    }
  }

  explicit ParsedSearchMetaValue(const rocksdb::Slice& internal_value_slice) {
    assert(internal_value_str.size() >= kSearchMetaValueSuffixLength);
    if (internal_value_slice.size() >= kSearchMetaValueSuffixLength) {
      size_t offset = 0;
      offset += kVersionLength;
      vector_type_ = static_cast<search::VectorType>(DecodeFixed8(internal_value_slice.data() + offset));
      offset += 1;
      stored_data_type_ = static_cast<DataType>(DecodeFixed8(internal_value_slice.data() + offset));
      offset += 1;
      dim_ = DecodeFixed16(internal_value_slice.data() + offset);
      offset += 2;
      capacity_ = DecodeFixed32(internal_value_slice.data() + offset);
      offset += 4;
      ef_construction_ = DecodeFixed32(internal_value_slice.data() + offset);
      offset += 4;
      ef_runtime_ = DecodeFixed32(internal_value_slice.data() + offset);
      offset += 4;
      epislon_ = DecodeDoubleFromUInt64(DecodeFixed64(internal_value_slice.data() + offset));
      offset += 4;
      num_level_ = DecodeFixed16(internal_value_slice.data() + offset);
    }
  }

  // seter and geter for members

 private:
  const size_t kSearchMetaValueSuffixLength =
      kVersionLength + kSearchValueIndexLength + kSuffixReserveLength + 2 * kTimestampLength;
  search::VectorType vector_type_;
  DataType stored_data_type_;
  uint16_t dim_;
  search::DistanceMetric distance_metric_;
  uint32_t capacity_ = 500000;
  uint32_t ef_construction_ = 200;
  uint32_t ef_runtime_ = 10;
  double epislon_ = 0.01;
  uint16_t num_level_ = 0;
};

}  // namespace storage