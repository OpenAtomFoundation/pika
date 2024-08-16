#pragma once

#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "fmt/core.h"
#include "glog/logging.h"
#include "include/pika_search_encoding.h"

namespace search {

template <typename T, typename F>
std::string StringJoin(
    const T &con, F &&f = [](const auto &v) -> decltype(auto) { return v; }, std::string_view sep = ", ") {
  std::string res;
  bool is_first = true;
  for (const auto &v : con) {
    if (is_first) {
      is_first = false;
    } else {
      res += sep;
    }
    res += std::forward<F>(f)(v);
  }
  return res;
}

using Null = std::monostate;

using Numeric = double;  // used for numeric fields

using String = std::string;  // e.g. a single tag

using NumericArray = std::vector<Numeric>;  // used for vector fields
using StringArray = std::vector<String>;    // used for tag fields, e.g. a list for tags

struct Value : std::variant<Null, Numeric, String, StringArray, NumericArray> {
  using Base = std::variant<Null, Numeric, String, StringArray, NumericArray>;

  using Base::Base;

  bool IsNull() const { return Is<Null>(); }

  template <typename T>
  bool Is() const {
    return std::holds_alternative<T>(*this);
  }

  template <typename T>
  bool IsOrNull() const {
    return Is<T>() || IsNull();
  }

  template <typename T>
  const auto &Get() const {
    CHECK(Is<T>());
    return std::get<T>(*this);
  }

  template <typename T>
  auto &Get() {
    CHECK(Is<T>());
    return std::get<T>(*this);
  }

  std::string ToString(const std::string &sep = ",") const {
    if (IsNull()) {
      return "";
    } else if (Is<Numeric>()) {
      return fmt::format("{}", Get<Numeric>());
    } else if (Is<String>()) {
      return Get<String>();
    } else if (Is<StringArray>()) {
      return StringJoin(Get<StringArray>(), [](const auto &v) -> decltype(auto) { return v; }, sep);
    } else if (Is<NumericArray>()) {
      return StringJoin(Get<NumericArray>(), [](const auto &v) -> decltype(auto) { return std::to_string(v); }, sep);
    }

    __builtin_unreachable();
  }

  std::string ToString(HnswNodeFieldMetadata *meta) const {
    if (IsNull()) {
      return "";
    } else if (Is<Numeric>()) {
      return fmt::format("{}", Get<Numeric>());
    } else if (Is<String>()) {
      return Get<String>();
    } else if (Is<NumericArray>()) {
      return StringJoin(Get<NumericArray>(), [](const auto &v) -> decltype(auto) { return std::to_string(v); });
    }

    __builtin_unreachable();
  }
};

template <typename T, typename... Args>
auto MakeValue(Args &&...args) {
  return Value(std::in_place_type<T>, std::forward<Args>(args)...);
}

}  // namespace search
