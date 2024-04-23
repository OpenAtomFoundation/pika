// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef __PSTD_INCLUDE_BASE_CONF_H__
#define __PSTD_INCLUDE_BASE_CONF_H__

#include <atomic>

#include <cstdio>
#include <cstdlib>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "pstd/include/pstd_define.h"

namespace pstd {

class BaseConf {
 public:
  struct Rep {
    std::string path;
    enum ConfType {
      kConf = 0,
      kComment = 1,
    };

    struct ConfItem {
      ConfType type;  // 0 means conf, 1 means comment
      std::string name;
      std::string value;
      ConfItem(ConfType t, std::string v) : type(t), value(std::move(v)) {}
      ConfItem(ConfType t, std::string n, std::string v) : type(t), name(std::move(n)), value(std::move(v)) {}
    };

    explicit Rep(std::string p) : path(std::move(p)) {}
    std::vector<ConfItem> item;
  };

  explicit BaseConf(const std::string& path);
  virtual ~BaseConf();

  int LoadConf();
  int32_t ReloadConf();

  // return false if the item dosen't exist
  template<typename T>
  bool GetConfInt(const std::string& name, T* value) const{
    for (auto& i : rep_->item) {
      if (i.type == Rep::kComment) {
        continue;
      }
      if (name == i.name) {
        (*value) = atoi(i.value.c_str());
        return true;
      }
    }
    return false;
  }

  template<typename T>
  bool GetConfIntHuman(const std::string& name, T* value) const {
    for (auto& i : rep_->item) {
      if (i.type == Rep::kComment) {
        continue;
      }
      if (name == i.name) {
        auto c_str = i.value.c_str();
        (*value) = static_cast<int32_t>(strtoll(c_str, nullptr, 10));
        char last = c_str[i.value.size() - 1];
        if (last == 'K' || last == 'k') {
          (*value) = (*value) * (1 << 10);
        } else if (last == 'M' || last == 'm') {
          (*value) = (*value) * (1 << 20);
        } else if (last == 'G' || last == 'g') {
          (*value) = (*value) * (1 << 30);
        }
        return true;
      }
    }
    return false;
  }

  template<typename T>
  bool GetConfInt64(const std::string& name, T* value) const {
    for (auto& i : rep_->item) {
      if (i.type == Rep::kComment) {
        continue;
      }
      if (name == i.name) {
        auto c_str = i.value.c_str();
        (*value) = strtoll(c_str, nullptr, 10);
        char last = c_str[i.value.size() - 1];
        if (last == 'K' || last == 'k') {
          (*value) = (*value) * (1 << 10);
        } else if (last == 'M' || last == 'm') {
          (*value) = (*value) * (1 << 20);
        } else if (last == 'G' || last == 'g') {
          (*value) = (*value) * (1 << 30);
        }
        return true;
      }
    }
    return false;
  }

  template<typename T> 
  bool GetConfInt64Human(const std::string& name, T* value) const {
    for (auto& i : rep_->item) {
      if (i.type == Rep::kComment) {
        continue;
      }
      if (name == i.name) {
        (*value) = strtoll(i.value.c_str(), nullptr, 10);
        return true;
      }
    }
    return false;
  }

  template<typename T>
  bool GetConfBool(const std::string& name, T* value) const {
    for (auto& i : rep_->item) {
      if (i.type == Rep::kComment) {
        continue;
      }
      if (name == i.name) {
        if (i.value == "true" || i.value == "1" || i.value == "yes") {
          (*value) = true;
        } else if (i.value == "false" || i.value == "0" || i.value == "no") {
          (*value) = false;
        }
        return true;
      }
    }
    return false;
  }

  bool GetConfStr(const std::string& name, std::string* value) const;
  bool GetConfStrVec(const std::string& name, std::vector<std::string>* value) const;
  bool GetConfDouble(const std::string& name, double* value) const;
  bool GetConfStrMulti(const std::string& name, std::vector<std::string>* values) const;

  bool SetConfInt(const std::string& name, int value);
  bool SetConfInt64(const std::string& name, int64_t value);

  bool SetConfStr(const std::string& name, const std::string& value);
  bool SetConfBool(const std::string& name, bool value);
  bool SetConfStrVec(const std::string& name, const std::vector<std::string>& value);
  bool SetConfDouble(const std::string& name, double value);

  bool CheckConfExist(const std::string& name) const;

  void DumpConf() const;
  bool WriteBack();
  void WriteSampleConf() const;

  void PushConfItem(const Rep::ConfItem& item);

 private:
  std::unique_ptr<Rep> rep_;
};

}  // namespace pstd

#endif  // __PSTD_INCLUDE_BASE_CONF_H__
