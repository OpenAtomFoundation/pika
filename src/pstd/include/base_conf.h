// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef __PSTD_INCLUDE_BASE_CONF_H__
#define __PSTD_INCLUDE_BASE_CONF_H__

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
  bool GetConfInt(const std::string& name, int* value) const;
  bool GetConfIntHuman(const std::string& name, int* value) const;
  bool GetConfInt64(const std::string& name, int64_t* value) const;
  bool GetConfInt64Human(const std::string& name, int64_t* value) const;

  bool GetConfStr(const std::string& name, std::string* value) const;
  bool GetConfBool(const std::string& name, bool* value) const;
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
