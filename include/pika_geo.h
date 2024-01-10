// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_GEO_H_
#define PIKA_GEO_H_

#include "include/acl.h"
#include "include/pika_command.h"
#include "include/pika_slot.h"

/*
 * zset
 */
enum Sort {
  Unsort,  // default
  Asc,
  Desc
};

struct GeoPoint {
  std::string member;
  double longitude;
  double latitude;
};

struct NeighborPoint {
  std::string member;
  double score;
  double distance;
};

struct GeoRange {
  std::string member;
  double longitude;
  double latitude;
  double distance;
  std::string unit;
  bool withdist;
  bool withhash;
  bool withcoord;
  int option_num;
  bool count;
  int count_limit;
  bool store;
  bool storedist;
  std::string storekey;
  Sort sort;
};

class GeoAddCmd : public Cmd {
 public:
  GeoAddCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::GEO)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new GeoAddCmd(*this); }

 private:
  std::string key_;
  std::vector<GeoPoint> pos_;
  void DoInitial() override;
};

class GeoPosCmd : public Cmd {
 public:
  GeoPosCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::GEO)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new GeoPosCmd(*this); }

 private:
  std::string key_;
  std::vector<std::string> members_;
  void DoInitial() override;
};

class GeoDistCmd : public Cmd {
 public:
  GeoDistCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::GEO)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new GeoDistCmd(*this); }

 private:
  std::string key_, first_pos_, second_pos_, unit_;
  void DoInitial() override;
};

class GeoHashCmd : public Cmd {
 public:
  GeoHashCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::GEO)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override{};
  Cmd* Clone() override { return new GeoHashCmd(*this); }

 private:
  std::string key_;
  std::vector<std::string> members_;
  void DoInitial() override;
};

class GeoRadiusCmd : public Cmd {
 public:
  GeoRadiusCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::GEO)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new GeoRadiusCmd(*this); }

 private:
  std::string key_;
  GeoRange range_;
  void DoInitial() override;
  void Clear() override {
    range_.withdist = false;
    range_.withcoord = false;
    range_.withhash = false;
    range_.count = false;
    range_.store = false;
    range_.storedist = false;
    range_.option_num = 0;
    range_.count_limit = 0;
    range_.sort = Unsort;
  }
};

class GeoRadiusByMemberCmd : public Cmd {
 public:
  GeoRadiusByMemberCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::GEO)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new GeoRadiusByMemberCmd(*this); }

 private:
  std::string key_;
  GeoRange range_;
  void DoInitial() override;
  void Clear() override {
    range_.withdist = false;
    range_.withcoord = false;
    range_.withhash = false;
    range_.count = false;
    range_.store = false;
    range_.storedist = false;
    range_.option_num = 0;
    range_.count_limit = 0;
    range_.sort = Unsort;
  }
};

#endif
