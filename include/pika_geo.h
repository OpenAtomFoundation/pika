// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_GEO_H_
#define PIKA_GEO_H_
#include "pika_command.h"
#include "nemo.h"


/*
 * zset
 */
 enum Sort
{
  Unsort,	//default
  Asc,
  Desc
};

 struct GeoPoint {
  std::string member;
  double longitude;
  double latitude;
};

 struct NeighborPoint{
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
  Sort sort;
 };

class GeoAddCmd : public Cmd {
public:
  GeoAddCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::vector<GeoPoint> pos_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class GeoPosCmd : public Cmd {
public:
  GeoPosCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::vector<std::string> member_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class GeoDistCmd : public Cmd {
public:
  GeoDistCmd() {}
  virtual void Do();
private:
  std::string key_, first_pos_, second_pos_, unit_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class GeoHashCmd : public Cmd {
public:
  GeoHashCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::vector<std::string> member_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class GeoRadiusCmd : public Cmd {
public:
  GeoRadiusCmd() {}
  virtual void Do();
private:
  std::string key_;
  GeoRange range_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    range_.withdist = false;
    range_.withcoord = false;
    range_.withhash = false;
    range_.count = false;
   	range_.option_num = 0;
   	range_.count_limit = 0;
  }
};

class GeoRadiusByMemberCmd : public Cmd {
public:
  GeoRadiusByMemberCmd() {}
  virtual void Do();
private:
  std::string key_;
  GeoRange range_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
    range_.withdist = false;
    range_.withcoord = false;
    range_.withhash = false;
    range_.count = false;
    range_.option_num = 0;
    range_.count_limit = 0;
    range_.sort = Unsort;
  }
};

#endif
