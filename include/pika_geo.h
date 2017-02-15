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

 struct GeoPoint {
  std::string name;
  double longitude;
  double latitude;
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
  std::vector<std::string> name_;
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
  std::vector<std::string> name_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

#endif
