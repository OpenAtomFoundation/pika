// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <sstream>
#include "slash_string.h"
#include "nemo.h"
#include "pika_geo.h"
#include "pika_server.h"
#include "pika_slot.h"
#include "pika_geohash_helper.h"

extern PikaServer *g_pika_server;

void GeoAddCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameGeoAdd);
    return;
  }
  size_t argc = argv.size();
  if ((argc - 2) % 3 != 0) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  key_ = argv[1];
  pos_.clear();
  size_t index = 2;
  for (; index < argc; index += 3) {
  	struct GeoPoint point;
  	double longitude, latitude;
    if (!slash::string2d(argv[index].data(), argv[index].size(), &longitude)) {
      res_.SetRes(CmdRes::kInvalidFloat);
      return;
    }
    if (!slash::string2d(argv[index+1].data(), argv[index+1].size(), &latitude)) {
      res_.SetRes(CmdRes::kInvalidFloat);
      return;
    }
    point.name = argv[index+2];
    point.longitude = longitude;
    point.latitude = latitude;
    pos_.push_back(point);
  }
  return;
}

void GeoAddCmd::Do() {
  nemo::Status s;
  int64_t response = 0, ret;
  bool exist = false;
  const std::shared_ptr<nemo::Nemo> db = g_pika_server->db();
  std::vector<GeoPoint>::const_iterator iter = pos_.begin();
  for (; iter != pos_.end(); iter++) {
  	// Convert coordinates to geohash
  	GeoHashBits hash;
    geohashEncodeWGS84(iter->longitude, iter->latitude, GEO_STEP_MAX, &hash);
    GeoHashFix52Bits bits = geohashAlign52Bits(hash);
  	// Convert uint64 to double
  	std::string str_bits = std::to_string(bits);
  	double previous_score, score;
  	slash::string2d(str_bits.data(), str_bits.size(), &score);
  	s = db->ZScore(key_, iter->name, &previous_score);
  	if (s.ok()) {
  	  exist = true;
  	} else if (s.IsNotFound()){
  	  exist = false;
  	}
    s = db->ZAdd(key_, score, iter->name, &ret); 
    if (s.ok() && !exist) {
      response = 1;
    } else if (s.ok() && exist) {
      response = 0;
    } else {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
  }
  SlotKeyAdd("z", key_);
  res_.AppendInteger(response);
  return;
}

void GeoPosCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameGeoPos);
    return;
  }
  key_ = argv[1];
  name_.clear();
  size_t pos = 2;
  while (pos < argv.size()) {
    name_.push_back(argv[pos++]);
  }
}

void GeoPosCmd::Do() {
  double score;
  res_.AppendArrayLen(name_.size());
  for (auto v : name_) {
    nemo::Status s = g_pika_server->db()->ZScore(key_, v, &score);
    if (s.ok()) {
      double xy[2];
      GeoHashBits hash = { .bits = (uint64_t)score, .step = GEO_STEP_MAX };
      geohashDecodeToLongLatWGS84(hash, xy);

      res_.AppendArrayLen(2);
      char longitude[32];
      int64_t len = slash::d2string(longitude, sizeof(longitude), xy[0]);
      res_.AppendStringLen(len);
      res_.AppendContent(longitude);

      char latitude[32];
      len = slash::d2string(latitude, sizeof(latitude), xy[1]);
      res_.AppendStringLen(len);
      res_.AppendContent(latitude);
    
    } else if (s.IsNotFound()) {
      res_.AppendStringLen(-1);
      continue;
    } else {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      continue;	
    }
  }
}

void GeoDistCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameGeoDist);
    return;
  }
  key_ = argv[1];
  first_pos_ = argv[2];
  second_pos_ = argv[3];
  if (argv.size() == 5) {
  	unit_ = argv[4];
  } else if (argv.size() > 5) {
  	res_.SetRes(CmdRes::kSyntaxErr);
  } else {
  	unit_ = "m";
  }
}

void GeoDistCmd::Do() {
  double first_score, second_score, first_xy[2], second_xy[2];
  nemo::Status s = g_pika_server->db()->ZScore(key_, first_pos_, &first_score);
  if (s.ok()) {
    GeoHashBits hash = { .bits = (uint64_t)first_score, .step = GEO_STEP_MAX };
    geohashDecodeToLongLatWGS84(hash, first_xy);
  } else if (s.IsNotFound()) {
    res_.AppendStringLen(-1);
    return;
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;	
  }

  s = g_pika_server->db()->ZScore(key_, second_pos_, &second_score);
  if (s.ok()) {
    GeoHashBits hash = { .bits = (uint64_t)second_score, .step = GEO_STEP_MAX };
    geohashDecodeToLongLatWGS84(hash, second_xy);
  } else if (s.IsNotFound()) {
    res_.AppendStringLen(-1);
    return;
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;	
  }

  double distance = geohashGetDistance(first_xy[0], first_xy[1], second_xy[0], second_xy[1]);
  if (unit_ == "m") {
  	distance = distance;
  } else if (unit_ == "km") {
  	distance = distance / 1000;
  } else if (unit_ == "ft") {
  	distance = distance / 0.3048;
  } else if (unit_ == "mi") {
  	distance = distance / 1609.34;
  } else {
  	res_.SetRes(CmdRes::kErrOther, "unsupported unit provided. please use m, km, ft, mi");
  	return;
  }
  char buf[32];
  int64_t len = slash::d2string(buf, sizeof(buf), distance);
  res_.AppendStringLen(len);
  res_.AppendContent(buf);
}

void GeoHashCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameGeoHash);
    return;
  }
  key_ = argv[1];
  name_.clear();
  size_t pos = 2;
  while (pos < argv.size()) {
    name_.push_back(argv[pos++]);
  }
}

void GeoHashCmd::Do() {
  char * geoalphabet= "0123456789bcdefghjkmnpqrstuvwxyz";
  res_.AppendArrayLen(name_.size());
  for (auto v : name_) {
  	double score;
  	nemo::Status s = g_pika_server->db()->ZScore(key_, v, &score);
    if (s.ok()) {
      double xy[2];
      GeoHashBits hash = { .bits = (uint64_t)score, .step = GEO_STEP_MAX };
      geohashDecodeToLongLatWGS84(hash, xy);
      GeoHashRange r[2];
      GeoHashBits encode_hash;
      r[0].min = -180;
      r[0].max = 180;
      r[1].min = -90;
      r[1].max = 90;
      geohashEncode(&r[0], &r[1], xy[0], xy[1], 26, &encode_hash);

      char buf[12];
      int i;
      for (i = 0; i < 11; i++) {
      	int idx = (encode_hash.bits >> (52-((i+1)*5))) & 0x1f;
        buf[i] = geoalphabet[idx];
      }
      buf[11] = '\0';
      res_.AppendStringLen(11);
      res_.AppendContent(buf);
      continue;
    } else if (s.IsNotFound()) {
      res_.AppendStringLen(-1);
      continue;
    } else {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      continue;	
    }
  }
}
