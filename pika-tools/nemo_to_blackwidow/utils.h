//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_UTILS_H_
#define INCLUDE_UTILS_H_

void EncodeKeyValue(const std::string& key, const std::string& value, std::string* dst);
void DecodeKeyValue(const std::string& dst, std::string* key, std::string* value);

#endif  //  INCLUDE_UILTS_H_
