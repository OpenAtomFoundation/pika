/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "memory_file.h"
#include "store.h"

namespace pikiwidb {

class PDBSaver {
 public:
  void Save(const char* qdbFile);
  void SaveType(const PObject& obj);
  void SaveKey(const PString& key);
  void SaveObject(const PObject& obj);
  void SaveString(const PString& str);
  void SaveLength(uint64_t len);  // big endian
  void SaveString(int64_t intVal);
  bool SaveLZFString(const PString& str);

  static void SaveDoneHandler(int exit, int signal);

 private:
  void saveDoubleValue(double val);

  void saveList(const PLIST& l);
  void saveset(const PSET& s);
  void saveHash(const PHASH& h);
  void saveZSet(const PZSET& ss);

  OutputMemoryFile qdb_;
};

extern time_t g_lastPDBSave;
extern pid_t g_qdbPid;

class PDBLoader {
 public:
  int Load(const char* filename);

  size_t LoadLength(bool& special);
  PObject LoadSpecialStringObject(size_t specialVal);
  PString LoadString(size_t strLen);
  PString LoadLZFString();

  PString LoadKey();
  PObject LoadObject(int8_t type);

 private:
  PString loadGenericString();
  PObject loadList();
  PObject loadSet();
  PObject loadHash();
  PObject loadZSet();
  double loadDoubleValue();
  PObject loadZipList(int8_t type);
  PObject loadZipList(const PString& zl, int8_t type);
  PObject loadIntset();
  PObject loadQuickList();

  void loadAux();
  void loadResizeDB();

  InputMemoryFile qdb_;
};

}  // namespace pikiwidb

