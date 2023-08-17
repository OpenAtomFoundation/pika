/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */


#include "db.h"
#include <arpa/inet.h>
#include <math.h>
#include <unistd.h>
#include <sstream>
#include "log.h"

extern "C" {
#include "lzf/lzf.h"
#include "redis_intset.h"
#include "redis_zip_list.h"
}

extern "C" uint64_t crc64(uint64_t crc, const unsigned char* s, uint64_t l);

namespace pikiwidb {

time_t g_lastPDBSave = 0;
pid_t g_qdbPid = -1;

// encoding
static const int8_t kTypeString = 0;
static const int8_t kTypeList = 1;
static const int8_t kTypeSet = 2;
static const int8_t kTypeZSet = 3;
static const int8_t kTypeHash = 4;

static const int8_t kTypeZipMap = 9;
static const int8_t kTypeZipList = 10;
static const int8_t kTypeIntSet = 11;
static const int8_t kTypeZSetZipList = 12;
static const int8_t kTypeHashZipList = 13;
static const int8_t kTypeQuickList = 14;

static const int8_t kPDBVersion = 6;

static const int8_t kAux = 0xFA;
static const int8_t kResizeDB = 0xFB;
static const int8_t kExpireMs = 0xFC;
static const int8_t kExpire = 0xFD;
static const int8_t kSelectDB = 0xFE;
static const int8_t kEOF = 0xFF;

static const int8_t k6Bits = 0;
static const int8_t k14bits = 1;
static const int8_t k32bits = 2;
static const int8_t kSpecial = 3;  //  the string may be interger, or lzf
static const int8_t kLow6Bits = 0x3F;

static const int8_t kEnc8Bits = 0;
static const int8_t kEnc16Bits = 1;
static const int8_t kEnc32Bits = 2;
static const int8_t kEncLZF = 3;

void PDBSaver::Save(const char* qdbFile) {
  char tmpFile[64] = "";
  snprintf(tmpFile, sizeof tmpFile, "tmp_qdb_file_%d", getpid());

  if (!qdb_.Open(tmpFile, false)) {
    assert(false);
  }

  char buf[16];
  snprintf(buf, sizeof buf, "REDIS%04d", kPDBVersion);
  qdb_.Write(buf, 9);

  for (int dbno = 0; true; ++dbno) {
    if (PSTORE.SelectDB(dbno) == -1) {
      break;
    }

    if (PSTORE.DBSize() == 0) {
      continue;  // But redis will save empty db
    }

    qdb_.Write(&kSelectDB, 1);
    SaveLength(dbno);

    uint64_t now = ::Now();
    for (const auto& kv : PSTORE) {
      int64_t ttl = PSTORE.TTL(kv.first, now);
      if (ttl > 0) {
        ttl += now;

        qdb_.Write(&kExpireMs, 1);
        qdb_.Write(&ttl, sizeof ttl);
      } else if (ttl == PStore::ExpireResult::expired) {
        continue;
      }

      SaveType(kv.second);
      SaveKey(kv.first);
      SaveObject(kv.second);
    }
  }

  qdb_.Write(&kEOF, 1);

  // crc 8 bytes
  InputMemoryFile file;
  file.Open(tmpFile);

  auto len = qdb_.Offset();
  auto data = file.Read(len);

  const uint64_t crc = crc64(0, (const unsigned char*)data, len);
  qdb_.Write(&crc, sizeof crc);

  if (::rename(tmpFile, qdbFile) != 0) {
    perror("rename error");
    assert(false);
  }
}

void PDBSaver::SaveType(const PObject& obj) {
  switch (obj.encoding) {
    case PEncode_raw:
    case PEncode_int:
      qdb_.Write(&kTypeString, 1);
      break;

    case PEncode_list:
      qdb_.Write(&kTypeList, 1);
      break;

    case PEncode_hash:
      qdb_.Write(&kTypeHash, 1);
      break;

    case PEncode_set:
      qdb_.Write(&kTypeSet, 1);
      break;

    case PEncode_zset:
      qdb_.Write(&kTypeZSet, 1);
      break;

    default:
      assert(false);
      break;
  }
}

void PDBSaver::SaveKey(const PString& key) { SaveString(key); }

void PDBSaver::SaveObject(const PObject& obj) {
  switch (obj.encoding) {
    case PEncode_raw:
    case PEncode_int:
      SaveString(*GetDecodedString(&obj));
      break;

    case PEncode_list:
      saveList(obj.CastList());
      break;

    case PEncode_set:
      saveset(obj.CastSet());
      break;

    case PEncode_hash:
      saveHash(obj.CastHash());
      break;

    case PEncode_zset:
      saveZSet(obj.CastSortedSet());
      break;

    default:
      break;
  }
}

/* Copy from redis~
 * Save a double value. Doubles are saved as strings prefixed by an unsigned
 * 8 bit integer specifying the length of the representation.
 * This 8 bit integer has special values in order to specify the following
 * conditions:
 * 253: not a number
 * 254: + inf
 * 255: - inf
 */
void PDBSaver::saveDoubleValue(double val) {
  unsigned char buf[128];
  int len;

  if (isnan(val)) {
    buf[0] = 253;
    len = 1;
  } else if (!std::isfinite(val)) {
    len = 1;
    buf[0] = (val < 0) ? 255 : 254;
  } else {
    snprintf((char*)buf + 1, sizeof(buf) - 1, "%.6g", val);
    buf[0] = strlen((char*)buf + 1);
    len = buf[0] + 1;
  }

  qdb_.Write(buf, len);
}

void PDBSaver::saveList(const PLIST& l) {
  SaveLength(l->size());

  for (const auto& e : *l) {
    SaveString(e);
  }
}

void PDBSaver::saveset(const PSET& s) {
  SaveLength(s->size());

  for (const auto& e : *s) {
    SaveString(e);
  }
}

void PDBSaver::saveHash(const PHASH& h) {
  SaveLength(h->size());

  for (const auto& e : *h) {
    SaveString(e.first);
    SaveString(e.second);
  }
}

void PDBSaver::saveZSet(const PZSET& ss) {
  SaveLength(ss->Size());

  for (const auto& e : *ss) {
    SaveString(e.first);
    saveDoubleValue(e.second);
  }
}

void PDBSaver::SaveString(const PString& str) {
  if (str.size() < 10) {
    long lVal;
    if (Strtol(str.data(), str.size(), &lVal)) {
      SaveString(lVal);
      return;
    }
  }

  if (!SaveLZFString(str)) {
    SaveLength(str.size());
    qdb_.Write(str.data(), str.size());
  }
}

void PDBSaver::SaveLength(uint64_t len) {
  assert((len & ~0xFFFFFFFF) == 0);

  if (len < (1 << 6)) {
    len &= kLow6Bits;
    len |= k6Bits << 6;
    qdb_.Write(&len, 1);
  } else if (len < (1 << 14)) {
    uint16_t encodeLen = (len >> 8) & kLow6Bits;
    encodeLen |= k14bits << 6;
    encodeLen |= (len & 0xFF) << 8;
    qdb_.Write(&encodeLen, 2);
  } else {
    int8_t encFlag = static_cast<int8_t>(k32bits << 6);
    qdb_.Write(&encFlag, 1);
    len = htonl(len);
    qdb_.Write(&len, 4);
  }
}

void PDBSaver::SaveString(int64_t intVal) {
  uint8_t specialByte = kSpecial << 6;

  if ((intVal & ~0x7F) == 0) {
    specialByte |= kEnc8Bits;
    qdb_.Write(&specialByte, 1);
    qdb_.Write(&intVal, 1);
  } else if ((intVal & ~0x7FFF) == 0) {
    specialByte |= kEnc16Bits;
    qdb_.Write(&specialByte, 1);
    qdb_.Write(&intVal, 2);
  } else if ((intVal & ~0x7FFFFFFF) == 0) {
    specialByte |= kEnc32Bits;
    qdb_.Write(&specialByte, 1);
    qdb_.Write(&intVal, 4);
  } else {
    char buf[64];
    auto len = Number2Str(buf, sizeof buf, intVal);
    SaveLength(static_cast<uint64_t>(len));
    qdb_.Write(buf, len);
  }
}

bool PDBSaver::SaveLZFString(const PString& str) {
  if (str.size() < 20) {
    return false;
  }

  unsigned outlen = static_cast<unsigned>(str.size() - 4);
  std::unique_ptr<char[]> outBuf(new char[outlen + 1]);

  auto compressLen = lzf_compress((const void*)str.data(), static_cast<unsigned>(str.size()), outBuf.get(), outlen);

  if (compressLen == 0) {
    ERROR("compress len = 0");
    return false;
  }

  int8_t specialByte = static_cast<int8_t>(kSpecial << 6) | kEncLZF;
  qdb_.Write(&specialByte, 1);

  // compress len + raw len + str data;
  SaveLength(compressLen);
  SaveLength(str.size());
  qdb_.Write(outBuf.get(), compressLen);

  DEBUG("compress len {}, raw len {}", compressLen, str.size());

  return true;
}

void PDBSaver::SaveDoneHandler(int exitRet, int whatSignal) {
  if (exitRet == 0 && whatSignal == 0) {
    INFO("save rdb success");
    g_lastPDBSave = time(nullptr);

    PStore::dirty_ = 0;
  } else {
    ERROR("save rdb failed with exit result {}, signal {}", exitRet, whatSignal);
  }

  g_qdbPid = -1;
}

int PDBLoader::Load(const char* filename) {
  if (!qdb_.Open(filename)) {
    return -__LINE__;
  }

  // check the magic string "REDIS" and version number
  size_t len = 9;
  const char* data = qdb_.Read(len);

  if (len != 9) {
    return -__LINE__;
  }

  long qdbversion;
  if (!Strtol(data + 5, 4, &qdbversion) || qdbversion < 6) {
    return -123;
  }
  qdb_.Skip(9);

  //  SELECTDB + dbno
  //  type1 + key + obj
  //  EOF + crc

  int64_t absTimeout = 0;
  bool eof = false;
  while (!eof) {
    int8_t indicator = qdb_.Read<int8_t>();

    switch (indicator) {
      case kEOF:
        DEBUG("encounter EOF");
        eof = true;
        break;

      case kAux:
        DEBUG("encounter AUX");
        loadAux();
        break;

      case kResizeDB:
        DEBUG("encounter kResizeDB");
        loadResizeDB();
        break;

      case kSelectDB: {
        bool special;
        auto dbno = LoadLength(special);
        assert(!special);
        // check db no
        if (dbno > kMaxDBNum) {
          ERROR("Abnormal db number {}", dbno);
          return false;
        }

        if (PSTORE.SelectDB(static_cast<int>(dbno)) == -1) {
          ERROR("DB NUMBER is differ from RDB file");
          return __LINE__;
        }
        DEBUG("encounter Select DB {}", dbno);
        break;
      }

      case kExpireMs:
        absTimeout = qdb_.Read<int64_t>();
        break;

      case kExpire:
        absTimeout = qdb_.Read<int64_t>();
        absTimeout *= 1000;
        break;

      case kTypeString:
      case kTypeList:
      case kTypeZipList:
      case kTypeSet:
      case kTypeIntSet:
      case kTypeHash:
      case kTypeHashZipList:
      case kTypeZipMap:
      case kTypeZSet:
      case kTypeZSetZipList:
      case kTypeQuickList: {
        PString key = LoadKey();
        PObject obj = LoadObject(indicator);
//        DEBUG("encounter key = {}, obj.encoding = {}", key, obj.encoding);

        assert(absTimeout >= 0);

        if (absTimeout == 0) {
          PSTORE.SetValue(key, std::move(obj));
        } else if (absTimeout > 0) {
          if (absTimeout > static_cast<int64_t>(::Now())) {
            DEBUG("key {} load timeout {}", key, absTimeout);
            PSTORE.SetValue(key, std::move(obj));
            PSTORE.SetExpire(key, absTimeout);
          } else {
            INFO("key {} is already time out", key);
          }

          absTimeout = 0;
        }
        break;
      }

      default:
        printf("%d is unknown\n", indicator);
        assert(false);
        break;
    }
  }

  return 0;
}

size_t PDBLoader::LoadLength(bool& special) {
  const int8_t byte = qdb_.Read<int8_t>();

  special = false;
  size_t lenResult = 0;

  switch ((byte & 0xC0) >> 6) {
    case k6Bits: {
      lenResult = byte & kLow6Bits;
      break;
    }

    case k14bits: {
      lenResult = byte & kLow6Bits;  // high 6 bits;
      lenResult <<= 8;

      const uint8_t bytelow = qdb_.Read<uint8_t>();

      lenResult |= bytelow;
      break;
    }

    case k32bits: {
      const int32_t fourbytes = qdb_.Read<int32_t>();

      lenResult = ntohl(fourbytes);
      break;
    }

    case kSpecial: {
      special = true;
      lenResult = byte & kLow6Bits;
      break;
    }

    default: {
      assert(false);
    }
  }

  return lenResult;
}

PObject PDBLoader::LoadSpecialStringObject(size_t specialVal) {
  bool isInt = true;
  long val;

  switch (specialVal) {
    case kEnc8Bits: {
      val = qdb_.Read<uint8_t>();
      break;
    }

    case kEnc16Bits: {
      val = qdb_.Read<uint16_t>();
      break;
    }

    case kEnc32Bits: {
      val = qdb_.Read<uint32_t>();
      break;
    }

    case kEncLZF: {
      isInt = false;
      break;
    }

    default: {
      assert(false);
    }
  }

  if (isInt) {
    return PObject::CreateString(val);
  } else {
    return PObject::CreateString(LoadLZFString());
  }
}

PString PDBLoader::LoadString(size_t strLen) {
  const char* str = qdb_.Read(strLen);
  qdb_.Skip(strLen);

  return PString(str, strLen);
}

PString PDBLoader::LoadLZFString() {
  bool special;
  size_t compressLen = LoadLength(special);
  assert(!special);

  unsigned rawLen = static_cast<unsigned>(LoadLength(special));
  assert(!special);

  const char* compressStr = qdb_.Read(compressLen);

  PString val;
  val.resize(rawLen);
  if (lzf_decompress(compressStr, static_cast<unsigned>(compressLen), &val[0], rawLen) == 0) {
    ERROR("decompress error");
    return PString();
  }

  qdb_.Skip(compressLen);
  return val;
}

PString PDBLoader::LoadKey() { return loadGenericString(); }

PObject PDBLoader::LoadObject(int8_t type) {
  switch (type) {
    case kTypeString: {
      bool special;
      size_t len = LoadLength(special);

      if (special) {
        return LoadSpecialStringObject(len);
      } else {
        return PObject::CreateString(LoadString(len));
      }
    }
    case kTypeList: {
      return loadList();
    }
    case kTypeZipList: {
      return loadZipList(kTypeZipList);
    }
    case kTypeSet: {
      return loadSet();
    }
    case kTypeIntSet: {
      return loadIntset();
    }
    case kTypeHash: {
      return loadHash();
    }
    case kTypeHashZipList: {
      return loadZipList(kTypeHashZipList);
    }
    case kTypeZipMap: {
      assert(!!!"zipmap should be replaced with ziplist");
      break;
    }
    case kTypeZSet: {
      return loadZSet();
    }
    case kTypeZSetZipList: {
      return loadZipList(kTypeZSetZipList);
    }
    case kTypeQuickList: {
      return loadQuickList();
    }

    default:
      break;
  }

  assert(false);
  return PObject(PType_invalid);
}

PString PDBLoader::loadGenericString() {
  bool special;
  size_t len = LoadLength(special);

  if (special) {
    PObject obj = LoadSpecialStringObject(len);
    return *GetDecodedString(&obj);
  } else {
    return LoadString(len);
  }
}

PObject PDBLoader::loadList() {
  bool special;
  const auto len = LoadLength(special);
  assert(!special);
  DEBUG("list length = {}", len);

  PObject obj(PObject::CreateList());
  PLIST list(obj.CastList());
  for (size_t i = 0; i < len; ++i) {
    const auto elemLen = LoadLength(special);
    PString elem;
    if (special) {
      PObject str = LoadSpecialStringObject(elemLen);
      elem = *GetDecodedString(&str);
    } else {
      elem = LoadString(elemLen);
    }

    list->push_back(elem);
    DEBUG("list elem : {}", elem);
  }

  return obj;
}

PObject PDBLoader::loadSet() {
  bool special;
  const auto len = LoadLength(special);
  assert(!special);
  DEBUG("set length = {}", len);

  PObject obj(PObject::CreateSet());
  PSET set(obj.CastSet());
  for (size_t i = 0; i < len; ++i) {
    const auto elemLen = LoadLength(special);
    PString elem;
    if (special) {
      PObject str = LoadSpecialStringObject(elemLen);
      elem = *GetDecodedString(&str);
    } else {
      elem = LoadString(elemLen);
    }

    set->insert(elem);
    DEBUG("set elem : {}", elem);
  }

  return obj;
}

PObject PDBLoader::loadHash() {
  bool special;
  const auto len = LoadLength(special);
  assert(!special);
  DEBUG("hash length = {}", len);

  PObject obj(PObject::CreateHash());
  PHASH hash(obj.CastHash());
  for (size_t i = 0; i < len; ++i) {
    const auto keyLen = LoadLength(special);
    PString key;
    if (special) {
      PObject str = LoadSpecialStringObject(keyLen);
      key = *GetDecodedString(&str);
    } else {
      key = LoadString(keyLen);
    }

    const auto valLen = LoadLength(special);
    PString val;
    if (special) {
      PObject str = LoadSpecialStringObject(valLen);
      val = *GetDecodedString(&str);
    } else {
      val = LoadString(valLen);
    }

    hash->insert(PHash::value_type(key, val));
    DEBUG("hash key : {}, val : {}", key, val);
  }

  return obj;
}

PObject PDBLoader::loadZSet() {
  bool special;
  const auto len = LoadLength(special);
  assert(!special);
  DEBUG("zset length = {}", len);

  PObject obj(PObject::CreateZSet());
  PZSET zset(obj.CastSortedSet());
  for (size_t i = 0; i < len; ++i) {
    const auto memberLen = LoadLength(special);
    PString member;
    if (special) {
      PObject str = LoadSpecialStringObject(memberLen);
      member = *GetDecodedString(&str);
    } else {
      member = LoadString(memberLen);
    }

    const auto score = loadDoubleValue();
    zset->AddMember(member, static_cast<long>(score));
    DEBUG("zset member : {}, score : {}", member, score);
  }

  return obj;
}

double PDBLoader::loadDoubleValue() {
  const uint8_t byte1st = qdb_.Read<uint8_t>();

  double dvalue;
  switch (byte1st) {
    case 253: {
      dvalue = NAN;
      break;
    }

    case 254: {
      dvalue = INFINITY;
      break;
    }

    case 255: {
      dvalue = INFINITY;
      break;
    }

    default: {
      size_t len = byte1st;
      const char* val = qdb_.Read(len);
      assert(len == byte1st);
      qdb_.Skip(len);

      std::istringstream is(std::string(val, len));
      is >> dvalue;

      break;
    }
  }

  DEBUG("load double value {}", dvalue);

  return dvalue;
}

struct ZipListElement {
  unsigned char* sval;
  unsigned int slen;

  long long lval;

  PString ToString() const {
    if (sval) {
      const PString str((const char*)sval, PString::size_type(slen));
      DEBUG("string zip list element {}", str);
      return str;
    } else
      return LongToString();
  }

  PString LongToString() const {
    assert(!sval);

    PString str(16, 0);
    auto len = Number2Str(&str[0], 16, lval);
    str.resize(len);

    DEBUG("long zip list element {}", str);
    return str;
  }
};

PObject PDBLoader::loadZipList(int8_t type) {
  PString zl = loadGenericString();
  return loadZipList(zl, type);
}

PObject PDBLoader::loadZipList(const PString& zl, int8_t type) {
  unsigned char* zlist = (unsigned char*)&zl[0];
  unsigned nElem = ziplistLen(zlist);

  std::vector<ZipListElement> elements;
  elements.resize(nElem);

  for (unsigned i = 0; i < nElem; ++i) {
    unsigned char* elem = ziplistIndex(zlist, (int)i);

    int succ = ziplistGet(elem, &elements[i].sval, &elements[i].slen, &elements[i].lval);
    assert(succ);
  }

  switch (type) {
    case kTypeZipList: {
      PObject obj(PObject::CreateList());
      PLIST list(obj.CastList());

      for (const auto& elem : elements) {
        list->push_back(elem.ToString());
      }

      return obj;
    }

    case kTypeHashZipList: {
      PObject obj(PObject::CreateHash());
      PHASH hash(obj.CastHash());

      assert(elements.size() % 2 == 0);

      for (auto it(elements.begin()); it != elements.end(); ++it) {
        auto key = it;
        auto value = ++it;

        hash->insert(PHash::value_type(key->ToString(), value->ToString()));
      }

      return obj;
    }

    case kTypeZSetZipList: {
      PObject obj(PObject::CreateZSet());
      PZSET zset(obj.CastSortedSet());

      assert(elements.size() % 2 == 0);

      for (auto it(elements.begin()); it != elements.end(); ++it) {
        const PString& member = it->ToString();
        ++it;

        double score;
        if (it->sval) {
          Strtod((const char*)it->sval, it->slen, &score);
        } else {
          score = it->lval;
        }

        DEBUG("zset member {}, score {}", member, score);
        zset->AddMember(member, score);
      }

      return obj;
    }

    default:
      assert(!!!"illegal data type");
      break;
  }

  return PObject(PType_invalid);
}

PObject PDBLoader::loadIntset() {
  PString str = loadGenericString();

  intset* iset = (intset*)&str[0];
  unsigned nElem = intsetLen(iset);

  std::vector<int64_t> elements;
  elements.resize(nElem);

  for (unsigned i = 0; i < nElem; ++i) {
    intsetGet(iset, i, &elements[i]);
  }

  PObject obj(PObject::CreateSet());
  PSET set(obj.CastSet());

  for (auto v : elements) {
    char buf[64];
    auto bytes = Number2Str<int64_t>(buf, sizeof buf, v);
    set->insert(PString(buf, bytes));
  }

  return obj;
}

PObject PDBLoader::loadQuickList() {
  bool special = true;
  auto nElem = LoadLength(special);

  PObject obj(PObject::CreateList());
  PLIST list(obj.CastList());
  while (nElem-- > 0) {
    PString zl = loadGenericString();
    if (zl.empty()) {
      continue;
    }

    PObject l = loadZipList(zl, kTypeZipList);
    PLIST tmplist(l.CastList());
    if (!tmplist->empty()) {
      list->splice(list->end(), *tmplist);
    }
  }

  return obj;
}

void PDBLoader::loadAux() {
  /* AUX: generic string-string fields. Use to add state to RDB
   * which is backward compatible. Implementations of RDB loading
   * are required to skip AUX fields they don't understand.
   *
   * An AUX field is composed of two strings: key and value. */
  PString auxkey = loadGenericString();
  PString auxvalue = loadGenericString();

  if (!auxkey.empty() && auxkey[0] == '%') {
    /* All the fields with a name staring with '%' are considered
     * information fields and are logged at startup with a log
     * level of NOTICE. */
    INFO("RDB '{}':{}", auxkey, auxvalue);
  } else {
    /* We ignore fields we don't understand, as by AUX field
     * contract. */
    DEBUG("Unrecognized RDB AUX field: '{}':{}", auxkey, auxvalue);
  }
}

void PDBLoader::loadResizeDB() {
  bool special = true;
  auto dbsize = LoadLength(special);
  assert(!special);

  auto expiresize = LoadLength(special);
  assert(!special);

  // just ignore this
  (void)special;
  (void)dbsize;
  (void)expiresize;
}

}  // namespace pikiwidb
