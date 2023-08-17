/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <cassert>
#include <fnmatch.h>
#include "log.h"
#include "store.h"

namespace pikiwidb {

PError type(const std::vector<PString>& params, UnboundedBuffer* reply) {
  const char* info = 0;
  PType type = PSTORE.KeyType(params[1]);
  switch (type) {
    case PType_hash:
      info = "hash";
      break;

    case PType_set:
      info = "set";
      break;

    case PType_string:
      info = "string";
      break;

    case PType_list:
      info = "list";
      break;

    case PType_sortedSet:
      info = "sortedSet";
      break;

    default:
      info = "none";
      break;
  }

  FormatSingle(info, reply);
  return PError_ok;
}

PError exists(const std::vector<PString>& params, UnboundedBuffer* reply) {
  if (PSTORE.ExistsKey(params[1])) {
    Format1(reply);
  } else {
    Format0(reply);
  }

  return PError_ok;
}

PError del(const std::vector<PString>& params, UnboundedBuffer* reply) {
  int nDel = 0;
  for (size_t i = 1; i < params.size(); ++i) {
    const PString& key = params[i];

    if (PSTORE.DeleteKey(key)) {
      PSTORE.ClearExpire(key);
      ++nDel;
    }
  }

  FormatInt(nDel, reply);
  return PError_ok;
}

static int setExpireByMs(const PString& key, uint64_t absTimeout) {
  INFO("try set expire, key {} , timeout is {}", key, absTimeout);

  int ret = 0;
  if (PSTORE.ExistsKey(key)) {
    PSTORE.SetExpire(key, absTimeout);
    ret = 1;
  }

  return ret;
}

PError expire(const std::vector<PString>& params, UnboundedBuffer* reply) {
  const PString& key = params[1];
  const uint64_t timeout = atoi(params[2].c_str());  // by seconds;

  int ret = setExpireByMs(key, ::Now() + timeout * 1000);

  FormatInt(ret, reply);
  return PError_ok;
}

PError pexpire(const std::vector<PString>& params, UnboundedBuffer* reply) {
  const PString& key = params[1];
  const uint64_t timeout = atoi(params[2].c_str());  // by milliseconds;

  int ret = setExpireByMs(key, ::Now() + timeout);

  FormatInt(ret, reply);
  return PError_ok;
}

PError expireat(const std::vector<PString>& params, UnboundedBuffer* reply) {
  const PString& key = params[1];
  const uint64_t timeout = atoi(params[2].c_str());  // by seconds;

  int ret = setExpireByMs(key, timeout * 1000);

  FormatInt(ret, reply);
  return PError_ok;
}

PError pexpireat(const std::vector<PString>& params, UnboundedBuffer* reply) {
  const PString& key = params[1];
  const uint64_t timeout = atoi(params[2].c_str());  // by milliseconds;

  int ret = setExpireByMs(key, timeout);

  FormatInt(ret, reply);
  return PError_ok;
}

static int64_t _ttl(const PString& key) {
  int64_t ret = PStore::ExpireResult::notExist;
  if (PSTORE.ExistsKey(key)) {
    int64_t ttl = PSTORE.TTL(key, ::Now());
    if (ttl < 0) {
      ret = PStore::ExpireResult::persist;
    } else {
      ret = ttl;
    }
  } else {
    ERROR("ttl not exist key:{}", key.c_str());
  }

  return ret;
}

PError ttl(const std::vector<PString>& params, UnboundedBuffer* reply) {
  const PString& key = params[1];

  int64_t ret = _ttl(key);
  if (ret > 0) {
    ret /= 1000;  // by seconds
  }

  FormatInt(ret, reply);
  return PError_ok;
}

PError pttl(const std::vector<PString>& params, UnboundedBuffer* reply) {
  const PString& key = params[1];

  int64_t ret = _ttl(key);  // by milliseconds

  FormatInt(ret, reply);
  return PError_ok;
}

PError persist(const std::vector<PString>& params, UnboundedBuffer* reply) {
  const PString& key = params[1];

  int ret = PSTORE.ClearExpire(key) ? 1 : 0;

  FormatInt(ret, reply);
  return PError_ok;
}

PError move(const std::vector<PString>& params, UnboundedBuffer* reply) {
  const PString& key = params[1];
  int toDB = atoi(params[2].c_str());

  int ret = 0;

  PObject* val;
  if (PSTORE.GetValue(key, val) == PError_ok) {
    int fromDB = PSTORE.SelectDB(toDB);
    if (fromDB >= 0 && fromDB != toDB && !PSTORE.ExistsKey(key)) {
      PSTORE.SelectDB(toDB);
      PSTORE.SetValue(key, std::move(*val));  // set to new db

      PSTORE.SelectDB(fromDB);
      PSTORE.ClearExpire(key);
      PSTORE.DeleteKey(key);  // delete from old db

      ret = 1;

      INFO("move {} to db {}, from db {} ", key, toDB, fromDB);
    } else {
      ERROR("move {} failed to db {}, from db {} ", key, toDB, fromDB);
    }
  } else {
    ERROR("move {} failed to db {}", key, toDB);
  }

  FormatInt(ret, reply);
  return PError_ok;
}

PError keys(const std::vector<PString>& params, UnboundedBuffer* reply) {
  const PString& pattern = params[1];

  std::vector<const PString*> results;
  for (const auto& kv : PSTORE) {
    if (fnmatch(pattern.c_str(), kv.first.c_str(), FNM_NOESCAPE) == 0) {
      results.push_back(&kv.first);
    }
  }

  PreFormatMultiBulk(results.size(), reply);
  for (auto e : results) {
    FormatBulk(*e, reply);
  }

  return PError_ok;
}

PError randomkey(const std::vector<PString>& params, UnboundedBuffer* reply) {
  const PString& res = PSTORE.RandomKey();

  if (res.empty()) {
    FormatNull(reply);
  } else {
    FormatBulk(res, reply);
  }

  return PError_ok;
}

static PError RenameKey(const PString& oldKey, const PString& newKey, bool force) {
  PObject* val;

  PError err = PSTORE.GetValue(oldKey, val);
  if (err != PError_ok) {
    return err;
  }

  if (!force && PSTORE.ExistsKey(newKey)) {
    return PError_exist;
  }

  auto now = ::Now();
  auto ttl = PSTORE.TTL(oldKey, now);

  if (ttl == PStore::expired) {
    return PError_notExist;
  }

  PSTORE.SetValue(newKey, std::move(*val));
  if (ttl > 0) {
    PSTORE.SetExpire(newKey, ttl + now);
  } else if (ttl == PStore::persist) {
    PSTORE.ClearExpire(newKey);
  }

  PSTORE.ClearExpire(oldKey);
  PSTORE.DeleteKey(oldKey);

  return PError_ok;
}

PError rename(const std::vector<PString>& params, UnboundedBuffer* reply) {
  PError err = RenameKey(params[1], params[2], true);

  ReplyError(err, reply);
  return err;
}

PError renamenx(const std::vector<PString>& params, UnboundedBuffer* reply) {
  PError err = RenameKey(params[1], params[2], false);

  if (err == PError_ok) {
    Format1(reply);
  } else {
    ReplyError(err, reply);
  }

  return err;
}

// helper func scan
static PError ParseScanOption(const std::vector<PString>& params, int start, long& count, const char*& pattern) {
  // scan cursor  MATCH pattern  COUNT 1
  count = -1;
  pattern = nullptr;
  for (std::size_t i = start; i < params.size(); i += 2) {
    if (params[i].size() == 5) {
      if (strncasecmp(params[i].c_str(), "match", 5) == 0) {
        if (!pattern) {
          pattern = params[i + 1].c_str();
          continue;
        }
      } else if (strncasecmp(params[i].c_str(), "count", 5) == 0) {
        if (count == -1) {
          if (Strtol(params[i + 1].c_str(), params[i + 1].size(), &count)) {
            continue;
          }
        }
      }
    }

    return PError_param;
  }

  return PError_ok;
}

PError scan(const std::vector<PString>& params, UnboundedBuffer* reply) {
  if (params.size() % 2 != 0) {
    ReplyError(PError_param, reply);
    return PError_param;
  }

  long cursor = 0;

  if (!Strtol(params[1].c_str(), params[1].size(), &cursor)) {
    ReplyError(PError_param, reply);
    return PError_param;
  }

  // scan cursor  MATCH pattern  COUNT 1
  long count = -1;
  const char* pattern = nullptr;

  PError err = ParseScanOption(params, 2, count, pattern);
  if (err != PError_ok) {
    ReplyError(err, reply);
    return err;
  }

  if (count < 0) {
    count = 5;
  }

  std::vector<PString> res;
  auto newCursor = PSTORE.ScanKey(cursor, count, res);

  // filter by pattern
  if (pattern) {
    for (auto it = res.begin(); it != res.end();) {
      if (fnmatch(pattern, (*it).c_str(), FNM_NOESCAPE) != 0) {
        it = res.erase(it);
      } else {
        ++it;
      }
    }
  }

  // reply
  PreFormatMultiBulk(2, reply);

  char buf[32];
  auto len = snprintf(buf, sizeof buf - 1, "%lu", newCursor);
  FormatBulk(buf, len, reply);

  PreFormatMultiBulk(res.size(), reply);
  for (const auto& s : res) {
    FormatBulk(s, reply);
  }

  return PError_ok;
}

PError hscan(const std::vector<PString>& params, UnboundedBuffer* reply) {
  // hscan key cursor COUNT 0 MATCH 0
  if (params.size() % 2 == 0) {
    ReplyError(PError_param, reply);
    return PError_param;
  }

  long cursor = 0;
  if (!Strtol(params[2].c_str(), params[2].size(), &cursor)) {
    ReplyError(PError_param, reply);
    return PError_param;
  }

  // find hash
  PObject* value;
  PError err = PSTORE.GetValueByType(params[1], value, PType_hash);
  if (err != PError_ok) {
    ReplyError(err, reply);
    return err;
  }

  // parse option
  long count = -1;
  const char* pattern = nullptr;

  err = ParseScanOption(params, 3, count, pattern);
  if (err != PError_ok) {
    ReplyError(err, reply);
    return err;
  }

  if (count < 0) {
    count = 5;
  }

  // scan
  std::vector<PString> res;
  auto newCursor = HScanKey(*value->CastHash(), cursor, count, res);

  // filter by pattern
  if (pattern) {
    for (auto it = res.begin(); it != res.end();) {
      if (fnmatch(pattern, (*it).c_str(), FNM_NOESCAPE) != 0) {
        it = res.erase(it);  // erase key
        it = res.erase(it);  // erase value
      } else {
        ++it, ++it;
      }
    }
  }

  // reply
  PreFormatMultiBulk(2, reply);

  char buf[32];
  auto len = snprintf(buf, sizeof buf - 1, "%lu", newCursor);
  FormatBulk(buf, len, reply);

  PreFormatMultiBulk(res.size(), reply);
  for (const auto& s : res) {
    FormatBulk(s, reply);
  }

  return PError_ok;
}

PError sscan(const std::vector<PString>& params, UnboundedBuffer* reply) {
  // sscan key cursor COUNT 0 MATCH 0
  if (params.size() % 2 == 0) {
    ReplyError(PError_param, reply);
    return PError_param;
  }

  long cursor = 0;
  if (!Strtol(params[2].c_str(), params[2].size(), &cursor)) {
    ReplyError(PError_param, reply);
    return PError_param;
  }

  // find set
  PObject* value;
  PError err = PSTORE.GetValueByType(params[1], value, PType_set);
  if (err != PError_ok) {
    ReplyError(err, reply);
    return err;
  }

  // parse option
  long count = -1;
  const char* pattern = nullptr;

  err = ParseScanOption(params, 3, count, pattern);
  if (err != PError_ok) {
    ReplyError(err, reply);
    return err;
  }

  if (count < 0) {
    count = 5;
  }

  // scan
  std::vector<PString> res;
  auto newCursor = SScanKey(*value->CastSet(), cursor, count, res);

  // filter by pattern
  if (pattern) {
    for (auto it = res.begin(); it != res.end();) {
      if (fnmatch(pattern, (*it).c_str(), FNM_NOESCAPE) != 0) {
        it = res.erase(it);
      } else {
        ++it;
      }
    }
  }

  // reply
  PreFormatMultiBulk(2, reply);

  char buf[32];
  auto len = snprintf(buf, sizeof buf - 1, "%lu", newCursor);
  FormatBulk(buf, len, reply);

  PreFormatMultiBulk(res.size(), reply);
  for (const auto& s : res) {
    FormatBulk(s, reply);
  }

  return PError_ok;
}

PError sort(const std::vector<PString>& params, UnboundedBuffer* reply) {
  // sort key desc/asc alpha
  PObject* value;
  PError err = PSTORE.GetValue(params[1], value);
  if (err != PError_ok) {
    ReplyError(err, reply);
    return err;
  }

  if (value->type != PType_list && value->type != PType_set && value->type != PType_sortedSet) {
    ReplyError(PError_type, reply);
    return PError_type;
  }

  bool asc = true;
  bool alpha = false;
  for (const auto& arg : params) {
    if (strncasecmp(arg.data(), "desc", 4) == 0) {
      asc = false;
    } else if (strncasecmp(arg.data(), "alpha", 5) == 0) {
      alpha = true;
    }
  }

  std::vector<const PString*> values;
  switch (value->type) {
    case PType_list: {
      PLIST l = value->CastList();
      std::for_each(l->begin(), l->end(), [&](const PString& v) { values.push_back(&v); });
    } break;

    case PType_set: {
      PSET s = value->CastSet();
      std::for_each(s->begin(), s->end(), [&](const PString& v) { values.push_back(&v); });
    } break;

    case PType_sortedSet: {
      // TODO
      FormatOK(reply);
      return PError_ok;
    } break;

    default:
      break;
  }

  std::sort(values.begin(), values.end(), [=](const PString* a, const PString* b) -> bool {
    if (!alpha) {
      long avalue = 0, bvalue = 0;
      TryStr2Long(a->data(), a->size(), avalue);
      TryStr2Long(b->data(), b->size(), bvalue);

      if (asc) {
        return avalue < bvalue;
      } else {
        return bvalue < avalue;
      }
    } else {
      if (asc) {
        return std::lexicographical_compare(a->begin(), a->end(), b->begin(), b->end());
      } else {
        return std::lexicographical_compare(b->begin(), b->end(), a->begin(), a->end());
      }
    }
  });

  PreFormatMultiBulk(values.size(), reply);
  for (const auto v : values) {
    FormatBulk(*v, reply);
  }

  return PError_ok;
}

}  // namespace pikiwidb
