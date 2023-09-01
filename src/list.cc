/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "list.h"
#include <algorithm>
#include <cassert>
#include "client.h"
#include "log.h"
#include "store.h"

using std::vector;

namespace pikiwidb {

PObject PObject::CreateList() {
  PObject list(PType_list);
  list.Reset(new PList);

  return list;
}

static PError push(const vector<PString>& params, UnboundedBuffer* reply, ListPosition pos,
                   bool createIfNotExist = true) {
  PObject* value;

  PError err = PSTORE.GetValueByType(params[1], value, PType_list);
  if (err != PError_ok) {
    if (err != PError_notExist) {
      ReplyError(err, reply);
      return err;
    } else if (createIfNotExist) {
      value = PSTORE.SetValue(params[1], PObject::CreateList());
    } else {
      ReplyError(err, reply);
      return err;
    }
  }

  auto list = value->CastList();
  bool mayReady = list->empty();
  for (size_t i = 2; i < params.size(); ++i) {
    if (pos == ListPosition::head) {
      list->push_front(params[i]);
    } else {
      list->push_back(params[i]);
    }
  }

  FormatInt(static_cast<long>(list->size()), reply);
  if (mayReady && !list->empty()) {
    if (reply)  {
      // push must before pop(serve)...
      Propagate(params);                    // the push
      PSTORE.ServeClient(params[1], list);  // the pop
    }
    return PError_nop;
  } else {
    return PError_ok;
  }
}

static PError GenericPop(const PString& key, ListPosition pos, PString& result) {
  PObject* value;

  PError err = PSTORE.GetValueByType(key, value, PType_list);
  if (err != PError_ok) {
    return err;
  }

  auto list = value->CastList();
  assert(!list->empty());

  if (pos == ListPosition::head) {
    result = std::move(list->front());
    list->pop_front();
  } else {
    result = std::move(list->back());
    list->pop_back();
  }

  if (list->empty()) {
    PSTORE.DeleteKey(key);
  }

  return PError_ok;
}

PError lpush(const vector<PString>& params, UnboundedBuffer* reply) { return push(params, reply, ListPosition::head); }

PError rpush(const vector<PString>& params, UnboundedBuffer* reply) { return push(params, reply, ListPosition::tail); }

PError lpushx(const vector<PString>& params, UnboundedBuffer* reply) {
  return push(params, reply, ListPosition::head, false);
}

PError rpushx(const vector<PString>& params, UnboundedBuffer* reply) {
  return push(params, reply, ListPosition::tail, false);
}

PError lpop(const vector<PString>& params, UnboundedBuffer* reply) {
  PString result;
  PError err = GenericPop(params[1], ListPosition::head, result);
  switch (err) {
    case PError_ok:
      FormatBulk(result, reply);
      break;

    default:
      ReplyError(err, reply);
      break;
  }

  return err;
}

PError rpop(const vector<PString>& params, UnboundedBuffer* reply) {
  PString result;
  PError err = GenericPop(params[1], ListPosition::tail, result);
  switch (err) {
    case PError_ok:
      FormatBulk(result, reply);
      break;

    default:
      ReplyError(err, reply);
      break;
  }

  return err;
}

static bool blockClient(PClient* client, const PString& key, uint64_t timeout, ListPosition pos,
                         const PString* dstList = 0) {
  auto now = ::Now();

  if (timeout > 0) {
    timeout += now;
  } else {
    timeout = std::numeric_limits<uint64_t>::max();
  }

  return PSTORE.BlockClient(key, client, timeout, pos, dstList);
}

static PError genericBlockedPop(vector<PString>::const_iterator keyBegin, vector<PString>::const_iterator keyEnd,
                                 UnboundedBuffer* reply, ListPosition pos, long timeout,
                                 const PString* target = nullptr, bool withKey = true) {
  for (auto it(keyBegin); it != keyEnd; ++it) {
    PString result;
    PError err = GenericPop(*it, pos, result);

    switch (err) {
      case PError_ok:
        if (withKey) {
          PreFormatMultiBulk(2, reply);
          FormatBulk(*it, reply);
        }
        FormatBulk(result, reply);

        if (target) {
          // the target process
        }

        {
          std::vector<PString> params;
          params.push_back(pos == ListPosition::head ? "lpop" : "rpop");
          params.push_back(*it);

          PClient::Current()->RewriteCmd(params);
        }
        return err;

      case PError_type:
        ReplyError(err, reply);
        return err;

      case PError_notExist:
        break;

      default:
        assert(!!!"Unknow error");
    }
  }

  // Do NOT block if in transaction
  if (PClient::Current() && PClient::Current()->IsFlagOn(ClientFlag_multi)) {
    FormatNull(reply);
    return PError_nop;
  }

  // Put client to the wait-list
  for (auto it(keyBegin); it != keyEnd; ++it) {
    blockClient(PClient::Current(), *it, timeout, pos, target);
  }

  return PError_nop;
}

PError blpop(const vector<PString>& params, UnboundedBuffer* reply) {
  long timeout = 0;
  if (!TryStr2Long(params.back().c_str(), params.back().size(), timeout)) {
    ReplyError(PError_nan, reply);
    return PError_nan;
  }

  timeout *= 1000;

  return genericBlockedPop(++params.begin(), --params.end(), reply, ListPosition::head, timeout);
}

PError brpop(const vector<PString>& params, UnboundedBuffer* reply) {
  long timeout = 0;
  if (!TryStr2Long(params.back().c_str(), params.back().size(), timeout)) {
    ReplyError(PError_nan, reply);
    return PError_nan;
  }

  timeout *= 1000;

  return genericBlockedPop(++params.begin(), --params.end(), reply, ListPosition::tail, timeout);
}

PError lindex(const vector<PString>& params, UnboundedBuffer* reply) {
  PObject* value = nullptr;
  PError err = PSTORE.GetValueByType(params[1], value, PType_list);
  if (err != PError_ok) {
    FormatNull(reply);
    return err;
  }

  long idx = 0;
  if (!TryStr2Long(params[2].c_str(), params[2].size(), idx)) {
    ReplyError(PError_nan, reply);
    return PError_nan;
  }

  auto list = value->CastList();
  const int size = static_cast<int>(list->size());
  if (idx < 0) {
    idx += size;
  }

  if (idx < 0 || idx >= size) {
    FormatNull(reply);
    return PError_ok;
  }

  const PString* result = nullptr;

  if (2 * idx < size) {
    auto it = list->begin();
    std::advance(it, idx);
    result = &*it;
  } else {
    auto it = list->rbegin();
    idx = size - 1 - idx;
    std::advance(it, idx);
    result = &*it;
  }

  FormatBulk(*result, reply);
  return PError_ok;
}

PError lset(const vector<PString>& params, UnboundedBuffer* reply) {
  PObject* value;
  PError err = PSTORE.GetValueByType(params[1], value, PType_list);
  if (err != PError_ok) {
    ReplyError(PError_notExist, reply);
    return err;
  }

  auto list = value->CastList();
  long idx;
  if (!TryStr2Long(params[2].c_str(), params[2].size(), idx)) {
    ReplyError(PError_param, reply);
    return PError_notExist;
  }

  const int size = static_cast<int>(list->size());
  if (idx < 0) {
    idx += size;
  }

  if (idx < 0 || idx >= size) {
    FormatNull(reply);
    return PError_ok;
  }

  PString* result = nullptr;

  if (2 * idx < size) {
    auto it = list->begin();
    std::advance(it, idx);
    result = &*it;
  } else {
    auto it = list->rbegin();
    idx = size - 1 - idx;
    std::advance(it, idx);
    result = &*it;
  }

  *result = params[3];

  FormatOK(reply);
  return PError_ok;
}

PError llen(const vector<PString>& params, UnboundedBuffer* reply) {
  PObject* value;
  PError err = PSTORE.GetValueByType(params[1], value, PType_list);
  if (err != PError_ok) {
    if (err == PError_type) {
      ReplyError(err, reply);
    } else {
      Format0(reply);
    }

    return err;
  }

  auto list = value->CastList();
  FormatInt(static_cast<long>(list->size()), reply);
  return PError_ok;
}

static void Index2Iterator(long start, long end, PList& list, PList::iterator* beginIt, PList::iterator* endIt) {
  assert(start >= 0 && end >= 0 && start <= end);
  assert(end < static_cast<long>(list.size()));

  long size = static_cast<long>(list.size());
  if (beginIt) {
    if (start * 2 < size) {
      *beginIt = list.begin();
      while (start-- > 0) {
        ++*beginIt;
      }
    } else {
      *beginIt = list.end();
      while (start++ < size) {
        --*beginIt;
      }
    }
  }

  if (endIt) {
    if (end * 2 < size) {
      *endIt = list.begin();
      while (end-- > 0) {
        ++*endIt;
      }
    } else {
      *endIt = list.end();
      while (end++ < size) {
        --*endIt;
      }
    }
  }
}

static size_t GetRange(long start, long end, PList& list, PList::iterator* beginIt = nullptr,
                       PList::iterator* endIt = nullptr) {
  size_t rangeLen = 0;
  if (start > end)  { // empty
    if (beginIt) {
      *beginIt = list.end();
    }
    if (endIt) {
      *endIt = list.end();
    }
  } else if (start != 0 || end + 1 != static_cast<long>(list.size())) {
    rangeLen = end - start + 1;
    Index2Iterator(start, end, list, beginIt, endIt);
  } else {
    rangeLen = list.size();
    if (beginIt) {
      *beginIt = list.begin();
    }
    if (endIt) {
      *endIt = --list.end();  // entire list
    }
  }

  return rangeLen;
}

PError ltrim(const vector<PString>& params, UnboundedBuffer* reply) {
  PObject* value;
  PError err = PSTORE.GetValueByType(params[1], value, PType_list);
  if (err != PError_ok) {
    ReplyError(err, reply);
    return err;
  }

  long start, end;
  if (!Strtol(params[2].c_str(), params[2].size(), &start) || !Strtol(params[3].c_str(), params[3].size(), &end)) {
    ReplyError(PError_param, reply);
    return err;
  }

  auto list = value->CastList();
  AdjustIndex(start, end, list->size());

  PList::iterator beginIt, endIt;
  GetRange(start, end, *list, &beginIt, &endIt);

  if (beginIt != list->end()) {
    assert(endIt != list->end());
    list->erase(list->begin(), beginIt);
    list->erase(++endIt, list->end());
  }

  FormatOK(reply);
  return PError_ok;
}

PError lrange(const vector<PString>& params, UnboundedBuffer* reply) {
  PObject* value;
  PError err = PSTORE.GetValueByType(params[1], value, PType_list);
  if (err != PError_ok) {
    ReplyError(err, reply);
    return err;
  }

  long start, end;
  if (!Strtol(params[2].c_str(), params[2].size(), &start) || !Strtol(params[3].c_str(), params[3].size(), &end)) {
    ReplyError(PError_param, reply);
    return err;
  }

  auto list = value->CastList();
  AdjustIndex(start, end, list->size());

  PList::iterator beginIt;
  size_t rangeLen = GetRange(start, end, *list, &beginIt);

  PreFormatMultiBulk(rangeLen, reply);
  if (beginIt != list->end()) {
    while (rangeLen != 0) {
      FormatBulk(beginIt->c_str(), beginIt->size(), reply);
      ++beginIt;
      --rangeLen;
    }
  }

  return PError_ok;
}

PError linsert(const vector<PString>& params, UnboundedBuffer* reply) {
  PObject* value;
  PError err = PSTORE.GetValueByType(params[1], value, PType_list);
  if (err != PError_ok) {
    Format0(reply);
    return err;
  }

  bool before = false;
  if (params[2] == "before") {
    before = true;
  } else if (params[2] == "after") {
    before = false;
  } else {
    ReplyError(PError_param, reply);
    return PError_param;
  }

  auto list = value->CastList();
  PList::iterator it = std::find(list->begin(), list->end(), params[3]);
  if (it == list->end()) {
    FormatInt(-1, reply);
    return PError_notExist;
  }

  if (before) {
    list->insert(it, params[4]);
  } else {
    list->insert(++it, params[4]);
  }

  FormatInt(static_cast<long>(list->size()), reply);
  return PError_ok;
}

PError lrem(const vector<PString>& params, UnboundedBuffer* reply) {
  PObject* value;
  PError err = PSTORE.GetValueByType(params[1], value, PType_list);
  if (err != PError_ok) {
    Format0(reply);
    return err;
  }

  long count;
  if (!Strtol(params[2].c_str(), params[2].size(), &count)) {
    ReplyError(PError_param, reply);
    return err;
  }

  auto list = value->CastList();
  ListPosition start = ListPosition::head;
  if (count < 0) {
    count = -count;
    start = ListPosition::tail;
  } else if (count == 0) {
    count = list->size();  // remove all elements equal to param[3]
  }

  long resultCount = 0;
  if (start == ListPosition::head) {
    auto it = list->begin();
    while (it != list->end() && resultCount < count) {
      if (*it == params[3]) {
        list->erase(it++);
        ++resultCount;
      } else {
        ++it;
      }
    }
  } else {
    auto it = list->rbegin();
    while (it != list->rend() && resultCount < count) {
      if (*it == params[3]) {
        list->erase((++it).base());  // Effective STL, item 28
        ++resultCount;
      } else {
        ++it;
      }
    }
  }

  FormatInt(resultCount, reply);
  return PError_ok;
}

PError rpoplpush(const vector<PString>& params, UnboundedBuffer* reply) {
  PObject* src;
  PError err = PSTORE.GetValueByType(params[1], src, PType_list);
  if (err != PError_ok) {
    FormatNull(reply);
    return err;
  }

  auto srclist = src->CastList();
  assert(!srclist->empty());

  PObject* dst;
  err = PSTORE.GetValueByType(params[2], dst, PType_list);
  if (err != PError_ok) {
    if (err != PError_notExist) {
      ReplyError(err, reply);
      return err;
    }

    dst = PSTORE.SetValue(params[2], PObject::CreateList());
  }

  auto dstlist = dst->CastList();
  dstlist->splice(dstlist->begin(), *srclist, (++srclist->rbegin()).base());

  FormatBulk(*(dstlist->begin()), reply);
  return PError_ok;
}

PError brpoplpush(const vector<PString>& params, UnboundedBuffer* reply) {
  // check timeout format
  long timeout;
  if (!TryStr2Long(params.back().c_str(), params.back().size(), timeout)) {
    ReplyError(PError_nan, reply);
    return PError_nan;
  }

  timeout *= 1000;

  // check target list
  PObject* dst;
  PError err = PSTORE.GetValueByType(params[2], dst, PType_list);
  if (err != PError_ok) {
    if (err != PError_notExist) {
      ReplyError(err, reply);
      return err;
    }
  }

  auto dstKeyIter = --(--params.end());
  return genericBlockedPop(++params.begin(), dstKeyIter, reply, ListPosition::tail, timeout, &*dstKeyIter, false);
}

}  // namespace pikiwidb
