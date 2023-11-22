//  Copyright (c) 2023-present The dory Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "cache/include/cache.h"
#include "pstd_defer.h"

namespace cache {

Status RedisCache::HDel(std::string &key, std::vector<std::string> &fields) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj **fields_obj = (robj **)zcallocate(sizeof(robj *) * fields.size());
  for (unsigned int i = 0; i < fields.size(); ++i) {
    fields_obj[i] = createObject(OBJ_STRING, sdsnewlen(fields[i].data(), fields[i].size()));
  }
  DEFER {
    DecrObjectsRefCount(kobj);
    FreeObjectList(fields_obj, fields.size());
  };
  unsigned long deleted;
  int ret = RcHDel(cache_, kobj, fields_obj, fields.size(), &deleted);
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcHGet failed");
  }

  return Status::OK();
}

Status RedisCache::HSet(std::string &key, std::string &field, std::string &value) {
  int res = RcFreeMemoryIfNeeded(cache_);
  if (C_OK != res) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *fobj = createObject(OBJ_STRING, sdsnewlen(field.data(), field.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  DEFER {
    DecrObjectsRefCount(kobj, fobj, vobj);
  };
  int ret = RcHSet(cache_, kobj, fobj, vobj);
  if (C_OK != ret) {
    return Status::Corruption("RcHSet failed");
  }

  return Status::OK();
}

Status RedisCache::HSetnx(std::string &key, std::string &field, std::string &value) {
  if (C_OK != RcFreeMemoryIfNeeded(cache_)) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *fobj = createObject(OBJ_STRING, sdsnewlen(field.data(), field.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  DEFER {
    DecrObjectsRefCount(kobj, fobj, vobj);
  };
  if (C_OK != RcHSetnx(cache_, kobj, fobj, vobj)) {
    return Status::Corruption("RcHSetnx failed");
  }

  return Status::OK();
}

Status RedisCache::HMSet(std::string &key, std::vector<storage::FieldValue> &fvs) {
  int res = RcFreeMemoryIfNeeded(cache_);
  if (C_OK != res) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  unsigned int items_size = fvs.size() * 2;
  robj **items = (robj **)zcallocate(sizeof(robj *) * items_size);
  for (unsigned int i = 0; i < fvs.size(); ++i) {
    items[i * 2] = createObject(OBJ_STRING, sdsnewlen(fvs[i].field.data(), fvs[i].field.size()));
    items[i * 2 + 1] = createObject(OBJ_STRING, sdsnewlen(fvs[i].value.data(), fvs[i].value.size()));
  }
  DEFER {
    FreeObjectList(items, items_size);
    DecrObjectsRefCount(kobj);
  };
  int ret = RcHMSet(cache_, kobj, items, items_size);
  if (C_OK != ret) {
    return Status::Corruption("RcHMSet failed");
  }
  return Status::OK();
}

Status RedisCache::HGet(std::string &key, std::string &field, std::string *value) {
  sds val;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *fobj = createObject(OBJ_STRING, sdsnewlen(field.data(), field.size()));
  DEFER {
    DecrObjectsRefCount(kobj, fobj);
  };
  int ret = RcHGet(cache_, kobj, fobj, &val);
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    } else if (REDIS_ITEM_NOT_EXIST == ret) {
      return Status::NotFound("field not exist");
    }
    return Status::Corruption("RcHGet failed");
  }

  value->clear();
  value->assign(val, sdslen(val));
  sdsfree(val);

  return Status::OK();
}

Status RedisCache::HMGet(std::string &key, std::vector<std::string> &fields, std::vector<storage::ValueStatus> *vss) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  hitem *items = (hitem *)zcallocate(sizeof(hitem) * fields.size());
  for (unsigned int i = 0; i < fields.size(); ++i) {
    items[i].field = sdsnewlen(fields[i].data(), fields[i].size());
  }
  DEFER {
    FreeHitemList(items, fields.size());
    DecrObjectsRefCount(kobj);
  };

  int ret = RcHMGet(cache_, kobj, items, fields.size());
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcHGet failed");
  }

  vss->clear();
  for (unsigned int i = 0; i < fields.size(); ++i) {
    if (C_OK == items[i].status) {
      vss->push_back({std::string(items[i].value, sdslen(items[i].value)), rocksdb::Status::OK()});
    } else {
      vss->push_back({std::string(), rocksdb::Status::NotFound()});
    }
  }

  return Status::OK();
}

Status RedisCache::HGetall(std::string &key, std::vector<storage::FieldValue> *fvs) {
  hitem *items = nullptr;
  unsigned long items_size = 0;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  int ret = RcHGetAll(cache_, kobj, &items, &items_size);
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcHGet failed");
  }

  for (uint64_t i = 0; i < items_size; ++i) {
    storage::FieldValue fv;
    fv.field.assign(items[i].field, sdslen(items[i].field));
    fv.value.assign(items[i].value, sdslen(items[i].value));
    fvs->push_back(fv);
  }

  FreeHitemList(items, items_size);
  return Status::OK();
}

Status RedisCache::HKeys(std::string &key, std::vector<std::string> *fields) {
  hitem *items = nullptr;
  unsigned long items_size = 0;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  int ret = RcHKeys(cache_, kobj, &items, &items_size);
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcHGet failed");
  }

  for (uint64_t i = 0; i < items_size; ++i) {
    fields->push_back(std::string(items[i].field, sdslen(items[i].field)));
  }

  FreeHitemList(items, items_size);
  return Status::OK();
}

Status RedisCache::HVals(std::string &key, std::vector<std::string> *values) {
  hitem *items = nullptr;
  unsigned long items_size = 0;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  int ret = RcHVals(cache_, kobj, &items, &items_size);
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcHGet failed");
  }

  for (uint64_t i = 0; i < items_size; ++i) {
    values->push_back(std::string(items[i].value, sdslen(items[i].value)));
  }

  FreeHitemList(items, items_size);
  return Status::OK();
}

Status RedisCache::HExists(std::string &key, std::string &field) {
  int is_exist = 0;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *fobj = createObject(OBJ_STRING, sdsnewlen(field.data(), field.size()));
  DEFER {
    DecrObjectsRefCount(kobj, fobj);
  };
  int ret = RcHExists(cache_, kobj, fobj, &is_exist);
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcHGet failed");
  }

  return is_exist ? Status::OK() : Status::NotFound("field not exist");
}

Status RedisCache::HIncrby(std::string &key, std::string &field, int64_t value) {
  int64_t result = 0;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *fobj = createObject(OBJ_STRING, sdsnewlen(field.data(), field.size()));
  DEFER {
    DecrObjectsRefCount(kobj, fobj);
  };
  int ret = RcHIncrby(cache_, kobj, fobj, value, (long long int*)&result);
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcHGet failed");
  }

  return Status::OK();
}

Status RedisCache::HIncrbyfloat(std::string &key, std::string &field, double value) {
  long double result = .0f;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *fobj = createObject(OBJ_STRING, sdsnewlen(field.data(), field.size()));
  DEFER {
    DecrObjectsRefCount(kobj, fobj);
  };
  int ret = RcHIncrbyfloat(cache_, kobj, fobj, value, &result);
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcHGet failed");
  }

  return Status::OK();
}

Status RedisCache::HLen(std::string &key, uint64_t *len) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  int ret = RcHlen(cache_, kobj, reinterpret_cast<unsigned long *>(len));
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcHGet failed");
  }

  return Status::OK();
}

Status RedisCache::HStrlen(std::string &key, std::string &field, uint64_t *len) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *fobj = createObject(OBJ_STRING, sdsnewlen(field.data(), field.size()));
  DEFER {
    DecrObjectsRefCount(kobj, fobj);
  };
  int ret = RcHStrlen(cache_, kobj, fobj, reinterpret_cast<unsigned long *>(len));
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcHGet failed");
  }

  return Status::OK();
}

}  // namespace cache

/* EOF */