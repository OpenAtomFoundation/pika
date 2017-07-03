// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "pika_admin.h"
#include "pika_slot.h"
#include "pika_kv.h"
#include "pika_hash.h"
#include "pika_list.h"
#include "pika_set.h"
#include "pika_zset.h"
#include "pika_bit.h"
#include "pika_hyperloglog.h"
#include "pika_geo.h"

static std::unordered_map<std::string, CmdInfo*> cmd_infos(300);    /* Table for CmdInfo */

//Remember the first arg is the command name
void InitCmdInfoTable() {
  //Admin
  ////Slaveof
  CmdInfo* slaveofptr = new CmdInfo(kCmdNameSlaveof, -3, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSlaveof, slaveofptr));
  ////Trysync
  CmdInfo* trysyncptr = new CmdInfo(kCmdNameTrysync, 5, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSuspend | kCmdFlagsAdminRequire);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameTrysync, trysyncptr));
  CmdInfo* authptr = new CmdInfo(kCmdNameAuth, 2, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameAuth, authptr));
  CmdInfo* bgsaveptr = new CmdInfo(kCmdNameBgsave, 1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSuspend);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameBgsave, bgsaveptr));
  CmdInfo* bgsaveoffptr = new CmdInfo(kCmdNameBgsaveoff, 1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameBgsaveoff, bgsaveoffptr));
  CmdInfo* compactptr = new CmdInfo(kCmdNameCompact, 1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameCompact, compactptr));
  CmdInfo* purgelogptr = new CmdInfo(kCmdNamePurgelogsto, 2, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNamePurgelogsto, purgelogptr));
  CmdInfo* pingptr = new CmdInfo(kCmdNamePing, 1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNamePing, pingptr));
  CmdInfo* selectptr = new CmdInfo(kCmdNameSelect, 2, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSelect, selectptr));
  CmdInfo* flushallptr = new CmdInfo(kCmdNameFlushall, 1, kCmdFlagsWrite | kCmdFlagsSuspend | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameFlushall, flushallptr));
  CmdInfo* readonlyptr = new CmdInfo(kCmdNameReadonly, 2, kCmdFlagsRead | kCmdFlagsSuspend | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameReadonly, readonlyptr));
  CmdInfo* clientptr = new CmdInfo(kCmdNameClient, -2, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameClient, clientptr));
  CmdInfo* shutdownptr = new CmdInfo(kCmdNameShutdown, 1, kCmdFlagsRead | kCmdFlagsLocal | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameShutdown, shutdownptr));
  CmdInfo* infoptr = new CmdInfo(kCmdNameInfo, -1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameInfo, infoptr));
  CmdInfo* configptr = new CmdInfo(kCmdNameConfig, -2, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameConfig, configptr));
  CmdInfo* monitorptr = new CmdInfo(kCmdNameMonitor, -1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameMonitor, monitorptr));
  CmdInfo* dbsizeptr = new CmdInfo(kCmdNameDbsize, 1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameDbsize, dbsizeptr));
  CmdInfo* timeptr = new CmdInfo(kCmdNameTime, 1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameTime, timeptr));
#ifdef TCMALLOC_EXTENSION
  CmdInfo* tcmallocptr = new CmdInfo(kCmdNameTcmalloc, -2, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameTcmalloc, tcmallocptr));
#endif


  //migrate slot
  CmdInfo* slotmgrtslotptr = new CmdInfo(kCmdNameSlotsMgrtSlot, 5, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSlotsMgrtSlot, slotmgrtslotptr));
  CmdInfo* slotmgrttagslotptr = new CmdInfo(kCmdNameSlotsMgrtTagSlot, 5, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSlotsMgrtTagSlot, slotmgrttagslotptr));
  CmdInfo* slotmgrtoneptr = new CmdInfo(kCmdNameSlotsMgrtOne, 5, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSlotsMgrtOne, slotmgrtoneptr));
  CmdInfo* slotmgrttagoneptr = new CmdInfo(kCmdNameSlotsMgrtTagOne, 5, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSlotsMgrtTagOne, slotmgrttagoneptr));
  CmdInfo* slotsinfoptr = new CmdInfo(kCmdNameSlotsInfo, -1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSlotsInfo, slotsinfoptr));
  CmdInfo* slotshashkeyptr = new CmdInfo(kCmdNameSlotsHashKey, -1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSlotsHashKey, slotshashkeyptr));
  CmdInfo* slotsreloadptr = new CmdInfo(kCmdNameSlotsReload, -1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSlotsReload, slotsreloadptr));
  CmdInfo* slotsreloadoffptr = new CmdInfo(kCmdNameSlotsReloadOff, -1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSlotsReloadOff, slotsreloadoffptr));
  CmdInfo* slotsdelptr = new CmdInfo(kCmdNameSlotsDel, -2, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSlotsDel, slotsdelptr));
  CmdInfo* slotsscanptr = new CmdInfo(kCmdNameSlotsScan, -3, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSlotsScan, slotsscanptr));


  //Kv
  ////SetCmd
  CmdInfo* setptr = new CmdInfo(kCmdNameSet, -3, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSet, setptr));
  ////GetCmd
  CmdInfo* getptr = new CmdInfo(kCmdNameGet, 2, kCmdFlagsRead | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameGet, getptr));
  ////DelCmd
  CmdInfo* delptr = new CmdInfo(kCmdNameDel, -2, kCmdFlagsWrite | kCmdFlagsKv); //whethre it should be kCmdFlagsKv
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameDel, delptr));
  ////IncrCmd
  CmdInfo* incrptr = new CmdInfo(kCmdNameIncr, 2, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameIncr, incrptr));
  ////IncrbyCmd
  CmdInfo* incrbyptr = new CmdInfo(kCmdNameIncrby, 3, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameIncrby, incrbyptr));
  ////IncrbyfloatCmd
  CmdInfo* incrbyfloatptr = new CmdInfo(kCmdNameIncrbyfloat, 3, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameIncrbyfloat, incrbyfloatptr));
  ////Decr
  CmdInfo* decrptr = new CmdInfo(kCmdNameDecr, 2, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameDecr, decrptr));
  ////Decrby
  CmdInfo* decrbyptr = new CmdInfo(kCmdNameDecrby, 3, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameDecrby, decrbyptr));
  ////Getset
  CmdInfo* getsetptr = new CmdInfo(kCmdNameGetset, 3, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameGetset, getsetptr));
  ////Append
  CmdInfo* appendptr = new CmdInfo(kCmdNameAppend, 3, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameAppend, appendptr));
  ////Mget
  CmdInfo* mgetptr = new CmdInfo(kCmdNameMget, -2, kCmdFlagsRead | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameMget, mgetptr));
  ////Keys
  CmdInfo* keysptr = new CmdInfo(kCmdNameKeys, 2, kCmdFlagsRead | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameKeys, keysptr));
  ////Setnx
  CmdInfo* setnxptr = new CmdInfo(kCmdNameSetnx, 3, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSetnx, setnxptr));
  ////Setex
  CmdInfo* setexptr = new CmdInfo(kCmdNameSetex, 4, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSetex, setexptr));
  ////MSet
  CmdInfo* msetptr = new CmdInfo(kCmdNameMset, -3, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameMset, msetptr));
  ////MSetnx
  CmdInfo* msetnxptr = new CmdInfo(kCmdNameMsetnx, -3, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameMsetnx, msetnxptr));
  ////Getrange
  CmdInfo* getrangeptr = new CmdInfo(kCmdNameGetrange, 4, kCmdFlagsRead | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameGetrange, getrangeptr));
  ////Setrange
  CmdInfo* setrangeptr = new CmdInfo(kCmdNameSetrange, 4, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSetrange, setrangeptr));
  ////Strlen
  CmdInfo* strlenptr = new CmdInfo(kCmdNameStrlen, 2, kCmdFlagsRead | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameStrlen, strlenptr));
  ////Exists
  CmdInfo* existsptr = new CmdInfo(kCmdNameExists, -2, kCmdFlagsRead | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameExists, existsptr));
  ////Expire
  CmdInfo* expireptr = new CmdInfo(kCmdNameExpire, 3, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameExpire, expireptr));
  ////Pexpire
  CmdInfo* pexpireptr = new CmdInfo(kCmdNamePexpire, 3, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNamePexpire, pexpireptr));
  ////Expireat
  CmdInfo* expireatptr = new CmdInfo(kCmdNameExpireat, 3, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameExpireat, expireatptr));
  ////Pexpireat
  CmdInfo* pexpireatptr = new CmdInfo(kCmdNamePexpireat, 3, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNamePexpireat, pexpireatptr));
  ////Ttl
  CmdInfo* ttlptr = new CmdInfo(kCmdNameTtl, 2, kCmdFlagsRead | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameTtl, ttlptr));
  ////Pttl
  CmdInfo* pttlptr = new CmdInfo(kCmdNameTtl, 2, kCmdFlagsRead | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNamePttl, pttlptr));
  ////Persist
  CmdInfo* persistptr = new CmdInfo(kCmdNamePersist, 2, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNamePersist, persistptr));
  ////Persist
  CmdInfo* typeptr = new CmdInfo(kCmdNameType, 2, kCmdFlagsRead | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameType, typeptr));
  ////Scan
  CmdInfo* scanptr = new CmdInfo(kCmdNameScan, -2, kCmdFlagsRead | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameScan, scanptr));

  //Hash
  ////HDel
  CmdInfo* hdelptr = new CmdInfo(kCmdNameHDel, -3, kCmdFlagsWrite | kCmdFlagsHash);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameHDel, hdelptr));
  ////HSet
  CmdInfo* hsetptr = new CmdInfo(kCmdNameHSet, 4, kCmdFlagsWrite | kCmdFlagsHash);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameHSet, hsetptr));
  ////HGet
  CmdInfo* hgetptr = new CmdInfo(kCmdNameHGet, 3, kCmdFlagsRead | kCmdFlagsHash);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameHGet, hgetptr));
  ////HGetall
  CmdInfo* hgetallptr = new CmdInfo(kCmdNameHGetall, 2, kCmdFlagsRead | kCmdFlagsHash);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameHGetall, hgetallptr));
  ////HExists
  CmdInfo* hexistsptr = new CmdInfo(kCmdNameHExists, 3, kCmdFlagsRead | kCmdFlagsHash);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameHExists, hexistsptr));
  ////HIncrby
  CmdInfo* hincrbyptr = new CmdInfo(kCmdNameHIncrby, 4, kCmdFlagsWrite | kCmdFlagsHash);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameHIncrby, hincrbyptr));
  ////HIncrbyfloat
  CmdInfo* hincrbyfloatptr = new CmdInfo(kCmdNameHIncrbyfloat, 4, kCmdFlagsWrite | kCmdFlagsHash);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameHIncrbyfloat, hincrbyfloatptr));
  ////HKeys
  CmdInfo* hkeysptr = new CmdInfo(kCmdNameHKeys, 2, kCmdFlagsRead | kCmdFlagsHash);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameHKeys, hkeysptr));
  ///HLen
  CmdInfo* hlenptr = new CmdInfo(kCmdNameHLen, 2, kCmdFlagsRead | kCmdFlagsHash);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameHLen, hlenptr));
  ///HMget
  CmdInfo* hmgetptr = new CmdInfo(kCmdNameHMget, -3, kCmdFlagsRead | kCmdFlagsHash);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameHMget, hmgetptr));
  ///HMset
  CmdInfo* hmsetptr = new CmdInfo(kCmdNameHMset, -4, kCmdFlagsWrite | kCmdFlagsHash);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameHMset, hmsetptr));
  ///HMset
  CmdInfo* hsetnxptr = new CmdInfo(kCmdNameHSetnx, 4, kCmdFlagsWrite | kCmdFlagsHash);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameHSetnx, hsetnxptr));
  ///HStrlen
  CmdInfo* hstrlenptr = new CmdInfo(kCmdNameHStrlen, 3, kCmdFlagsRead | kCmdFlagsHash);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameHStrlen, hstrlenptr));
  ///HVals
  CmdInfo* hvalsptr = new CmdInfo(kCmdNameHVals, 2, kCmdFlagsRead | kCmdFlagsHash);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameHVals, hvalsptr));
  ///HScan
  CmdInfo* hscanptr = new CmdInfo(kCmdNameHScan, -3, kCmdFlagsRead | kCmdFlagsHash);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameHScan, hscanptr));

  //List
  ////LIndex
  CmdInfo* lindexptr = new CmdInfo(kCmdNameLIndex, 3, kCmdFlagsRead | kCmdFlagsList);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameLIndex, lindexptr));
  CmdInfo* linsertptr = new CmdInfo(kCmdNameLInsert, 5, kCmdFlagsWrite | kCmdFlagsList);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameLInsert, linsertptr));
  CmdInfo* llenptr = new CmdInfo(kCmdNameLLen, 2, kCmdFlagsRead | kCmdFlagsList);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameLLen, llenptr));
  CmdInfo* lpopptr = new CmdInfo(kCmdNameLPop, 2, kCmdFlagsWrite | kCmdFlagsList);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameLPop, lpopptr));
  CmdInfo* lpushptr = new CmdInfo(kCmdNameLPush, -3, kCmdFlagsWrite | kCmdFlagsList);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameLPush, lpushptr));
  CmdInfo* lpushxptr = new CmdInfo(kCmdNameLPushx, 3, kCmdFlagsWrite | kCmdFlagsList);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameLPushx, lpushxptr));
  CmdInfo* lrangeptr = new CmdInfo(kCmdNameLRange, 4, kCmdFlagsRead | kCmdFlagsList);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameLRange, lrangeptr));
  CmdInfo* lremptr = new CmdInfo(kCmdNameLRem, 4, kCmdFlagsWrite | kCmdFlagsList);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameLRem, lremptr));
  CmdInfo* lsetptr = new CmdInfo(kCmdNameLSet, 4, kCmdFlagsWrite | kCmdFlagsList);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameLSet, lsetptr));
  CmdInfo* ltrimptr = new CmdInfo(kCmdNameLTrim, 4, kCmdFlagsWrite | kCmdFlagsList);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameLTrim, ltrimptr));
  CmdInfo* rpopptr = new CmdInfo(kCmdNameRPop, 2, kCmdFlagsWrite | kCmdFlagsList);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameRPop, rpopptr));
  CmdInfo* rpoplpushptr = new CmdInfo(kCmdNameRPopLPush, 3, kCmdFlagsWrite | kCmdFlagsList);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameRPopLPush, rpoplpushptr));
  CmdInfo* rpushptr = new CmdInfo(kCmdNameRPush, -3, kCmdFlagsWrite | kCmdFlagsList);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameRPush, rpushptr));
  CmdInfo* rpushxptr = new CmdInfo(kCmdNameRPushx, 3, kCmdFlagsWrite | kCmdFlagsList);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameRPushx, rpushxptr));

  //Zset
  ////ZAdd
  CmdInfo* zaddptr = new CmdInfo(kCmdNameZAdd, -4, kCmdFlagsWrite | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZAdd, zaddptr));
  ////ZCard
  CmdInfo* zcardptr = new CmdInfo(kCmdNameZCard, 2, kCmdFlagsRead | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZCard, zcardptr));
  ////ZScan
  CmdInfo* zscanptr = new CmdInfo(kCmdNameZScan, -3, kCmdFlagsRead | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZScan, zscanptr));
  ////ZIncrby
  CmdInfo* zincrbyptr = new CmdInfo(kCmdNameZIncrby, 4, kCmdFlagsWrite | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZIncrby, zincrbyptr));
  ////ZRange
  CmdInfo* zrangeptr = new CmdInfo(kCmdNameZRange, -4, kCmdFlagsRead | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZRange, zrangeptr));
  ////ZRevrange
  CmdInfo* zrevrangeptr = new CmdInfo(kCmdNameZRevrange, -4, kCmdFlagsRead | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZRevrange, zrevrangeptr));
  ////ZRangebyscore
  CmdInfo* zrangebyscoreptr = new CmdInfo(kCmdNameZRangebyscore, -4, kCmdFlagsRead | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZRangebyscore, zrangebyscoreptr));
  ////ZRevrangebyscore
  CmdInfo* zrevrangebyscoreptr = new CmdInfo(kCmdNameZRevrangebyscore, -4, kCmdFlagsRead | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZRevrangebyscore, zrevrangebyscoreptr));
  ////ZCount
  CmdInfo* zcountptr = new CmdInfo(kCmdNameZCount, 4, kCmdFlagsRead | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZCount, zcountptr));
  ////ZRem
  CmdInfo* zremptr = new CmdInfo(kCmdNameZRem, -3, kCmdFlagsWrite | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZRem, zremptr));
  ////ZUnionstore
  CmdInfo* zunionstoreptr = new CmdInfo(kCmdNameZUnionstore, -4, kCmdFlagsWrite | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZUnionstore, zunionstoreptr));
  ////ZInterstore
  CmdInfo* zinterstoreptr = new CmdInfo(kCmdNameZInterstore, -4, kCmdFlagsWrite | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZInterstore, zinterstoreptr));
  ////ZRank
  CmdInfo* zrankptr = new CmdInfo(kCmdNameZRank, 3, kCmdFlagsRead | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZRank, zrankptr));
  ////ZRevrank
  CmdInfo* zrevrankptr = new CmdInfo(kCmdNameZRevrank, 3, kCmdFlagsRead | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZRevrank, zrevrankptr));
  ////ZScore
  CmdInfo* zscoreptr = new CmdInfo(kCmdNameZScore, 3, kCmdFlagsRead | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZScore, zscoreptr));
  ////ZRangebylex
  CmdInfo* zrangebylexptr = new CmdInfo(kCmdNameZRangebylex, -4, kCmdFlagsRead | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZRangebylex, zrangebylexptr));
  ////ZRevrangebylex
  CmdInfo* zrevrangebylexptr = new CmdInfo(kCmdNameZRevrangebylex, -4, kCmdFlagsRead | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZRevrangebylex, zrevrangebylexptr));
  ////ZLexcount
  CmdInfo* zlexcountptr = new CmdInfo(kCmdNameZLexcount, 4, kCmdFlagsRead | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZLexcount, zlexcountptr));
  ////ZRemrangebyrank
  CmdInfo* zremrangebyrankptr = new CmdInfo(kCmdNameZRemrangebyrank, 4, kCmdFlagsWrite | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZRemrangebyrank, zremrangebyrankptr));
  ////ZRemrangebyscore
  CmdInfo* zremrangebyscoreptr = new CmdInfo(kCmdNameZRemrangebyscore, 4, kCmdFlagsWrite | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZRemrangebyscore, zremrangebyscoreptr));
  ////ZRemrangebylex
  CmdInfo* zremrangebylexptr = new CmdInfo(kCmdNameZRemrangebylex, 4, kCmdFlagsWrite | kCmdFlagsZset);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameZRemrangebylex, zremrangebylexptr));

  //Set
  ////SAdd
  CmdInfo* saddptr = new CmdInfo(kCmdNameSAdd, -3, kCmdFlagsWrite | kCmdFlagsSet);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSAdd, saddptr));
  ////SPop
  CmdInfo* spopptr = new CmdInfo(kCmdNameSPop, 2, kCmdFlagsWrite | kCmdFlagsSet);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSPop, spopptr));
  ////SCard
  CmdInfo* scardptr = new CmdInfo(kCmdNameSCard, 2, kCmdFlagsRead | kCmdFlagsSet);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSCard, scardptr));
  ////SMembers
  CmdInfo* smembersptr = new CmdInfo(kCmdNameSMembers, 2, kCmdFlagsRead | kCmdFlagsSet);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSMembers, smembersptr));
  ////SMembers
  CmdInfo* sscanptr = new CmdInfo(kCmdNameSScan, -3, kCmdFlagsRead | kCmdFlagsSet);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSScan, sscanptr));
  ////SRem
  CmdInfo* sremptr = new CmdInfo(kCmdNameSRem, -3, kCmdFlagsWrite | kCmdFlagsSet);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSRem, sremptr));
  ////SUnion
  CmdInfo* sunionptr = new CmdInfo(kCmdNameSUnion, -2, kCmdFlagsRead | kCmdFlagsSet);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSUnion, sunionptr));
  ////SUnion
  CmdInfo* sunionstoreptr = new CmdInfo(kCmdNameSUnionstore, -3, kCmdFlagsWrite | kCmdFlagsSet);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSUnionstore, sunionstoreptr));
  ////SInter
  CmdInfo* sinterptr = new CmdInfo(kCmdNameSInter, -2, kCmdFlagsRead | kCmdFlagsSet);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSInter, sinterptr));
  ////SInterstore
  CmdInfo* sinterstoreptr = new CmdInfo(kCmdNameSInterstore, -3, kCmdFlagsWrite | kCmdFlagsSet);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSInterstore, sinterstoreptr));
  ////SIsmember
  CmdInfo* sismemberptr = new CmdInfo(kCmdNameSIsmember, 3, kCmdFlagsRead | kCmdFlagsSet);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSIsmember, sismemberptr));
  ////SDiff
  CmdInfo* sdiffptr = new CmdInfo(kCmdNameSDiff, -2, kCmdFlagsRead | kCmdFlagsSet);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSDiff, sdiffptr));
  ////SDiffstore
  CmdInfo* sdiffstoreptr = new CmdInfo(kCmdNameSDiffstore, -3, kCmdFlagsWrite | kCmdFlagsSet);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSDiffstore, sdiffstoreptr));
  ////SMove
  CmdInfo* smoveptr = new CmdInfo(kCmdNameSMove, 4, kCmdFlagsWrite | kCmdFlagsSet);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSMove, smoveptr));
  ////SRandmember
  CmdInfo* srandmemberptr = new CmdInfo(kCmdNameSRandmember, -2, kCmdFlagsRead | kCmdFlagsSet);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSRandmember, srandmemberptr));

  //BitMap
  ////BitSet
  CmdInfo* bitsetptr = new CmdInfo(kCmdNameBitSet, 4, kCmdFlagsWrite | kCmdFlagsBit);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameBitSet, bitsetptr)).second;
  ////BitGet
  CmdInfo* bitgetptr = new CmdInfo(kCmdNameBitGet, 3, kCmdFlagsRead | kCmdFlagsBit);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameBitGet, bitgetptr)).second;
  ////BitPos
  CmdInfo* bitposptr = new CmdInfo(kCmdNameBitPos, -3, kCmdFlagsRead | kCmdFlagsBit);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameBitPos, bitposptr)).second;
  ////BitOp
  CmdInfo* bitopptr = new CmdInfo(kCmdNameBitOp, -3, kCmdFlagsWrite | kCmdFlagsBit);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameBitOp, bitopptr)).second;
  ////BitCount
  CmdInfo* bitcountptr = new CmdInfo(kCmdNameBitCount, -2, kCmdFlagsRead | kCmdFlagsBit);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameBitCount, bitcountptr)).second;

  //HyperLogLog
  ////PfAdd
  CmdInfo* pfaddptr = new CmdInfo(kCmdNamePfAdd, -2, kCmdFlagsWrite | kCmdFlagsHyperLogLog);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNamePfAdd, pfaddptr));
  ////PfCount
  CmdInfo* pfcountptr = new CmdInfo(kCmdNamePfCount, -2, kCmdFlagsRead | kCmdFlagsHyperLogLog);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNamePfCount, pfcountptr));
  ////PfMerge
  CmdInfo* pfmergeptr = new CmdInfo(kCmdNamePfMerge, -3, kCmdFlagsWrite | kCmdFlagsHyperLogLog);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNamePfMerge, pfmergeptr));

  //GEO
  ////GeoAdd
  CmdInfo* geoaddptr = new CmdInfo(kCmdNameGeoAdd, -5, kCmdFlagsWrite | kCmdFlagsGeo);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameGeoAdd, geoaddptr));
  ////GeoPos
  CmdInfo* geoposptr = new CmdInfo(kCmdNameGeoPos, -2, kCmdFlagsRead | kCmdFlagsGeo);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameGeoPos, geoposptr));
  ////GeoDist
  CmdInfo* geodistptr = new CmdInfo(kCmdNameGeoDist, -4, kCmdFlagsRead | kCmdFlagsGeo);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameGeoDist, geodistptr));
  ////GeoHash
  CmdInfo* geohashptr = new CmdInfo(kCmdNameGeoHash, -2, kCmdFlagsRead | kCmdFlagsGeo);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameGeoHash, geohashptr));
  ////GeoRadius
  CmdInfo* georadiusptr = new CmdInfo(kCmdNameGeoRadius, -6, kCmdFlagsRead | kCmdFlagsGeo);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameGeoRadius, georadiusptr));
  ////GeoRadiusByMember
  CmdInfo* georadiusbymemberptr = new CmdInfo(kCmdNameGeoRadiusByMember, -5, kCmdFlagsRead | kCmdFlagsGeo);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameGeoRadiusByMember, georadiusbymemberptr));
}

void DestoryCmdInfoTable() {
  std::unordered_map<std::string, CmdInfo*>::const_iterator it = cmd_infos.begin();
  for (; it != cmd_infos.end(); ++it) {
    delete it->second;
  }
}

const CmdInfo* GetCmdInfo(const std::string& opt) {
  std::unordered_map<std::string, CmdInfo*>::const_iterator it = cmd_infos.find(opt);
  if (it != cmd_infos.end()) {
    return it->second;
  }
  return NULL;
}

void InitCmdTable(std::unordered_map<std::string, Cmd*> *cmd_table) {
  //Admin
  ////Slaveof
  Cmd* slaveofptr = new SlaveofCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSlaveof, slaveofptr));
  ////Trysync
  Cmd* trysyncptr = new TrysyncCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameTrysync, trysyncptr));
  Cmd* authptr = new AuthCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameAuth, authptr));
  Cmd* bgsaveptr = new BgsaveCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameBgsave, bgsaveptr));
  Cmd* bgsaveoffptr = new BgsaveoffCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameBgsaveoff, bgsaveoffptr));
  Cmd* compactptr = new CompactCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameCompact, compactptr));
  Cmd* purgelogptr = new PurgelogstoCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePurgelogsto, purgelogptr));
  Cmd* pingptr = new PingCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePing, pingptr));
  Cmd* selectptr = new SelectCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSelect, selectptr));
  Cmd* flushallptr = new FlushallCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameFlushall, flushallptr));
  Cmd* readonlyptr = new ReadonlyCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameReadonly, readonlyptr));
  Cmd* clientptr = new ClientCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameClient, clientptr));
  Cmd* shutdownptr = new ShutdownCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameShutdown, shutdownptr));
  Cmd* infoptr = new InfoCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameInfo, infoptr));
  Cmd* configptr = new ConfigCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameConfig, configptr));
  Cmd* monitorptr = new MonitorCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameMonitor, monitorptr));
  Cmd* dbsizeptr = new DbsizeCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameDbsize, dbsizeptr));
  Cmd* timeptr = new TimeCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameTime, timeptr));
#ifdef TCMALLOC_EXTENSION
  Cmd* tcmallocptr = new TcmallocCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameTcmalloc, tcmallocptr));
#endif

  //migrate slot
  Cmd* slotmgrtslotptr = new SlotsMgrtTagSlotCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSlotsMgrtSlot, slotmgrtslotptr));
  Cmd* slotmgrttagslotptr = new SlotsMgrtTagSlotCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSlotsMgrtTagSlot, slotmgrttagslotptr));
  Cmd* slotmgrtoneptr = new SlotsMgrtTagOneCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSlotsMgrtOne, slotmgrtoneptr));
  Cmd* slotmgrttagoneptr = new SlotsMgrtTagOneCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSlotsMgrtTagOne, slotmgrttagoneptr));
  Cmd* slotsinfoptr = new SlotsInfoCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSlotsInfo, slotsinfoptr));
  Cmd* slotshashkeyptr = new SlotsHashKeyCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSlotsHashKey, slotshashkeyptr));
  Cmd* slotsreloadptr = new SlotsReloadCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSlotsReload, slotsreloadptr));
  Cmd* slotsreloadoffptr = new SlotsReloadOffCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSlotsReloadOff, slotsreloadoffptr));
  Cmd* slotsdelptr = new SlotsDelCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSlotsDel, slotsdelptr));
  Cmd* slotsscanptr = new SlotsScanCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSlotsScan, slotsscanptr));

  //Kv
  ////SetCmd
  Cmd* setptr = new SetCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSet, setptr));
  ////GetCmd
  Cmd* getptr = new GetCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGet, getptr));
  ////DelCmd
  Cmd* delptr = new DelCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameDel, delptr));
  ////IncrCmd
  Cmd* incrptr = new IncrCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameIncr, incrptr));
  ////IncrbyCmd
  Cmd* incrbyptr = new IncrbyCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameIncrby, incrbyptr));
  ////IncrbyfloatCmd
  Cmd* incrbyfloatptr = new IncrbyfloatCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameIncrbyfloat, incrbyfloatptr));
  ////DecrCmd
  Cmd* decrptr = new DecrCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameDecr, decrptr));
  ////DecrbyCmd
  Cmd* decrbyptr = new DecrbyCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameDecrby, decrbyptr));
  ////GetsetCmd
  Cmd* getsetptr = new GetsetCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGetset, getsetptr));
  ////AppendCmd
  Cmd* appendptr = new AppendCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameAppend, appendptr));
  ////MgetCmd
  Cmd* mgetptr = new MgetCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameMget, mgetptr));
  ////KeysCmd
  Cmd* keysptr = new KeysCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameKeys, keysptr));
  ////SetnxCmd
  Cmd* setnxptr = new SetnxCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSetnx, setnxptr));
  ////SetexCmd
  Cmd* setexptr = new SetexCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSetex, setexptr));
  ////MSetCmd
  Cmd* msetptr = new MsetCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameMset, msetptr));
  ////MSetCmd
  Cmd* msetnxptr = new MsetnxCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameMsetnx, msetnxptr));
  ////GetrangeCmd
  Cmd* getrangeptr = new GetrangeCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGetrange, getrangeptr));
  ////SetrangeCmd
  Cmd* setrangeptr = new SetrangeCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSetrange, setrangeptr));
  ////StrlenCmd
  Cmd* strlenptr = new StrlenCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameStrlen, strlenptr));
  ////ExistsCmd
  Cmd* existsptr = new ExistsCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameExists, existsptr));
  ////ExpireCmd
  Cmd* expireptr = new ExpireCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameExpire, expireptr));
  ////PexpireCmd
  Cmd* pexpireptr = new PexpireCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePexpire, pexpireptr));
  ////ExpireatCmd
  Cmd* expireatptr = new ExpireatCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameExpireat, expireatptr));
  ////PexpireatCmd
  Cmd* pexpireatptr = new PexpireatCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePexpireat, pexpireatptr));
  ////TtlCmd
  Cmd* ttlptr = new TtlCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameTtl, ttlptr));
  ////PttlCmd
  Cmd* pttlptr = new PttlCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePttl, pttlptr));
  ////PersistCmd
  Cmd* persistptr = new PersistCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePersist, persistptr));
  ////TypeCmd
  Cmd* typeptr = new TypeCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameType, typeptr));
  ////ScanCmd
  Cmd* scanptr = new ScanCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameScan, scanptr));
  //Hash
  ////HDelCmd
  Cmd* hdelptr = new HDelCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHDel, hdelptr));
  ////HSetCmd
  Cmd* hsetptr = new HSetCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHSet, hsetptr));
  ////HGetCmd
  Cmd* hgetptr = new HGetCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHGet, hgetptr));
  ////HGetallCmd
  Cmd* hgetallptr = new HGetallCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHGetall, hgetallptr));
  ////HExistsCmd
  Cmd* hexistsptr = new HExistsCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHExists, hexistsptr));
  ////HIncrbyCmd
  Cmd* hincrbyptr = new HIncrbyCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHIncrby, hincrbyptr));
  ////HIncrbyfloatCmd
  Cmd* hincrbyfloatptr = new HIncrbyfloatCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHIncrbyfloat, hincrbyfloatptr));
  ////HKeysCmd
  Cmd* hkeysptr = new HKeysCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHKeys, hkeysptr));
  ////HLenCmd
  Cmd* hlenptr = new HLenCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHLen, hlenptr));
  ////HMgetCmd
  Cmd* hmgetptr = new HMgetCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHMget, hmgetptr));
  ////HMsetCmd
  Cmd* hmsetptr = new HMsetCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHMset, hmsetptr));
  ////HSetnxCmd
  Cmd* hsetnxptr = new HSetnxCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHSetnx, hsetnxptr));
  ////HStrlenCmd
  Cmd* hstrlenptr = new HStrlenCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHStrlen, hstrlenptr));
  ////HValsCmd
  Cmd* hvalsptr = new HValsCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHVals, hvalsptr));
  ////HScanCmd
  Cmd* hscanptr = new HScanCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHScan, hscanptr));
  //List
  Cmd* lindexptr = new LIndexCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLIndex, lindexptr));
  Cmd* linsertptr = new LInsertCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLInsert, linsertptr));
  Cmd* llenptr = new LLenCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLLen, llenptr));
  Cmd* lpopptr = new LPopCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLPop, lpopptr));
  Cmd* lpushptr = new LPushCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLPush, lpushptr));
  Cmd* lpushxptr = new LPushxCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLPushx, lpushxptr));
  Cmd* lrangeptr = new LRangeCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLRange, lrangeptr));
  Cmd* lremptr = new LRemCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLRem, lremptr));
  Cmd* lsetptr = new LSetCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLSet, lsetptr));
  Cmd* ltrimptr = new LTrimCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLTrim, ltrimptr));
  Cmd* rpopptr = new RPopCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameRPop, rpopptr));
  Cmd* rpoplpushptr = new RPopLPushCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameRPopLPush, rpoplpushptr));
  Cmd* rpushptr = new RPushCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameRPush, rpushptr));
  Cmd* rpushxptr = new RPushxCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameRPushx, rpushxptr));

  //Zset
  ////ZAddCmd
  Cmd* zaddptr = new ZAddCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZAdd, zaddptr));
  ////ZCardCmd
  Cmd* zcardptr = new ZCardCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZCard, zcardptr));
  ////ZScanCmd
  Cmd* zscanptr = new ZScanCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZScan, zscanptr));
  ////ZIncrbyCmd
  Cmd* zincrbyptr = new ZIncrbyCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZIncrby, zincrbyptr));
  ////ZRangeCmd
  Cmd* zrangeptr = new ZRangeCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRange, zrangeptr));
  ////ZRevrangeCmd
  Cmd* zrevrangeptr = new ZRevrangeCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRevrange, zrevrangeptr));
  ////ZRangebyscoreCmd
  Cmd* zrangebyscoreptr = new ZRangebyscoreCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRangebyscore, zrangebyscoreptr));
  ////ZRevrangebyscoreCmd
  Cmd* zrevrangebyscoreptr = new ZRevrangebyscoreCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRevrangebyscore, zrevrangebyscoreptr));
  ////ZCountCmd
  Cmd* zcountptr = new ZCountCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZCount, zcountptr));
  ////ZRemCmd
  Cmd* zremptr = new ZRemCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRem, zremptr));
  ////ZUnionstoreCmd
  Cmd* zunionstoreptr = new ZUnionstoreCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZUnionstore, zunionstoreptr));
  ////ZInterstoreCmd
  Cmd* zinterstoreptr = new ZInterstoreCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZInterstore, zinterstoreptr));
  ////ZRankCmd
  Cmd* zrankptr = new ZRankCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRank, zrankptr));
  ////ZRevrankCmd
  Cmd* zrevrankptr = new ZRevrankCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRevrank, zrevrankptr));
  ////ZScoreCmd
  Cmd* zscoreptr = new ZScoreCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZScore, zscoreptr));
  ////ZRangebylexCmd
  Cmd* zrangebylexptr = new ZRangebylexCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRangebylex, zrangebylexptr));
  ////ZRevrangebylexCmd
  Cmd* zrevrangebylexptr = new ZRevrangebylexCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRevrangebylex, zrevrangebylexptr));
  ////ZLexcountCmd
  Cmd* zlexcountptr = new ZLexcountCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZLexcount, zlexcountptr));
  ////ZRemrangebyrankCmd
  Cmd* zremrangebyrankptr = new ZRemrangebyrankCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRemrangebyrank, zremrangebyrankptr));
  ////ZRemrangebyscoreCmd
  Cmd* zremrangebyscoreptr = new ZRemrangebyscoreCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRemrangebyscore, zremrangebyscoreptr));
  ////ZRemrangebylexCmd
  Cmd* zremrangebylexptr = new ZRemrangebylexCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRemrangebylex, zremrangebylexptr));

  //Set
  ////SAddCmd
  Cmd* saddptr = new SAddCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSAdd, saddptr));
  ////SPopCmd
  Cmd* spopptr = new SPopCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSPop, spopptr));
  ////SCardCmd
  Cmd* scardptr = new SCardCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSCard, scardptr));
  ////SMembersCmd
  Cmd* smembersptr = new SMembersCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSMembers, smembersptr));
  ////SScanCmd
  Cmd* sscanptr = new SScanCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSScan, sscanptr));
  ////SScanCmd
  Cmd* sremptr = new SRemCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSRem, sremptr));
  ////SUnionCmd
  Cmd* sunionptr = new SUnionCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSUnion, sunionptr));
  ////SUnionstoreCmd
  Cmd* sunionstoreptr = new SUnionstoreCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSUnionstore, sunionstoreptr));
  ////SInterCmd
  Cmd* sinterptr = new SInterCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSInter, sinterptr));
  ////SInterstoreCmd
  Cmd* sinterstoreptr = new SInterstoreCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSInterstore, sinterstoreptr));
  ////SIsmemberCmd
  Cmd* sismemberptr = new SIsmemberCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSIsmember, sismemberptr));
  ////SDiffCmd
  Cmd* sdiffptr = new SDiffCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSDiff, sdiffptr));
  ////SDiffstoreCmd
  Cmd* sdiffstoreptr = new SDiffstoreCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSDiffstore, sdiffstoreptr));
  ////SMoveCmd
  Cmd* smoveptr = new SMoveCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSMove, smoveptr));
  ////SRandmemberCmd
  Cmd* srandmemberptr = new SRandmemberCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSRandmember, srandmemberptr));

  //BitMap
  ////bitsetCmd
  Cmd* bitsetptr = new BitSetCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameBitSet, bitsetptr));
  ////bitgetCmd
  Cmd* bitgetptr = new BitGetCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameBitGet, bitgetptr));
  ////bitcountCmd
  Cmd* bitcountptr = new BitCountCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameBitCount, bitcountptr));
  ////bitposCmd
  Cmd* bitposptr = new BitPosCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameBitPos, bitposptr));
  ////bitopCmd
  Cmd* bitopptr = new BitOpCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameBitOp, bitopptr));

  //HyperLogLog
  ////pfaddCmd
  Cmd * pfaddptr = new PfAddCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePfAdd, pfaddptr));
  ////pfcountCmd
  Cmd * pfcountptr = new PfCountCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePfCount, pfcountptr));
  ////pfmergeCmd
  Cmd * pfmergeptr = new PfMergeCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePfMerge, pfmergeptr));

  //GEO
  ////GepAdd
  Cmd * geoaddptr = new GeoAddCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGeoAdd, geoaddptr));
  ////GeoPos
  Cmd * geoposptr = new GeoPosCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGeoPos, geoposptr));
  ////GeoDist
  Cmd * geodistptr = new GeoDistCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGeoDist, geodistptr));
  ////GeoHash
  Cmd * geohashptr = new GeoHashCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGeoHash, geohashptr));
  ////GeoRadius
  Cmd * georadiusptr = new GeoRadiusCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGeoRadius, georadiusptr));
  ////GeoRadiusByMember
  Cmd * georadiusbymemberptr = new GeoRadiusByMemberCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGeoRadiusByMember, georadiusbymemberptr));
}

Cmd* GetCmdFromTable(const std::string& opt,
    const std::unordered_map<std::string, Cmd*> &cmd_table) {
  std::unordered_map<std::string, Cmd*>::const_iterator it = cmd_table.find(opt);
  if (it != cmd_table.end()) {
    return it->second;
  }
  return NULL;
}

void DestoryCmdTable(std::unordered_map<std::string, Cmd*> &cmd_table) {
  std::unordered_map<std::string, Cmd*>::const_iterator it = cmd_table.begin();
  for (; it != cmd_table.end(); ++it) {
    delete it->second;
  }
}
