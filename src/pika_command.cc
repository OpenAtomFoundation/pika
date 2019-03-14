// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_command.h"

#include "include/pika_kv.h"
#include "include/pika_bit.h"
#include "include/pika_set.h"
#include "include/pika_geo.h"
#include "include/pika_list.h"
#include "include/pika_zset.h"
#include "include/pika_hash.h"
#include "include/pika_admin.h"
#include "include/pika_pubsub.h"
#include "include/pika_server.h"
#include "include/pika_hyperloglog.h"

extern PikaServer* g_pika_server;

void InitCmdTable(std::unordered_map<std::string, Cmd*> *cmd_table) {
  //Admin
  ////Slaveof
  Cmd* slaveofptr = new SlaveofCmd(kCmdNameSlaveof, -3, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSlaveof, slaveofptr));
  Cmd* authptr = new AuthCmd(kCmdNameAuth, 2, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameAuth, authptr));
  Cmd* bgsaveptr = new BgsaveCmd(kCmdNameBgsave, 1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSuspend);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameBgsave, bgsaveptr));
  Cmd* compactptr = new CompactCmd(kCmdNameCompact, -1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameCompact, compactptr));
  Cmd* purgelogptr = new PurgelogstoCmd(kCmdNamePurgelogsto, 2, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePurgelogsto, purgelogptr));
  Cmd* pingptr = new PingCmd(kCmdNamePing, 1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePing, pingptr));
  Cmd* selectptr = new SelectCmd(kCmdNameSelect, 2, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSelect, selectptr));
  Cmd* flushallptr = new FlushallCmd(kCmdNameFlushall, 1, kCmdFlagsWrite | kCmdFlagsSuspend | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameFlushall, flushallptr));
  Cmd* flushdbptr = new FlushdbCmd(kCmdNameFlushdb, 2, kCmdFlagsWrite | kCmdFlagsSuspend | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameFlushdb, flushdbptr));
  Cmd* clientptr = new ClientCmd(kCmdNameClient, -2, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameClient, clientptr));
  Cmd* shutdownptr = new ShutdownCmd(kCmdNameShutdown, 1, kCmdFlagsRead | kCmdFlagsLocal | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameShutdown, shutdownptr));
  Cmd* infoptr = new InfoCmd(kCmdNameInfo, -1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameInfo, infoptr));
  Cmd* configptr = new ConfigCmd(kCmdNameConfig, -2, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameConfig, configptr));
  Cmd* monitorptr = new MonitorCmd(kCmdNameMonitor, -1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameMonitor, monitorptr));
  Cmd* dbsizeptr = new DbsizeCmd(kCmdNameDbsize, 1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameDbsize, dbsizeptr));
  Cmd* timeptr = new TimeCmd(kCmdNameTime, 1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameTime, timeptr));
  Cmd* delbackupptr = new DelbackupCmd(kCmdNameDelbackup, 1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameDelbackup, delbackupptr));
  Cmd* echoptr = new EchoCmd(kCmdNameEcho, 2, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameEcho, echoptr));
  Cmd* scandbptr = new ScandbCmd(kCmdNameScandb, -1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameScandb, scandbptr));
  Cmd* slowlogptr = new SlowlogCmd(kCmdNameSlowlog, -2, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSlowlog, slowlogptr));
#ifdef TCMALLOC_EXTENSION
  Cmd* tcmallocptr = new TcmallocCmd(kCmdNameTcmalloc, -2, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameTcmalloc, tcmallocptr));
#endif

  //Kv
  ////SetCmd
  Cmd* setptr = new SetCmd(kCmdNameSet, -3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSet, setptr));
  ////GetCmd
  Cmd* getptr = new GetCmd(kCmdNameGet, 2, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGet, getptr));
  ////DelCmd
  Cmd* delptr = new DelCmd(kCmdNameDel, -2, kCmdFlagsWrite | kCmdFlagsMultiPartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameDel, delptr));
  ////IncrCmd
  Cmd* incrptr = new IncrCmd(kCmdNameIncr, 2, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameIncr, incrptr));
  ////IncrbyCmd
  Cmd* incrbyptr = new IncrbyCmd(kCmdNameIncrby, 3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameIncrby, incrbyptr));
  ////IncrbyfloatCmd
  Cmd* incrbyfloatptr = new IncrbyfloatCmd(kCmdNameIncrbyfloat, 3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameIncrbyfloat, incrbyfloatptr));
  ////DecrCmd
  Cmd* decrptr = new DecrCmd(kCmdNameDecr, 2, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameDecr, decrptr));
  ////DecrbyCmd
  Cmd* decrbyptr = new DecrbyCmd(kCmdNameDecrby, 3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameDecrby, decrbyptr));
  ////GetsetCmd
  Cmd* getsetptr = new GetsetCmd(kCmdNameGetset, 3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGetset, getsetptr));
  ////AppendCmd
  Cmd* appendptr = new AppendCmd(kCmdNameAppend, 3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameAppend, appendptr));
  ////MgetCmd
  Cmd* mgetptr = new MgetCmd(kCmdNameMget, -2, kCmdFlagsRead | kCmdFlagsMultiPartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameMget, mgetptr));
  ////KeysCmd
  Cmd* keysptr = new KeysCmd(kCmdNameKeys, -2, kCmdFlagsRead | kCmdFlagsMultiPartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameKeys, keysptr));
  ////SetnxCmd
  Cmd* setnxptr = new SetnxCmd(kCmdNameSetnx, 3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSetnx, setnxptr));
  ////SetexCmd
  Cmd* setexptr = new SetexCmd(kCmdNameSetex, 4, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSetex, setexptr));
  ////PsetexCmd
  Cmd* psetexptr = new PsetexCmd(kCmdNamePsetex, 4, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePsetex, psetexptr));
  ////DelvxCmd
  Cmd* delvxptr = new DelvxCmd(kCmdNameDelvx, 3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameDelvx, delvxptr));
  ////MSetCmd
  Cmd* msetptr = new MsetCmd(kCmdNameMset, -3, kCmdFlagsWrite | kCmdFlagsMultiPartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameMset, msetptr));
  ////MSetnxCmd
  Cmd* msetnxptr = new MsetnxCmd(kCmdNameMsetnx, -3, kCmdFlagsWrite | kCmdFlagsMultiPartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameMsetnx, msetnxptr));
  ////GetrangeCmd
  Cmd* getrangeptr = new GetrangeCmd(kCmdNameGetrange, 4, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGetrange, getrangeptr));
  ////SetrangeCmd
  Cmd* setrangeptr = new SetrangeCmd(kCmdNameSetrange, 4, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSetrange, setrangeptr));
  ////StrlenCmd
  Cmd* strlenptr = new StrlenCmd(kCmdNameStrlen, 2, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameStrlen, strlenptr));
  ////ExistsCmd
  Cmd* existsptr = new ExistsCmd(kCmdNameExists, -2, kCmdFlagsRead | kCmdFlagsMultiPartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameExists, existsptr));
  ////ExpireCmd
  Cmd* expireptr = new ExpireCmd(kCmdNameExpire, 3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameExpire, expireptr));
  ////PexpireCmd
  Cmd* pexpireptr = new PexpireCmd(kCmdNamePexpire, 3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePexpire, pexpireptr));
  ////ExpireatCmd
  Cmd* expireatptr = new ExpireatCmd(kCmdNameExpireat, 3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameExpireat, expireatptr));
  ////PexpireatCmd
  Cmd* pexpireatptr = new PexpireatCmd(kCmdNamePexpireat, 3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePexpireat, pexpireatptr));
  ////TtlCmd
  Cmd* ttlptr = new TtlCmd(kCmdNameTtl, 2, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameTtl, ttlptr));
  ////PttlCmd
  Cmd* pttlptr = new PttlCmd(kCmdNamePttl, 2, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePttl, pttlptr));
  ////PersistCmd
  Cmd* persistptr = new PersistCmd(kCmdNamePersist, 2, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePersist, persistptr));
  ////TypeCmd
  Cmd* typeptr = new TypeCmd(kCmdNameType, 2, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameType, typeptr));
  ////ScanCmd
  Cmd* scanptr = new ScanCmd(kCmdNameScan, -2, kCmdFlagsRead | kCmdFlagsMultiPartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameScan, scanptr));
  ////ScanxCmd
  Cmd* scanxptr = new ScanxCmd(kCmdNameScanx, -3, kCmdFlagsRead | kCmdFlagsMultiPartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameScanx, scanxptr));
  ////PKSetexAtCmd
  Cmd* pksetexatptr = new PKSetexAtCmd(kCmdNamePKSetexAt, 4, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePKSetexAt, pksetexatptr));
  ////PKScanRange
  Cmd* pkscanrangeptr = new PKScanRangeCmd(kCmdNamePKScanRange, -4, kCmdFlagsRead | kCmdFlagsMultiPartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePKScanRange, pkscanrangeptr));
  ////PKRScanRange
  Cmd* pkrscanrangeptr = new PKRScanRangeCmd(kCmdNamePKRScanRange, -4, kCmdFlagsRead | kCmdFlagsMultiPartition | kCmdFlagsKv);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePKRScanRange, pkrscanrangeptr));

  //Hash
  ////HDelCmd
  Cmd* hdelptr = new HDelCmd(kCmdNameHDel, -3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsHash);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHDel, hdelptr));
  ////HSetCmd
  Cmd* hsetptr = new HSetCmd(kCmdNameHSet, 4, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsHash);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHSet, hsetptr));
  ////HGetCmd
  Cmd* hgetptr = new HGetCmd(kCmdNameHGet, 3, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsHash);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHGet, hgetptr));
  ////HGetallCmd
  Cmd* hgetallptr = new HGetallCmd(kCmdNameHGetall, 2, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsHash);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHGetall, hgetallptr));
  ////HExistsCmd
  Cmd* hexistsptr = new HExistsCmd(kCmdNameHExists, 3, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsHash);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHExists, hexistsptr));
  ////HIncrbyCmd
  Cmd* hincrbyptr = new HIncrbyCmd(kCmdNameHIncrby, 4, kCmdFlagsWrite |kCmdFlagsSinglePartition | kCmdFlagsHash);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHIncrby, hincrbyptr));
  ////HIncrbyfloatCmd
  Cmd* hincrbyfloatptr = new HIncrbyfloatCmd(kCmdNameHIncrbyfloat, 4, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsHash);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHIncrbyfloat, hincrbyfloatptr));
  ////HKeysCmd
  Cmd* hkeysptr = new HKeysCmd(kCmdNameHKeys, 2, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsHash);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHKeys, hkeysptr));
  ////HLenCmd
  Cmd* hlenptr = new HLenCmd(kCmdNameHLen, 2, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsHash);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHLen, hlenptr));
  ////HMgetCmd
  Cmd* hmgetptr = new HMgetCmd(kCmdNameHMget, -3, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsHash);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHMget, hmgetptr));
  ////HMsetCmd
  Cmd* hmsetptr = new HMsetCmd(kCmdNameHMset, -4, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsHash);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHMset, hmsetptr));
  ////HSetnxCmd
  Cmd* hsetnxptr = new HSetnxCmd(kCmdNameHSetnx, 4, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsHash);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHSetnx, hsetnxptr));
  ////HStrlenCmd
  Cmd* hstrlenptr = new HStrlenCmd(kCmdNameHStrlen, 3, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsHash);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHStrlen, hstrlenptr));
  ////HValsCmd
  Cmd* hvalsptr = new HValsCmd(kCmdNameHVals, 2, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsHash);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHVals, hvalsptr));
  ////HScanCmd
  Cmd* hscanptr = new HScanCmd(kCmdNameHScan, -3, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsHash);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHScan, hscanptr));
  ////HScanxCmd
  Cmd* hscanxptr = new HScanxCmd(kCmdNameHScanx, -3, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsHash);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameHScanx, hscanxptr));
  ////PKHScanRange
  Cmd* pkhscanrangeptr = new PKHScanRangeCmd(kCmdNamePKHScanRange, -4, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsHash);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePKHScanRange, pkhscanrangeptr));
  ////PKHRScanRange
  Cmd* pkhrscanrangeptr = new PKHRScanRangeCmd(kCmdNamePKHRScanRange, -4, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsHash);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePKHRScanRange, pkhrscanrangeptr));

  //List
  Cmd* lindexptr = new LIndexCmd(kCmdNameLIndex, 3, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsList);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLIndex, lindexptr));
  Cmd* linsertptr = new LInsertCmd(kCmdNameLInsert, 5, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsList);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLInsert, linsertptr));
  Cmd* llenptr = new LLenCmd(kCmdNameLLen, 2, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsList);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLLen, llenptr));
  Cmd* lpopptr = new LPopCmd(kCmdNameLPop, 2, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsList);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLPop, lpopptr));
  Cmd* lpushptr = new LPushCmd(kCmdNameLPush, -3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsList);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLPush, lpushptr));
  Cmd* lpushxptr = new LPushxCmd(kCmdNameLPushx, 3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsList);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLPushx, lpushxptr));
  Cmd* lrangeptr = new LRangeCmd(kCmdNameLRange, 4, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsList);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLRange, lrangeptr));
  Cmd* lremptr = new LRemCmd(kCmdNameLRem, 4, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsList);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLRem, lremptr));
  Cmd* lsetptr = new LSetCmd(kCmdNameLSet, 4, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsList);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLSet, lsetptr));
  Cmd* ltrimptr = new LTrimCmd(kCmdNameLTrim, 4, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsList);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameLTrim, ltrimptr));
  Cmd* rpopptr = new RPopCmd(kCmdNameRPop, 2, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsList);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameRPop, rpopptr));
  Cmd* rpoplpushptr = new RPopLPushCmd(kCmdNameRPopLPush, 3, kCmdFlagsWrite | kCmdFlagsMultiPartition | kCmdFlagsList);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameRPopLPush, rpoplpushptr));
  Cmd* rpushptr = new RPushCmd(kCmdNameRPush, -3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsList);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameRPush, rpushptr));
  Cmd* rpushxptr = new RPushxCmd(kCmdNameRPushx, 3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsList);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameRPushx, rpushxptr));

  //Zset
  ////ZAddCmd
  Cmd* zaddptr = new ZAddCmd(kCmdNameZAdd, -4, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZAdd, zaddptr));
  ////ZCardCmd
  Cmd* zcardptr = new ZCardCmd(kCmdNameZCard, 2, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZCard, zcardptr));
  ////ZScanCmd
  Cmd* zscanptr = new ZScanCmd(kCmdNameZScan, -3, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZScan, zscanptr));
  ////ZIncrbyCmd
  Cmd* zincrbyptr = new ZIncrbyCmd(kCmdNameZIncrby, 4, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZIncrby, zincrbyptr));
  ////ZRangeCmd
  Cmd* zrangeptr = new ZRangeCmd(kCmdNameZRange, -4, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRange, zrangeptr));
  ////ZRevrangeCmd
  Cmd* zrevrangeptr = new ZRevrangeCmd(kCmdNameZRevrange, -4, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRevrange, zrevrangeptr));
  ////ZRangebyscoreCmd
  Cmd* zrangebyscoreptr = new ZRangebyscoreCmd(kCmdNameZRangebyscore, -4, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRangebyscore, zrangebyscoreptr));
  ////ZRevrangebyscoreCmd
  Cmd* zrevrangebyscoreptr = new ZRevrangebyscoreCmd(kCmdNameZRevrangebyscore, -4, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRevrangebyscore, zrevrangebyscoreptr));
  ////ZCountCmd
  Cmd* zcountptr = new ZCountCmd(kCmdNameZCount, 4, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZCount, zcountptr));
  ////ZRemCmd
  Cmd* zremptr = new ZRemCmd(kCmdNameZRem, -3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRem, zremptr));
  ////ZUnionstoreCmd
  Cmd* zunionstoreptr = new ZUnionstoreCmd(kCmdNameZUnionstore, -4, kCmdFlagsWrite | kCmdFlagsMultiPartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZUnionstore, zunionstoreptr));
  ////ZInterstoreCmd
  Cmd* zinterstoreptr = new ZInterstoreCmd(kCmdNameZInterstore, -4, kCmdFlagsWrite | kCmdFlagsMultiPartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZInterstore, zinterstoreptr));
  ////ZRankCmd
  Cmd* zrankptr = new ZRankCmd(kCmdNameZRank, 3, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRank, zrankptr));
  ////ZRevrankCmd
  Cmd* zrevrankptr = new ZRevrankCmd(kCmdNameZRevrank, 3, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRevrank, zrevrankptr));
  ////ZScoreCmd
  Cmd* zscoreptr = new ZScoreCmd(kCmdNameZScore, 3, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZScore, zscoreptr));
  ////ZRangebylexCmd
  Cmd* zrangebylexptr = new ZRangebylexCmd(kCmdNameZRangebylex, -4, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRangebylex, zrangebylexptr));
  ////ZRevrangebylexCmd
  Cmd* zrevrangebylexptr = new ZRevrangebylexCmd(kCmdNameZRevrangebylex, -4, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRevrangebylex, zrevrangebylexptr));
  ////ZLexcountCmd
  Cmd* zlexcountptr = new ZLexcountCmd(kCmdNameZLexcount, 4, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZLexcount, zlexcountptr));
  ////ZRemrangebyrankCmd
  Cmd* zremrangebyrankptr = new ZRemrangebyrankCmd(kCmdNameZRemrangebyrank, 4, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRemrangebyrank, zremrangebyrankptr));
  ////ZRemrangebyscoreCmd
  Cmd* zremrangebyscoreptr = new ZRemrangebyscoreCmd(kCmdNameZRemrangebyscore, 4, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRemrangebyscore, zremrangebyscoreptr));
  ////ZRemrangebylexCmd
  Cmd* zremrangebylexptr = new ZRemrangebylexCmd(kCmdNameZRemrangebylex, 4, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsZset);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameZRemrangebylex, zremrangebylexptr));

  //Set
  ////SAddCmd
  Cmd* saddptr = new SAddCmd(kCmdNameSAdd, -3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsSet);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSAdd, saddptr));
  ////SPopCmd
  Cmd* spopptr = new SPopCmd(kCmdNameSPop, 2, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsSet);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSPop, spopptr));
  ////SCardCmd
  Cmd* scardptr = new SCardCmd(kCmdNameSCard, 2, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsSet);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSCard, scardptr));
  ////SMembersCmd
  Cmd* smembersptr = new SMembersCmd(kCmdNameSMembers, 2, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsSet);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSMembers, smembersptr));
  ////SScanCmd
  Cmd* sscanptr = new SScanCmd(kCmdNameSScan, -3, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsSet);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSScan, sscanptr));
  ////SRemCmd
  Cmd* sremptr = new SRemCmd(kCmdNameSRem, -3, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsSet);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSRem, sremptr));
  ////SUnionCmd
  Cmd* sunionptr = new SUnionCmd(kCmdNameSUnion, -2, kCmdFlagsRead | kCmdFlagsMultiPartition | kCmdFlagsSet);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSUnion, sunionptr));
  ////SUnionstoreCmd
  Cmd* sunionstoreptr = new SUnionstoreCmd(kCmdNameSUnionstore, -3, kCmdFlagsWrite | kCmdFlagsMultiPartition | kCmdFlagsSet);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSUnionstore, sunionstoreptr));
  ////SInterCmd
  Cmd* sinterptr = new SInterCmd(kCmdNameSInter, -2, kCmdFlagsRead | kCmdFlagsMultiPartition | kCmdFlagsSet);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSInter, sinterptr));
  ////SInterstoreCmd
  Cmd* sinterstoreptr = new SInterstoreCmd(kCmdNameSInterstore, -3, kCmdFlagsWrite | kCmdFlagsMultiPartition | kCmdFlagsSet);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSInterstore, sinterstoreptr));
  ////SIsmemberCmd
  Cmd* sismemberptr = new SIsmemberCmd(kCmdNameSIsmember, 3, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsSet);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSIsmember, sismemberptr));
  ////SDiffCmd
  Cmd* sdiffptr = new SDiffCmd(kCmdNameSDiff, -2, kCmdFlagsRead | kCmdFlagsMultiPartition | kCmdFlagsSet);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSDiff, sdiffptr));
  ////SDiffstoreCmd
  Cmd* sdiffstoreptr = new SDiffstoreCmd(kCmdNameSDiffstore, -3, kCmdFlagsWrite | kCmdFlagsMultiPartition | kCmdFlagsSet);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSDiffstore, sdiffstoreptr));
  ////SMoveCmd
  Cmd* smoveptr = new SMoveCmd(kCmdNameSMove, 4, kCmdFlagsWrite | kCmdFlagsMultiPartition | kCmdFlagsSet);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSMove, smoveptr));
  ////SRandmemberCmd
  Cmd* srandmemberptr = new SRandmemberCmd(kCmdNameSRandmember, -2, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsSet);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSRandmember, srandmemberptr));

  //BitMap
  ////bitsetCmd
  Cmd* bitsetptr = new BitSetCmd(kCmdNameBitSet, 4, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsBit);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameBitSet, bitsetptr));
  ////bitgetCmd
  Cmd* bitgetptr = new BitGetCmd(kCmdNameBitGet, 3, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsBit);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameBitGet, bitgetptr));
  ////bitcountCmd
  Cmd* bitcountptr = new BitCountCmd(kCmdNameBitCount, -2, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsBit);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameBitCount, bitcountptr));
  ////bitposCmd
  Cmd* bitposptr = new BitPosCmd(kCmdNameBitPos, -3, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsBit);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameBitPos, bitposptr));
  ////bitopCmd
  Cmd* bitopptr = new BitOpCmd(kCmdNameBitOp, -3, kCmdFlagsWrite | kCmdFlagsMultiPartition | kCmdFlagsBit);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameBitOp, bitopptr));

  //HyperLogLog
  ////pfaddCmd
  Cmd * pfaddptr = new PfAddCmd(kCmdNamePfAdd, -2, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsHyperLogLog);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePfAdd, pfaddptr));
  ////pfcountCmd
  Cmd * pfcountptr = new PfCountCmd(kCmdNamePfCount, -2, kCmdFlagsRead | kCmdFlagsMultiPartition | kCmdFlagsHyperLogLog);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePfCount, pfcountptr));
  ////pfmergeCmd
  Cmd * pfmergeptr = new PfMergeCmd(kCmdNamePfMerge, -3, kCmdFlagsWrite | kCmdFlagsMultiPartition | kCmdFlagsHyperLogLog);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePfMerge, pfmergeptr));

  //GEO
  ////GepAdd
  Cmd * geoaddptr = new GeoAddCmd(kCmdNameGeoAdd, -5, kCmdFlagsWrite | kCmdFlagsSinglePartition | kCmdFlagsGeo);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGeoAdd, geoaddptr));
  ////GeoPos
  Cmd * geoposptr = new GeoPosCmd(kCmdNameGeoPos, -2, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsGeo);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGeoPos, geoposptr));
  ////GeoDist
  Cmd * geodistptr = new GeoDistCmd(kCmdNameGeoDist, -4, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsGeo);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGeoDist, geodistptr));
  ////GeoHash
  Cmd * geohashptr = new GeoHashCmd(kCmdNameGeoHash, -2, kCmdFlagsRead | kCmdFlagsSinglePartition | kCmdFlagsGeo);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGeoHash, geohashptr));
  ////GeoRadius
  Cmd * georadiusptr = new GeoRadiusCmd(kCmdNameGeoRadius, -6, kCmdFlagsRead | kCmdFlagsMultiPartition | kCmdFlagsGeo);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGeoRadius, georadiusptr));
  ////GeoRadiusByMember
  Cmd * georadiusbymemberptr = new GeoRadiusByMemberCmd(kCmdNameGeoRadiusByMember, -5, kCmdFlagsRead | kCmdFlagsMultiPartition | kCmdFlagsGeo);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGeoRadiusByMember, georadiusbymemberptr));

  //PubSub
  ////Publish
  Cmd * publishptr = new PublishCmd(kCmdNamePublish, 3, kCmdFlagsRead | kCmdFlagsPubSub);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePublish, publishptr));
  ////Subscribe
  Cmd * subscribeptr = new SubscribeCmd(kCmdNameSubscribe, -2, kCmdFlagsRead | kCmdFlagsPubSub);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSubscribe, subscribeptr));
  ////UnSubscribe
  Cmd * unsubscribeptr = new UnSubscribeCmd(kCmdNameUnSubscribe, -1, kCmdFlagsRead | kCmdFlagsPubSub);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameUnSubscribe, unsubscribeptr));
  ////PSubscribe
  Cmd * psubscribeptr = new PSubscribeCmd(kCmdNamePSubscribe, -2, kCmdFlagsRead | kCmdFlagsPubSub);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePSubscribe, psubscribeptr));
  ////PUnSubscribe
  Cmd * punsubscribeptr = new PUnSubscribeCmd(kCmdNamePUnSubscribe, -1, kCmdFlagsRead | kCmdFlagsPubSub);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePUnSubscribe, punsubscribeptr));
  ////PubSub
  Cmd * pubsubptr = new PubSubCmd(kCmdNamePubSub, -2, kCmdFlagsRead | kCmdFlagsPubSub);
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNamePubSub, pubsubptr));
}

Cmd* GetCmdFromTable(const std::string& opt, const CmdTable& cmd_table) {
  CmdTable::const_iterator it = cmd_table.find(opt);
  if (it != cmd_table.end()) {
    return it->second;
  }
  return NULL;
}

void DestoryCmdTable(CmdTable* cmd_table) {
  CmdTable::const_iterator it = cmd_table->begin();
  for (; it != cmd_table->end(); ++it) {
    delete it->second;
  }
}

void Cmd::Initial(const PikaCmdArgsType& argv,
                  const std::string& table_name) {
  argv_ = argv;
  table_name_ = table_name;
  res_.clear(); // Clear res content
  Clear();      // Clear cmd, Derived class can has own implement
  DoInitial();
};

std::string Cmd::current_key() const {
  return "";
}

void Cmd::Execute() {
  if (is_single_partition()
    || g_pika_server->GetPartitionNumByTable(table_name_) == 1) {
    ProcessSinglePartitionCmd();
  } else if (is_multi_partition()) {
    ProcessMultiPartitionCmd();
  } else {
    ProcessDoNotSpecifyPartitionCmd();
  }
}

void Cmd::ProcessSinglePartitionCmd() {
  std::shared_ptr<Partition> partition;
  if (is_single_partition()) {
    partition = g_pika_server->GetTablePartitionByKey(table_name_, current_key());
  } else {
    partition = g_pika_server->GetTablePartitionById(table_name_, 0);
  }

  if (!partition) {
    res_.SetRes(CmdRes::kErrOther, "Partition not found");
    return;
  }
  partition->DoCommand(this);
}

void Cmd::ProcessMultiPartitionCmd() {
}

void Cmd::ProcessDoNotSpecifyPartitionCmd() {
  Do();
}


bool Cmd::is_write() const {
  return ((flag_ & kCmdFlagsMaskRW) == kCmdFlagsWrite);
}
bool Cmd::is_local() const {
  return ((flag_ & kCmdFlagsMaskLocal) == kCmdFlagsLocal);
}
// Others need to be suspended when a suspend command run
bool Cmd::is_suspend() const {
  return ((flag_ & kCmdFlagsMaskSuspend) == kCmdFlagsSuspend);
}
// Must with admin auth
bool Cmd::is_admin_require() const {
  return ((flag_ & kCmdFlagsMaskAdminRequire) == kCmdFlagsAdminRequire);
}
bool Cmd::is_single_partition() const {
  return ((flag_ & kCmdFlagsMaskPartition) == kCmdFlagsSinglePartition);
}
bool Cmd::is_multi_partition() const {
  return ((flag_ & kCmdFlagsMaskPartition) == kCmdFlagsMultiPartition);
}

std::string Cmd::name() const {
  return name_;
}
CmdRes& Cmd::res() {
  return res_;
}

std::string Cmd::ToBinlog(uint32_t exec_time,
                          const std::string& server_id,
                          uint64_t logic_id,
                          uint32_t filenum,
                          uint64_t offset) {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, argv_.size(), "*");

  for (const auto& v : argv_) {
    RedisAppendLen(content, v.size(), "$");
    RedisAppendContent(content, v);
  }

  return PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst,
                                             exec_time,
                                             std::stoi(server_id),
                                             logic_id,
                                             filenum,
                                             offset,
                                             content,
                                             {});
}

bool Cmd::CheckArg(int num) const {
  if ((arity_ > 0 && num != arity_)
    || (arity_ < 0 && num < -arity_)) {
    return false;
  }
  return true;
}
