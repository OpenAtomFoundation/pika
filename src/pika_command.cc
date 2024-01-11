// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <memory>
#include <utility>

#include <glog/logging.h>
#include "include/pika_acl.h"
#include "include/pika_admin.h"
#include "include/pika_bit.h"
#include "include/pika_cmd_table_manager.h"
#include "include/pika_command.h"
#include "include/pika_geo.h"
#include "include/pika_hash.h"
#include "include/pika_hyperloglog.h"
#include "include/pika_kv.h"
#include "include/pika_list.h"
#include "include/pika_pubsub.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "include/pika_set.h"
#include "include/pika_slot_command.h"
#include "include/pika_stream.h"
#include "include/pika_transaction.h"
#include "include/pika_zset.h"
#include "pstd_defer.h"
#include "include/pika_cache.h"

using pstd::Status;

extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;
extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;

void InitCmdTable(CmdTable* cmd_table) {
  // Admin
  ////Slaveof
  std::unique_ptr<Cmd> slaveofptr =
      std::make_unique<SlaveofCmd>(kCmdNameSlaveof, -3, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSlaveof, std::move(slaveofptr)));

  std::unique_ptr<Cmd> dbslaveofptr =
      std::make_unique<DbSlaveofCmd>(kCmdNameDbSlaveof, -2, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameDbSlaveof, std::move(dbslaveofptr)));

  std::unique_ptr<Cmd> authptr =
      std::make_unique<AuthCmd>(kCmdNameAuth, -2, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsNoAuth | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameAuth, std::move(authptr)));

  std::unique_ptr<Cmd> bgsaveptr = std::make_unique<BgsaveCmd>(
      kCmdNameBgsave, -1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSuspend | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameBgsave, std::move(bgsaveptr)));

  std::unique_ptr<Cmd> compactptr =
      std::make_unique<CompactCmd>(kCmdNameCompact, -1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameCompact, std::move(compactptr)));

  std::unique_ptr<Cmd> compactrangeptr = std::make_unique<CompactRangeCmd>(kCmdNameCompactRange, 5, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameCompactRange, std::move(compactrangeptr)));
  std::unique_ptr<Cmd> purgelogsto =
      std::make_unique<PurgelogstoCmd>(kCmdNamePurgelogsto, -2, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePurgelogsto, std::move(purgelogsto)));

  std::unique_ptr<Cmd> pingptr =
      std::make_unique<PingCmd>(kCmdNamePing, 1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePing, std::move(pingptr)));

  std::unique_ptr<Cmd> helloptr =
      std::make_unique<HelloCmd>(kCmdNameHello, -1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsNoAuth | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameHello, std::move(helloptr)));

  std::unique_ptr<Cmd> selectptr =
      std::make_unique<SelectCmd>(kCmdNameSelect, 2, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSelect, std::move(selectptr)));

  std::unique_ptr<Cmd> flushallptr = std::make_unique<FlushallCmd>(
      kCmdNameFlushall, 1, kCmdFlagsWrite | kCmdFlagsSuspend | kCmdFlagsAdmin  | kCmdFlagsUpdateCache | kCmdFlagsDoThroughDB | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameFlushall, std::move(flushallptr)));

  std::unique_ptr<Cmd> flushdbptr = std::make_unique<FlushdbCmd>(
      kCmdNameFlushdb, -1, kCmdFlagsWrite | kCmdFlagsSuspend | kCmdFlagsAdmin  | kCmdFlagsUpdateCache | kCmdFlagsDoThroughDB | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameFlushdb, std::move(flushdbptr)));

  std::unique_ptr<Cmd> clientptr =
      std::make_unique<ClientCmd>(kCmdNameClient, -2, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameClient, std::move(clientptr)));

  std::unique_ptr<Cmd> shutdownptr = std::make_unique<ShutdownCmd>(
      kCmdNameShutdown, 1, kCmdFlagsRead | kCmdFlagsLocal | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameShutdown, std::move(shutdownptr)));

  std::unique_ptr<Cmd> infoptr =
      std::make_unique<InfoCmd>(kCmdNameInfo, -1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameInfo, std::move(infoptr)));

  std::unique_ptr<Cmd> configptr =
      std::make_unique<ConfigCmd>(kCmdNameConfig, -2, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameConfig, std::move(configptr)));

  std::unique_ptr<Cmd> monitorptr =
      std::make_unique<MonitorCmd>(kCmdNameMonitor, -1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameMonitor, std::move(monitorptr)));

  std::unique_ptr<Cmd> dbsizeptr =
      std::make_unique<DbsizeCmd>(kCmdNameDbsize, 1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameDbsize, std::move(dbsizeptr)));

  std::unique_ptr<Cmd> timeptr =
      std::make_unique<TimeCmd>(kCmdNameTime, 1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameTime, std::move(timeptr)));

  std::unique_ptr<Cmd> delbackupptr =
      std::make_unique<DelbackupCmd>(kCmdNameDelbackup, 1, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameDelbackup, std::move(delbackupptr)));

  std::unique_ptr<Cmd> echoptr =
      std::make_unique<EchoCmd>(kCmdNameEcho, 2, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameEcho, std::move(echoptr)));

  std::unique_ptr<Cmd> scandbptr =
      std::make_unique<ScandbCmd>(kCmdNameScandb, -1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameScandb, std::move(scandbptr)));

  std::unique_ptr<Cmd> slowlogptr =
      std::make_unique<SlowlogCmd>(kCmdNameSlowlog, -2, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSlowlog, std::move(slowlogptr)));

  std::unique_ptr<Cmd> paddingptr = std::make_unique<PaddingCmd>(kCmdNamePadding, 2, kCmdFlagsWrite | kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePadding, std::move(paddingptr)));

  std::unique_ptr<Cmd> pkpatternmatchdelptr =
      std::make_unique<PKPatternMatchDelCmd>(kCmdNamePKPatternMatchDel, 3, kCmdFlagsWrite | kCmdFlagsAdmin);
  cmd_table->insert(
      std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePKPatternMatchDel, std::move(pkpatternmatchdelptr)));

  std::unique_ptr<Cmd> dummyptr = std::make_unique<DummyCmd>(kCmdDummy, 0, kCmdFlagsWrite | kCmdFlagsSingleSlot);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdDummy, std::move(dummyptr)));

  std::unique_ptr<Cmd> quitptr =
      std::make_unique<QuitCmd>(kCmdNameQuit, 1, kCmdFlagsRead | kCmdFlagsNoAuth | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameQuit, std::move(quitptr)));

  std::unique_ptr<Cmd> diskrecoveryptr =
      std::make_unique<DiskRecoveryCmd>(kCmdNameDiskRecovery, 1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameDiskRecovery, std::move(diskrecoveryptr)));

  std::unique_ptr<Cmd> clearreplicationidptr = std::make_unique<ClearReplicationIDCmd>(
      kCmdNameClearReplicationID, 1, kCmdFlagsWrite | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(
      std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameClearReplicationID, std::move(clearreplicationidptr)));
  std::unique_ptr<Cmd> disablewalptr = std::make_unique<DisableWalCmd>(kCmdNameDisableWal, 2, kCmdFlagsAdmin);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameDisableWal, std::move(disablewalptr)));
  std::unique_ptr<Cmd> cacheptr = std::make_unique<CacheCmd>(kCmdNameCache, -2, kCmdFlagsAdmin | kCmdFlagsRead);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameCache, std::move(cacheptr)));
  std::unique_ptr<Cmd> clearcacheptr = std::make_unique<ClearCacheCmd>(kCmdNameClearCache, 1, kCmdFlagsAdmin | kCmdFlagsWrite);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameClearCache, std::move(clearcacheptr)));
  std::unique_ptr<Cmd> lastsaveptr = std::make_unique<LastsaveCmd>(kCmdNameLastSave, 1, kCmdFlagsAdmin | kCmdFlagsRead | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameLastSave, std::move(lastsaveptr)));

#ifdef WITH_COMMAND_DOCS
  std::unique_ptr<Cmd> commandptr =
      std::make_unique<CommandCmd>(kCmdNameCommand, -1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameCommand, std::move(commandptr)));
#endif

  // Slots related
  std::unique_ptr<Cmd> slotsinfoptr =
      std::make_unique<SlotsInfoCmd>(kCmdNameSlotsInfo, -1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSlotsInfo, std::move(slotsinfoptr)));

  std::unique_ptr<Cmd> slotmgrttagslotasyncptr = std::make_unique<SlotsMgrtTagSlotAsyncCmd>(
      kCmdNameSlotsMgrtTagSlotAsync, 8, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(
      std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSlotsMgrtTagSlotAsync, std::move(slotmgrttagslotasyncptr)));

  std::unique_ptr<Cmd> slotmgrtasyncstatus = std::make_unique<SlotsMgrtAsyncStatusCmd>(
      kCmdNameSlotsMgrtAsyncStatus, 1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(
      std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSlotsMgrtAsyncStatus, std::move(slotmgrtasyncstatus)));

  std::unique_ptr<Cmd> slotmgrtasynccancel = std::make_unique<SlotsMgrtAsyncCancelCmd>(
      kCmdNameSlotsMgrtAsyncCancel, 1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(
      std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSlotsMgrtAsyncCancel, std::move(slotmgrtasynccancel)));

  std::unique_ptr<Cmd> slotmgrttagoneptr =
      std::make_unique<SlotsMgrtTagOneCmd>(kCmdNameSlotsMgrtTagOne, 5, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(
      std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSlotsMgrtTagOne, std::move(slotmgrttagoneptr)));

  std::unique_ptr<Cmd> slotmgrtoneptr =
      std::make_unique<SlotsMgrtTagOneCmd>(kCmdNameSlotsMgrtOne, 5, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSlotsMgrtOne, std::move(slotmgrtoneptr)));

  std::unique_ptr<Cmd> slotmgrttagslotptr = std::make_unique<SlotsMgrtTagSlotCmd>(
      kCmdNameSlotsMgrtTagSlot, 5, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(
      std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSlotsMgrtTagSlot, std::move(slotmgrttagslotptr)));

  std::unique_ptr<Cmd> slotmgrttagslottagptr =
      std::make_unique<SlotsMgrtTagSlotCmd>(kCmdNameSlotsMgrtSlot, 5, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(
      std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSlotsMgrtSlot, std::move(slotmgrttagslottagptr)));

  std::unique_ptr<Cmd> slotsdelptr =
      std::make_unique<SlotsDelCmd>(kCmdNameSlotsDel, -2, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSlotsDel, std::move(slotsdelptr)));

  std::unique_ptr<Cmd> slotshashkeyptr =
      std::make_unique<SlotsHashKeyCmd>(kCmdNameSlotsHashKey, -2, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSlotsHashKey, std::move(slotshashkeyptr)));

  std::unique_ptr<Cmd> slotsscanptr =
      std::make_unique<SlotsScanCmd>(kCmdNameSlotsScan, -3, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSlotsScan, std::move(slotsscanptr)));

  std::unique_ptr<Cmd> slotsmgrtexecwrapper = std::make_unique<SlotsMgrtExecWrapperCmd>(
      kCmdNameSlotsMgrtExecWrapper, -3, kCmdFlagsWrite | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(
      std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSlotsMgrtExecWrapper, std::move(slotsmgrtexecwrapper)));

  std::unique_ptr<Cmd> slotsreloadptr =
      std::make_unique<SlotsReloadCmd>(kCmdNameSlotsReload, 1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSlotsReload, std::move(slotsreloadptr)));

  std::unique_ptr<Cmd> slotsreloadoffptr =
      std::make_unique<SlotsReloadOffCmd>(kCmdNameSlotsReloadOff, -1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSlotsReloadOff, std::move(slotsreloadoffptr)));

  std::unique_ptr<Cmd> slotscleanupptr =
      std::make_unique<SlotsCleanupCmd>(kCmdNameSlotsCleanup, -2, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSlotsCleanup, std::move(slotscleanupptr)));

  std::unique_ptr<Cmd> slotscleanupoffptr =
      std::make_unique<SlotsCleanupOffCmd>(kCmdNameSlotsCleanupOff, -1, kCmdFlagsRead | kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(
      std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSlotsCleanupOff, std::move(slotscleanupoffptr)));

  // Kv
  ////SetCmd
  std::unique_ptr<Cmd> setptr =
      std::make_unique<SetCmd>(kCmdNameSet, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSet, std::move(setptr)));
  ////GetCmd
  std::unique_ptr<Cmd> getptr =
      std::make_unique<GetCmd>(kCmdNameGet, 2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsKv  | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsReadCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameGet, std::move(getptr)));
  ////DelCmd
  std::unique_ptr<Cmd> delptr =
      std::make_unique<DelCmd>(kCmdNameDel, -2, kCmdFlagsWrite | kCmdFlagsMultiSlot | kCmdFlagsOperateKey  | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameDel, std::move(delptr)));
  std::unique_ptr<Cmd> Unlinkptr =
      std::make_unique<DelCmd>(kCmdNameUnlink, -2, kCmdFlagsWrite | kCmdFlagsMultiSlot | kCmdFlagsOperateKey | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameUnlink, std::move(Unlinkptr)));
  ////IncrCmd
  std::unique_ptr<Cmd> incrptr =
      std::make_unique<IncrCmd>(kCmdNameIncr, 2, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameIncr, std::move(incrptr)));
  ////IncrbyCmd
  std::unique_ptr<Cmd> incrbyptr = std::make_unique<IncrbyCmd>(
      kCmdNameIncrby, 3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameIncrby, std::move(incrbyptr)));
  ////IncrbyfloatCmd
  std::unique_ptr<Cmd> incrbyfloatptr = std::make_unique<IncrbyfloatCmd>(
      kCmdNameIncrbyfloat, 3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv  | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameIncrbyfloat, std::move(incrbyfloatptr)));
  ////DecrCmd
  std::unique_ptr<Cmd> decrptr =
      std::make_unique<DecrCmd>(kCmdNameDecr, 2, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameDecr, std::move(decrptr)));
  ////DecrbyCmd
  std::unique_ptr<Cmd> decrbyptr = std::make_unique<DecrbyCmd>(
      kCmdNameDecrby, 3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameDecrby, std::move(decrbyptr)));
  ////GetsetCmd
  std::unique_ptr<Cmd> getsetptr = std::make_unique<GetsetCmd>(
      kCmdNameGetset, 3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv  | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameGetset, std::move(getsetptr)));
  ////AppendCmd
  std::unique_ptr<Cmd> appendptr = std::make_unique<AppendCmd>(
      kCmdNameAppend, 3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv  | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameAppend, std::move(appendptr)));
  ////MgetCmd
  std::unique_ptr<Cmd> mgetptr =
      std::make_unique<MgetCmd>(kCmdNameMget, -2, kCmdFlagsRead | kCmdFlagsMultiSlot | kCmdFlagsKv  | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsReadCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameMget, std::move(mgetptr)));
  ////KeysCmd
  std::unique_ptr<Cmd> keysptr =
      std::make_unique<KeysCmd>(kCmdNameKeys, -2, kCmdFlagsRead | kCmdFlagsMultiSlot | kCmdFlagsOperateKey | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameKeys, std::move(keysptr)));
  ////SetnxCmd
  std::unique_ptr<Cmd> setnxptr =
      std::make_unique<SetnxCmd>(kCmdNameSetnx, 3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSetnx, std::move(setnxptr)));
  ////SetexCmd
  std::unique_ptr<Cmd> setexptr =
      std::make_unique<SetexCmd>(kCmdNameSetex, 4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSetex, std::move(setexptr)));
  ////PsetexCmd
  std::unique_ptr<Cmd> psetexptr =
      std::make_unique<PsetexCmd>(kCmdNamePsetex, 4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePsetex, std::move(psetexptr)));
  ////DelvxCmd
  std::unique_ptr<Cmd> delvxptr =
      std::make_unique<DelvxCmd>(kCmdNameDelvx, 3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameDelvx, std::move(delvxptr)));
  ////MSetCmd
  std::unique_ptr<Cmd> msetptr =
      std::make_unique<MsetCmd>(kCmdNameMset, -3, kCmdFlagsWrite | kCmdFlagsMultiSlot | kCmdFlagsKv | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameMset, std::move(msetptr)));
  ////MSetnxCmd
  std::unique_ptr<Cmd> msetnxptr = std::make_unique<MsetnxCmd>(
      kCmdNameMsetnx, -3, kCmdFlagsWrite | kCmdFlagsMultiSlot | kCmdFlagsKv | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameMsetnx, std::move(msetnxptr)));
  ////GetrangeCmd
  std::unique_ptr<Cmd> getrangeptr = std::make_unique<GetrangeCmd>(
      kCmdNameGetrange, 4, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsKv | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsReadCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameGetrange, std::move(getrangeptr)));
  ////SetrangeCmd
  std::unique_ptr<Cmd> setrangeptr = std::make_unique<SetrangeCmd>(
      kCmdNameSetrange, 4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv  | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSetrange, std::move(setrangeptr)));
  ////StrlenCmd
  std::unique_ptr<Cmd> strlenptr =
      std::make_unique<StrlenCmd>(kCmdNameStrlen, 2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsKv | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsReadCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameStrlen, std::move(strlenptr)));
  ////ExistsCmd
  std::unique_ptr<Cmd> existsptr =
      std::make_unique<ExistsCmd>(kCmdNameExists, -2, kCmdFlagsRead | kCmdFlagsMultiSlot | kCmdFlagsOperateKey | kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameExists, std::move(existsptr)));
  ////ExpireCmd
  std::unique_ptr<Cmd> expireptr = std::make_unique<ExpireCmd>(
      kCmdNameExpire, 3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsOperateKey | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameExpire, std::move(expireptr)));
  ////PexpireCmd
  std::unique_ptr<Cmd> pexpireptr = std::make_unique<PexpireCmd>(
      kCmdNamePexpire, 3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsOperateKey | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePexpire, std::move(pexpireptr)));
  ////ExpireatCmd
  std::unique_ptr<Cmd> expireatptr =
      std::make_unique<ExpireatCmd>(kCmdNameExpireat, 3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsOperateKey | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameExpireat, std::move(expireatptr)));
  ////PexpireatCmd
  std::unique_ptr<Cmd> pexpireatptr =
      std::make_unique<PexpireatCmd>(kCmdNamePexpireat, 3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsOperateKey | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePexpireat, std::move(pexpireatptr)));
  ////TtlCmd
  std::unique_ptr<Cmd> ttlptr =
      std::make_unique<TtlCmd>(kCmdNameTtl, 2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsOperateKey | kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameTtl, std::move(ttlptr)));
  ////PttlCmd
  std::unique_ptr<Cmd> pttlptr =
      std::make_unique<PttlCmd>(kCmdNamePttl, 2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsOperateKey | kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePttl, std::move(pttlptr)));
  ////PersistCmd
  std::unique_ptr<Cmd> persistptr =
      std::make_unique<PersistCmd>(kCmdNamePersist, 2, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsOperateKey | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePersist, std::move(persistptr)));
  ////TypeCmd
  std::unique_ptr<Cmd> typeptr =
      std::make_unique<TypeCmd>(kCmdNameType, 2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsOperateKey  | kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameType, std::move(typeptr)));
  ////PTypeCmd
  std::unique_ptr<Cmd> pTypeptr =
      std::make_unique<PTypeCmd>(kCmdNamePType, 2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsOperateKey | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePType, std::move(pTypeptr)));
  ////ScanCmd
  std::unique_ptr<Cmd> scanptr =
      std::make_unique<ScanCmd>(kCmdNameScan, -2, kCmdFlagsRead | kCmdFlagsMultiSlot | kCmdFlagsOperateKey | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameScan, std::move(scanptr)));
  ////ScanxCmd
  std::unique_ptr<Cmd> scanxptr =
      std::make_unique<ScanxCmd>(kCmdNameScanx, -3, kCmdFlagsRead | kCmdFlagsMultiSlot | kCmdFlagsOperateKey | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameScanx, std::move(scanxptr)));
  ////PKSetexAtCmd
  std::unique_ptr<Cmd> pksetexatptr = std::make_unique<PKSetexAtCmd>(
      kCmdNamePKSetexAt, 4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePKSetexAt, std::move(pksetexatptr)));
  ////PKScanRange
  std::unique_ptr<Cmd> pkscanrangeptr = std::make_unique<PKScanRangeCmd>(
      kCmdNamePKScanRange, -4, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsOperateKey | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePKScanRange, std::move(pkscanrangeptr)));
  ////PKRScanRange
  std::unique_ptr<Cmd> pkrscanrangeptr = std::make_unique<PKRScanRangeCmd>(
      kCmdNamePKRScanRange, -4, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsOperateKey | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePKRScanRange, std::move(pkrscanrangeptr)));

  // Hash
  ////HDelCmd
  std::unique_ptr<Cmd> hdelptr =
      std::make_unique<HDelCmd>(kCmdNameHDel, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsHash | kCmdFlagsUpdateCache | kCmdFlagsDoThroughDB | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameHDel, std::move(hdelptr)));
  ////HSetCmd
  std::unique_ptr<Cmd> hsetptr =
      std::make_unique<HSetCmd>(kCmdNameHSet, 4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsHash | kCmdFlagsUpdateCache | kCmdFlagsDoThroughDB | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameHSet, std::move(hsetptr)));
  ////HGetCmd
  std::unique_ptr<Cmd> hgetptr =
      std::make_unique<HGetCmd>(kCmdNameHGet, 3, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsHash | kCmdFlagsUpdateCache | kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameHGet, std::move(hgetptr)));
  ////HGetallCmd
  std::unique_ptr<Cmd> hgetallptr =
      std::make_unique<HGetallCmd>(kCmdNameHGetall, 2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsHash | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameHGetall, std::move(hgetallptr)));
  ////HExistsCmd
  std::unique_ptr<Cmd> hexistsptr =
      std::make_unique<HExistsCmd>(kCmdNameHExists, 3, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsHash | kCmdFlagsUpdateCache | kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameHExists, std::move(hexistsptr)));
  ////HIncrbyCmd
  std::unique_ptr<Cmd> hincrbyptr =
      std::make_unique<HIncrbyCmd>(kCmdNameHIncrby, 4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsHash | kCmdFlagsUpdateCache | kCmdFlagsDoThroughDB | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameHIncrby, std::move(hincrbyptr)));
  ////HIncrbyfloatCmd
  std::unique_ptr<Cmd> hincrbyfloatptr =
      std::make_unique<HIncrbyfloatCmd>(kCmdNameHIncrbyfloat, 4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsHash | kCmdFlagsUpdateCache | kCmdFlagsDoThroughDB | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameHIncrbyfloat, std::move(hincrbyfloatptr)));
  ////HKeysCmd
  std::unique_ptr<Cmd> hkeysptr =
      std::make_unique<HKeysCmd>(kCmdNameHKeys, 2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsHash | kCmdFlagsUpdateCache | kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameHKeys, std::move(hkeysptr)));
  ////HLenCmd
  std::unique_ptr<Cmd> hlenptr =
      std::make_unique<HLenCmd>(kCmdNameHLen, 2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsHash | kCmdFlagsUpdateCache | kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameHLen, std::move(hlenptr)));
  ////HMgetCmd
  std::unique_ptr<Cmd> hmgetptr =
      std::make_unique<HMgetCmd>(kCmdNameHMget, -3, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsHash | kCmdFlagsUpdateCache | kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameHMget, std::move(hmgetptr)));
  ////HMsetCmd
  std::unique_ptr<Cmd> hmsetptr =
      std::make_unique<HMsetCmd>(kCmdNameHMset, -4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsHash | kCmdFlagsUpdateCache | kCmdFlagsDoThroughDB | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameHMset, std::move(hmsetptr)));
  ////HSetnxCmd
  std::unique_ptr<Cmd> hsetnxptr =
      std::make_unique<HSetnxCmd>(kCmdNameHSetnx, 4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsHash | kCmdFlagsUpdateCache | kCmdFlagsDoThroughDB | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameHSetnx, std::move(hsetnxptr)));
  ////HStrlenCmd
  std::unique_ptr<Cmd> hstrlenptr =
      std::make_unique<HStrlenCmd>(kCmdNameHStrlen, 3, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsHash | kCmdFlagsUpdateCache | kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameHStrlen, std::move(hstrlenptr)));
  ////HValsCmd
  std::unique_ptr<Cmd> hvalsptr =
      std::make_unique<HValsCmd>(kCmdNameHVals, 2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsHash | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameHVals, std::move(hvalsptr)));
  ////HScanCmd
  std::unique_ptr<Cmd> hscanptr = std::make_unique<HScanCmd>(
      kCmdNameHScan, -3, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsHash | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameHScan, std::move(hscanptr)));
  ////HScanxCmd
  std::unique_ptr<Cmd> hscanxptr = std::make_unique<HScanxCmd>(
      kCmdNameHScanx, -3, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsHash | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameHScanx, std::move(hscanxptr)));
  ////PKHScanRange
  std::unique_ptr<Cmd> pkhscanrangeptr = std::make_unique<PKHScanRangeCmd>(
      kCmdNamePKHScanRange, -4, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsHash | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePKHScanRange, std::move(pkhscanrangeptr)));
  ////PKHRScanRange
  std::unique_ptr<Cmd> pkhrscanrangeptr = std::make_unique<PKHRScanRangeCmd>(
      kCmdNamePKHRScanRange, -4, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsHash | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePKHRScanRange, std::move(pkhrscanrangeptr)));

  // List
  std::unique_ptr<Cmd> lindexptr =
      std::make_unique<LIndexCmd>(kCmdNameLIndex, 3, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsList | kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameLIndex, std::move(lindexptr)));
  std::unique_ptr<Cmd> linsertptr =
      std::make_unique<LInsertCmd>(kCmdNameLInsert, 5, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameLInsert, std::move(linsertptr)));

  std::unique_ptr<Cmd> llenptr =
      std::make_unique<LLenCmd>(kCmdNameLLen, 2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsList | kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameLLen, std::move(llenptr)));

  std::unique_ptr<Cmd> blpopptr = std::make_unique<BLPopCmd>(
      kCmdNameBLPop, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameBLPop, std::move(blpopptr)));

  std::unique_ptr<Cmd> lpopptr =
      std::make_unique<LPopCmd>(kCmdNameLPop, -2, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList |kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameLPop, std::move(lpopptr)));

  std::unique_ptr<Cmd> lpushptr = std::make_unique<LPushCmd>(
      kCmdNameLPush, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameLPush, std::move(lpushptr)));

  std::unique_ptr<Cmd> lpushxptr = std::make_unique<LPushxCmd>(kCmdNameLPushx, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameLPushx, std::move(lpushxptr)));

  std::unique_ptr<Cmd> lrangeptr = std::make_unique<LRangeCmd>(
      kCmdNameLRange, 4, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsList | kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameLRange, std::move(lrangeptr)));
  std::unique_ptr<Cmd> lremptr =
      std::make_unique<LRemCmd>(kCmdNameLRem, 4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameLRem, std::move(lremptr)));
  std::unique_ptr<Cmd> lsetptr =
      std::make_unique<LSetCmd>(kCmdNameLSet, 4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameLSet, std::move(lsetptr)));
  std::unique_ptr<Cmd> ltrimptr =
      std::make_unique<LTrimCmd>(kCmdNameLTrim, 4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList |kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameLTrim, std::move(ltrimptr)));

  std::unique_ptr<Cmd> brpopptr = std::make_unique<BRPopCmd>(
      kCmdNameBRpop, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameBRpop, std::move(brpopptr)));
  std::unique_ptr<Cmd> rpopptr =
      std::make_unique<RPopCmd>(kCmdNameRPop, -2, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameRPop, std::move(rpopptr)));

  std::unique_ptr<Cmd> rpoplpushptr = std::make_unique<RPopLPushCmd>(
      kCmdNameRPopLPush, 3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameRPopLPush, std::move(rpoplpushptr)));
  std::unique_ptr<Cmd> rpushptr =
      std::make_unique<RPushCmd>(kCmdNameRPush, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot |kCmdFlagsList | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameRPush, std::move(rpushptr)));
  std::unique_ptr<Cmd> rpushxptr =
      std::make_unique<RPushxCmd>(kCmdNameRPushx, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList);
  std::make_unique<RPushxCmd>(kCmdNameRPushx, 3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameRPushx, std::move(rpushxptr)));

  // Zset
  ////ZAddCmd
  std::unique_ptr<Cmd> zaddptr =
      std::make_unique<ZAddCmd>(kCmdNameZAdd, -4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsZset |kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZAdd, std::move(zaddptr)));
  ////ZCardCmd
  std::unique_ptr<Cmd> zcardptr =
      std::make_unique<ZCardCmd>(kCmdNameZCard, 2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsZset | kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZCard, std::move(zcardptr)));
  ////ZScanCmd
  std::unique_ptr<Cmd> zscanptr = std::make_unique<ZScanCmd>(
      kCmdNameZScan, -3, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsZset | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZScan, std::move(zscanptr)));
  ////ZIncrbyCmd
  std::unique_ptr<Cmd> zincrbyptr =
      std::make_unique<ZIncrbyCmd>(kCmdNameZIncrby, 4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsZset | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast) ;
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZIncrby, std::move(zincrbyptr)));
  ////ZRangeCmd
  std::unique_ptr<Cmd> zrangeptr =
      std::make_unique<ZRangeCmd>(kCmdNameZRange, -4, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsZset |kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZRange, std::move(zrangeptr)));
  ////ZRevrangeCmd
  std::unique_ptr<Cmd> zrevrangeptr =
      std::make_unique<ZRevrangeCmd>(kCmdNameZRevrange, -4, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsZset |kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZRevrange, std::move(zrevrangeptr)));
  ////ZRangebyscoreCmd
  std::unique_ptr<Cmd> zrangebyscoreptr = std::make_unique<ZRangebyscoreCmd>(
      kCmdNameZRangebyscore, -4, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsZset | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZRangebyscore, std::move(zrangebyscoreptr)));
  ////ZRevrangebyscoreCmd
  std::unique_ptr<Cmd> zrevrangebyscoreptr = std::make_unique<ZRevrangebyscoreCmd>(
      kCmdNameZRevrangebyscore, -4, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsZset | kCmdFlagsSlow);
  cmd_table->insert(
      std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZRevrangebyscore, std::move(zrevrangebyscoreptr)));
  ////ZCountCmd
  std::unique_ptr<Cmd> zcountptr =
      std::make_unique<ZCountCmd>(kCmdNameZCount, 4, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsZset |kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZCount, std::move(zcountptr)));
  ////ZRemCmd
  std::unique_ptr<Cmd> zremptr =
      std::make_unique<ZRemCmd>(kCmdNameZRem, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsZset | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZRem, std::move(zremptr)));
  ////ZUnionstoreCmd
  std::unique_ptr<Cmd> zunionstoreptr =
      std::make_unique<ZUnionstoreCmd>(kCmdNameZUnionstore, -4, kCmdFlagsWrite | kCmdFlagsMultiSlot | kCmdFlagsZset |kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZUnionstore, std::move(zunionstoreptr)));
  ////ZInterstoreCmd
  std::unique_ptr<Cmd> zinterstoreptr =
      std::make_unique<ZInterstoreCmd>(kCmdNameZInterstore, -4, kCmdFlagsWrite | kCmdFlagsMultiSlot | kCmdFlagsZset |kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZInterstore, std::move(zinterstoreptr)));
  ////ZRankCmd
  std::unique_ptr<Cmd> zrankptr =
      std::make_unique<ZRankCmd>(kCmdNameZRank, 3, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsZset | kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZRank, std::move(zrankptr)));
  ////ZRevrankCmd
  std::unique_ptr<Cmd> zrevrankptr =
      std::make_unique<ZRevrankCmd>(kCmdNameZRevrank, 3, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsZset |kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZRevrank, std::move(zrevrankptr)));
  ////ZScoreCmd
  std::unique_ptr<Cmd> zscoreptr =
      std::make_unique<ZScoreCmd>(kCmdNameZScore, 3, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsZset |kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZScore, std::move(zscoreptr)));
  ////ZRangebylexCmd
  std::unique_ptr<Cmd> zrangebylexptr =
      std::make_unique<ZRangebylexCmd>(kCmdNameZRangebylex, -4, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsZset | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZRangebylex, std::move(zrangebylexptr)));
  ////ZRevrangebylexCmd
  std::unique_ptr<Cmd> zrevrangebylexptr = std::make_unique<ZRevrangebylexCmd>(
      kCmdNameZRevrangebylex, -4, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsZset | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZRevrangebylex, std::move(zrevrangebylexptr)));
  ////ZLexcountCmd
  std::unique_ptr<Cmd> zlexcountptr =
      std::make_unique<ZLexcountCmd>(kCmdNameZLexcount, 4, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsZset | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZLexcount, std::move(zlexcountptr)));
  ////ZRemrangebyrankCmd
  std::unique_ptr<Cmd> zremrangebyrankptr = std::make_unique<ZRemrangebyrankCmd>(
      kCmdNameZRemrangebyrank, 4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsZset |kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(
      std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZRemrangebyrank, std::move(zremrangebyrankptr)));
  ////ZRemrangebyscoreCmd
  std::unique_ptr<Cmd> zremrangebyscoreptr = std::make_unique<ZRemrangebyscoreCmd>(
      kCmdNameZRemrangebyscore, 4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsZset |kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(
      std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZRemrangebyscore, std::move(zremrangebyscoreptr)));
  ////ZRemrangebylexCmd
  std::unique_ptr<Cmd> zremrangebylexptr = std::make_unique<ZRemrangebylexCmd>(
      kCmdNameZRemrangebylex, 4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsZset |kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZRemrangebylex, std::move(zremrangebylexptr)));
  ////ZPopmax
  std::unique_ptr<Cmd> zpopmaxptr = std::make_unique<ZPopmaxCmd>(
      kCmdNameZPopmax, -2, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsZset | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZPopmax, std::move(zpopmaxptr)));
  ////ZPopmin
  std::unique_ptr<Cmd> zpopminptr = std::make_unique<ZPopminCmd>(
      kCmdNameZPopmin, -2, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsZset | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameZPopmin, std::move(zpopminptr)));

  // Set
  ////SAddCmd
  std::unique_ptr<Cmd> saddptr =
      std::make_unique<SAddCmd>(kCmdNameSAdd, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsSet |kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSAdd, std::move(saddptr)));
  ////SPopCmd
  std::unique_ptr<Cmd> spopptr =
      std::make_unique<SPopCmd>(kCmdNameSPop, -2, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsSet | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSPop, std::move(spopptr)));
  ////SCardCmd
  std::unique_ptr<Cmd> scardptr =
      std::make_unique<SCardCmd>(kCmdNameSCard, 2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsSet | kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSCard, std::move(scardptr)));
  ////SMembersCmd
  std::unique_ptr<Cmd> smembersptr =
      std::make_unique<SMembersCmd>(kCmdNameSMembers, 2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsSet | kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSMembers, std::move(smembersptr)));
  ////SScanCmd
  std::unique_ptr<Cmd> sscanptr =
      std::make_unique<SScanCmd>(kCmdNameSScan, -3, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsSet | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSScan, std::move(sscanptr)));
  ////SRemCmd
  std::unique_ptr<Cmd> sremptr =
      std::make_unique<SRemCmd>(kCmdNameSRem, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsSet |kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSRem, std::move(sremptr)));
  ////SUnionCmd
  std::unique_ptr<Cmd> sunionptr = std::make_unique<SUnionCmd>(
      kCmdNameSUnion, -2, kCmdFlagsRead | kCmdFlagsMultiSlot | kCmdFlagsSet | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSUnion, std::move(sunionptr)));
  ////SUnionstoreCmd
  std::unique_ptr<Cmd> sunionstoreptr =
      std::make_unique<SUnionstoreCmd>(kCmdNameSUnionstore, -3, kCmdFlagsWrite | kCmdFlagsMultiSlot | kCmdFlagsSet | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSUnionstore, std::move(sunionstoreptr)));
  ////SInterCmd
  std::unique_ptr<Cmd> sinterptr = std::make_unique<SInterCmd>(
      kCmdNameSInter, -2, kCmdFlagsRead | kCmdFlagsMultiSlot | kCmdFlagsSet | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSInter, std::move(sinterptr)));
  ////SInterstoreCmd
  std::unique_ptr<Cmd> sinterstoreptr =
      std::make_unique<SInterstoreCmd>(kCmdNameSInterstore, -3, kCmdFlagsWrite | kCmdFlagsMultiSlot | kCmdFlagsSet | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSInterstore, std::move(sinterstoreptr)));
  ////SIsmemberCmd
  std::unique_ptr<Cmd> sismemberptr =
      std::make_unique<SIsmemberCmd>(kCmdNameSIsmember, 3, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsSet |kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSIsmember, std::move(sismemberptr)));
  ////SDiffCmd
  std::unique_ptr<Cmd> sdiffptr =
      std::make_unique<SDiffCmd>(kCmdNameSDiff, -2, kCmdFlagsRead | kCmdFlagsMultiSlot | kCmdFlagsSet | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSDiff, std::move(sdiffptr)));
  ////SDiffstoreCmd
  std::unique_ptr<Cmd> sdiffstoreptr =
      std::make_unique<SDiffstoreCmd>(kCmdNameSDiffstore, -3, kCmdFlagsWrite | kCmdFlagsMultiSlot | kCmdFlagsSet |kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSDiffstore, std::move(sdiffstoreptr)));
  ////SMoveCmd
  std::unique_ptr<Cmd> smoveptr =
      std::make_unique<SMoveCmd>(kCmdNameSMove, 4, kCmdFlagsWrite | kCmdFlagsMultiSlot | kCmdFlagsSet | kCmdFlagsDoThroughDB | kCmdFlagsUpdateCache | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSMove, std::move(smoveptr)));
  ////SRandmemberCmd
  std::unique_ptr<Cmd> srandmemberptr =
      std::make_unique<SRandmemberCmd>(kCmdNameSRandmember, -2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsSet|kCmdFlagsDoThroughDB | kCmdFlagsReadCache | kCmdFlagsUpdateCache | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSRandmember, std::move(srandmemberptr)));

  // BitMap
  ////bitsetCmd
  std::unique_ptr<Cmd> bitsetptr =
      std::make_unique<BitSetCmd>(kCmdNameBitSet, 4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsBit | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameBitSet, std::move(bitsetptr)));
  ////bitgetCmd
  std::unique_ptr<Cmd> bitgetptr =
      std::make_unique<BitGetCmd>(kCmdNameBitGet, 3, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsBit | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameBitGet, std::move(bitgetptr)));
  ////bitcountCmd
  std::unique_ptr<Cmd> bitcountptr =
      std::make_unique<BitCountCmd>(kCmdNameBitCount, -2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsBit | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameBitCount, std::move(bitcountptr)));
  ////bitposCmd
  std::unique_ptr<Cmd> bitposptr =
      std::make_unique<BitPosCmd>(kCmdNameBitPos, -3, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsBit | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameBitPos, std::move(bitposptr)));
  ////bitopCmd
  std::unique_ptr<Cmd> bitopptr =
      std::make_unique<BitOpCmd>(kCmdNameBitOp, -3, kCmdFlagsWrite | kCmdFlagsMultiSlot | kCmdFlagsBit | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameBitOp, std::move(bitopptr)));

  // HyperLogLog
  ////pfaddCmd
  std::unique_ptr<Cmd> pfaddptr = std::make_unique<PfAddCmd>(
      kCmdNamePfAdd, -2, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsHyperLogLog | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePfAdd, std::move(pfaddptr)));
  ////pfcountCmd
  std::unique_ptr<Cmd> pfcountptr = std::make_unique<PfCountCmd>(
      kCmdNamePfCount, -2, kCmdFlagsRead | kCmdFlagsMultiSlot | kCmdFlagsHyperLogLog | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePfCount, std::move(pfcountptr)));
  ////pfmergeCmd
  std::unique_ptr<Cmd> pfmergeptr = std::make_unique<PfMergeCmd>(
      kCmdNamePfMerge, -3, kCmdFlagsWrite | kCmdFlagsMultiSlot | kCmdFlagsHyperLogLog | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePfMerge, std::move(pfmergeptr)));

  // GEO
  ////GepAdd
  std::unique_ptr<Cmd> geoaddptr = std::make_unique<GeoAddCmd>(
      kCmdNameGeoAdd, -5, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsGeo | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameGeoAdd, std::move(geoaddptr)));
  ////GeoPos
  std::unique_ptr<Cmd> geoposptr = std::make_unique<GeoPosCmd>(
      kCmdNameGeoPos, -2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsGeo | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameGeoPos, std::move(geoposptr)));
  ////GeoDist
  std::unique_ptr<Cmd> geodistptr = std::make_unique<GeoDistCmd>(
      kCmdNameGeoDist, -4, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsGeo | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameGeoDist, std::move(geodistptr)));
  ////GeoHash
  std::unique_ptr<Cmd> geohashptr = std::make_unique<GeoHashCmd>(
      kCmdNameGeoHash, -2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsGeo | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameGeoHash, std::move(geohashptr)));
  ////GeoRadius
  std::unique_ptr<Cmd> georadiusptr = std::make_unique<GeoRadiusCmd>(
      kCmdNameGeoRadius, -6, kCmdFlagsRead | kCmdFlagsMultiSlot | kCmdFlagsGeo | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameGeoRadius, std::move(georadiusptr)));
  ////GeoRadiusByMember
  std::unique_ptr<Cmd> georadiusbymemberptr = std::make_unique<GeoRadiusByMemberCmd>(
      kCmdNameGeoRadiusByMember, -5, kCmdFlagsRead | kCmdFlagsMultiSlot | kCmdFlagsGeo | kCmdFlagsSlow);
  cmd_table->insert(
      std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameGeoRadiusByMember, std::move(georadiusbymemberptr)));

  // PubSub
  ////Publish
  std::unique_ptr<Cmd> publishptr =
      std::make_unique<PublishCmd>(kCmdNamePublish, 3, kCmdFlagsRead | kCmdFlagsPubSub | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePublish, std::move(publishptr)));
  ////Subscribe
  std::unique_ptr<Cmd> subscribeptr =
      std::make_unique<SubscribeCmd>(kCmdNameSubscribe, -2, kCmdFlagsRead | kCmdFlagsPubSub | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameSubscribe, std::move(subscribeptr)));
  ////UnSubscribe
  std::unique_ptr<Cmd> unsubscribeptr =
      std::make_unique<UnSubscribeCmd>(kCmdNameUnSubscribe, -1, kCmdFlagsRead | kCmdFlagsPubSub | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameUnSubscribe, std::move(unsubscribeptr)));
  ////PSubscribe
  std::unique_ptr<Cmd> psubscribeptr =
      std::make_unique<PSubscribeCmd>(kCmdNamePSubscribe, -2, kCmdFlagsRead | kCmdFlagsPubSub | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePSubscribe, std::move(psubscribeptr)));
  ////PUnSubscribe
  std::unique_ptr<Cmd> punsubscribeptr =
      std::make_unique<PUnSubscribeCmd>(kCmdNamePUnSubscribe, -1, kCmdFlagsRead | kCmdFlagsPubSub | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePUnSubscribe, std::move(punsubscribeptr)));
  ////PubSub
  std::unique_ptr<Cmd> pubsubptr =
      std::make_unique<PubSubCmd>(kCmdNamePubSub, -2, kCmdFlagsRead | kCmdFlagsPubSub | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNamePubSub, std::move(pubsubptr)));

  ////ACL
  std::unique_ptr<Cmd> aclptr = std::make_unique<PikaAclCmd>(KCmdNameAcl, -2, kCmdFlagsAdmin | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(KCmdNameAcl, std::move(aclptr)));

  // Transaction
  ////Multi
  std::unique_ptr<Cmd> multiptr =
      std::make_unique<MultiCmd>(kCmdNameMulti, 1, kCmdFlagsRead | kCmdFlagsMultiSlot | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameMulti, std::move(multiptr)));
  ////Exec
  std::unique_ptr<Cmd> execptr = std::make_unique<ExecCmd>(
      kCmdNameExec, 1, kCmdFlagsRead | kCmdFlagsWrite | kCmdFlagsMultiSlot | kCmdFlagsSuspend | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameExec, std::move(execptr)));
  ////Discard
  std::unique_ptr<Cmd> discardptr = std::make_unique<DiscardCmd>(kCmdNameDiscard, 1, kCmdFlagsRead | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameDiscard, std::move(discardptr)));
  ////Watch
  std::unique_ptr<Cmd> watchptr = std::make_unique<WatchCmd>(kCmdNameWatch, -2, kCmdFlagsRead | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameWatch, std::move(watchptr)));
  ////Unwatch
  std::unique_ptr<Cmd> unwatchptr = std::make_unique<UnwatchCmd>(kCmdNameUnWatch, 1, kCmdFlagsRead | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameUnWatch, std::move(unwatchptr)));

  // Stream
  ////XAdd
  std::unique_ptr<Cmd> xaddptr =
      std::make_unique<XAddCmd>(kCmdNameXAdd, -4, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsStream | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameXAdd, std::move(xaddptr)));
  ////XLen
  std::unique_ptr<Cmd> xlenptr =
      std::make_unique<XLenCmd>(kCmdNameXLen, 2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsStream | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameXLen, std::move(xlenptr)));
  ////XRead
  std::unique_ptr<Cmd> xreadptr =
      std::make_unique<XReadCmd>(kCmdNameXRead, -3, kCmdFlagsRead | kCmdFlagsMultiSlot | kCmdFlagsStream | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameXRead, std::move(xreadptr)));
  ////XRange
  std::unique_ptr<Cmd> xrangeptr =
      std::make_unique<XRangeCmd>(kCmdNameXRange, -4, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsStream | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameXRange, std::move(xrangeptr)));
  ////XRerange
  std::unique_ptr<Cmd> xrerverangeptr =
        std::make_unique<XRevrangeCmd>(kCmdNameXRevrange, -4, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsStream | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameXRevrange, std::move(xrerverangeptr)));
  ////XTrim
  std::unique_ptr<Cmd> xtrimptr =
      std::make_unique<XTrimCmd>(kCmdNameXTrim, -2, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsStream | kCmdFlagsSlow);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameXTrim, std::move(xtrimptr)));
  ////XDel
  std::unique_ptr<Cmd> xdelptr =
      std::make_unique<XDelCmd>(kCmdNameXDel, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsStream | kCmdFlagsFast);
  cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameXDel, std::move(xdelptr)));
  ////XINFO
    std::unique_ptr<Cmd> xinfoptr =
        std::make_unique<XInfoCmd>(kCmdNameXInfo, -2, kCmdFlagsRead | kCmdFlagsSingleSlot | kCmdFlagsStream | kCmdFlagsSlow);
    cmd_table->insert(std::pair<std::string, std::unique_ptr<Cmd>>(kCmdNameXInfo, std::move(xinfoptr)));
}

Cmd* GetCmdFromDB(const std::string& opt, const CmdTable& cmd_table) {
  auto it = cmd_table.find(opt);
  if (it != cmd_table.end()) {
    return it->second.get();
  }
  return nullptr;
}

Cmd::Cmd(std::string name, int arity, uint32_t flag, uint32_t aclCategory)
    : name_(std::move(name)), arity_(arity), flag_(flag), aclCategory_(aclCategory) {
  // assign cmd id
  cmdId_ = g_pika_cmd_table_manager->GetCmdId();
}

void Cmd::Initial(const PikaCmdArgsType& argv, const std::string& db_name) {
  argv_ = argv;
  db_name_ = db_name;
  res_.clear();  // Clear res content
  Clear();       // Clear cmd, Derived class can has own implement
  DoInitial();
};

std::vector<std::string> Cmd::current_key() const { return {""}; }

void Cmd::Execute() { ProcessSingleSlotCmd(); }

void Cmd::ProcessSingleSlotCmd() {
  std::shared_ptr<Slot> slot;
  slot = g_pika_server->GetSlotByDBName(db_name_);

  if (!slot) {
    res_.SetRes(CmdRes::kErrOther, "Slot not found");
    return;
  }

  std::shared_ptr<SyncMasterSlot> sync_slot =
      g_pika_rm->GetSyncMasterSlotByName(SlotInfo(slot->GetDBName(), slot->GetSlotID()));
  if (!sync_slot) {
    res_.SetRes(CmdRes::kErrOther, "Slot not found");
    return;
  }
  ProcessCommand(slot, sync_slot);
}

void Cmd::ProcessCommand(const std::shared_ptr<Slot>& slot, const std::shared_ptr<SyncMasterSlot>& sync_slot,
                         const HintKeys& hint_keys) {
  if (stage_ == kNone) {
    InternalProcessCommand(slot, sync_slot, hint_keys);
  } else {
    if (stage_ == kBinlogStage) {
      DoBinlog(sync_slot);
    } else if (stage_ == kExecuteStage) {
      DoCommand(slot, hint_keys);
    }
  }
}

void Cmd::InternalProcessCommand(const std::shared_ptr<Slot>& slot, const std::shared_ptr<SyncMasterSlot>& sync_slot,
                                 const HintKeys& hint_keys) {
  pstd::lock::MultiRecordLock record_lock(slot->LockMgr());
  if (is_write()) {
    record_lock.Lock(current_key());
  }

  uint64_t start_us = 0;
  if (g_pika_conf->slowlog_slower_than() >= 0) {
    start_us = pstd::NowMicros();
  }
  DoCommand(slot, hint_keys);
  if (g_pika_conf->slowlog_slower_than() >= 0) {
    do_duration_ += pstd::NowMicros() - start_us;
  }

  DoBinlog(sync_slot);

  if (is_write()) {
    record_lock.Unlock(current_key());
  }
}

void Cmd::DoCommand(const std::shared_ptr<Slot>& slot, const HintKeys& hint_keys) {
  if (!IsSuspend()) {
    slot->DbRWLockReader();
  }
  DEFER {
    if (!IsSuspend()) {
      slot->DbRWUnLock();
    }
  };
  if (IsNeedCacheDo()
      && PIKA_CACHE_NONE != g_pika_conf->cache_model()
      && slot->cache()->CacheStatus() == PIKA_CACHE_STATUS_OK) {
    if (IsNeedReadCache()) {
      ReadCache(slot);
    }
    if (is_read() && res().CacheMiss()) {
      pstd::lock::MultiScopeRecordLock record_lock(slot->LockMgr(), current_key());
      DoThroughDB(slot);
      if (IsNeedUpdateCache()) {
        DoUpdateCache(slot);
      }
    } else if (is_write()) {
      DoThroughDB(slot);
      if (IsNeedUpdateCache()) {
        DoUpdateCache(slot);
      }
    }
  } else {
    Do(slot);
  }
}

void Cmd::DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) {
  if (res().ok() && is_write() && g_pika_conf->write_binlog()) {
    std::shared_ptr<net::NetConn> conn_ptr = GetConn();
    std::shared_ptr<std::string> resp_ptr = GetResp();
    // Consider that dummy cmd appended by system, both conn and resp are null.
    if ((!conn_ptr || !resp_ptr) && (name_ != kCmdDummy)) {
      if (!conn_ptr) {
        LOG(WARNING) << slot->SyncSlotInfo().ToString() << " conn empty.";
      }
      if (!resp_ptr) {
        LOG(WARNING) << slot->SyncSlotInfo().ToString() << " resp empty.";
      }
      res().SetRes(CmdRes::kErrOther);
      return;
    }

    Status s = slot->ConsensusProposeLog(shared_from_this());
    if (!s.ok()) {
      LOG(WARNING) << slot->SyncSlotInfo().ToString() << " Writing binlog failed, maybe no space left on device "
                   << s.ToString();
      res().SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
  }
}

void Cmd::ProcessMultiSlotCmd() {
  std::shared_ptr<Slot> slot;
  std::vector<std::string> cur_key = current_key();
  if (cur_key.empty()) {
    res_.SetRes(CmdRes::kErrOther, "Internal Error");
    return;
  }

  int hint = 0;
  std::unordered_map<uint32_t, ProcessArg> process_map;
  // split cur_key into slots
  std::shared_ptr<DB> db = g_pika_server->GetDB(db_name_);
  if (!db) {
    res_.SetRes(CmdRes::kErrOther, "DB not found");
    return;
  }

  CmdStage current_stage = stage_;
  for (auto& key : cur_key) {
    // in sharding mode we select slot by key
    uint32_t slot_id = g_pika_cmd_table_manager->DistributeKey(key, db->SlotNum());
    auto iter = process_map.find(slot_id);
    if (iter == process_map.end()) {
      std::shared_ptr<Slot> slot = db->GetSlotById(slot_id);
      if (!slot) {
        res_.SetRes(CmdRes::kErrOther, "Slot not found");
        return;
      }
      std::shared_ptr<SyncMasterSlot> sync_slot =
          g_pika_rm->GetSyncMasterSlotByName(SlotInfo(slot->GetDBName(), slot->GetSlotID()));
      if (!sync_slot) {
        res_.SetRes(CmdRes::kErrOther, "Slot not found");
        return;
      }
      HintKeys hint_keys;
      hint_keys.Push(key, hint);
      process_map[slot_id] = ProcessArg(slot, sync_slot, hint_keys);
    } else {
      iter->second.hint_keys.Push(key, hint);
    }
    hint++;
  }
  for (auto& iter : process_map) {
    ProcessArg& arg = iter.second;
    ProcessCommand(arg.slot, arg.sync_slot, arg.hint_keys);
    if (!res_.ok()) {
      return;
    }
  }
  if (current_stage == kNone || current_stage == kExecuteStage) {
    Merge();
  }
}

uint32_t Cmd::flag() const { return flag_; }
bool Cmd::hasFlag(uint32_t flag) const { return (flag_ & flag); }
bool Cmd::is_read() const { return (flag_ & kCmdFlagsRead); }
bool Cmd::is_write() const { return (flag_ & kCmdFlagsWrite); }
bool Cmd::IsLocal() const { return (flag_ & kCmdFlagsLocal); }

int8_t Cmd::SubCmdIndex(const std::string& cmdName) {
  if (subCmdName_.empty()) {
    return -1;
  }
  for (size_t i = 0; i < subCmdName_.size(); ++i) {
    if (!strcasecmp(subCmdName_[i].data(), cmdName.data())) {
      return i;
    }
  }
  return -1;
}

// Others need to be suspended when a suspend command run
bool Cmd::IsSuspend() const { return (flag_ & kCmdFlagsSuspend); }
// Must with admin auth
bool Cmd::is_single_slot() const { return (flag_ & kCmdFlagsSingleSlot); }
bool Cmd::is_multi_slot() const { return (flag_ & kCmdFlagsMultiSlot); }
// std::string Cmd::CurrentSubCommand() const { return ""; };
bool Cmd::HasSubCommand() const { return subCmdName_.size() > 0; };
std::vector<std::string> Cmd::SubCommand() const { return subCmdName_; };
bool Cmd::IsAdminRequire() const { return (flag_ & kCmdFlagsAdminRequire); }
bool Cmd::IsNeedUpdateCache() const { return (flag_ & kCmdFlagsUpdateCache); }
bool Cmd::IsNeedCacheDo() const {
  if (g_pika_conf->IsCacheDisabledTemporarily()) {
    return false;
  }

  if (hasFlag(kCmdFlagsKv)) {
    if (!g_pika_conf->GetCacheString()) {
      return false;
    }
  } else if (hasFlag(kCmdFlagsSet)) {
    if (!g_pika_conf->GetCacheSet()) {
      return false;
    }
  } else if (hasFlag(kCmdFlagsZset)) {
    if (!g_pika_conf->GetCacheZset()) {
      return false;
    }
  } else if (hasFlag(kCmdFlagsHash)) {
    if (!g_pika_conf->GetCacheHash()) {
      return false;
    }
  } else if (hasFlag(kCmdFlagsList)) {
    if (!g_pika_conf->GetCacheList()) {
      return false;
    }
  } else if (hasFlag(kCmdFlagsBit)) {
    if (!g_pika_conf->GetCacheBit()) {
      return false;
    }
  }
  return (hasFlag(kCmdFlagsDoThroughDB));
}

bool Cmd::IsNeedReadCache() const { return hasFlag(kCmdFlagsReadCache); }

bool Cmd::HashtagIsConsistent(const std::string& lhs, const std::string& rhs) const { return true; }

std::string Cmd::name() const { return name_; }
CmdRes& Cmd::res() { return res_; }

std::string Cmd::db_name() const { return db_name_; }

PikaCmdArgsType& Cmd::argv() { return argv_; }

uint32_t Cmd::AclCategory() const { return aclCategory_; }

void Cmd::AddAclCategory(uint32_t aclCategory) { aclCategory_ |= aclCategory; }

std::string Cmd::ToRedisProtocol() {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLenUint64(content, argv_.size(), "*");

  for (const auto& v : argv_) {
    RedisAppendLenUint64(content, v.size(), "$");
    RedisAppendContent(content, v);
  }

  return content;
}

bool Cmd::CheckArg(uint64_t num) const { return !((arity_ > 0 && num != arity_) || (arity_ < 0 && num < -arity_)); }

void Cmd::LogCommand() const {
  std::string command;
  for (const auto& item : argv_) {
    command.append(" ");
    command.append(item);
  }
  LOG(INFO) << "command:" << command;
}

void Cmd::SetConn(const std::shared_ptr<net::NetConn>& conn) { conn_ = conn; }

std::shared_ptr<net::NetConn> Cmd::GetConn() { return conn_.lock(); }

void Cmd::SetResp(const std::shared_ptr<std::string>& resp) { resp_ = resp; }

std::shared_ptr<std::string> Cmd::GetResp() { return resp_.lock(); }

void Cmd::SetStage(CmdStage stage) { stage_ = stage; }
