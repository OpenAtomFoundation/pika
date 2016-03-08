#include "pika_admin.h"
#include "pika_kv.h"

static std::unordered_map<std::string, CmdInfo*> cmd_infos(300);    /* Table for CmdInfo */

//Remember the first arg is the command name
void InitCmdInfoTable() {
  //Admin
  ////Slaveof
  CmdInfo* slaveofptr = new CmdInfo(kCmdNameSlaveof, -3, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSlaveof, slaveofptr));
  ////Trysync
  CmdInfo* trysyncptr = new CmdInfo(kCmdNameTrysync, 5, kCmdFlagsRead | kCmdFlagsAdmin);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameTrysync, trysyncptr));

  //Kv
  ////SetCmd
  CmdInfo* setptr = new CmdInfo(kCmdNameSet, -3, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameSet, setptr));
  ////GetCmd
  CmdInfo* getptr = new CmdInfo(kCmdNameGet, 2, kCmdFlagsRead | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameGet, getptr));

  //Hash

  //List

  //Zset

  //Set
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

  //Kv
  ////SetCmd
  Cmd* setptr = new SetCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameSet, setptr));
  ////GetCmd
  Cmd* getptr = new GetCmd();
  cmd_table->insert(std::pair<std::string, Cmd*>(kCmdNameGet, getptr));
  
  //Hash
  
  //List

  //Zset

  //Set

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

