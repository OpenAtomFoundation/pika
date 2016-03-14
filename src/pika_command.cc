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
  CmdInfo* mgetptr = new CmdInfo(kCmdNameMget, -2, kCmdFlagsWrite | kCmdFlagsKv);
  cmd_infos.insert(std::pair<std::string, CmdInfo*>(kCmdNameMget, mgetptr));
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

