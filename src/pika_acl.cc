// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_acl.h"
#include "fmt/format.h"
#include "include/pika_client_conn.h"
#include "include/pika_cmd_table_manager.h"

extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;

void PikaAclCmd::Do(std::shared_ptr<Slot> slot) {
  if (subCmd_ == "cat") {
    Cat();
  } else if (subCmd_ == "deluser") {
    DelUser();
  } else if (subCmd_ == "dryrun") {
    DryRun();
  } else if (subCmd_ == "genpass") {
    GenPass();
  } else if (subCmd_ == "getuser") {
    GetUser();
  } else if (subCmd_ == "list") {
    List();
  } else if (subCmd_ == "load") {
    Load();
  } else if (subCmd_ == "log") {
    Log();
  } else if (subCmd_ == "save") {
    Save();
  } else if (subCmd_ == "setuser") {
    SetUser();
  } else if (subCmd_ == "users") {
    Users();
  } else if (subCmd_ == "whoami") {
    WhoAmI();
  } else if (subCmd_ == "help") {
    Help();
  } else {
    res_.SetRes(CmdRes::kSyntaxErr, KCmdNameAcl);
    return;
  }
}

void PikaAclCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, KCmdNameAcl);
    return;
  }

  subCmd_ = argv_[1];
  pstd::StringToLower(subCmd_);

  if (argv_.size() < 3) {
    if (subCmd_ == "setuser" || subCmd_ == "deluser" || subCmd_ == "getuser") {
      res_.SetRes(CmdRes::kWrongNum, fmt::format("'acl|{}'", subCmd_));
      return;
    }
  }

  if (subCmd_ == "save" || subCmd_ == "load") {
    if (g_pika_conf->acl_file().empty()) {
      res().SetRes(CmdRes::kErrOther,
                   "This Pika is not configured to use an ACL file. You may want to specify users via the "
                   "ACL SETUSER command and then issue a CONFIG REWRITE (assuming you have a Redis configuration file "
                   "set) in order to store users in the Pika configuration.");
      return;
    }
  }
}

void PikaAclCmd::Cat() {
  if (argv_.size() == 2) {
    res().AppendStringVector(Acl::GetAllCategoryName());
    return;
  }
  auto category = g_pika_server->Acl()->GetCommandCategoryFlagByName(argv_[2]);
  if (category == 0) {
    res().SetRes(CmdRes::kErrOther, fmt::format("Unknown category '{}'", argv_[2]));
    return;
  }
  res().AppendStringVector(g_pika_cmd_table_manager->GetAclCategoryCmdNames(category));
}

void PikaAclCmd::DelUser() {
  std::vector<std::string> userNames(argv_.begin() + 2, argv_.end());
  auto delUserNames = g_pika_server->Acl()->DeleteUser(userNames);
  res().AppendInteger(delUserNames.size());

  g_pika_server->AllClientUnAuth(delUserNames);
}

void PikaAclCmd::DryRun() {}

void PikaAclCmd::GenPass() {}

void PikaAclCmd::GetUser() {
  auto user = g_pika_server->Acl()->GetUser(argv_[2]);

  if (!user) {
    res().AppendStringLen(-1);
    return;
  }

  user->GetUserDescribe(&res_);
}

void PikaAclCmd::List() {
  std::vector<std::string> result;
  g_pika_server->Acl()->DescribeAllUser(&result);

  res().AppendStringVector(result);
}

void PikaAclCmd::Load() {
  auto status = g_pika_server->Acl()->LoadUserFromFile(g_pika_conf->acl_file());
  if (status.ok()) {
    res().SetRes(CmdRes::kOk);
    return;
  }

  res().SetRes(CmdRes::kErrOther, status.ToString());
}

void PikaAclCmd::Log() {}

void PikaAclCmd::Save() {
  auto status = g_pika_server->Acl()->SaveToFile();

  if (status.ok()) {
    res().SetRes(CmdRes::kOk);
  } else {
    res().SetRes(CmdRes::kErrOther, status.ToString());
  }
}

void PikaAclCmd::SetUser() {
  std::vector<std::string> rule;
  if (argv_.size() > 3) {
    rule = std::vector<std::string>(argv_.begin() + 3, argv_.end());
  }
  auto status = g_pika_server->Acl()->SetUser(argv_[2], rule);
  if (status.ok()) {
    res().SetRes(CmdRes::kOk);
    return;
  }
  LOG(ERROR) << "Error in ACL SETUSER modifier " + status.ToString();
  res().SetRes(CmdRes::kErrOther, "Error in ACL SETUSER modifier " + status.ToString());
}

void PikaAclCmd::Users() { res().AppendStringVector(g_pika_server->Acl()->Users()); }

void PikaAclCmd::WhoAmI() {
  std::shared_ptr<PikaClientConn> conn = std::dynamic_pointer_cast<PikaClientConn>(GetConn());
  auto name = conn->UserName();

  if (name.empty()) {
    res().AppendString(name);
  } else {
    res().SetRes(CmdRes::kNone);
  }
}

void PikaAclCmd::Help() {
  const std::vector<std::string> info = {
      "CAT [<category>]",
      "    List all commands that belong to <category>, or all command categories",
      "    when no category is specified.",
      "DELUSER <username> [<username> ...]",
      "    Delete a list of users.",
      "DRYRUN <username> <command> [<arg> ...]",
      "    Returns whether the user can execute the given command without executing the command.",
      "GETUSER <username>",
      "    Get the user's details.",
      "GENPASS [<bits>]",
      "    Generate a secure 256-bit user password. The optional `bits` argument can",
      "    be used to specify a different size.",
      "LIST",
      "    Show users details in config file format.",
      "LOAD",
      "    Reload users from the ACL file.",
      "LOG [<count> | RESET]",
      "    Show the ACL log entries.",
      "SAVE",
      "    Save the current config to the ACL file.",
      "SETUSER <username> <attribute> [<attribute> ...]",
      "    Create or modify a user with the specified attributes.",
      "USERS",
      "    List all the registered usernames.",
      "WHOAMI",
      "    Return the current connection username."};

  res().AppendStringVector(info);
}
