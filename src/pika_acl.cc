// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <fmt/format.h>

#include "include/pika_acl.h"
#include "include/pika_client_conn.h"
#include "include/pika_cmd_table_manager.h"

const static int AclGenPassMaxBit = 4096;  //

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

  if (subCmd_ == "dryrun" && argv_.size() < 4) {
    res_.SetRes(CmdRes::kWrongNum, "'acl|dryrun'");
    return;
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
  auto category = Acl::GetCommandCategoryFlagByName(argv_[2]);
  if (category == 0) {
    res().SetRes(CmdRes::kErrOther, fmt::format("Unknown category '{}'", argv_[2]));
    return;
  }
  res().AppendStringVector(g_pika_cmd_table_manager->GetAclCategoryCmdNames(category));
}

void PikaAclCmd::DelUser() {
  std::vector<std::string> userNames(argv_.begin() + 2, argv_.end());
  auto delUserNames = g_pika_server->Acl()->DeleteUser(userNames);
  res().AppendInteger(static_cast<int64_t>(delUserNames.size()));

  g_pika_server->AllClientUnAuth(delUserNames);
}

void PikaAclCmd::DryRun() {
  auto user = g_pika_server->Acl()->GetUser(argv_[2], true);

  if (!user) {
    res().SetRes(CmdRes::kErrOther, fmt::format("User '{}' not found", argv_[2]));
    return;
  }
  auto cmd = g_pika_cmd_table_manager->GetCmd(argv_[3]);

  if (!cmd) {
    res().SetRes(CmdRes::kErrOther, fmt::format("Command '{}' not found", argv_[3]));
    return;
  }

  PikaCmdArgsType args;
  if (argv_.size() > 4) {
    args = PikaCmdArgsType(argv_.begin() + 3, argv_.end());
  }
  AclDeniedCmd checkRes = user->CheckUserPermission(cmd, args);

  switch (checkRes) {
    case AclDeniedCmd::OK:
      res().SetRes(CmdRes::kOk);
      break;
    case AclDeniedCmd::CMD:
      res().SetRes(CmdRes::kErrOther,
                   cmd->HasSubCommand()
                       ? fmt::format("This user has no permissions to run the '{}|{}' command", argv_[3], argv_[4])
                       : fmt::format("This user has no permissions to run the '{}' command", argv_[3]));
      break;
    case AclDeniedCmd::KEY:
      res().SetRes(CmdRes::kErrOther,
                   cmd->HasSubCommand()
                       ? fmt::format("This user has no permissions to run the '{}|{}' key", argv_[3], argv_[4])
                       : fmt::format("This user has no permissions to run the '{}' key", argv_[3]));
      break;
    case AclDeniedCmd::CHANNEL:
      res().SetRes(CmdRes::kErrOther,
                   cmd->HasSubCommand()
                       ? fmt::format("This user has no permissions to run the '{}|{}' channel", argv_[3], argv_[4])
                       : fmt::format("This user has no permissions to run the '{}' channel", argv_[3]));
      break;
    case AclDeniedCmd::NUMBER:
      res().SetRes(CmdRes::kErrOther, fmt::format("wrong number of arguments for '{}' command", argv_[3]));
      break;
  }
}

void PikaAclCmd::GenPass() {
  int bits = 256;
  if (argv_.size() > 2) {
    try {
      bits = std::stoi(argv_[2]);
    } catch (std::exception& e) {
      res().SetRes(CmdRes::kErrOther, fmt::format("Invalid bits value: {}", argv_[2]));
      return;
    }
  }

  if (bits <= 0 || bits > AclGenPassMaxBit) {
    res().SetRes(
        CmdRes::kErrOther,
        fmt::format(
            "ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096 {}",
            bits));
    return;
  }

  std::string pass = pstd::getRandomHexChars((bits + 3) / 4);
  res().AppendString(pass);
}

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
  auto status = g_pika_server->Acl()->LoadUserFromFile();
  if (status.ok()) {
    res().SetRes(CmdRes::kOk);
    return;
  }

  res().SetRes(CmdRes::kErrOther, status.ToString());
}

void PikaAclCmd::Log() { res().SetRes(CmdRes::kErrOther, "Not implemented"); }

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

  if (pstd::isspace(argv_[2])){
    res().SetRes(CmdRes::kErrOther, "Usernames can't contain spaces or null characters");
    return;
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
    res().AppendString(Acl::DefaultUser);
  } else {
    res().AppendString(name);
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