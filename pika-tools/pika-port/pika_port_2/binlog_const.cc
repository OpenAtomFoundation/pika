#include "binlog_const.h"

#include "include/pika_define.h"

std::string PikaState(int state) {
  switch (state) {
    case PIKA_REPL_NO_CONNECT:
    return "PIKA_REPL_NO_CONNECT";

    case PIKA_REPL_CONNECT:
    return "PIKA_REPL_CONNECT";

    case PIKA_REPL_CONNECTING:
    return "PIKA_REPL_CONNECTING";

    case PIKA_REPL_CONNECTED:
    return "PIKA_REPL_CONNECTED";

    case PIKA_REPL_WAIT_DBSYNC:
    return "PIKA_REPL_WAIT_DBSYNC";

    case PIKA_REPL_ERROR:
    return "PIKA_REPL_ERROR";

    default:
    return "PIKA_REPL_UNKNOWN";
  }

  return "PIKA_REPL_UNKNOWN";
}

std::string PikaRole(int role) {
  std::string roleStr = "|";
  if (role == PIKA_ROLE_SINGLE) {
    roleStr += " PIKA_ROLE_SINGLE |";
  }

  if (role == PIKA_ROLE_SLAVE) {
    roleStr += " PIKA_ROLE_SLAVE |";
  }

  if (role == PIKA_ROLE_MASTER) {
    roleStr += " PIKA_ROLE_MASTER |";
  }

  if (role == PIKA_ROLE_DOUBLE_MASTER) {
    roleStr += " PIKA_ROLE_DOUBLE_MASTER |";
  }

  if (role == PIKA_ROLE_PORT) {
    roleStr += " PIKA_ROLE_PORT |";
  }

  return roleStr;
}

