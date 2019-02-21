// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_client_thread.h"

PikaReplClientThread::PikaReplClientThread(int cron_interval, int keepalive_timeout) :
  ClientThread(&conn_factory_, cron_interval, keepalive_timeout, &handle_, NULL) {
}


