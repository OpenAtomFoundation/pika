#include "include/pika_stream.h"
#include "include/pika_command.h"
#include "include/pika_data_distribution.h"
#include "include/pika_slot_command.h"
#include "include/pika_stream_util.h"
#include "pstd/include/pstd_string.h"

void XAddCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLIndex);
    return;
  }
  key_ = argv_[1];
  int idpos{-1};
  res_ = StreamUtil::ParseAddOrTrimArgs(argv_, args_, idpos, true);
  if (!res_.ok()) {
    return;
  } else if (idpos < 0) {
    LOG(FATAL) << "Invalid idpos: " << idpos;
    res_.SetRes(CmdRes::kErrOther);
  }

  // Korpse TODO: get the filed_values_ from argv_
}

void XAddCmd::Do(std::shared_ptr<Slot> slot) {}
