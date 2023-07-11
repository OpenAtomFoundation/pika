#include "include/pika_stream.h"
#include "include/pika_data_distribution.h"
#include "include/pika_slot_command.h"
#include "pstd/include/pstd_string.h"

void XAddCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLIndex);
    return;
  }
  key_ = argv_[1];

  int i = 2;
  int limit_given = 0;
  for (; i < argv_.size(); ++i) {
    int moreargs = argv_.size() - 1 - i;
    const std::string& opt = argv_[i];
    if (opt == "*" && moreargs) {
      break;
    } else if (opt == "maxlen" && moreargs) {
      if (trim_strategy != StreamTrimStrategy::TRIM_STRATEGY_NONE) {
        throw std::runtime_error("syntax error, MAXLEN and MINID options at the same time are not compatible");
      }
      approx_trim = 0;
      std::string next = argv_[i + 1];
      if (moreargs >= 2 && next == "~") {
        approx_trim = 1;
        ++i;
      } else if (moreargs >= 2 && next == "=") {
        ++i;
      }
      maxlen = std::stoull(argv_[i + 1]);
      if (maxlen < 0) {
        throw std::runtime_error("The MAXLEN argument must be >= 0.");
      }
      ++i;
      trim_strategy = StreamTrimStrategy::TRIM_STRATEGY_MAXLEN;
      trim_strategy_arg_idx = i;
    } else if (opt == "minid" && moreargs) {
      // similar processing for minid
    } else if (opt == "limit" && moreargs) {
      limit = std::stoull(argv_[i + 1]);
      if (limit < 0) {
        throw std::runtime_error("The LIMIT argument must be >= 0.");
      }
      limit_given = 1;
      ++i;
    } else if (opt == "nomkstream") {
      no_mkstream = 1;
    } else {
      // process ID
    }
  }

  // After processing options, the rest are field-value pairs
  for (int j = i + 1; j < argv_.size(); j += 2) {
    filed_values_.emplace_back(argv_[j], argv_[j + 1]);
  }
}

void XAddCmd::Do(std::shared_ptr<Slot> slot) {}
