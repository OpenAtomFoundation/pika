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

  int i = 2;
  int limit_given = 0;
  for (; i < argv_.size(); ++i) {
    int moreargs = argv_.size() - 1 - i;
    const std::string& opt = argv_[i];
    if (opt == "*" && moreargs) {
      /* This is just a fast path for the common case of auto-ID
       * creation. */
      break;

    } else if (opt == "maxlen" && moreargs) {
      if (trim_strategy_ != StreamTrimStrategy::TRIM_STRATEGY_NONE) {
        res_.SetRes(CmdRes::kSyntaxErr, "syntax error, MAXLEN and MINID options at the same time are not compatible");
        return;
      }
      approx_trim_ = 0;
      auto next = argv_[i + 1];
      if (moreargs >= 2 && next == "~") {
        approx_trim_ = 1;
        i++;
      } else if (moreargs >= 2 && next == "=") {
        i++;
      }
      // FIXME: should catch exception and return CmdRes::kInvalidParameter
      maxlen_ = std::stoull(argv_[i + 1]);
      if (maxlen_ < 0) {
        res_.SetRes(CmdRes::kInvalidParameter, "The MAXLEN argument must be >= 0.");
      }
      i++;
      trim_strategy_ = StreamTrimStrategy::TRIM_STRATEGY_MAXLEN;
      trim_strategy_arg_idx_ = i;

    } else if (opt == "minid" && moreargs) {
      if (trim_strategy_ != StreamTrimStrategy::TRIM_STRATEGY_NONE) {
        res_.SetRes(CmdRes::kSyntaxErr, "syntax error, MAXLEN and MINID options at the same time are not compatible");
        return;
      }
      approx_trim_ = 0;
      auto next = argv_[i + 1];
      if (moreargs >= 2 && next == "~") {
        approx_trim_ = 1;
        i++;
      } else if (moreargs >= 2 && next == "=") {
        i++;
      }

      auto ret = StreamUtil::streamGenericParseIDOrReply(argv_[i + 1], &minid_, 0, 0, nullptr);
      if (!ret.ok()) {
        res_ = ret;
        return;
      }
      // Assuming that minid_ is defined as similar to maxlen_
      minid_ = std::stoull(argv_[i + 1]);
      if (minid_ < 0) {
        res_.SetRes(CmdRes::kInvalidParameter, "The MINID argument must be >= 0.");
      }
      i++;
      trim_strategy_ = StreamTrimStrategy::TRIM_STRATEGY_MINID;
      trim_strategy_arg_idx_ = i;

    } else if (opt == "limit" && moreargs) {
      limit_ = std::stoull(argv_[i + 1]);
      if (limit_ < 0) {
        throw std::runtime_error("The LIMIT argument must be >= 0.");
      }
      limit_given = 1;
      ++i;

    } else if (opt == "nomkstream") {
      no_mkstream_ = 1;
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
