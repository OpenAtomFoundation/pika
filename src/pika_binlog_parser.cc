#include "include/pika_binlog_parser.h"

#include "include/pika_binlog_transverter.h"

PikaBinlogParser::PikaBinlogParser()
    : processed_item_content_len_(0), parse_status_(kBinlogParserNone)  {
} 

PikaBinlogParser::~PikaBinlogParser() {
}

void PikaBinlogParser::ResetStatus() {
  BinlogHeader tmp_header;
  binlog_header_ = tmp_header;
  BinlogItem tmp_item;
  binlog_item_ = tmp_item;
  processed_item_content_len_ = 0;
  parse_status_ = kBinlogParserNone;
  half_binlog_buf_.clear();
}

// just scrub one command at most
pink::ReadStatus PikaBinlogParser::ScrubReadBuffer(const char* rbuf, int len, int* processed_len, int* scrubed_len, BinlogHeader* binlog_header, BinlogItem* binlog_item) {
  if (parse_status_ == kBinlogParserNone) {
    return BinlogParseHeader(rbuf, len, processed_len, scrubed_len, binlog_header, binlog_item);
  } else if (parse_status_ == kBinlogParserHeaderDone) {
    return BinlogParseDecode(rbuf, len, processed_len, scrubed_len, binlog_header, binlog_item);
  } else if (parse_status_ == kBinlogParserDecodeDone) {
    return BinlogParseContent(rbuf, len, processed_len, scrubed_len, binlog_header, binlog_item);
  } else {
    return pink::kReadError;
  }
}

pink::ReadStatus PikaBinlogParser::BinlogParseHeader(const char* rbuf, int len, int* processed_len, int* scrubed_len, BinlogHeader* binlog_header, BinlogItem* binlog_item) {
 if (len + half_binlog_buf_.size() < HEADER_LEN) {
    half_binlog_buf_ += std::string(rbuf, len);
    *processed_len += len;
    return pink::kReadHalf;
  } else {
    int buf_len = len < HEADER_LEN ? len : HEADER_LEN;
    std::string buffer_str(rbuf, buf_len);
    if (!PikaBinlogTransverter::BinlogHeaderDecode(TypeFirst, half_binlog_buf_ + buffer_str, &binlog_header_)) {
      return pink::kParseError;
    }
    int actual_scrubed_len = HEADER_LEN - half_binlog_buf_.size();
    *scrubed_len += actual_scrubed_len;
    *processed_len += actual_scrubed_len;
    *binlog_header = binlog_header_;
    parse_status_ = kBinlogParserHeaderDone;
    half_binlog_buf_.clear();
    return BinlogParseDecode(rbuf + actual_scrubed_len, len - actual_scrubed_len, processed_len, scrubed_len, binlog_header, binlog_item);
  }
}

pink::ReadStatus PikaBinlogParser::BinlogParseDecode(const char* rbuf, int len, int* processed_len, int* scrubed_len, BinlogHeader* binlog_header, BinlogItem* binlog_item) {
  if (binlog_header_.header_type_ == kTypeAuth) {
    parse_status_ = kBinlogParserDecodeDone;
    return ScrubReadBuffer(rbuf, len, processed_len, scrubed_len, binlog_header, binlog_item);
  }  else if (len + half_binlog_buf_.size() < BINLOG_ENCODE_LEN) {
    half_binlog_buf_ += std::string(rbuf, len);
    *processed_len += len;
    return pink::kReadHalf;
  } else {
    int buf_len = len < BINLOG_ENCODE_LEN ? len : BINLOG_ENCODE_LEN;
    std::string buffer_str(rbuf, buf_len);
    if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, half_binlog_buf_ + buffer_str, &binlog_item_)) {
      return pink::kParseError;
    }
    int actual_scrubed_len = BINLOG_ENCODE_LEN - half_binlog_buf_.size();
    *scrubed_len += actual_scrubed_len;
    *processed_len += actual_scrubed_len;
    *binlog_item = binlog_item_;
    parse_status_ = kBinlogParserDecodeDone;
    half_binlog_buf_.clear();
    return BinlogParseContent(rbuf + actual_scrubed_len, len - actual_scrubed_len, processed_len, scrubed_len, binlog_header, binlog_item);
  }
}

pink::ReadStatus PikaBinlogParser::BinlogParseContent(const char* rbuf, int len, int* processed_len, int* scrubed_len, BinlogHeader* binlog_header, BinlogItem* binlog_item) {
  uint32_t content_len = binlog_header_.item_length_
    - ((binlog_header_.header_type_ == kTypeBinlog) ? BINLOG_ENCODE_LEN : 0);
  // safe to do static_cast here cause len is 512M maximum
  uint32_t remain_item_content_len = content_len - processed_item_content_len_;
  uint32_t min_len = (remain_item_content_len < static_cast<uint32_t>(len) ? remain_item_content_len : len);
  *processed_len += min_len;
  processed_item_content_len_ += min_len;
  if (processed_item_content_len_ == content_len) {
    ResetStatus();
  }
  return pink::kReadAll;
}
