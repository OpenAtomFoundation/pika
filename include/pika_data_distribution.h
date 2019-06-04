#ifndef PIKA_DATA_DISTRIBUTION_H_
#define PIKA_DATA_DISTRIBUTION_H_

#include "slash/include/slash_status.h"

// polynomial reserved Crc32 magic num
const uint32_t IEEE_POLY = 0xedb88320;

class PikaDataDistribution {
 public:
 virtual ~PikaDataDistribution() = default; 
  // Initialization
  virtual void Init() = 0;
  // key map to partition id
  virtual uint32_t Distribute(const std::string& str, uint32_t partition_num) = 0;
};

class HashModulo : public PikaDataDistribution {
 public:
  virtual ~HashModulo() = default;
  virtual void Init();
  virtual uint32_t Distribute(const std::string& str, uint32_t partition_num);
};

class Crc32 : public PikaDataDistribution {
 public:
  virtual void Init();
  virtual uint32_t Distribute(const std::string& str, uint32_t partition_num);
 private:
  void Crc32TableInit(uint32_t poly);
  uint32_t Crc32Update(uint32_t crc, const char* buf, int len);
  uint32_t crc32tab[256];
};

#endif
