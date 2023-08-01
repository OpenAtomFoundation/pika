#include <iostream>
#include <string>

#include "pstd/include/pstd_hash.h"

using namespace pstd;
int32_t main() {
  std::string input = "grape";
  std::string output1 = sha256(input);
  std::string output2 = md5(input);

  std::cout << "sha256('" << input << "'): " << output1 << std::endl;
  std::cout << "md5('" << input << "'): " << output2 << std::endl;

  return 0;
}
