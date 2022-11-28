#include <string>
#include <iostream>

#include "slash/include/slash_hash.h"

using namespace slash;
int main() {
  std::string input = "grape";
  std::string output1 = sha256(input);
  std::string output2 = md5(input);

  std::cout << "sha256('"<< input << "'): " << output1 << std::endl;
  std::cout << "md5('"<< input << "'): " << output2<< std::endl;

  return 0;
}
