#ifndef SLASH_INCLUDE_TESTUTIL_H_
#define SLASH_INCLUDE_TESTUTIL_H_

#include <string>

namespace slash {

extern std::string RandomString(const int len);
extern int RandomSeed();
extern int GetTestDirectory(std::string *result);

};  // namespace slash


#endif  // SLASH_INCLUDE_TESTUTIL_H_
