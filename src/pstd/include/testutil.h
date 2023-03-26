#ifndef __PSTD_INCLUDE_TESTUTIL_H__
#define __PSTD_INCLUDE_TESTUTIL_H__

#include <string>

namespace pstd {

extern std::string RandomString(const int len);
extern int RandomSeed();
extern int GetTestDirectory(std::string *result);

};  // namespace pstd


#endif  // __PSTD_INCLUDE_TESTUTIL_H__
