CMAKE_MINIMUM_REQUIRED(VERSION 3.14)
PROJECT(PikiwiDB)

SET(CMAKE_CXX_STANDARD 20)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated -g -D'GIT_COMMIT_ID=\"${GIT_COMMIT_ID}\"' -D'BUILD_TIME=\"${BUILD_TIME}\"'")

# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
IF (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
    CMAKE_POLICY(SET CMP0135 NEW)
ENDIF ()

SET(CMAKE_CXX_STANDARD 20)
SET(CMAKE_EXPORT_COMPILE_COMMANDS ON)
ENABLE_TESTING()

IF (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # using Clang
    IF (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "10.0")
        MESSAGE(FATAL_ERROR "Clang version must be greater than 10.0")
    ENDIF ()
ELSEIF (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # using GCC
    IF (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "10.0")
        MESSAGE(FATAL_ERROR "GCC G++ version must be greater than 10.0")
    ENDIF ()
ENDIF ()


############# You should enable sanitizer if you are developing pika #############
# Uncomment the following two lines to enable AddressSanitizer to detect memory leaks and other memory-related bugs.
# SET(CMAKE_BUILD_TYPE "Debug")
# SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address  -O0 -fno-omit-frame-pointer -fno-optimize-sibling-calls")

# [Notice] AddressSanitizer and ThreadSanitizer can not be enabled at the same time.

# Uncomment the following two lines to enable ThreadSanitizer to detect data race and other thread-related issue.
# SET(CMAKE_BUILD_TYPE "Debug")
# SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=thread -O0 -fno-omit-frame-pointer -fno-optimize-sibling-calls")

EXECUTE_PROCESS(COMMAND uname -p OUTPUT_VARIABLE HOST_ARCH)
STRING(TOLOWER ${HOST_ARCH} HOST_ARCH)

IF (NOT CMAKE_BUILD_TYPE)
    SET(CMAKE_BUILD_TYPE RELEASE)
ENDIF ()

# NOTE !!! CMAKE_BUILD_TYPE is case-sensitive
STRING(TOUPPER ${CMAKE_BUILD_TYPE} BUILD_TYPE)

IF (${BUILD_TYPE} STREQUAL DEBUG)
    SET(LIB_BUILD_TYPE DEBUG)
ELSEIF (${BUILD_TYPE} STREQUAL MINSIZEREL)
    SET(LIB_BUILD_TYPE MINSIZEREL)
ELSEIF (${BUILD_TYPE} STREQUAL RELWITHDEBINFO)
    SET(LIB_BUILD_TYPE RELWITHDEBINFO)
ELSE ()
    SET(LIB_BUILD_TYPE RELEASE)
    SET(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")
ENDIF ()

IF (CMAKE_SYSTEM_NAME MATCHES "Darwin")
    SET(CMAKE_CXX_FLAGS "-pthread")
    ADD_DEFINITIONS(-DOS_MACOSX)
ELSEIF (CMAKE_SYSTEM_NAME MATCHES "Linux")
    IF (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        SET(CMAKE_EXE_LINKER_FLAGS "-stdlib=libc++ -fuse-ld=lld -lc++ -lc++abi ${CMAKE_EXE_LINKER_FLAGS}")
        SET(CMAKE_CXX_FLAGS "-stdlib=libc++ -pthread ${CMAKE_CXX_FLAGS}")
    ELSEIF (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        SET(CMAKE_EXE_LINKER_FLAGS "-static-libgcc -static-libstdc++")
        SET(CMAKE_CXX_FLAGS "-pthread -Wl,--no-as-needed -ldl")
    ENDIF ()
    ADD_DEFINITIONS(-DOS_LINUX)
ELSE ()
    MESSAGE(FATAL_ERROR "only support linux or macOS")
ENDIF ()

IF (HOST_ARCH MATCHES "x86_64" OR HOST_ARCH MATCHES "i386")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse -msse4.2")
ELSEIF (HOST_ARCH MATCHES "arm")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=armv8-a+crc+crypto -moutline-atomics")
ENDIF ()

SET(EP_BASE_SUFFIX "buildtrees")
SET_PROPERTY(DIRECTORY PROPERTY EP_BASE ${CMAKE_CURRENT_SOURCE_DIR}/${EP_BASE_SUFFIX})
LIST(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake/modules/")
SET(STAGED_INSTALL_PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/deps)
SET(CMAKE_UTILS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/utils)
SET(INSTALL_INCLUDEDIR ${STAGED_INSTALL_PREFIX}/include)
SET(INSTALL_LIBDIR ${STAGED_INSTALL_PREFIX}/lib)
SET(INSTALL_LIBDIR_64 ${STAGED_INSTALL_PREFIX}/lib64)
SET(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${STAGED_INSTALL_PREFIX})

MESSAGE(STATUS "${PROJECT_NAME} staged install: ${STAGED_INSTALL_PREFIX}")
MESSAGE(STATUS "Current platform: ${OS_VERSION} ")
CMAKE_HOST_SYSTEM_INFORMATION(RESULT CPU_CORE QUERY NUMBER_OF_LOGICAL_CORES)
MESSAGE(STATUS "CPU core ${CPU_CORE}")


INCLUDE(FetchContent)
#include(cmake/CmakeLists.txt)

include(cmake/findTools.cmake)
include(cmake/leveldb.cmake)
include(cmake/libevent.cmake)
include(cmake/llhttp.cmake)
include(cmake/spdlog.cmake)

ADD_SUBDIRECTORY(src/std)
ADD_SUBDIRECTORY(src/net)
ADD_SUBDIRECTORY(src)

