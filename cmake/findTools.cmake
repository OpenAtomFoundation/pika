FIND_PROGRAM(AUTOCONF autoconf PATHS /usr/bin /usr/local/bin)

IF(${AUTOCONF} MATCHES AUTOCONF-NOTFOUND)
    MESSAGE(FATAL_ERROR "not find autoconf on localhost")
ENDIF()

#set(CLANG_SEARCH_PATH "/usr/local/bin" "/usr/bin" "/usr/local/opt/llvm/bin"
#                      "/usr/local/opt/llvm@12/bin")
FIND_PROGRAM(CLANG_TIDY_BIN
        NAMES clang-tidy clang-tidy-12
        HINTS ${CLANG_SEARCH_PATH})
IF("${CLANG_TIDY_BIN}" STREQUAL "CLANG_TIDY_BIN-NOTFOUND")
    MESSAGE(WARNING "couldn't find clang-tidy.")
ELSE()
    MESSAGE(STATUS "found clang-tidy at ${CLANG_TIDY_BIN}")
ENDIF()

FIND_PROGRAM(CLANG_APPLY_REPLACEMENTS_BIN
        NAMES clang-apply-replacements clang-apply-replacements-12
        HINTS ${CLANG_SEARCH_PATH})

IF("${CLANG_APPLY_REPLACEMENTS_BIN}" STREQUAL "CLANG_APPLY_REPLACEMENTS_BIN-NOTFOUND")
    MESSAGE(WARNING "couldn't find clang-apply-replacements.")
ELSE()
    MESSAGE(STATUS "found clang-apply-replacements at ${CLANG_APPLY_REPLACEMENTS_BIN}")
ENDIF()

OPTION(WITH_COMMAND_DOCS "build with command docs support" OFF)
IF(WITH_COMMAND_DOCS)
    ADD_DEFINITIONS(-DWITH_COMMAND_DOCS)
ENDIF()

IF(${CMAKE_BUILD_TYPE} MATCHES "RELEASE")
    MESSAGE(STATUS "make RELEASE version")
    ADD_DEFINITIONS(-DBUILD_RELEASE)
    SET(BuildType "Release")
ELSE()
    MESSAGE(STATUS "make DEBUG version")
    ADD_DEFINITIONS(-DBUILD_DEBUG)
    SET(BuildType "Debug")
ENDIF()