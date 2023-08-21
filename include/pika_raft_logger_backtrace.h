// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_RAFT_LOGGER_BACKTRACE_
#define PIKA_RAFT_LOGGER_BACKTRACE_

#include <cstddef>
#include <sstream>
#include <string>

#include <cxxabi.h>
#include <execinfo.h>
#include <inttypes.h>
#include <stdio.h>
#include <signal.h>

namespace raft_logger {

#define SIZE_T_UNUSED   size_t      __attribute__((unused))
#define VOID_UNUSED     void        __attribute__((unused))
#define UINT64_T_UNUSED uint64_t    __attribute__((unused))
#define STR_UNUSED      std::string __attribute__((unused))
#define INTPTR_UNUSED   intptr_t    __attribute__((unused))

#ifdef __APPLE__
#include <mach-o/getsect.h>
#include <mach-o/dyld.h>

static UINT64_T_UNUSED static_base_address(void) {
    const struct segment_command_64* command = getsegbyname(SEG_TEXT /*"__TEXT"*/);
    uint64_t addr = command->vmaddr;
    return addr;
}

static STR_UNUSED get_exec_path() {
    char path[1024];
    uint32_t size = sizeof(path);
    if (_NSGetExecutablePath(path, &size) != 0) return std::string();

    return path;
}

static STR_UNUSED get_file_part(const std::string& full_path) {
    size_t pos = full_path.rfind("/");
    if (pos == std::string::npos) return full_path;

    return full_path.substr(pos + 1, full_path.size() - pos - 1);
}

static INTPTR_UNUSED image_slide(void) {
    std::string exec_path = get_exec_path();
    if (exec_path.empty()) return -1;

    auto image_count = _dyld_image_count();
    for (decltype(image_count) i = 0; i < image_count; i++) {
        if ( strcmp( _dyld_get_image_name(i),
                     exec_path.c_str() ) == 0 ) {
            return _dyld_get_image_vmaddr_slide(i);
        }
    }
    return -1;
}
#endif


#define _snprintf(msg, avail_len, cur_len, msg_len, ...)            \
    avail_len = (avail_len > cur_len) ? (avail_len - cur_len) : 0;  \
    msg_len = snprintf( msg + cur_len, avail_len, __VA_ARGS__ );    \
    cur_len += (avail_len > msg_len) ? msg_len : avail_len

static SIZE_T_UNUSED
_stack_backtrace(void** stack_ptr, size_t stack_ptr_capacity) {
    return backtrace(stack_ptr, stack_ptr_capacity);
}

static SIZE_T_UNUSED _stack_interpret_linux(void** stack_ptr,
                                            char** stack_msg,
                                            int stack_size,
                                            char* output_buf,
                                            size_t output_buflen);

static SIZE_T_UNUSED _stack_interpret_apple(void** stack_ptr,
                                            char** stack_msg,
                                            int stack_size,
                                            char* output_buf,
                                            size_t output_buflen);

static SIZE_T_UNUSED _stack_interpret_other(void** stack_ptr,
                                            char** stack_msg,
                                            int stack_size,
                                            char* output_buf,
                                            size_t output_buflen);

static SIZE_T_UNUSED
_stack_interpret(void** stack_ptr,
                 int stack_size,
                 char* output_buf,
                 size_t output_buflen)
{
    char** stack_msg = nullptr;
    stack_msg = backtrace_symbols(stack_ptr, stack_size);

    size_t len = 0;

#if defined(__linux__)
    len = _stack_interpret_linux( stack_ptr,
                                  stack_msg,
                                  stack_size,
                                  output_buf,
                                  output_buflen );

#elif defined(__APPLE__)
    len = _stack_interpret_apple( stack_ptr,
                                  stack_msg,
                                  stack_size,
                                  output_buf,
                                  output_buflen );

#else
    len = _stack_interpret_other( stack_ptr,
                                  stack_msg,
                                  stack_size,
                                  output_buf,
                                  output_buflen );

#endif
    free(stack_msg);

    return len;
}

static SIZE_T_UNUSED _stack_interpret_linux(void** stack_ptr,
                                            char** stack_msg,
                                            int stack_size,
                                            char* output_buf,
                                            size_t output_buflen)
{
    size_t cur_len = 0;
#ifdef __linux__
    size_t frame_num = 0;

    // NOTE: starting from 1, skipping this frame.
    for (int i = 1; i < stack_size; ++i) {
        // `stack_msg[x]` format:
        //   /foo/bar/executable() [0xabcdef]
        //   /lib/x86_64-linux-gnu/libc.so.6(__libc_start_main+0xf0) [0x123456]

        // NOTE: with ASLR
        //   /foo/bar/executable(+0x5996) [0x555555559996]

        int fname_len = 0;
        while ( stack_msg[i][fname_len] != '(' &&
                stack_msg[i][fname_len] != ' ' &&
                stack_msg[i][fname_len] != 0x0 ) {
            ++fname_len;
        }

        char addr_str[256];
        uintptr_t actual_addr = 0x0;
        if ( stack_msg[i][fname_len] == '(' &&
             stack_msg[i][fname_len+1] == '+' ) {
            // ASLR is enabled, get the offset from here.
            int upto = fname_len + 2;
            while ( stack_msg[i][upto] != ')' &&
                    stack_msg[i][upto] != 0x0 ) {
                upto++;
            }
            snprintf( addr_str, 256, "%.*s",
                      upto - fname_len - 2,
                      &stack_msg[i][fname_len + 2] );

            // Convert hex string -> integer address.
            std::stringstream ss;
            ss << std::hex << addr_str;
            ss >> actual_addr;

        } else {
            actual_addr = (uintptr_t)stack_ptr[i];
            snprintf(addr_str, 256, "%" PRIxPTR, actual_addr);
        }

        char cmd[1024];
        snprintf( cmd, 1024, "addr2line -f -e %.*s %s",
                  fname_len, stack_msg[i], addr_str );
        FILE* fp = popen(cmd, "r");
        if (!fp) continue;

        char mangled_name[1024];
        char file_line[1024];
        int ret = fscanf(fp, "%1023s %1023s", mangled_name, file_line);
        (void)ret;
        pclose(fp);

        size_t msg_len = 0;
        size_t avail_len = output_buflen;
        _snprintf( output_buf, avail_len, cur_len, msg_len,
                   "#%-2zu 0x%016" PRIxPTR " in ",
                   frame_num++,
                   actual_addr );

        int status;
        char *cc = abi::__cxa_demangle(mangled_name, 0, 0, &status);
        if (cc) {
            _snprintf(output_buf, avail_len, cur_len, msg_len, "%s at ", cc);
        } else {
            std::string msg_str = stack_msg[i];
            std::string _func_name = msg_str;
            size_t s_pos = msg_str.find("(");
            size_t e_pos = msg_str.rfind("+");
            if (e_pos == std::string::npos) e_pos = msg_str.rfind(")");
            if ( s_pos != std::string::npos &&
                 e_pos != std::string::npos ) {
                _func_name = msg_str.substr(s_pos+1, e_pos-s_pos-1);
            }
            _snprintf( output_buf, avail_len, cur_len, msg_len,
                       "%s() at ",
                       ( _func_name.empty()
                         ? mangled_name
                         : _func_name.c_str() ) );
        }

        _snprintf(output_buf, avail_len, cur_len, msg_len, "%s\n", file_line);
    }

#endif
    return cur_len;
}

static VOID_UNUSED skip_whitespace(const std::string base_str, size_t& cursor) {
    while (base_str[cursor] == ' ') cursor++;
}

static VOID_UNUSED skip_glyph(const std::string base_str, size_t& cursor) {
    while (base_str[cursor] != ' ') cursor++;
}

static SIZE_T_UNUSED _stack_interpret_apple(void** stack_ptr,
                                            char** stack_msg,
                                            int stack_size,
                                            char* output_buf,
                                            size_t output_buflen)
{
    size_t cur_len = 0;
#ifdef __APPLE__

    size_t frame_num = 0;
    (void)frame_num;

    std::string exec_full_path = get_exec_path();
    std::string exec_file = get_file_part( exec_full_path );
    uint64_t load_base = (uint64_t)image_slide() + static_base_address();

    // NOTE: starting from 1, skipping this frame.
    for (int i = 1; i < stack_size; ++i) {
        // `stack_msg[x]` format:
        //   8   foobar    0x000000010fd490da main + 1322
        if (!stack_msg[i] || stack_msg[i][0] == 0x0) continue;

        std::string base_str = stack_msg[i];

        size_t s_pos = 0;
        size_t len = 0;
        size_t cursor = 0;

        // Skip frame number part.
        skip_glyph(base_str, cursor);

        // Skip whitespace.
        skip_whitespace(base_str, cursor);
        s_pos = cursor;
        // Filename part.
        skip_glyph(base_str, cursor);
        len = cursor - s_pos;
        std::string filename = base_str.substr(s_pos, len);

        // Skip whitespace.
        skip_whitespace(base_str, cursor);
        s_pos = cursor;
        // Address part.
        skip_glyph(base_str, cursor);
        len = cursor - s_pos;
        std::string address = base_str.substr(s_pos, len);
        if (!address.empty() && address[0] == '?') continue;

        // Skip whitespace.
        skip_whitespace(base_str, cursor);
        s_pos = cursor;
        // Mangled function name part.
        skip_glyph(base_str, cursor);
        len = cursor - s_pos;
        std::string func_mangled = base_str.substr(s_pos, len);

        size_t msg_len = 0;
        size_t avail_len = output_buflen;

        _snprintf(output_buf, avail_len, cur_len, msg_len,
                  "#%-2zu %s in ",
                  frame_num++, address.c_str() );

        if (filename != exec_file) {
            // Dynamic library.
            int status;
            char *cc = abi::__cxa_demangle(func_mangled.c_str(), 0, 0, &status);
            if (cc) {
                _snprintf( output_buf, avail_len, cur_len, msg_len,
                           "%s at %s\n", cc, filename.c_str() );
            } else {
                _snprintf( output_buf, avail_len, cur_len, msg_len,
                           "%s() at %s\n",
                           func_mangled.c_str(),
                           filename.c_str() );
            }
        } else {
            // atos return format:
            //   bbb(char) (in crash_example) (crash_example.cc:37)
            std::stringstream ss;
            ss << "atos -l 0x";
            ss << std::hex << load_base;
            ss << " -o " << exec_full_path;
            ss << " " << address;
            FILE* fp = popen(ss.str().c_str(), "r");
            if (!fp) continue;

            char atos_cstr[4096];
            fgets(atos_cstr, 4095, fp);

            std::string atos_str = atos_cstr;
            size_t d_pos = atos_str.find(" (in ");
            if (d_pos == std::string::npos) continue;
            std::string function_part = atos_str.substr(0, d_pos);

            d_pos = atos_str.find(") (", d_pos);
            if (d_pos == std::string::npos) continue;
            std::string source_part = atos_str.substr(d_pos + 3);
            source_part = source_part.substr(0, source_part.size() - 2);

            _snprintf( output_buf, avail_len, cur_len, msg_len,
                       "%s at %s\n",
                       function_part.c_str(),
                       source_part.c_str() );
        }
    }

#endif
    return cur_len;
}

static SIZE_T_UNUSED _stack_interpret_other(void** stack_ptr,
                                            char** stack_msg,
                                            int stack_size,
                                            char* output_buf,
                                            size_t output_buflen)
{
    size_t cur_len = 0;
    size_t frame_num = 0;
    (void)frame_num;

    // NOTE: starting from 1, skipping this frame.
    for (int i=1; i<stack_size; ++i) {
        // On non-Linux platform, just use the raw symbols.
        size_t msg_len = 0;
        size_t avail_len = output_buflen;
        _snprintf(output_buf, avail_len, cur_len, msg_len, "%s\n", stack_msg[i]);
    }
    return cur_len;
}


static SIZE_T_UNUSED stack_backtrace(char* output_buf, size_t output_buflen) {
    void* stack_ptr[256];
    int stack_size = _stack_backtrace(stack_ptr, 256);
    return _stack_interpret(stack_ptr, stack_size, output_buf, output_buflen);
}

}

#endif
