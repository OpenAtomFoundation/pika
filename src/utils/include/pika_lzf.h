#ifndef PIKA_LZF_h
#define PIKA_LZF_h

#define STANDALONE 1 /* at the moment, this is ok. */

#ifndef STANDALONE
//# include "lzf.h"
#endif

/*
 * Size of hashtable is (1 << HLOG) * sizeof (char *)
 * decompression is independent of the hash table size
 * the difference between 15 and 14 is very small
 * for small blocks (and 14 is usually a bit faster).
 * For a low-memory/faster configuration, use HLOG == 13;
 * For best compression, use 15 or 16 (or more, up to 23).
 */
#ifndef HLOG
# define HLOG 16
#endif

/*
 * Sacrifice very little compression quality in favour of compression speed.
 * This gives almost the same compression as the default code, and is
 * (very roughly) 15% faster. This is the preferred mode of operation.
 */
#ifndef VERY_FAST
# define VERY_FAST 1
#endif

/*
 * Sacrifice some more compression quality in favour of compression speed.
 * (roughly 1-2% worse compression for large blocks and
 * 9-10% for small, redundant, blocks and >>20% better speed in both cases)
 * In short: when in need for speed, enable this for binary data,
 * possibly disable this for text data.
 */
#ifndef ULTRA_FAST
# define ULTRA_FAST 0
#endif

/*
 * Unconditionally aligning does not cost very much, so do it if unsure
 */
#ifndef STRICT_ALIGN
# define STRICT_ALIGN !(defined(__i386) || defined (__amd64))
#endif

/*
 * You may choose to pre-set the hash table (might be faster on some
 * modern cpus and large (>>64k) blocks, and also makes compression
 * deterministic/repeatable when the configuration otherwise is the same).
 */
#ifndef INIT_HTAB
# define INIT_HTAB 0
#endif

/*
 * Avoid assigning values to errno variable? for some embedding purposes
 * (linux kernel for example), this is necessary. NOTE: this breaks
 * the documentation in lzf.h.
 */
#ifndef AVOID_ERRNO
# define AVOID_ERRNO 0
#endif

/*
 * Whether to pass the LZF_STATE variable as argument, or allocate it
 * on the stack. For small-stack environments, define this to 1.
 * NOTE: this breaks the prototype in lzf.h.
 */
#ifndef LZF_STATE_ARG
# define LZF_STATE_ARG 0
#endif

/*
 * Whether to add extra checks for input validity in lzf_decompress
 * and return EINVAL if the input stream has been corrupted. This
 * only shields against overflowing the input buffer and will not
 * detect most corrupted streams.
 * This check is not normally noticeable on modern hardware
 * (<1% slowdown), but might slow down older cpus considerably.
 */
#ifndef CHECK_INPUT
# define CHECK_INPUT 1
#endif

/*****************************************************************************/
/* nothing should be changed below */

typedef unsigned char u8;

typedef const u8 *LZF_STATE[1 << (HLOG)];

#if !STRICT_ALIGN
/* for unaligned accesses we need a 16 bit datatype. */
# include <limits.h>
# if USHRT_MAX == 65535
    typedef unsigned short u16;
# elif UINT_MAX == 65535
    typedef unsigned int u16;
# else
#  undef STRICT_ALIGN
#  define STRICT_ALIGN 1
# endif
#endif

#if ULTRA_FAST
# if defined(VERY_FAST)
#  undef VERY_FAST
# endif
#endif

#if INIT_HTAB
# ifdef __cplusplus
#  include <cstring>
# else
#  include <string.h>
# endif
#endif



#define LZF_VERSION 0x0105 /* 1.5, API version */
    
unsigned int lzf_decompress (const void *const in_data,  unsigned int in_len,
                void *out_data, unsigned int out_len);


#endif

