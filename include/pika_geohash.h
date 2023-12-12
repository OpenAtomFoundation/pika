/*
 * Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>.
 * Copyright (c) 2015, Salvatore Sanfilippo <antirez@gmail.com>.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef PIKA_GEOHASH_H_
#define PIKA_GEOHASH_H_

#include <cstddef>
#include <cstdint>

#if defined(__cplusplus)
extern "C" {
#endif

#define HASHISZERO(r) (!(r).bits && !(r).step)
#define RANGEISZERO(r) (!(r).max && !(r).min)
#define RANGEPISZERO(r) ((r) == nullptr || RANGEISZERO(*(r)))

#define GEO_STEP_MAX 26 /* 26*2 = 52 bits. */

/* Limits from EPSG:900913 / EPSG:3785 / OSGEO:41001 */
constexpr double GEO_LAT_MIN{-85.05112878};
constexpr double GEO_LAT_MAX{85.05112878};
constexpr int64_t GEO_LONG_MIN{-180};
constexpr int64_t GEO_LONG_MAX{180};

struct GeoHashBits {
  uint64_t bits;
  uint8_t step;
};

struct GeoHashRange {
  double min;
  double max;
};

struct GeoHashArea {
  GeoHashBits hash;
  GeoHashRange longitude;
  GeoHashRange latitude;
};

struct GeoHashNeighbors {
  GeoHashBits north;
  GeoHashBits east;
  GeoHashBits west;
  GeoHashBits south;
  GeoHashBits north_east;
  GeoHashBits south_east;
  GeoHashBits north_west;
  GeoHashBits south_west;
};

/*
 * 0:success
 * -1:failed
 */
void geohashGetCoordRange(GeoHashRange* long_range, GeoHashRange* lat_range);
int geohashEncode(const GeoHashRange* long_range, const GeoHashRange* lat_range, double longitude, double latitude,
                  uint8_t step, GeoHashBits* hash);
int geohashEncodeType(double longitude, double latitude, uint8_t step, GeoHashBits* hash);
int geohashEncodeWGS84(double longitude, double latitude, uint8_t step, GeoHashBits* hash);
int geohashDecode(GeoHashRange long_range, GeoHashRange lat_range, GeoHashBits hash,
                  GeoHashArea* area);
int geohashDecodeType(GeoHashBits hash, GeoHashArea* area);
int geohashDecodeAreaToLongLat(const GeoHashArea* area, double* xy);
int geohashDecodeToLongLatType(GeoHashBits hash, double* xy);
int geohashDecodeToLongLatWGS84(GeoHashBits hash, double* xy);
void geohashNeighbors(const GeoHashBits* hash, GeoHashNeighbors* neighbors);

#if defined(__cplusplus)
}
#endif
#endif /* PIKA_GEOHASH_H_ */
