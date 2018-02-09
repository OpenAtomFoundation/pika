#include "pika_redis.h"
#include <glog/logging.h>

static const uint64_t crc64_tab[256] = {
    UINT64_C(0x0000000000000000), UINT64_C(0x7ad870c830358979),
    UINT64_C(0xf5b0e190606b12f2), UINT64_C(0x8f689158505e9b8b),
    UINT64_C(0xc038e5739841b68f), UINT64_C(0xbae095bba8743ff6),
    UINT64_C(0x358804e3f82aa47d), UINT64_C(0x4f50742bc81f2d04),
    UINT64_C(0xab28ecb46814fe75), UINT64_C(0xd1f09c7c5821770c),
    UINT64_C(0x5e980d24087fec87), UINT64_C(0x24407dec384a65fe),
    UINT64_C(0x6b1009c7f05548fa), UINT64_C(0x11c8790fc060c183),
    UINT64_C(0x9ea0e857903e5a08), UINT64_C(0xe478989fa00bd371),
    UINT64_C(0x7d08ff3b88be6f81), UINT64_C(0x07d08ff3b88be6f8),
    UINT64_C(0x88b81eabe8d57d73), UINT64_C(0xf2606e63d8e0f40a),
    UINT64_C(0xbd301a4810ffd90e), UINT64_C(0xc7e86a8020ca5077),
    UINT64_C(0x4880fbd87094cbfc), UINT64_C(0x32588b1040a14285),
    UINT64_C(0xd620138fe0aa91f4), UINT64_C(0xacf86347d09f188d),
    UINT64_C(0x2390f21f80c18306), UINT64_C(0x594882d7b0f40a7f),
    UINT64_C(0x1618f6fc78eb277b), UINT64_C(0x6cc0863448deae02),
    UINT64_C(0xe3a8176c18803589), UINT64_C(0x997067a428b5bcf0),
    UINT64_C(0xfa11fe77117cdf02), UINT64_C(0x80c98ebf2149567b),
    UINT64_C(0x0fa11fe77117cdf0), UINT64_C(0x75796f2f41224489),
    UINT64_C(0x3a291b04893d698d), UINT64_C(0x40f16bccb908e0f4),
    UINT64_C(0xcf99fa94e9567b7f), UINT64_C(0xb5418a5cd963f206),
    UINT64_C(0x513912c379682177), UINT64_C(0x2be1620b495da80e),
    UINT64_C(0xa489f35319033385), UINT64_C(0xde51839b2936bafc),
    UINT64_C(0x9101f7b0e12997f8), UINT64_C(0xebd98778d11c1e81),
    UINT64_C(0x64b116208142850a), UINT64_C(0x1e6966e8b1770c73),
    UINT64_C(0x8719014c99c2b083), UINT64_C(0xfdc17184a9f739fa),
    UINT64_C(0x72a9e0dcf9a9a271), UINT64_C(0x08719014c99c2b08),
    UINT64_C(0x4721e43f0183060c), UINT64_C(0x3df994f731b68f75),
    UINT64_C(0xb29105af61e814fe), UINT64_C(0xc849756751dd9d87),
    UINT64_C(0x2c31edf8f1d64ef6), UINT64_C(0x56e99d30c1e3c78f),
    UINT64_C(0xd9810c6891bd5c04), UINT64_C(0xa3597ca0a188d57d),
    UINT64_C(0xec09088b6997f879), UINT64_C(0x96d1784359a27100),
    UINT64_C(0x19b9e91b09fcea8b), UINT64_C(0x636199d339c963f2),
    UINT64_C(0xdf7adabd7a6e2d6f), UINT64_C(0xa5a2aa754a5ba416),
    UINT64_C(0x2aca3b2d1a053f9d), UINT64_C(0x50124be52a30b6e4),
    UINT64_C(0x1f423fcee22f9be0), UINT64_C(0x659a4f06d21a1299),
    UINT64_C(0xeaf2de5e82448912), UINT64_C(0x902aae96b271006b),
    UINT64_C(0x74523609127ad31a), UINT64_C(0x0e8a46c1224f5a63),
    UINT64_C(0x81e2d7997211c1e8), UINT64_C(0xfb3aa75142244891),
    UINT64_C(0xb46ad37a8a3b6595), UINT64_C(0xceb2a3b2ba0eecec),
    UINT64_C(0x41da32eaea507767), UINT64_C(0x3b024222da65fe1e),
    UINT64_C(0xa2722586f2d042ee), UINT64_C(0xd8aa554ec2e5cb97),
    UINT64_C(0x57c2c41692bb501c), UINT64_C(0x2d1ab4dea28ed965),
    UINT64_C(0x624ac0f56a91f461), UINT64_C(0x1892b03d5aa47d18),
    UINT64_C(0x97fa21650afae693), UINT64_C(0xed2251ad3acf6fea),
    UINT64_C(0x095ac9329ac4bc9b), UINT64_C(0x7382b9faaaf135e2),
    UINT64_C(0xfcea28a2faafae69), UINT64_C(0x8632586aca9a2710),
    UINT64_C(0xc9622c4102850a14), UINT64_C(0xb3ba5c8932b0836d),
    UINT64_C(0x3cd2cdd162ee18e6), UINT64_C(0x460abd1952db919f),
    UINT64_C(0x256b24ca6b12f26d), UINT64_C(0x5fb354025b277b14),
    UINT64_C(0xd0dbc55a0b79e09f), UINT64_C(0xaa03b5923b4c69e6),
    UINT64_C(0xe553c1b9f35344e2), UINT64_C(0x9f8bb171c366cd9b),
    UINT64_C(0x10e3202993385610), UINT64_C(0x6a3b50e1a30ddf69),
    UINT64_C(0x8e43c87e03060c18), UINT64_C(0xf49bb8b633338561),
    UINT64_C(0x7bf329ee636d1eea), UINT64_C(0x012b592653589793),
    UINT64_C(0x4e7b2d0d9b47ba97), UINT64_C(0x34a35dc5ab7233ee),
    UINT64_C(0xbbcbcc9dfb2ca865), UINT64_C(0xc113bc55cb19211c),
    UINT64_C(0x5863dbf1e3ac9dec), UINT64_C(0x22bbab39d3991495),
    UINT64_C(0xadd33a6183c78f1e), UINT64_C(0xd70b4aa9b3f20667),
    UINT64_C(0x985b3e827bed2b63), UINT64_C(0xe2834e4a4bd8a21a),
    UINT64_C(0x6debdf121b863991), UINT64_C(0x1733afda2bb3b0e8),
    UINT64_C(0xf34b37458bb86399), UINT64_C(0x8993478dbb8deae0),
    UINT64_C(0x06fbd6d5ebd3716b), UINT64_C(0x7c23a61ddbe6f812),
    UINT64_C(0x3373d23613f9d516), UINT64_C(0x49aba2fe23cc5c6f),
    UINT64_C(0xc6c333a67392c7e4), UINT64_C(0xbc1b436e43a74e9d),
    UINT64_C(0x95ac9329ac4bc9b5), UINT64_C(0xef74e3e19c7e40cc),
    UINT64_C(0x601c72b9cc20db47), UINT64_C(0x1ac40271fc15523e),
    UINT64_C(0x5594765a340a7f3a), UINT64_C(0x2f4c0692043ff643),
    UINT64_C(0xa02497ca54616dc8), UINT64_C(0xdafce7026454e4b1),
    UINT64_C(0x3e847f9dc45f37c0), UINT64_C(0x445c0f55f46abeb9),
    UINT64_C(0xcb349e0da4342532), UINT64_C(0xb1eceec59401ac4b),
    UINT64_C(0xfebc9aee5c1e814f), UINT64_C(0x8464ea266c2b0836),
    UINT64_C(0x0b0c7b7e3c7593bd), UINT64_C(0x71d40bb60c401ac4),
    UINT64_C(0xe8a46c1224f5a634), UINT64_C(0x927c1cda14c02f4d),
    UINT64_C(0x1d148d82449eb4c6), UINT64_C(0x67ccfd4a74ab3dbf),
    UINT64_C(0x289c8961bcb410bb), UINT64_C(0x5244f9a98c8199c2),
    UINT64_C(0xdd2c68f1dcdf0249), UINT64_C(0xa7f41839ecea8b30),
    UINT64_C(0x438c80a64ce15841), UINT64_C(0x3954f06e7cd4d138),
    UINT64_C(0xb63c61362c8a4ab3), UINT64_C(0xcce411fe1cbfc3ca),
    UINT64_C(0x83b465d5d4a0eece), UINT64_C(0xf96c151de49567b7),
    UINT64_C(0x76048445b4cbfc3c), UINT64_C(0x0cdcf48d84fe7545),
    UINT64_C(0x6fbd6d5ebd3716b7), UINT64_C(0x15651d968d029fce),
    UINT64_C(0x9a0d8ccedd5c0445), UINT64_C(0xe0d5fc06ed698d3c),
    UINT64_C(0xaf85882d2576a038), UINT64_C(0xd55df8e515432941),
    UINT64_C(0x5a3569bd451db2ca), UINT64_C(0x20ed197575283bb3),
    UINT64_C(0xc49581ead523e8c2), UINT64_C(0xbe4df122e51661bb),
    UINT64_C(0x3125607ab548fa30), UINT64_C(0x4bfd10b2857d7349),
    UINT64_C(0x04ad64994d625e4d), UINT64_C(0x7e7514517d57d734),
    UINT64_C(0xf11d85092d094cbf), UINT64_C(0x8bc5f5c11d3cc5c6),
    UINT64_C(0x12b5926535897936), UINT64_C(0x686de2ad05bcf04f),
    UINT64_C(0xe70573f555e26bc4), UINT64_C(0x9ddd033d65d7e2bd),
    UINT64_C(0xd28d7716adc8cfb9), UINT64_C(0xa85507de9dfd46c0),
    UINT64_C(0x273d9686cda3dd4b), UINT64_C(0x5de5e64efd965432),
    UINT64_C(0xb99d7ed15d9d8743), UINT64_C(0xc3450e196da80e3a),
    UINT64_C(0x4c2d9f413df695b1), UINT64_C(0x36f5ef890dc31cc8),
    UINT64_C(0x79a59ba2c5dc31cc), UINT64_C(0x037deb6af5e9b8b5),
    UINT64_C(0x8c157a32a5b7233e), UINT64_C(0xf6cd0afa9582aa47),
    UINT64_C(0x4ad64994d625e4da), UINT64_C(0x300e395ce6106da3),
    UINT64_C(0xbf66a804b64ef628), UINT64_C(0xc5bed8cc867b7f51),
    UINT64_C(0x8aeeace74e645255), UINT64_C(0xf036dc2f7e51db2c),
    UINT64_C(0x7f5e4d772e0f40a7), UINT64_C(0x05863dbf1e3ac9de),
    UINT64_C(0xe1fea520be311aaf), UINT64_C(0x9b26d5e88e0493d6),
    UINT64_C(0x144e44b0de5a085d), UINT64_C(0x6e963478ee6f8124),
    UINT64_C(0x21c640532670ac20), UINT64_C(0x5b1e309b16452559),
    UINT64_C(0xd476a1c3461bbed2), UINT64_C(0xaeaed10b762e37ab),
    UINT64_C(0x37deb6af5e9b8b5b), UINT64_C(0x4d06c6676eae0222),
    UINT64_C(0xc26e573f3ef099a9), UINT64_C(0xb8b627f70ec510d0),
    UINT64_C(0xf7e653dcc6da3dd4), UINT64_C(0x8d3e2314f6efb4ad),
    UINT64_C(0x0256b24ca6b12f26), UINT64_C(0x788ec2849684a65f),
    UINT64_C(0x9cf65a1b368f752e), UINT64_C(0xe62e2ad306bafc57),
    UINT64_C(0x6946bb8b56e467dc), UINT64_C(0x139ecb4366d1eea5),
    UINT64_C(0x5ccebf68aecec3a1), UINT64_C(0x2616cfa09efb4ad8),
    UINT64_C(0xa97e5ef8cea5d153), UINT64_C(0xd3a62e30fe90582a),
    UINT64_C(0xb0c7b7e3c7593bd8), UINT64_C(0xca1fc72bf76cb2a1),
    UINT64_C(0x45775673a732292a), UINT64_C(0x3faf26bb9707a053),
    UINT64_C(0x70ff52905f188d57), UINT64_C(0x0a2722586f2d042e),
    UINT64_C(0x854fb3003f739fa5), UINT64_C(0xff97c3c80f4616dc),
    UINT64_C(0x1bef5b57af4dc5ad), UINT64_C(0x61372b9f9f784cd4),
    UINT64_C(0xee5fbac7cf26d75f), UINT64_C(0x9487ca0fff135e26),
    UINT64_C(0xdbd7be24370c7322), UINT64_C(0xa10fceec0739fa5b),
    UINT64_C(0x2e675fb4576761d0), UINT64_C(0x54bf2f7c6752e8a9),
    UINT64_C(0xcdcf48d84fe75459), UINT64_C(0xb71738107fd2dd20),
    UINT64_C(0x387fa9482f8c46ab), UINT64_C(0x42a7d9801fb9cfd2),
    UINT64_C(0x0df7adabd7a6e2d6), UINT64_C(0x772fdd63e7936baf),
    UINT64_C(0xf8474c3bb7cdf024), UINT64_C(0x829f3cf387f8795d),
    UINT64_C(0x66e7a46c27f3aa2c), UINT64_C(0x1c3fd4a417c62355),
    UINT64_C(0x935745fc4798b8de), UINT64_C(0xe98f353477ad31a7),
    UINT64_C(0xa6df411fbfb21ca3), UINT64_C(0xdc0731d78f8795da),
    UINT64_C(0x536fa08fdfd90e51), UINT64_C(0x29b7d047efec8728),
};

uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l) {
    uint64_t j;

    for (j = 0; j < l; j++) {
        uint8_t byte = s[j];
        crc = crc64_tab[(uint8_t)crc ^ byte] ^ (crc >> 8);
    }
    return crc;
}

//for intset
#define INTSET_ENC_INT16 (sizeof(int16_t))
#define INTSET_ENC_INT32 (sizeof(int32_t))
#define INTSET_ENC_INT64 (sizeof(int64_t))
typedef struct intset {
    uint32_t encoding;
    uint32_t length;
    int8_t contents[];
} intset;


static uint8_t intsetGet(intset *is, uint32_t pos, int64_t *value) {
    int64_t v64;
    int32_t v32;
    int16_t v16;
    uint8_t enc;
    if (is == NULL || value == NULL){
        return 0;
    }

    enc = is->encoding;   
    //if (pos < intrev32ifbe(is->length)) {
    if (pos < is->length) {
        if (enc == INTSET_ENC_INT64) {
            memcpy(&v64,((int64_t*)is->contents)+pos,sizeof(v64));
            //memrev64ifbe(&v64);
            *value = v64;
        } else if (enc == INTSET_ENC_INT32) {
            memcpy(&v32,((int32_t*)is->contents)+pos,sizeof(v32));
            //memrev32ifbe(&v32);
            *value =  v32;
        } else {
            memcpy(&v16,((int16_t*)is->contents)+pos,sizeof(v16));
            //memrev16ifbe(&v16);
            *value =  v16;
        }

        return 1;
    }

    return 0;
}

//rio
/* Returns 1 or 0 for success/failure. */
size_t rioBufferRead(rio *r, void *buf, size_t len) {
    if (r == NULL || buf == NULL){
        return 0;
    }

    if (r->io.buffer.len - r->io.buffer.pos < len){
        return 0; /* not enough buffer to return len bytes. */
    }

    memcpy(buf, r->io.buffer.ptr + r->io.buffer.pos, len);
    r->io.buffer.pos += len;

    return 1;
}

void rioInitWithBuffer(rio *r, const char *s, size_t len) {
    const rio rioBufferIO = {
      rioBufferRead,
      NULL,           /* update_checksum */
      0,              /* current checksum */
      0,              /* bytes read or written */
      0,              /* read/write chunk size */
      { { NULL, 0, 0 } } /* union for io-specific vars */
    };

    *r = rioBufferIO;
    r->io.buffer.ptr = s;
    r->io.buffer.pos = 0;
    r->io.buffer.len = len;
}


static inline size_t rioRead(rio *r, void *buf, size_t len) {
    if (r == NULL || buf == NULL){
        return 0;
    }
    while (len) {
        size_t bytes_to_read = (r->max_processing_chunk && r->max_processing_chunk < len) ? r->max_processing_chunk : len;
        if (r->read(r,buf,bytes_to_read) == 0)
            return 0;

        if (r->update_cksum) r->update_cksum(r,buf,bytes_to_read);

        buf = (char*)buf + bytes_to_read;
        len -= bytes_to_read;
        r->processed_bytes += bytes_to_read;
    }

    return 1;
}

/* Load a "type" in RDB format, that is a one byte unsigned integer.
 * This function is not only used to load object types, but also special
 * "types" like the end-of-file type, the EXPIRE type, and so forth. */
static int rdbLoadType(rio *rdb) {
    unsigned char type;
    if (rioRead(rdb,&type,1) == 0) return -1;
    return type;
}

/* Use rdbLoadType() to load a TYPE in RDB format, but returns -1 if the
 * type is not specifically a valid Object Type. */
int rdbLoadObjectType(rio *rdb) {
    int type;
    if ((type = rdbLoadType(rdb)) == -1) return -1;
    if (!rdbIsObjectType(type)) return -1;
    return type;
}


/* Verify that the RDB version of the dump payload matches the one of this Redis
 * instance and that the checksum is ok.
 * If the DUMP payload looks valid REDIS_OK is returned, otherwise REDIS_ERR
 * is returned. */
int verifyDumpPayload(unsigned char *p, size_t len) {
    unsigned char *footer;
    uint16_t rdbver;
    uint64_t crc;

    /* At least 2 bytes of RDB version and 8 of CRC64 should be present. */
    if (len < 10) return REDIS_ERR;
    footer = p+(len-10);

    /* Verify RDB version */
    rdbver = (footer[1] << 8) | footer[0];
    if (rdbver != REDIS_RDB_VERSION) return REDIS_ERR;

    /* Verify CRC64 */
    crc = crc64(0,p,len-8);
    //memrev64ifbe(&crc);   //default LITTLE_ENDIAN
    return (memcmp(&crc,footer+2,8) == 0) ? REDIS_OK : REDIS_ERR;
}

/* Load an encoded length. The "isencoded" argument is set to 1 if the length
 * is not actually a length but an "encoding type". See the REDIS_RDB_ENC_*
 * definitions in rdb.h for more information. */
uint32_t rdbLoadLen(rio *rdb, int *isencoded) {
    unsigned char buf[2];
    uint32_t len;
    int type;

    if (isencoded) *isencoded = 0;

    if (rioRead(rdb,buf,1) == 0) return REDIS_RDB_LENERR;
    type = (buf[0]&0xC0)>>6;
    if (type == REDIS_RDB_ENCVAL) {
        /* Read a 6 bit encoding type. */
        if (isencoded) *isencoded = 1;
        return buf[0]&0x3F;
    } else if (type == REDIS_RDB_6BITLEN) {
        /* Read a 6 bit len. */
        return buf[0]&0x3F;
    } else if (type == REDIS_RDB_14BITLEN) {
        /* Read a 14 bit len. */
        if (rioRead(rdb,buf+1,1) == 0) return REDIS_RDB_LENERR;
        return ((buf[0]&0x3F)<<8)|buf[1];
    } else {
        /* Read a 32 bit len. */
        if (rioRead(rdb,&len,4) == 0) return REDIS_RDB_LENERR;
        return ntohl(len);
    }
}

/* Load a Redis object of the specified type from the specified file.
 * On success REDIS_OK is returned, otherwise return REDIS_ERR. */
int rdbLoadObject(int rdbtype, rio *rdb, restore_value *dbvalue) {

    if (rdbtype == REDIS_RDB_TYPE_STRING) {
        /* Read string value */
        dbvalue->type = nemo::kKV_DB;
        std::string value;
        if (rdbLoadStringObject(rdb, &value) != REDIS_OK){
            LOG(ERROR) << "rdbLoadEncodedStringObject : error";
            return REDIS_ERR;
        } 

        //the value may be ""
        dbvalue->kvv = value;
    }else if (rdbtype == REDIS_RDB_TYPE_LIST) {
        /* Read list value */
        size_t len;
        dbvalue->type = nemo::kLIST_DB;
        if ((len = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR){
            LOG(ERROR) << "REDIS_RDB_TYPE_LIST : get len error";
            return REDIS_ERR;
        } 

        /* Load every single element of the list */
        while(len--) {
            std::string ele;
            if (rdbLoadStringObject(rdb, &ele) != REDIS_OK){
                LOG(ERROR) << "rdbLoadEncodedStringObject : error";
                return REDIS_ERR;
            } 

            dbvalue->listv.push_back(ele);
        }
    }else if (rdbtype == REDIS_RDB_TYPE_SET) {
        /* Read list/set value */
        size_t len;
        dbvalue->type = nemo::kSET_DB;
        if ((len = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR){
            LOG(ERROR) << "REDIS_RDB_TYPE_SET : get len error";
            return REDIS_ERR;
        }

        /* Load every single element of the list/set */
        while (len--) {
            std::string ele;
            if (rdbLoadStringObject(rdb, &ele) != REDIS_OK){
                LOG(ERROR) << "rdbLoadEncodedStringObject : error";
                return REDIS_ERR;
            }

            dbvalue->setv.push_back(ele);
        }
    }else if (rdbtype == REDIS_RDB_TYPE_ZSET) {
        /* Read list/set value */
        size_t len;
        dbvalue->type = nemo::kZSET_DB;
        if ((len = rdbLoadLen(rdb, NULL)) == REDIS_RDB_LENERR){
            LOG(ERROR) << "REDIS_RDB_TYPE_SET : get len error";
            return REDIS_ERR;
        }

        /* Load every single element of the list/set */
        while(len--) {
            std::string member;
            double score;

            if (rdbLoadStringObject(rdb, &member) != REDIS_OK){
                LOG(ERROR) << "rdbLoadEncodedStringObject : error";
                return REDIS_ERR;
            }

            if (rdbLoadDoubleValue(rdb, &score) != REDIS_OK){
                LOG(ERROR) << "rdbLoadDoubleValue : error";
                return REDIS_ERR;
            }

            dbvalue->zsetv.push_back({score, member});
        }
    } else if (rdbtype == REDIS_RDB_TYPE_HASH) {
        size_t len;
        dbvalue->type = nemo::kHASH_DB;
        if ((len = rdbLoadLen(rdb, NULL)) == REDIS_RDB_LENERR){
            LOG(ERROR) << "REDIS_RDB_TYPE_SET : get len error";
            return REDIS_ERR;
        }

        /* Load remaining fields and values into the hash table */
        while (len--) {
            std::string field;
            std::string value;
            /* Load raw strings */
            if (rdbLoadStringObject(rdb, &field) != REDIS_OK){
                LOG(ERROR) << "rdbLoadEncodedStringObject : error";
                return REDIS_ERR;
            }

            if (rdbLoadStringObject(rdb, &value) != REDIS_OK){
                LOG(ERROR) << "rdbLoadStringObject : error";
                return REDIS_ERR;
            }

            /* Add pair to hash table */
            dbvalue->hashv.push_back({field, value});
        }
    } else if (rdbtype == REDIS_RDB_TYPE_HASH_ZIPMAP  ||
               rdbtype == REDIS_RDB_TYPE_LIST_ZIPLIST ||
               rdbtype == REDIS_RDB_TYPE_SET_INTSET   ||
               rdbtype == REDIS_RDB_TYPE_ZSET_ZIPLIST ||
               rdbtype == REDIS_RDB_TYPE_HASH_ZIPLIST)
    {
        std::string aux;
        if (rdbLoadStringObject(rdb, &aux) != REDIS_OK){
            LOG(ERROR) << "rdbLoadStringObject : error";
            return REDIS_ERR;
        }

        switch(rdbtype) {
            case REDIS_RDB_TYPE_LIST_ZIPLIST:
                {
                    return  getListFromZiplist(aux, dbvalue);
                }
                break;
            case REDIS_RDB_TYPE_ZSET_ZIPLIST:
                {
                    return  getZsetFromZiplist(aux, dbvalue);
                }
                break;
            case REDIS_RDB_TYPE_HASH_ZIPLIST:
                {
                    return  getHashFromZiplist(aux, dbvalue);
                }
                break;
            case REDIS_RDB_TYPE_SET_INTSET:
                {
                    return  getSetFromIntset(aux, dbvalue);
                }
                break;
            case REDIS_RDB_TYPE_HASH_ZIPMAP:
                {
                    //this encoding has be deprecated since redis 2.6
                    LOG(ERROR) << "dont support REDIS_RDB_TYPE_HASH_ZIPMAP encoding";
                    return REDIS_ERR;
                }
                break;
            default:
                LOG(ERROR) << "Unknown encoding";
                return REDIS_ERR;
        }
    }else{
        dbvalue->type = nemo::kNONE_DB;
    }

    return REDIS_OK;
}

int rdbLoadIntegerObject(rio *rdb, int enctype, std::string *str) {
    unsigned char enc[4];
    long long val;

    if (enctype == REDIS_RDB_ENC_INT8) {
        if (rioRead(rdb,enc,1) == 0) return REDIS_ERR;
        val = (signed char)enc[0];
    } else if (enctype == REDIS_RDB_ENC_INT16) {
        uint16_t v;
        if (rioRead(rdb,enc,2) == 0) return REDIS_ERR;
        v = enc[0]|(enc[1]<<8);
        val = (int16_t)v;
    } else if (enctype == REDIS_RDB_ENC_INT32) {
        uint32_t v;
        if (rioRead(rdb,enc,4) == 0) return REDIS_ERR;
        v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
        val = (int32_t)v;
    } else {
        //val = 0; /* anti-warning */
        LOG(ERROR) << "Unknown RDB integer encoding type";
        return REDIS_ERR;
    }

    *str = std::to_string(val);
    
    return REDIS_OK;
}

int rdbLoadLzfStringObject(rio *rdb, std::string *str) {
    unsigned int len, clen;
    unsigned char *c = NULL;
    char *val = NULL;

    if ((clen = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR) return REDIS_ERR;
    if ((len = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR) return REDIS_ERR;
    if ((c = (unsigned char *)malloc(clen)) == NULL) goto err;
    if ((val = (char *)malloc(len)) == NULL) goto err;
    if (rioRead(rdb,c,clen) == 0) goto err;
    if (lzf_decompress(c,clen,val,len) == 0) goto err;

    str->assign(val, len);

    free(c);
    free(val);

    return REDIS_OK;
err:
    free(c);
    free(val);
    return REDIS_ERR;
}

int rdbLoadStringObject(rio *rdb, std::string *str) {
    int isencoded;
    uint32_t len;
    char *buf = NULL;

    len = rdbLoadLen(rdb, &isencoded);

    if (isencoded) {
        switch(len) {
        case REDIS_RDB_ENC_INT8:
        case REDIS_RDB_ENC_INT16:
        case REDIS_RDB_ENC_INT32:
            return rdbLoadIntegerObject(rdb, len, str);
        case REDIS_RDB_ENC_LZF:
            return rdbLoadLzfStringObject(rdb, str);
        default:
            LOG(ERROR) << "Unknown RDB encoding type";
            return REDIS_ERR;
        }
    }

    //when len is 0, the value is ""
    if (len == REDIS_RDB_LENERR) return REDIS_ERR;

    buf = (char *)malloc( len );
    if(buf == NULL){
        return REDIS_ERR;
    }

    if (len && rioRead(rdb, buf, len) == 0) {
        free(buf);
        return REDIS_ERR;
    }

    //len may be 0
    str->assign(buf, len);
    free(buf);

    return REDIS_OK;
}

/* For information about double serialization check rdbSaveDoubleValue() */
int rdbLoadDoubleValue(rio *rdb, double *val) {
    char buf[256];
    unsigned char len;

    double R_Zero, R_PosInf, R_NegInf, R_Nan;
    R_Zero = 0.0;
    R_PosInf = 1.0/R_Zero;
    R_NegInf = -1.0/R_Zero;
    R_Nan = R_Zero/R_Zero;

    if (rioRead(rdb,&len,1) == 0) return REDIS_ERR;
    switch(len) {
    case 255: *val = R_NegInf; return REDIS_OK;
    case 254: *val = R_PosInf; return REDIS_OK;
    case 253: *val = R_Nan; return REDIS_OK;
    default:
        if (rioRead(rdb,buf,len) == 0) return REDIS_ERR;
        buf[len] = '\0';
        sscanf(buf, "%lg", val);
        return REDIS_OK;
    }
}

int getZsetFromZiplist(const std::string &aux, restore_value *dbvalue)
{
    unsigned char *zl = (unsigned char *)aux.data();
    std::string ele;
    double score;
    unsigned char *eptr = NULL, *sptr = NULL;

    dbvalue->type = nemo::kZSET_DB;

    eptr = ziplistIndex(zl,0);
    if (eptr == NULL) {
        return REDIS_ERR;
    }

    sptr = ziplistNext(zl,eptr);


    while (eptr != NULL && sptr != NULL) {
        if (zzlGetScore(sptr, &score) == REDIS_ERR || zzlGetString(eptr, &ele) == REDIS_ERR){
            return REDIS_ERR;
        }

        dbvalue->zsetv.push_back({score, ele});

        if (zzlNext(zl,&eptr,&sptr) == REDIS_ERR){
            return REDIS_ERR;
        }
    }

    return REDIS_OK;
}

int getListFromZiplist(const std::string &aux, restore_value *dbvalue)
{
    unsigned char *zl = (unsigned char *)aux.data();
    std::string ele;
    unsigned char *eptr = NULL;

    dbvalue->type = nemo::kLIST_DB;

    eptr = ziplistIndex(zl,0);
    if (eptr == NULL) {
        return REDIS_ERR;
    }

    while (eptr != NULL) {
        if (zzlGetString(eptr, &ele) == REDIS_ERR){
            return REDIS_ERR;
        }

        dbvalue->listv.push_back(ele);

        eptr = ziplistNext(zl, eptr);
    }

    return REDIS_OK;
}

int getHashFromZiplist(const std::string &aux, restore_value *dbvalue)
{
    unsigned char *zl = (unsigned char *)aux.data();
    std::string field;
    std::string value;
    unsigned char *fptr = NULL, *vptr = NULL;

    dbvalue->type = nemo::kHASH_DB;

    fptr = ziplistIndex(zl,0);
    if (fptr == NULL) {
        return REDIS_ERR;
    }

    while (fptr != NULL) {
        vptr = ziplistNext(zl,fptr);
        if (vptr == NULL){
            return REDIS_ERR;
        }

        if (zzlGetString(fptr, &field) == REDIS_ERR){
            return REDIS_ERR;
        }

        if (zzlGetString(vptr, &value) == REDIS_ERR){
            return REDIS_ERR;
        }

        dbvalue->hashv.push_back({field, value});

        fptr = ziplistNext(zl,vptr);
    }

    return REDIS_OK;
}

int getSetFromIntset(const std::string &aux, restore_value *dbvalue)
{
    unsigned char *ptr = (unsigned char *)aux.data();
    int64_t     intele;
    uint32_t    index = 0;

    dbvalue->type = nemo::kSET_DB;

    while (intsetGet((intset *)ptr, index, &intele)) {
        dbvalue->setv.push_back(std::to_string(intele));

        index++;
    }

    return REDIS_OK;
}

int zzlGetScore(unsigned char *sptr, double *score) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    char buf[128];

    if (sptr == NULL || ziplistGet(sptr,&vstr,&vlen,&vlong) == 0){
        return REDIS_ERR;
    }

    if (vstr) {
        memcpy(buf,vstr,vlen);
        buf[vlen] = '\0';
        *score = strtod(buf,NULL);
    } else {
        *score = vlong;
    }

    return REDIS_OK;
}

int zzlGetString(unsigned char *sptr, std::string *value) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;

    if (sptr == NULL || ziplistGet(sptr,&vstr,&vlen,&vlong) == 0){
        return REDIS_ERR;
    }

    if (vstr == NULL)
        *value = std::to_string(vlong);
    else
        (*value).assign((char*)vstr, vlen);

    return REDIS_OK;
}

/* Move to next entry based on the values in eptr and sptr. Both are set to
 * NULL when there is no next entry. */
int zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;
    if (*eptr == NULL || *sptr == NULL){
        return REDIS_ERR;
    }

    _eptr = ziplistNext(zl,*sptr);
    if (_eptr != NULL) {
        _sptr = ziplistNext(zl,_eptr);
        if (_sptr == NULL){
            return REDIS_ERR;
        }
    } else {
        /* No next entry. */
        _sptr = NULL;
    }

    *eptr = _eptr;
    *sptr = _sptr;

    return REDIS_OK;
}