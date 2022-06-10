#include <iostream>
#include <fstream>
#include <stdint.h>

typedef uint64_t TX_ID;

struct DPN {
  public:
    uint8_t  used : 1;    // occupied or not
    uint8_t  local : 1;   // owned by a write transaction, thus to-be-commit
    uint8_t  synced : 1;  // if the pack data in memory is up to date with the version on disk
    uint8_t  null_compressed : 1;
    uint8_t  data_compressed : 1;
    uint8_t  no_compress : 1;
    uint8_t  padding[3];
    uint32_t base;  // index of the DPN from which we copied, used by local pack
    uint64_t addr;  // data start address
    uint64_t len;   // data length
    uint32_t nr;    // number of records
    uint32_t nn;    // number of nulls
    TX_ID    xmin;  // creation trx id
    TX_ID    xmax;  // delete trx id
    union {
        int64_t min;
        double  min_d;
    };
    union {
        int64_t max;
        double  max_d;
    };
    union {
        int64_t  sum;
        double   sum_d;
        uint64_t maxlen;
    };
    uint64_t tagged_ptr;
};

int main(int argc, char **argv) {
    if (argc < 2) {
        std::cout << "Usage: " << argv[0] << " /path/to/DN" << std::endl;
        return 1;
    }
    std::ifstream dpn_file(argv[1], std::ifstream::binary | std::ifstream::in);
    DPN dpn;
    while (dpn_file) {
        dpn_file.read((char *)&dpn, sizeof dpn);
        std::cout << (int)dpn.used << ";"
                  << (int)dpn.local << ";"
                  << (int)dpn.synced << ";"
                  << (int)dpn.null_compressed << ";"
                  << (int)dpn.data_compressed << ";"
                  << (int)dpn.no_compress << ";"
                  << dpn.base << ";"
                  << dpn.addr << ";"
                  << dpn.len << ";"
                  << dpn.nr << ";"
                  << dpn.nn << ";"
                  << dpn.xmin << ";"
                  << dpn.xmax << ";"
                  << dpn.min << ";"
                  << dpn.max << ";"
                  << dpn.maxlen << std::endl;
    }
}
