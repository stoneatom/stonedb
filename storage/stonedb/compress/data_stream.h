/* Copyright (c) 2022 StoneAtom, Inc. All rights reserved.
   Use is subject to license terms

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1335 USA
*/
#ifndef STONEDB_COMPRESS_DATA_STREAM_H_
#define STONEDB_COMPRESS_DATA_STREAM_H_
#pragma once

#include <cstring>

#include "common/assert.h"
#include "compress/defs.h"

namespace stonedb {

class ErrBufOverrun {};
namespace compress {

// The bits in a byte are counted from the Least Signif. to the MS.
class DataStream {
 protected:
  uchar *buf;
  uint len,  // length of 'buf', in bits
      pos;   // number of bits of buf already read/wrote, counting from the beg.
             // of buf (0 <= pos < len)

  virtual void BufOverrun() { throw ErrBufOverrun(); }

 public:
  using uint64_t = unsigned long long;

  void Reset() { pos = 0; }
  void Reset(uint len_, uint pos_ = 0) {
    len = len_;
    pos = pos_;
  }
  void Reset(char *buf_, uint len_, uint pos_ = 0) {
    buf = (uchar *)buf_;
    len = len_;
    pos = pos_;
  }

  DataStream() : buf(NULL) { Reset(0, 0, 0); }
  DataStream(char *buf_, uint len_, uint pos_ = 0) : buf(NULL) { Reset(buf_, len_, pos_); }
  virtual ~DataStream() {}
  char *GetBuf() { return (char *)buf; }
  uint GetPos() { return pos; }
  void SetPos(uint pos_) { pos = pos_; }
  bool CanRead() { return pos < len; }
  bool CanWrite() { return pos < len; }
  virtual uchar Get() = 0;
  virtual void Put(uchar v) = 0;
};

// Note: do NOT use the same BitStream for mixed reading and writing.
// It is asssumed that exclusively Get or exclusively Put will be used.
// During writing, some more bits than is needed can be cleared in advance - due
// to efficiency reasons.
class BitStream : public DataStream {
  uint clrlen;  // index of the last cleared bit of 'buf' plus 1 (0 - no bit
                // cleared yet)

  // clear bits of 'buf', from bit no. 'beg' to 'end'-1;
  // there is no checking of buffer overrun
  void ZeroBits(uint beg, uint end);
  // clear some number of bits in advance, beggining from 'pos', to allow faster
  // Put operations
  void ClearBits();

 public:
  // len_, pos_ - numbers of BITS
  void Reset() {
    DataStream::Reset();
    clrlen = 0;
  }
  void Reset(uint len_, uint pos_ = 0) {
    DataStream::Reset(len_, pos_);
    clrlen = 0;
  }
  void Reset(char *buf_, uint len_, uint pos_ = 0) {
    DataStream::Reset(buf_, len_, pos_);
    clrlen = 0;
  }

  // len_, pos_ - numbers of BITS
  BitStream() { Reset(0, 0, 0); }
  BitStream(char *buf_, uint len_, uint pos_ = 0) { Reset(buf_, len_, pos_); }
  virtual ~BitStream() {}
  uchar NextBit();              // show the next bit, but don't remove it from the stream
  uchar NextByte();             // show the next byte, but don't remove it from the stream
  void SkipBit(uint skip = 1);  // advances 'pos', checks for buffer overflow

  // these two methods are NOT virtual, so can be used directly to slightly
  // increase efficiency
  uchar GetBit();        // returns the next bit in the Least Signif Bit; advances 'pos'
  void PutBit(uchar b);  // appends the Least Signif. Bit of 'b' to the data
  void PutBit0();
  void PutBit1();

  void PutByte(uchar b);
  uchar GetByte();

  void PutUInt(uint n, unsigned short int no_bits);
  uint GetUInt(unsigned short int no_bits);
  void PutUInt64(uint64_t n, unsigned short int no_bits);
  uint64_t GetUInt64(unsigned short int no_bits);

  void FastPutBlock(char *data,
                    uint n);              // append 'n' bytes of 'data', starting at the
                                          // byte boundary in 'buf'
  void FastGetBlock(char *data, uint n);  // read 'n' bytes to 'data'

  uchar Get() override { return GetBit(); }
  void Put(uchar v) override { PutBit(v); }
};

inline uchar BitStream::NextBit() {
  if (pos >= len) BufOverrun();
  return (buf[pos >> 3] >> (pos & 7)) & 1;
}

inline uchar BitStream::NextByte() {
  if (pos + 7 >= len) BufOverrun();
  unsigned char result = 0;
  result |= (buf[pos >> 3] >> pos % 8);
  result |= (buf[(pos >> 3) + 1] _SHL_(8 - pos % 8));
  return result;
}

inline void BitStream::SkipBit(uint skip) {
  if ((pos > pos + skip) || ((pos += skip) > len)) BufOverrun();
}

inline uchar BitStream::GetBit() {
  if (pos >= len) BufOverrun();
  uchar result = (buf[pos >> 3] >> (pos & 7)) & 1;
  pos++;
  return result;
}

inline void BitStream::PutBit(uchar b) {
  if (pos >= clrlen) ClearBits();
  buf[pos >> 3] |= ((b & 1) << (pos & 7));
  pos++;
}

inline void BitStream::PutBit0() {
  if (pos >= clrlen) ClearBits();
  pos++;
}

inline void BitStream::PutBit1() {
  if (pos >= clrlen) ClearBits();
  buf[pos >> 3] |= (1 << (pos & 7));
  pos++;
}

inline void BitStream::PutByte(uchar b) {
  if (pos + 7 >= clrlen) ClearBits();
  if (pos & 7) {
    buf[pos >> 3] |= (b << (pos & 7));
    buf[(pos >> 3) + 1] |= (b _SHR_(8 - (pos & 7)));
  } else
    buf[pos >> 3] = b;
  pos += 8;
}

inline uchar BitStream::GetByte() {
  if (pos + 7 >= len) BufOverrun();
  uchar result = 0;
  if (pos & 7) {
    result |= (buf[pos >> 3] >> (pos & 7));
    result |= (buf[(pos >> 3) + 1] _SHL_(8 - (pos & 7)));
  } else
    result = buf[pos >> 3];
  pos += 8;
  return result;
}

inline void BitStream::PutUInt(uint n, unsigned short int no_bits) {
  DEBUG_ASSERT(no_bits <= 32);
  int i = 0;  // no. of bits already stored
  for (; no_bits - i >= 8; i += 8) PutByte((uchar)(n >> i));
  for (; i < no_bits; i++) PutBit((uchar)(n >> i));
}

inline uint BitStream::GetUInt(unsigned short int no_bits) {
  DEBUG_ASSERT(no_bits <= 32);
  uint result = 0;
  int i = 0;  // no. of bits already read
  for (; no_bits - i >= 8; i += 8) result |= (((uint)GetByte()) << i);
  for (; i < no_bits; i++) result |= (((uint)GetBit()) << i);
  return result;
}

inline void BitStream::PutUInt64(uint64_t n, unsigned short int no_bits) {
  DEBUG_ASSERT(no_bits <= 64);
  int i = 0;  // no. of bits already stored
  for (; no_bits - i >= 8; i += 8) PutByte((uchar)(n >> i));
  for (; i < no_bits; i++) PutBit((uchar)(n >> i));
}

inline unsigned long long BitStream::GetUInt64(unsigned short int no_bits) {
  DEBUG_ASSERT(no_bits <= 64);
  uint64_t result = 0;
  int i = 0;  // no. of bits already read
  for (; no_bits - i >= 8; i += 8) result |= (((uint64_t)GetByte()) << i);
  for (; i < no_bits; i++) result |= (((uint64_t)GetBit()) << i);
  return result;
}

inline void BitStream::FastPutBlock(char *data, uint n) {
  pos = ((pos + 7) / 8) * 8;  // round 'pos' up to the byte boundary
  uint pos1 = pos + 8 * n;
  if ((pos > pos1) || (pos1 > len)) BufOverrun();
  std::memcpy(buf + pos / 8, data, n);
  pos = pos1;
}

inline void BitStream::FastGetBlock(char *data, uint n) {
  pos = ((pos + 7) / 8) * 8;  // round 'pos' up to the byte boundary
  uint pos1 = pos + 8 * n;
  if ((pos > pos1) || (pos1 > len)) BufOverrun();
  std::memcpy(data, buf + pos / 8, n);
  pos = pos1;
}

}  // namespace compress
}  // namespace stonedb

#endif  // STONEDB_COMPRESS_DATA_STREAM_H_
