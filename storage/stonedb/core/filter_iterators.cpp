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

#include "core/filter.h"
#include "core/pack_orderer.h"

namespace stonedb {
namespace core {
FilterOnesIterator::FilterOnesIterator() : buffer(max_ahead) {
  valid = false;
  f = NULL;
  cur_position = -1;
  b = 0;
  bln = 0;
  lastn = 0;
  bitsLeft = 0;
  prev_block = -1;
  iterator_n = -1;
  iterator_b = prev_iterator_b = -1;
  cur_block_full = false;
  cur_block_empty = false;
  inside_one_pack = true;
  packs_to_go = -1;  // all packs
  packs_done = 0;
  pack_power = 0;
  pack_def = 0;
}

FilterOnesIterator::FilterOnesIterator(Filter *ff, uint32_t power) : buffer(max_ahead) { Init(ff, power); }

void FilterOnesIterator::Init(Filter *ff,
                              uint32_t power)  //!< reinitialize iterator (possibly with the new filter)
{
  pack_power = power;
  pack_def = 1 << pack_power;
  f = ff;
  packs_done = 0;    // all packs
  packs_to_go = -1;  // all packs
  bool nonempty_pack_found = false;
  iterator_b = prev_iterator_b = 0;
  inside_one_pack = true;
  while (iterator_b < f->no_blocks) {  // check whether there is more than one pack involved
    if (f->block_status[iterator_b] != f->FB_EMPTY) {
      if (nonempty_pack_found) {
        inside_one_pack = false;
        break;
      }
      nonempty_pack_found = true;
    }
    iterator_b++;
  }
  FilterOnesIterator::Rewind();  //!< initialize on the beginning
}

void FilterOnesIterator::SetNoPacksToGo(int n) {
  packs_to_go = n;
  // packs_done = 0;
}

void FilterOnesIterator::Reset() {
  cur_position = -1;
  b = 0;
  bln = 0;
  lastn = 0;
  bitsLeft = 0;
  prev_block = -1;
  valid = true;
  iterator_n = pack_def - 1;
  iterator_b = prev_iterator_b = -1;
  buffer.Reset();
}

void FilterOnesIterator::Rewind()  //!< iterate from the beginning (the first 1
                                   //!< in Filter)
{
  Reset();
  FilterOnesIterator::operator++();
}

void FilterOnesIterator::RewindToRow(const int64_t row)  // note: if row not exists , set IsValid() to false
{
  size_t pack = row >> pack_power;
  if (pack >= f->no_blocks || (pack == f->no_blocks - 1 && (row & (pack_def - 1)) >= f->no_of_bits_in_last_block))
    valid = false;
  else
    valid = true;
  DEBUG_ASSERT(pack < f->no_blocks);

  iterator_b = prev_iterator_b = pack;
  iterator_n = int(row & (pack_def - 1));
  uchar stat = f->block_status[iterator_b];
  cur_block_full = (stat == f->FB_FULL);
  cur_block_empty = (stat == f->FB_EMPTY);
  buffer.Reset();  // random order - forget history
  if (cur_block_empty) {
    valid = false;
    return;
  }
  packs_done = 0;
  if (stat == f->FB_MIXED) {
    if (!f->Get(row))
      valid = false;
    else {
      int already_bits = 0;
      for (int64_t index = pack << pack_power; index < row; ++index) {
        if (f->Get(index)) already_bits++;
      }
      ones_left_in_block = f->blocks[iterator_b]->no_set_bits - already_bits;
    }
  }
  cur_position = ((int64_t(iterator_b)) << pack_power) + iterator_n;
}

bool FilterOnesIterator::RewindToPack(size_t pack) {
  // if SetNoPacksToGo() has been used, then RewindToPack can be done only to
  // the previous pack DEBUG_ASSERT(packs_to_go == -1 || pack == prev_iterator_b
  // || pack == iterator_b);
  if (pack >= f->no_blocks)
    valid = false;
  else
    valid = true;
  packs_done = pack;
  // if(iterator_b != pack)
  packs_done--;
  iterator_b = prev_iterator_b = pack;
  prev_block = pack;
  b = 0;
  bitsLeft = 0;
  lastn = -2;
  bln = 0;
  iterator_n = 0;
  uchar stat = f->block_status[iterator_b];
  cur_block_full = (stat == f->FB_FULL);
  cur_block_empty = (stat == f->FB_EMPTY);
  //	DEBUG_ASSERT(buffer.Empty());			// random order - forget
  // history, WARNING: may lead to omitting
  // packs!
  buffer.Reset();
  if (cur_block_empty) {
    NextPack();
    return false;
  }
  if (!cur_block_full) {
    iterator_n = -1;
    FilterOnesIterator::operator++();
    ones_left_in_block = f->blocks[iterator_b]->no_set_bits;
  }
  cur_position = ((int64_t(iterator_b)) << pack_power) + iterator_n;
  return true;
}

void FilterOnesIterator::NextPack() {
  iterator_n = pack_def - 1;
  FilterOnesIterator::operator++();
}

int64_t FilterOnesIterator::GetPackSizeLeft()  // how many 1-s in the current block left
                                               // (including the current one)
{
  if (!valid) return 0;
  if (cur_block_full) return int64_t(f->block_last_one[iterator_b]) + 1 - iterator_n;
  DEBUG_ASSERT(f->block_status[iterator_b] != f->FB_EMPTY);
  return ones_left_in_block;
}

int FilterOnesIterator::Lookahead(int n) {
  if (!IsValid()) return -1;
  if (n == 0) return iterator_b;

  if (buffer.Elems() >= n) return buffer.Nth(n - 1);

  size_t tmp_b = buffer.Empty() ? iterator_b : buffer.GetLast();
  n -= buffer.Elems();

  while (n > 0) {
    ++tmp_b;
    while (tmp_b < f->no_blocks && f->block_status[tmp_b] == f->FB_EMPTY) ++tmp_b;
    if (tmp_b < f->no_blocks) {
      n--;
      buffer.Put(tmp_b);
    } else
      return buffer.Empty() ? iterator_b : buffer.GetLast();
  }
  return tmp_b;
}

bool FilterOnesIterator::IteratorBpp() {
  iterator_n = 0;
  prev_iterator_b = iterator_b;
  if (buffer.Empty()) {
    iterator_b++;
    if (packs_to_go != -1 && iterator_b > static_cast<unsigned int>(packs_to_go)) return false;
    while (iterator_b < f->no_blocks && f->block_status[iterator_b] == f->FB_EMPTY) {
      iterator_b++;
      if (packs_to_go != -1 && iterator_b > static_cast<unsigned int>(packs_to_go)) return false;
    }
    if (iterator_b >= f->no_blocks) return false;
  } else {
    iterator_b = buffer.Get();
    if (packs_to_go != -1 && iterator_b > static_cast<unsigned int>(packs_to_go)) return false;
  }

  uchar stat = f->block_status[iterator_b];
  cur_block_full = (stat == f->FB_FULL);
  cur_block_empty = (stat == f->FB_EMPTY);
  if (packs_to_go == -1 || ++packs_done < packs_to_go)
    return true;
  else
    return false;
}

bool FilterOnesIterator::NextInsidePack()  // return false if we just restarted
                                           // the pack after going to its end
{
  bool inside_pack = true;
  if (IsEndOfBlock()) {
    inside_pack = false;
    iterator_n = 0;
    prev_block = -1;
  } else
    iterator_n++;  // now we are on the first suspected position - go forward if
                   // it is not 1

  if (cur_block_full) {
    cur_position = ((int64_t(iterator_b)) << pack_power) + iterator_n;
    return inside_pack;
  } else if (cur_block_empty) {  // Updating iterator can cause this
    return false;
  } else {
    bool found = false;
    while (!found) {
      found = FindOneInsidePack();
      if (!found) {
        inside_pack = false;
        iterator_n = 0;
        prev_block = -1;
      }
    }
  }
  cur_position = ((int64_t(iterator_b)) << pack_power) + iterator_n;
  return inside_pack;
}

bool FilterOnesIterator::FindOneInsidePack() {
  bool found = false;
  // inter-block iteration
  DEBUG_ASSERT(f->block_status[iterator_b] == f->FB_MIXED);  // IteratorBpp() omits empty blocks
  Filter::Block *cur_block = f->blocks[iterator_b];
  int cb_no_obj = cur_block->no_obj;
  if (iterator_b == static_cast<unsigned int>(prev_block)) {
    ones_left_in_block--;  // we already were in this block and the previous bit
                           // is after us
  } else {
    b = 0;
    bitsLeft = 0;
    lastn = -2;
    bln = 0;
    prev_block = iterator_b;
    ones_left_in_block = cur_block->no_set_bits;
  }
  DEBUG_ASSERT(iterator_n < cb_no_obj || ones_left_in_block == 0);
  if (iterator_n < cb_no_obj) {
    // else leave "found == false", which will iterate to the next block
    if (lastn + 1 == iterator_n) {
      if (bitsLeft > 0) bitsLeft--;
      b >>= 1;
    } else {
      bln = iterator_n >> 5;
      int bitno = iterator_n & 0x1f;
      b = cur_block->block_table[bln];
      b >>= bitno;
      bitsLeft = 32 - bitno;
    }
    // find bit == 1
    while (!found && iterator_n < cb_no_obj) {
      // first find a portion with 1
      if (b == 0) {
        iterator_n += bitsLeft;
        if (++bln >= cur_block->block_size) break;  // leave found == false
        while (cur_block->block_table[bln] == 0) {
          iterator_n += 32;
          if (iterator_n >= cb_no_obj) break;
          ++bln;
        }
        if (iterator_n >= cb_no_obj) break;
        b = cur_block->block_table[bln];
        bitsLeft = 32;
      }

      while (iterator_n < cb_no_obj) {
        // non-zero value found - process 32bit portion
        int skip = cur_block->posOf1[b & 0xff];
        if (skip > bitsLeft) {
          // end of 32bit portion
          iterator_n += bitsLeft;
          break;
        } else {
          iterator_n += skip;
          if (iterator_n >= cb_no_obj) break;
          bitsLeft -= skip;
          b >>= skip;
          if (skip < 8) {
            // found 1
            lastn = iterator_n;
            found = true;
            break;
          }
        }
      }
    }
  }

  return found;
}

FilterOnesIterator *FilterOnesIterator::Copy(int packs_to_go) {
  FilterOnesIterator *f = new FilterOnesIterator();
  *f = *this;
  f->packs_to_go = packs_to_go;
  return f;
}

FilterOnesIteratorOrdered::FilterOnesIteratorOrdered() : FilterOnesIterator(), po(NULL){};

FilterOnesIteratorOrdered::FilterOnesIteratorOrdered(Filter *ff, PackOrderer *po, uint32_t power) {
  Init(ff, po, power);
}

void FilterOnesIteratorOrdered::Init(Filter *ff, PackOrderer *_po, uint32_t power) {
  po = _po;
  Reset();
  po->Rewind();
  FilterOnesIterator::Init(ff, power);
}

void FilterOnesIteratorOrdered::Rewind() {
  Reset();
  po->Rewind();
  FilterOnesIteratorOrdered::operator++();
}

int FilterOnesIteratorOrdered::Lookahead(int n) {
  if (n == 0) return iterator_b;

  if (buffer.Elems() >= n) return buffer.Nth(n - 1);

  int tmp_b = po->Current();
  n -= buffer.Elems();

  while (n > 0) {
    tmp_b = (++(*po)).Current();
    while (po->IsValid() && f->block_status[tmp_b] == f->FB_EMPTY) tmp_b = (++(*po)).Current();
    if (po->IsValid()) {
      n--;
      buffer.Put(tmp_b);
    } else
      return buffer.Empty() ? iterator_b : buffer.GetLast();
  }
  return tmp_b;
}

bool FilterOnesIteratorOrdered::NaturallyOrdered() { return po->NaturallyOrdered(); }

bool FilterOnesIteratorOrdered::IteratorBpp() {
  iterator_n = 0;
  prev_iterator_b = iterator_b;
  if (buffer.Empty()) {
    iterator_b = (++(*po)).Current();
    while (po->IsValid() && f->block_status[iterator_b] == f->FB_EMPTY) iterator_b = (++(*po)).Current();
    if (!po->IsValid()) return false;
  } else
    iterator_b = buffer.Get();

  uchar stat = f->block_status[iterator_b];
  cur_block_full = (stat == f->FB_FULL);
  cur_block_empty = (stat == f->FB_EMPTY);
  if (packs_to_go == -1 || ++packs_done < packs_to_go)
    return true;
  else
    return false;
}

FilterOnesIteratorOrdered *FilterOnesIteratorOrdered::Copy(int packs_to_go) {
  FilterOnesIteratorOrdered *f = new FilterOnesIteratorOrdered();
  *f = *this;
  f->po = new PackOrderer();
  f->packs_to_go = packs_to_go;
  *(f->po) = *po;
  return f;
}

void FilterOnesIteratorOrdered::NextPack() {
  if (IteratorBpp()) {
    if (cur_block_full) {
      cur_position = ((int64_t(iterator_b)) << pack_power) + iterator_n;
      return;
    } else {
      ones_left_in_block = f->blocks[iterator_b]->no_set_bits;
      if (f->blocks[iterator_b]->block_table[0] & 1)
        cur_position = ((int64_t(iterator_b)) << pack_power) + iterator_n;
      else
        FilterOnesIteratorOrdered::operator++();
    }
  } else
    valid = false;
}
}  // namespace core
}  // namespace stonedb
