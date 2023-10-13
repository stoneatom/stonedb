/* Copyright (c) 2018, 2022, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
 The fundmental code for imcs. The chunk is used to store the data which
 transfer from row-based format to column-based format.

 Created 5/2/2023 Hom.LEE
*/

#include "imcs.h"

#include "common/assert.h"
#include "data/bucket.h"
#include "data/cell.h"
#include "data/chunk.h"

namespace Tianmu {
namespace IMCS {

// the total memory size of tianmu rapid, be set at initialization phase.
unsigned long srv_rapid_memory_size = 0;

// the tianmu rapid populate buffer size.
unsigned long srv_rapid_populate_buffer_size = 0;

// the hash table used for fast accessing the meta info of imcus. ???
hash_table_t *st_imcs_imcus_meta_ = nullptr;

/* structure to pass status variables to MySQL */
rapid_export_var_t export_vars;

/*********************Start of IMCS***************************************/

// do initialize for imcs. allocation the memory for imcus and launch the
// populateion worker to monitor the population pubuffer.
IMCS_STATUS Imcs::initialization() {
  srv_rapid_memory_size = 100;
  srv_rapid_populate_buffer_size = 100;

  inited_ = true;

  // allocation a memory pool to hold the data.
  // here, to allocate all the imcus.

  return IMCS_STATUS::SUCCESS;
}

// do deinitialize for imcs.
IMCS_STATUS Imcs::deinitialization() {
  inited_ = false;

  return IMCS_STATUS::SUCCESS;
}

int Imcs::scan(Imcs_context &context) {
  int ret = RET_SUCCESS;

  uint32 imcu_idx = context.get_cur_imcu_idx();
  if (imcu_idx >= context.share->imcu_idx_list_.size()) {
    return RET_INDEX_OUT_OF_BOUND;
  }
  Imcu *imcu = imcus_[context.share->imcu_idx_list_[imcu_idx]];
  if (RET_FAIL(imcu->scan(context))) {
    return ret;
  }
  return RET_SUCCESS;
}

void Imcs::srv_export_tianmu_rapid_status() {}

int Imcs::insert(TianmuSecondaryShare *share, std::vector<Field *> &field_lst,
                 size_t primary_key_idx, uint64 *row_id, uint64 trx_id) {
  std::lock_guard<std::mutex> lock_(mutex_);
  int ret = RET_SUCCESS;

  Field *primary_key_field = field_lst[primary_key_idx];
  PK pk(primary_key_field);
  // generate row id
  // TODO: double lock?
  if (RET_FAIL(share->row_manager_->gen_row_id(pk, row_id))) {
    return ret;
  }
  uint32 imcu_idx = *row_id / share->imcu_rows_;
  uint32 in_imcu_idx = *row_id % share->imcu_rows_;
  uint32 n_imcus = share->imcu_idx_list_.size();
  Imcu *imcu = nullptr;
  if (imcu_idx == n_imcus) {
    if (RET_NULL(imcu =
                     create_imcu(share, static_cast<uint64>(imcus_.size())))) {
      return RET_CREATE_IMCU_ERR;
    };
  } else if (imcu_idx + 1 == n_imcus) {
    imcu = imcus_[share->imcu_idx_list_[imcu_idx]];
  } else {
    return RET_IMCS_INNER_ERROR;
  }

  if (RET_FAIL(imcu->insert(in_imcu_idx, field_lst, trx_id))) {
    return ret;
  }
  return ret;
}

int Imcs::remove(TianmuSecondaryShare *share, Field *primary_key_field,
                 uint64 trx_id) {
  std::lock_guard<std::mutex> lock_(mutex_);
  int ret = RET_SUCCESS;

  PK pk(primary_key_field);
  uint64 row_id;
  if (RET_FAIL(share->row_manager_->get_row_id(pk, &row_id))) {
    return ret;
  }
  uint32 imcu_idx = row_id / share->imcu_rows_;
  uint32 in_imcu_idx = row_id % share->imcu_rows_;
  uint32 n_imcus = share->imcu_idx_list_.size();
  assert(imcu_idx < n_imcus);
  Imcu *imcu = imcus_[share->imcu_idx_list_[imcu_idx]];
  if (RET_FAIL(imcu->remove(in_imcu_idx, trx_id))) {
    return ret;
  }
  return ret;
}

Imcu *Imcs::create_imcu(TianmuSecondaryShare *share, uint64 imcu_id) {
  int ret = RET_SUCCESS;
  uint32 imcu_size = share->imcu_size_;
  Arena *arena = new Arena(imcu_size);

  uint32 n_buckets = share->n_buckets_, n_cells = share->n_cells_;

  uchar *ptr = nullptr;

  if (RET_NULL(ptr = arena->allocate(sizeof(Imcu)))) {
    return nullptr;
  }
  Imcu *imcu = new (ptr) Imcu(arena, imcu_id, share);
  Imcu_meta *imcu_meta = reinterpret_cast<Imcu_meta *>(imcu);

  size_t n_fields = share->fields_.size();
  if (RET_NULL(ptr = arena->allocate(sizeof(Chunk) * n_fields))) {
    return nullptr;
  }
  imcu->set_chunks(reinterpret_cast<Chunk *>(ptr));
  uchar *chunks_ptr = ptr;
  if (RET_NULL(ptr = arena->allocate(sizeof(Row) * n_buckets * n_cells))) {
    return nullptr;
  }
  imcu->set_rows(reinterpret_cast<Row *>(ptr));
  for (uint32 i = 0; i < n_buckets * n_cells; ++i) {
    new (ptr + i * sizeof(Row)) Row();
  }
  for (size_t i = 0; i < n_fields; ++i) {
    uint32 len = share->fields_[i].rpd_length;
    Chunk *chunk = new (chunks_ptr + i * sizeof(Chunk)) Chunk(i, share);

    if (RET_NULL(ptr = arena->allocate(sizeof(Bucket) * n_buckets))) {
      return nullptr;
    }
    chunk->set_buckets((Bucket *)ptr);
    uchar *bucket_ptr = ptr;
    for (size_t j = 0; j < n_buckets; ++j) {
      Bucket *bucket =
          new (bucket_ptr + j * sizeof(Bucket)) Bucket(j, i, share);
      if (RET_NULL(ptr = arena->allocate(sizeof(Cell) * n_cells))) {
        return nullptr;
      }
      bucket->set_cells((Cell *)ptr);
      uchar *cells_ptr = ptr;
      for (uint32 k = 0; k < n_cells; ++k) {
        Cell *cell = new (cells_ptr + k * sizeof(Cell)) Cell(len);
        if (RET_NULL(ptr = arena->allocate(len))) {
          return nullptr;
        }
        cell->set_data(ptr);
      }
      if (RET_NULL(ptr = arena->allocate(sizeof(Row *) * n_cells))) {
        return nullptr;
      }
      bucket->set_rows(reinterpret_cast<Row **>(ptr));
      for (uint32 k = 0; k < n_cells; ++k) {
        Cell *cell = new (cells_ptr + k * sizeof(Cell)) Cell(len);
        if (RET_NULL(ptr = arena->allocate(len))) {
          return nullptr;
        }
        cell->set_data(ptr);
      }
    }
  }
  imcus_.push_back(imcu);
  share->imcu_idx_list_.push_back(imcu_id);
  return imcu;
}

Imcs *IMCS_Instance::pinstance_{nullptr};
std::mutex IMCS_Instance::mutex_;

/**
 * The first time we call GetInstance we will lock the storage location
 *      and then we make sure again that the variable is null and then we
 *      set the value.  */
Imcs *IMCS_Instance::GetInstance(const std::string &value) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (pinstance_ == nullptr) {
    pinstance_ = new Imcs(srv_rapid_memory_size);
  }
  return pinstance_;
}

}  // namespace IMCS
}  // namespace Tianmu
