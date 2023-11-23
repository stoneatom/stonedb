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
 Some global objects used for imcs in whole imcs lifetime are defined here. such
 as imcu_meta hash table. buffer pool which was used to store imcus. and etc.
 Created 4/2/2023 Hom.LEE
*/

#ifndef __IMCS_H__
#define __IMCS_H__

#include <memory>
#include <mutex>
#include <vector>

#include "buffer/populate.h"
#include "common/error.h"
#include "data/imcu.h"  //for imcus.
#include "filter/table_filter.h"
#include "imcs_base.h"
#include "row_manager.h"

namespace Tianmu {
namespace IMCS {

struct rapid_export_var_t {
  // the server memory size.
  unsigned long srv_memory_size;

  // the population buffer size of tainm
  unsigned long srv_populate_buffer_size;

  // the other vars to export.
};

// the total memory size of tianmu rapid, be set at initialization phase.
extern unsigned long srv_rapid_memory_size;

// the tianmu rapid populate buffer size.
extern unsigned long srv_rapid_populate_buffer_size;

// forward declaration.
class hash_table_t;

// the hash table used for fast accessing the meta info of imcus. ???
extern hash_table_t *st_imcs_imcus_meta_;

/** Status variables to be passed to MySQL */
extern struct rapid_export_var_t export_vars;

// forward declaration.
class Imcs;
class IMCS_Instance {
  /**
   * The Singleton's constructor/destructor should always be private to
   * prevent direct construction/desctruction calls with the `new`/`delete`
   * operator.
   */
 private:
  static Imcs *pinstance_;
  static std::mutex mutex_;

 protected:
  IMCS_Instance(const std::string value) : value_(value) {}
  ~IMCS_Instance() {}
  std::string value_;

 public:
  /**
   * Singletons should not be cloneable.
   */
  IMCS_Instance(IMCS_Instance &other) = delete;
  /**
   * Singletons should not be assignable.
   */
  void operator=(const IMCS_Instance &) = delete;
  /**
   * This is the static method that controls the access to the singleton
   * instance. On the first run, it creates a singleton object and places it
   * into the static field. On subsequent runs, it returns the client existing
   * object stored in the static field.
   */
  static Imcs *GetInstance(const std::string &value);

  std::string value() const { return value_; }
};

class Imcs {
 public:
  Imcs(long int mem_size) : mem_size_(mem_size) { initialization(); }
  ~Imcs() { deinitialization(); }

  Imcs(const Imcs &) = delete;
  Imcs &operator=(const Imcs &) = delete;

  int scan(Imcs_context &context);

  // get the mem pool ptr, for new placement.
  void *get_mem_pool() const { return mem_pool_; }
  // gets all first of imcus from beginning.
  std::vector<Imcu *>::iterator getIMCUs() { return imcus_.begin(); }

  // the the imcu at index.
  Imcu *getIMCU(const uint32 index) { return imcus_[index]; }

  void unloadIMCU(const uint32 index) { imcus_[index] = nullptr; }

  void srv_export_tianmu_rapid_status();

  bool initalized() { return inited_; }

  // adds a row into imcs.
  int insert(TianmuSecondaryShare *share, std::vector<Field *> &field_lst,
             size_t primary_key_idx, uint64 *row_id, uint64 trx_id);

  // delete a row
  int remove(TianmuSecondaryShare *share, Field *primary_key_field,
             uint64 trx_id);

 private:
  Imcu *create_imcu(TianmuSecondaryShare *share, uint64 imcu_id);

  // how many size of data use in.
  long int mem_size_;

  IMCS_STATUS initialization();
  IMCS_STATUS deinitialization();

  // the number of imcus.
  // uint32 n_imcus_;

  // all imcus of this imcs.
  std::vector<Imcu *> imcus_;

  // initialized or not.
  bool inited_;

  // tne pool of memory
  void *mem_pool_;

  std::mutex mutex_;
};  // class Imcs

}  // namespace IMCS
}  // namespace Tianmu

#endif
