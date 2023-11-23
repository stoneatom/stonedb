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
   The compression algorithm basic class. This class is used for encapsulating
   all the compression algorithm, to provide an uniform access method.

   Created 7/2/2023 Hom.LEE
 */

#ifndef __COMPESSION_H__
#define __COMPESSION_H__

#include <cstdarg>         //for vargs.

#include "../imcs_base.h"  //for the basic data types

#include "arith_coder.h"
#include "data_stream.h"

namespace Tianmu {
namespace IMCS {

namespace Compression {

//the base class for compression algr. To provide an uniform access method.
//TODO:
class Compress_algorithm {
public:
  Compress_algorithm() : type_(COMPRESS_TYPE::ZLIB) {}
  Compress_algorithm(COMPRESS_TYPE type) : type_(type) {}
  virtual ~Compress_algorithm() {}

  virtual void Init() = 0;
  virtual void Compress() = 0;
  virtual void Decompress() = 0;

  //gets the compression type.
  COMPRESS_TYPE type() { return type_; }
private:
  COMPRESS_TYPE type_;
};


//to encapsulate zlib compression algr.
class Zlib : public Compress_algorithm {
public:
  Zlib () : Compress_algorithm(COMPRESS_TYPE::ZLIB) {}
  virtual ~Zlib() {}

  void Init() final;
  void Compress() final;
  void Decompress() final;
};

//to encapsulate zstd compression algr.
class Zstd : public Compress_algorithm {
public: 
  Zstd() : Compress_algorithm(COMPRESS_TYPE::ZSTD) {}
  virtual ~Zstd() {}

  void Init() final;
  void Compress() final;
  void Decompress() final;
};


} // namespace Compression
}  //namespace IMCS
}  //namespace Tianmu


#endif //__COMPESSION_H__
