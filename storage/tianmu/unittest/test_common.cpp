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

#include <iostream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "common/common_definitions.h"

using namespace std;
using namespace Tianmu;
using namespace Tianmu::common;

class TianmuCommon : public testing::Test {
 protected:
  virtual void SetUp() { cout << "Test TianmuCommon SetUp" << endl; };

  virtual void TearDown() { cout << "Test TianmuCommon SetUp" << endl; };
};

TEST_F(TianmuCommon, TX_ID) {
  // default UINT64_MAX
  TX_ID tx_id_default = TX_ID();
  TX_ID tx_id_UINT64_MAX = TX_ID(18446744073709551615);
  TX_ID tx_id1 = TX_ID(1000);
  TX_ID tx_id2 = TX_ID(2000);

  // test operator<
  EXPECT_TRUE(tx_id1 < tx_id2);
  EXPECT_FALSE(tx_id_default < tx_id2);

  // false, should be equal
  cout << UINT64_MAX << endl;
  EXPECT_FALSE(tx_id_default < tx_id_UINT64_MAX);

  // Do it in the future.
  // auto str = tx_id1.ToString();
  // cout << "tx_id1: " << str << endl;
}

TEST_F(TianmuCommon, double_int_t) {
  // test double
  double_int_t d_value((double)5.5);
  EXPECT_EQ(5.5, d_value.d);

  // test int64_t
  double_int_t i_value((int64_t)88);
  EXPECT_EQ((int64_t)88, i_value.i);
}

TEST_F(TianmuCommon, Tribool) {
  // test default
  Tribool t_default;
  EXPECT_TRUE(t_default == TRIBOOL_UNKNOWN);

  // test true
  Tribool tri_true1(true);
  Tribool tri_true2(true);
  EXPECT_TRUE(tri_true1 != t_default);
  EXPECT_TRUE(tri_true1 == Tribool::And(tri_true1, tri_true2));
  EXPECT_TRUE(tri_true1 == Tribool::Or(tri_true1, tri_true2));

  // test false
  Tribool tri_false1(false);
  Tribool tri_false2(false);
  EXPECT_TRUE(tri_true1 != t_default);
  EXPECT_TRUE(tri_false1 == Tribool::And(tri_false1, tri_false2));
  EXPECT_TRUE(tri_false1 == Tribool::Or(tri_false1, tri_false2));
  EXPECT_TRUE(tri_true1 == !tri_false1);
}

TEST_F(TianmuCommon, get_enum_field_types_name) {
  EXPECT_EQ("decimal", get_enum_field_types_name(MYSQL_TYPE_DECIMAL));
  EXPECT_EQ("tiny", get_enum_field_types_name(MYSQL_TYPE_TINY));
  EXPECT_EQ("short", get_enum_field_types_name(MYSQL_TYPE_SHORT));
  EXPECT_EQ("long", get_enum_field_types_name(MYSQL_TYPE_LONG));
  EXPECT_EQ("float", get_enum_field_types_name(MYSQL_TYPE_FLOAT));
  EXPECT_EQ("double", get_enum_field_types_name(MYSQL_TYPE_DOUBLE));
  EXPECT_EQ("null", get_enum_field_types_name(MYSQL_TYPE_NULL));
  EXPECT_EQ("timestamp", get_enum_field_types_name(MYSQL_TYPE_TIMESTAMP));
  EXPECT_EQ("longlong", get_enum_field_types_name(MYSQL_TYPE_LONGLONG));
  EXPECT_EQ("int24", get_enum_field_types_name(MYSQL_TYPE_INT24));
  EXPECT_EQ("date", get_enum_field_types_name(MYSQL_TYPE_DATE));
  EXPECT_EQ("time", get_enum_field_types_name(MYSQL_TYPE_TIME));
  EXPECT_EQ("datetime", get_enum_field_types_name(MYSQL_TYPE_DATETIME));
  EXPECT_EQ("year", get_enum_field_types_name(MYSQL_TYPE_YEAR));
  EXPECT_EQ("newdate", get_enum_field_types_name(MYSQL_TYPE_NEWDATE));
  EXPECT_EQ("varchar", get_enum_field_types_name(MYSQL_TYPE_VARCHAR));
  EXPECT_EQ("bit", get_enum_field_types_name(MYSQL_TYPE_BIT));
  EXPECT_EQ("timestamp2", get_enum_field_types_name(MYSQL_TYPE_TIMESTAMP2));
  EXPECT_EQ("datetime2", get_enum_field_types_name(MYSQL_TYPE_DATETIME2));
  EXPECT_EQ("time2", get_enum_field_types_name(MYSQL_TYPE_TIME2));
  EXPECT_EQ("json", get_enum_field_types_name(MYSQL_TYPE_JSON));
  EXPECT_EQ("newdecimal", get_enum_field_types_name(MYSQL_TYPE_NEWDECIMAL));
  EXPECT_EQ("enum", get_enum_field_types_name(MYSQL_TYPE_ENUM));
  EXPECT_EQ("set", get_enum_field_types_name(MYSQL_TYPE_SET));
  EXPECT_EQ("tiny_blob", get_enum_field_types_name(MYSQL_TYPE_TINY_BLOB));
  EXPECT_EQ("medium_blob", get_enum_field_types_name(MYSQL_TYPE_MEDIUM_BLOB));
  EXPECT_EQ("long_blob", get_enum_field_types_name(MYSQL_TYPE_LONG_BLOB));
  EXPECT_EQ("blob", get_enum_field_types_name(MYSQL_TYPE_BLOB));
  EXPECT_EQ("var_string", get_enum_field_types_name(MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ("string", get_enum_field_types_name(MYSQL_TYPE_STRING));
  EXPECT_EQ("geometry", get_enum_field_types_name(MYSQL_TYPE_GEOMETRY));
  EXPECT_EQ("unkonwn type", get_enum_field_types_name(888));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
