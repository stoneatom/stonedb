////
//// Created by dfx on 22-12-27.
////
//
// #ifndef MYSQL_STORAGE_TIANMU_CORE_RECORD_ENCODE_H_
// #define MYSQL_STORAGE_TIANMU_CORE_RECORD_ENCODE_H_
//
// #include <field.h>
// #include "common/common_definitions.h"
// #include "string"
// #include "util/bitset.h"
// #include "vector"
//
// namespace Tianmu {
// namespace core {
//
////enum class RecordType { RecordType_min, kSchema, kInsert, kUpdate, kDelete, RecordType_max };
//
// class Record {
// public:
//  Record(RecordType type, int32_t table_id, const std::string &table_path)
//      : type_(type), table_id_(table_id), table_path_(table_path) {}
//  virtual void Parse(const std::string &str) = 0;
//  virtual std::string Encode() const = 0;
//
// private:
//  RecordType type_;
//  int32_t table_id_;
//  std::string table_path_;
//};
//
// class InsertRecord : public Record {
// public:
// public:
//  InsertRecord() = default;
////  InsertRecord(int table_id, const std::string &table_path, Field **field, size_t field_size, size_t blobs)
////      : Record(RecordType::kInsert, table_id, table_path), field_size_(field_size) {
////    // init
////    for (uint i = 0; i < field_size_; i++) {
////      Field *f = field[i];
////      size_t length;
////      if (f->flags & BLOB_FLAG)
////        length = dynamic_cast<Field_blob *>(f)->get_length();
////      else
////        length = f->row_pack_length();
////      length += 8;
////
////      if (f->is_null()) {
////        null_mask_.set(i);
////        continue;
////      }
////    }
////  }
//  ~InsertRecord() = default;
//  void Parse(const std::string &str) override;
//  std::string Encode() const override;
//
// private:
//  utils::BitSet null_mask_;
//  size_t field_size_;
//  std::unordered_map<std::string, int> fields_;
//};
//
// class DeleteRecord : public Record {
// public:
//  void Parse(const std::string &str) override;
//  std::string Encode() const override;
//};
//
// class UpdateRecord : public Record {
// public:
//  void Parse(const std::string &str) override;
//  std::string Encode() const override;
//};
//
//}  // namespace core
//}  // namespace Tianmu
//
// #endif  // MYSQL_STORAGE_TIANMU_CORE_RECORD_ENCODE_H_
