#include "record/column.h"
#include "glog/logging.h"

void InitGoogleLog(char *argv) {
  FLAGS_logtostderr = true;
  FLAGS_colorlogtostderr = true;
  google::InitGoogleLogging(argv);
}

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index),
          nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt :
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat :
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), len_(length),
          table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
                                      table_ind_(other->table_ind_), nullable_(other->nullable_),
                                      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  //count the offset
  int32_t offset = 0;

  //write magic_num
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
  offset += sizeof(COLUMN_MAGIC_NUM) / sizeof(char);
  //write name, type, length, tableindex, isnull, isunique in turn.
  MACH_WRITE_INT32(buf + offset, name_.length());
  MACH_WRITE_STRING(buf + offset + 4, name_);
  offset += MACH_STR_SERIALIZED_SIZE(name_);

  MACH_WRITE_INT32(buf + offset, type_);
  offset += sizeof(int32_t) / sizeof(char);

  MACH_WRITE_UINT32(buf + offset, len_);
  offset += sizeof(uint32_t) / sizeof(char);

  MACH_WRITE_UINT32(buf + offset, table_ind_);
  offset += sizeof(uint32_t) / sizeof(char);

  MACH_WRITE_INT32(buf + offset, nullable_);
  offset += sizeof(int32_t) / sizeof(char);

  MACH_WRITE_INT32(buf + offset, unique_);
  offset += sizeof(int32_t) / sizeof(char);

  return static_cast<uint32_t>(offset);
}

uint32_t Column::GetSerializedSize() const {
  uint32_t size = 0;
  // string name length and the record of its length
  size += MACH_STR_SERIALIZED_SIZE(name_);

  // signed int: type_, nullable_, unique_
  // unsigned int: magic_num, len_, table_ind_
  size += (sizeof(int32_t) / sizeof(char)) * 3 + (sizeof(uint32_t) / sizeof(char)) * 3;
  return size;
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  if (column != nullptr) {
    LOG(WARNING) << "Pointer to column is not null in column deserialize." << std::endl;
  }
  /* deserialize field from buf */
  int32_t offset = 0;
  uint32_t magic_num = MACH_READ_UINT32(buf);
  ASSERT(magic_num == COLUMN_MAGIC_NUM, "This buf do not store column data."); //ensure this buf read column data
  offset += sizeof(uint32_t) / sizeof(char);

  int32_t name_len = MACH_READ_INT32(buf + offset);
  offset += sizeof(int32_t) / sizeof(char);

  std::string column_name = "";
  column_name.append(buf+offset, name_len);
  offset += name_len;

  int32_t type_int = MACH_READ_INT32(buf + offset);
  enum TypeId type;
  type = static_cast<enum TypeId>(type_int);
  offset += sizeof(int32_t) / sizeof(char);

  uint32_t length = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t) / sizeof(char);

  uint32_t col_ind = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t) / sizeof(char);

  bool nullable = MACH_READ_INT32(buf + offset);
  offset += sizeof(int32_t) / sizeof(char);

  bool unique = MACH_READ_INT32(buf + offset);
  offset += sizeof(int32_t) / sizeof(char);
  // can be replaced by: 
  //		ALLOC_P(heap, Column)(column_name, type, col_ind, nullable, unique);
  void *mem = heap->Allocate(sizeof(Column));
  if(type == TypeId::kTypeChar)
  {
    column = new(mem)Column(column_name, type, length, col_ind, nullable, unique);
  }
  else
  {
    column = new(mem)Column(column_name, type, col_ind, nullable, unique);
  }


  return static_cast<uint32_t>(offset);
}
