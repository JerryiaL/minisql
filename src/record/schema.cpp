#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  int32_t offset = 0;
  // write magic_num first
  MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM);
  offset += sizeof(uint32_t) / sizeof(char);

  // write column size of the schema
  uint32_t column_len = columns_.size();
  MACH_WRITE_UINT32(buf + offset, column_len);
  offset += sizeof(uint32_t) / sizeof(char);

  // serialize column in the schema(pointer address and column contents)
  for(auto it = columns_.begin(); it != columns_.end(); it++)
  {
    // write value of the column
    (*it)->SerializeTo(buf + offset);
    offset += (*it)->GetSerializedSize();
  }
  // replace with your code here
  return static_cast<uint32_t>(offset);
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size = 0;
  size += sizeof(uint32_t) / sizeof(char);
  for(auto it = columns_.begin(); it != columns_.end(); it++)
  {
    size += (*it)->GetSerializedSize();
  }
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  // replace with your code here
if (schema != nullptr) {
    LOG(WARNING) << "Pointer to shcema is not null in schema deserialize." << std::endl;
  }
  /* deserialize field from buf */
  int32_t offset = 0;
  uint32_t magic_num = MACH_READ_UINT32(buf);
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "This buf do not store schema data."); //ensure this buf read schema data
  offset += sizeof(uint32_t) / sizeof(char);

  uint32_t column_len = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t) / sizeof(char);

  std::vector<Column *>cols(column_len);

  for(uint32_t i = 0; i < column_len; i++)
  {
    cols[i]->DeserializeFrom(buf + offset, cols[i], heap);
    offset += cols[i]->GetSerializedSize();
  }
  void *mem = heap->Allocate(sizeof(Schema));
  schema = new(mem) Schema(cols);

  return static_cast<uint32_t>(offset);
}
