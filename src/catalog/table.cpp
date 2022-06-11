#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  MACH_WRITE_TO(uint32_t, buf + offset, TABLE_METADATA_MAGIC_NUM);
  offset += sizeof(uint32_t);

  MACH_WRITE_TO(table_id_t, buf + offset, table_id_);
  offset += sizeof(table_id_t);

  MACH_WRITE_TO(uint32_t, buf + offset, table_name_.size());
  offset += sizeof(uint32_t);

  for(uint32_t i = 0; i < table_name_.size(); i++){
    MACH_WRITE_TO(char, buf + offset, table_name_[i]);
    offset += sizeof(char);
  }

  MACH_WRITE_TO(page_id_t, buf + offset, root_page_id_);
  offset += sizeof(page_id_t);

  offset += schema_->SerializeTo(buf+offset);

  return offset;
}

uint32_t TableMetadata::GetSerializedSize() const {
  return sizeof(table_id_t) + sizeof(page_id_t) + sizeof(uint32_t) * 2
  + table_name_.size() * sizeof(char) + schema_->GetSerializedSize();
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  uint32_t offset = 0;
  uint32_t temp_magic_num = MACH_READ_FROM(uint32_t, buf + offset);
  offset += sizeof(uint32_t);

  //table_meta = ALLOC_P(heap, TableMetadata);
  table_id_t temp_table_id_ = MACH_READ_FROM(table_id_t, buf + offset);
  offset += sizeof(table_id_t);

  uint32_t size = MACH_READ_FROM(uint32_t, buf+offset);
  offset += sizeof(uint32_t);
  std::string temp_table_name_;
  for(uint32_t i = 0; i < size; i++){
    temp_table_name_ = temp_table_name_ + MACH_READ_FROM(char, buf+offset);
    offset += sizeof(char);
  }

  page_id_t temp_root_page_id_ = MACH_READ_FROM(page_id_t, buf + offset);
  offset += sizeof(page_id_t);

  Schema *schema = nullptr;
  offset += schema->DeserializeFrom(buf + offset, schema, heap);

  void *mem = heap->Allocate(sizeof(TableMetadata));
  table_meta = new(mem)TableMetadata(temp_table_id_, temp_table_name_, temp_root_page_id_, schema);

  return offset;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
