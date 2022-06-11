#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  MACH_WRITE_TO(uint32_t, buf + offset, INDEX_METADATA_MAGIC_NUM);
  offset += sizeof(uint32_t);

  MACH_WRITE_TO(index_id_t, buf + offset, index_id_);
  offset += sizeof(index_id_t);

  MACH_WRITE_TO(uint32_t, buf + offset, index_name_.size());
  offset += sizeof(uint32_t);

  for(uint32_t i = 0; i < index_name_.size(); i++){
    MACH_WRITE_TO(char, buf + offset, index_name_[i]);
    offset += sizeof(char);
  }

  MACH_WRITE_TO(table_id_t, buf + offset, table_id_);
  offset += sizeof(table_id_t);

  MACH_WRITE_TO(uint32_t, buf + offset, key_map_.size());
  offset += sizeof(uint32_t);

  for(uint32_t i = 0; i < key_map_.size(); i++){
    MACH_WRITE_TO(uint32_t, buf + offset, key_map_[i]);
    offset += sizeof(uint32_t);
  }
  return offset;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  return  sizeof(char) * index_name_.size() + sizeof(uint32_t) * (3 + key_map_.size()) + sizeof(index_id_t) + sizeof(table_id_t);
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  uint32_t offset = 0;
  uint32_t temp_magic_num = MACH_READ_FROM(uint32_t, buf+offset);
  offset += sizeof(uint32_t);

  //index_meta = ALLOC_P(heap, IndexMetadata);
  index_id_t temp_index_id_ = MACH_READ_FROM(index_id_t, buf+offset);
  offset += sizeof(index_id_t);

  uint32_t size = MACH_READ_FROM(uint32_t, buf+offset);
  offset += sizeof(uint32_t);
  std::string temp_index_name_;
  for(uint32_t i = 0; i < size; i++){
    temp_index_name_ = temp_index_name_ + MACH_READ_FROM(char, buf+offset);
    offset += sizeof(char);
  }

  table_id_t temp_table_id_ = MACH_READ_FROM(table_id_t, buf+offset);
  offset += sizeof(table_id_t);

  size = MACH_READ_FROM(uint32_t, buf+offset);
  offset += sizeof(uint32_t);
  std::vector<uint32_t> temp_key_map_;
  for(uint32_t i = 0; i < size; i++){
    uint32_t tempRead = MACH_READ_FROM(uint32_t, buf+offset);
    temp_key_map_.push_back(tempRead);
    offset += sizeof(uint32_t);
  }

  void *mem = heap->Allocate(sizeof(IndexMetadata));
  index_meta = new(mem)IndexMetadata(temp_index_id_, temp_index_name_, temp_table_id_, temp_key_map_);
  return offset;
}
