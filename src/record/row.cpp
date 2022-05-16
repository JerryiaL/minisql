#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  // should be magic num here?
  size_t count = GetFieldCount();
  uint32_t offset = 0;

  MACH_WRITE_TO(RowId, buf + offset, GetRowId());
  offset += sizeof(RowId);

  MACH_WRITE_TO(size_t, buf + offset, count);
  offset += sizeof(size_t);

  for(size_t i = 0; i < count; i++){
    Field* f = GetField(i);
    if(f == NULL)MACH_WRITE_TO(bool, buf + offset, 0);
    else MACH_WRITE_TO(bool, buf + offset, 1);
    offset += sizeof(bool);
  }

  for(size_t i = 0; i < count; i++){
    Field* f = GetField(i);
    if(f == NULL)continue;
    const TypeId type_id = (*(schema->GetColumn(i))).GetType();
    MACH_WRITE_TO(TypeId, buf + offset, type_id);
    offset += sizeof(TypeId);

    offset += (*f).SerializeTo(buf+offset);
  }

  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  uint32_t offset = 0;
  fields_.clear();

  SetRowId(MACH_READ_FROM(RowId, buf + offset));
  offset += sizeof(RowId);
  size_t count = MACH_READ_FROM(size_t, buf + offset);
  offset += sizeof(size_t);
  std::vector<bool> NULLV;
  std::vector<Field* > tempfields;
  for(size_t i = 0; i < count; i++){
    bool tempB = MACH_READ_FROM(bool, buf + offset);
    offset += sizeof(bool);
    NULLV.push_back(tempB);
  }

  for(size_t i = 0; i < count; i++){
    if(NULLV[i] == false)tempfields.push_back(NULL);
    else{
      TypeId type_id = MACH_READ_FROM(TypeId, buf + offset);
      offset += sizeof(TypeId);
      Field *tempf;
      offset += (*tempf).DeserializeFrom(buf + offset, type_id, &tempf, 0, heap_);
      tempfields.push_back(tempf);
    }
  }

  fields_ = tempfields;
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  size_t count = GetFieldCount();
  uint32_t offset = sizeof(RowId) + sizeof(size_t) + count*sizeof(bool);
  for(size_t i = 0; i < count; i++){
    Field* f = GetField(i);
    if(f == NULL)continue;
    offset += (*f).GetSerializedSize() + sizeof(TypeId);
  }
  return offset;
}
