#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  uint32_t count = GetFieldCount();
  // offset record fixed length to write in buf
  uint32_t offset = 0;

  MACH_WRITE_TO(RowId, buf + offset, GetRowId());
  offset += sizeof(RowId);

  MACH_WRITE_TO(uint32_t, buf + offset, count);
  offset += sizeof(uint32_t);
  

  // offset_field record dynamic length to field write in buf
  uint32_t offset_field = 0;
  for(uint32_t i = 0; i < count; i++){
    Field* f = GetField(i);
    // null must write the type
    if(f->IsNull())
    {
      MACH_WRITE_TO(bool, buf + offset + i, 1);
      const TypeId type_id = (*(schema->GetColumn(i))).GetType();
      // tyep_id for length 4, I optimize it to a char(length 1) for each type
      if(type_id == kTypeInt)
      {
        MACH_WRITE_TO(char, buf + offset + count * sizeof(bool) + offset_field, '1');
      }
      else if (type_id == kTypeFloat)
      {
        MACH_WRITE_TO(char, buf + offset + count * sizeof(bool) + offset_field, '2');
      }
      else
      {
        MACH_WRITE_TO(char, buf + offset + count * sizeof(bool) + offset_field, '3');
      }
      offset_field += 1;
    }
    else
    {
      MACH_WRITE_TO(bool, buf + offset + i, 0);
      const TypeId type_id = (*(schema->GetColumn(i))).GetType();
      if(type_id == kTypeInt)
      {
        MACH_WRITE_TO(char, buf + offset + count * sizeof(bool) + offset_field, '1');
      }
      else if (type_id == kTypeFloat)
      {
        MACH_WRITE_TO(char, buf + offset + count * sizeof(bool) + offset_field, '2');
      }
      else
      {
        MACH_WRITE_TO(char, buf + offset + count * sizeof(bool) + offset_field, '3');
      }
      offset_field += 1;
      offset_field += (*f).SerializeTo(buf + offset + count * sizeof(bool) + offset_field);
    }
  }
  offset = offset + count * sizeof(bool) + offset_field;
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  uint32_t offset = 0;
  fields_.clear();

  SetRowId(MACH_READ_FROM(RowId, buf + offset));

  // offset record fixed length to read in buf
  offset += sizeof(RowId);
  uint32_t count = MACH_READ_FROM(uint32_t, buf + offset);
  offset += sizeof(uint32_t);

  // offset_field record dynamic length to field write in buf
  uint32_t offset_field = 0;
  std::vector<Field* > tempfields;
  for(size_t i = 0; i < count; i++)
  {
    bool tempB = MACH_READ_FROM(bool, buf + offset + i);
    Field *tempf;
    char type = MACH_READ_FROM(char, buf + offset + count * sizeof(bool) + offset_field);
    TypeId this_type;
    // one-to-one correspond from char to TypeId
    if(type == '1')
    {
      this_type = kTypeInt;
    }
    else if(type == '2')
    {
      this_type = kTypeFloat;
    }
    else
    {
      this_type = kTypeChar;
    }
    offset_field += 1;
    offset_field += Field::DeserializeFrom(buf + offset + count * sizeof(bool) + offset_field, this_type, &tempf, tempB, heap_);
    tempfields.push_back(tempf);
  }

  fields_ = tempfields;
  offset = offset + count * sizeof(bool) + offset_field;

  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  uint32_t count = GetFieldCount();
  uint32_t offset = sizeof(RowId) + sizeof(uint32_t) + count * sizeof(bool);
  for(size_t i = 0; i < count; i++){
    Field* f = GetField(i);
    offset += (*f).GetSerializedSize() + sizeof(char);
  }
  return offset;
}
