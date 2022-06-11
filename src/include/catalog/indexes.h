#ifndef MINISQL_INDEXES_H
#define MINISQL_INDEXES_H

#include <memory>

#include "catalog/table.h"
#include "index/generic_key.h"
#include "index/b_plus_tree_index.h"
#include "record/schema.h"

class IndexMetadata {
  friend class IndexInfo;

public:
  static IndexMetadata *Create(const index_id_t index_id, const std::string &index_name,
                               const table_id_t table_id, const std::vector<uint32_t> &key_map,
                               MemHeap *heap);

  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap);

  inline std::string GetIndexName() const { return index_name_; }

  inline table_id_t GetTableId() const { return table_id_; }

  uint32_t GetIndexColumnCount() const { return key_map_.size(); }

  inline const std::vector<uint32_t> &GetKeyMapping() const { return key_map_; }

  inline index_id_t GetIndexId() const { return index_id_; }

private:
  IndexMetadata() = delete;

  explicit IndexMetadata(const index_id_t index_id, const std::string &index_name,
                         const table_id_t table_id, const std::vector<uint32_t> &key_map)
  {
    index_id_ = index_id;
    index_name_ = index_name;
    table_id_ = table_id;
    key_map_ = key_map;
  }

private:
  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
  index_id_t index_id_;
  std::string index_name_;
  table_id_t table_id_;
  std::vector<uint32_t> key_map_;  /** The mapping of index key to tuple key */
};

/**
 * The IndexInfo class maintains metadata about a index.
 */
class IndexInfo {
public:
  static IndexInfo *Create(MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(IndexInfo));
    return new(buf)IndexInfo();
  }

  ~IndexInfo() {
    delete heap_;
  }

  void Init(IndexMetadata *meta_data, TableInfo *table_info, BufferPoolManager *buffer_pool_manager) {
    // Step1: init index metadata and table info
    this->meta_data_ = meta_data;
    this->table_info_ = table_info;
    // Step2: mapping index key to key schema
    
    /* Not sure*/
    key_schema_ = key_schema_->ShallowCopySchema(table_info_->GetSchema(), meta_data_->key_map_, heap_);
    LOG(INFO) << "key_schema_shallow copy is ok";
    LOG(INFO) << "shallow copy result:" << key_schema_->GetColumnCount();
    // Step3: call CreateIndex to create the index
    index_ = CreateIndex(buffer_pool_manager);
    LOG(INFO) << "Create index is ok";
    //ASSERT(false, "Not Implemented yet.");
  }

  inline Index *GetIndex() { return index_; }

  inline std::string GetIndexName() { return meta_data_->GetIndexName(); }

  inline IndexSchema *GetIndexKeySchema() { return key_schema_; }

  inline MemHeap *GetMemHeap() const { return heap_; }

  inline TableInfo *GetTableInfo() const { return table_info_; }

private:
  explicit IndexInfo() : meta_data_{nullptr}, index_{nullptr}, table_info_{nullptr},
                         key_schema_{nullptr}, heap_(new SimpleMemHeap()) {}

  Index *CreateIndex(BufferPoolManager *buffer_pool_manager) {
    /* not sure what to do
     * just have a try 
    */
    Index *ind = nullptr;
    uint32_t index_total_len = 12; //for rowid and count
    for(uint32_t i = 0; i < key_schema_->GetColumnCount(); i++)
    {
      index_total_len += 1;
      index_total_len += key_schema_->GetColumn(i)->GetLength();
    }
    if(index_total_len <= 4)
    {
      ind = new BPlusTreeIndex<GenericKey<4>,RowId,GenericComparator<4>>(meta_data_->GetIndexId() ,key_schema_, buffer_pool_manager);
    }
    else if(index_total_len <= 8)
    {
      ind = new BPlusTreeIndex<GenericKey<8>,RowId,GenericComparator<8>>(meta_data_->GetIndexId() ,key_schema_, buffer_pool_manager);
    }
    else if(index_total_len <= 16)
    {
      ind = new BPlusTreeIndex<GenericKey<16>,RowId,GenericComparator<16>>(meta_data_->GetIndexId() ,key_schema_, buffer_pool_manager);
    }
    else if(index_total_len <= 32)
    {
      ind = new BPlusTreeIndex<GenericKey<32>,RowId,GenericComparator<32>>(meta_data_->GetIndexId() ,key_schema_, buffer_pool_manager);
    }
    else
    {
      ind = new BPlusTreeIndex<GenericKey<64>,RowId,GenericComparator<64>>(meta_data_->GetIndexId() ,key_schema_, buffer_pool_manager);
    }
    return ind;
  }

private:
  IndexMetadata *meta_data_;
  Index *index_;
  TableInfo *table_info_;
  IndexSchema *key_schema_;
  MemHeap *heap_;
};

#endif //MINISQL_INDEXES_H
