#include "catalog/catalog.h"

// serialize map by writing map's size then map's every pair of key and value 
template <class T, class V>
uint32_t map_serialize(char *buf, std::map<T, V> mapA){
  uint32_t offset = 0;
  MACH_WRITE_TO(int, buf + offset, mapA.size());
  offset += sizeof(int);
  auto it = mapA.begin();
  auto itEnd = mapA.end();
  while (it != itEnd) {
	  MACH_WRITE_TO(T, buf + offset, it->first);
    offset += sizeof(T);
    MACH_WRITE_TO(V, buf + offset, it->second);
    offset += sizeof(V);
	  it++;
  }
  return offset;
}

// deserialize map by first reading map's size then map's every pair of key and value 
template <class T, class V>
uint32_t map_deserialize(char *buf, std::map<T, V> &mapA){
  uint32_t offset = 0;
  uint32_t size = MACH_READ_FROM(int, buf + offset);
  offset += sizeof(int);
  uint32_t i = 0;
  auto it = mapA.begin();
  while (i != size) {
	  T tempT = MACH_READ_FROM(T, buf + offset);
    offset += sizeof(T);
    V tempV = MACH_READ_FROM(V, buf + offset);
    offset += sizeof(V);
    mapA.emplace(tempT,tempV);
	  it++;
    i++;
  }
  return offset;
}

//get map's serialize size 
template <class T, class V>
uint32_t map_get_serialize_size(std::map<T, V> mapA){
  uint32_t offset = 0;
  offset += sizeof(int);
  uint32_t size = mapA.size();
  offset += size * sizeof(T);
  offset += size * sizeof(V);
  return offset;
}

void CatalogMeta::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  // write magic num
  MACH_WRITE_TO(uint32_t, buf + offset, CATALOG_METADATA_MAGIC_NUM);

  //write maps
  offset += sizeof(uint32_t);
  offset += map_serialize(buf + offset, table_meta_pages_);
  offset += map_serialize(buf + offset, index_meta_pages_);
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  uint32_t offset = 0;
  
  //read magic num
  uint32_t tmp_magic_num = MACH_READ_FROM(uint32_t, buf + offset);
  offset += sizeof(uint32_t); 
  std::map<table_id_t, page_id_t> tmp_table_meta_pages_;
  std::map<index_id_t, page_id_t> tmp_index_meta_pages_;

//deserialize maps
  offset += map_deserialize(buf + offset, tmp_table_meta_pages_);
  offset += map_deserialize(buf + offset, tmp_index_meta_pages_);

  //create catalogmeta
  CatalogMeta* meta = NewInstance(heap);
  std::map<table_id_t, page_id_t> * tmpTableMeta = meta->GetTableMetaPages();
  *tmpTableMeta = tmp_table_meta_pages_;
  std::map<index_id_t, page_id_t> * tmpIndexMeta = meta->GetIndexMetaPages();
  *tmpIndexMeta = tmp_index_meta_pages_;
  return meta;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  return sizeof(uint32_t) + map_get_serialize_size(table_meta_pages_) + map_get_serialize_size(index_meta_pages_);
}

CatalogMeta::CatalogMeta() {}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) 
{
  if(init == true)
  {
    catalog_meta_ = catalog_meta_->NewInstance(heap_);
    next_index_id_ = 1;
    next_table_id_ = 1;
  }
  else
  {
    Page *p = buffer_pool_manager_->FetchPage(META_PAGE_ID);
    catalog_meta_ = catalog_meta_->DeserializeFrom(p->GetData(), heap_);
   
    //buffer_pool_manager_->UnpinPage(META_PAGE_ID, true);
    int i = 0;
    for(auto it:catalog_meta_->table_meta_pages_)
    {
      LoadTable(it.first, it.second);
    }
    for(auto it:catalog_meta_->index_meta_pages_)
    {
      LoadIndex(it.first, it.second);
    }
  }
}

CatalogManager::~CatalogManager() {
  delete heap_;
}

/* CreateTable
 * Input: given table_name and corresponding shcema and null table_info pointer
 * Output: create table_info and return dberr_t
 */ 

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) 
{
  if(table_names_.find(table_name) != table_names_.end())
  {
    return DB_TABLE_ALREADY_EXIST;
  }
  else
  {
    uint32_t this_table_id = next_table_id_.load();
    table_names_[table_name] = this_table_id;

    TableHeap *tp;
    tp = tp->Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_, heap_);
    
    // this page is for specific data in table 
    page_id_t this_table_heap_page = tp->GetFirstPageId();
    TableMetadata *tm;
    tm = tm->Create(this_table_id, table_name, this_table_heap_page, schema, heap_);
    table_info = table_info->Create(heap_);
    table_info->Init(tm, tp);
    tables_[this_table_id] = table_info;
    next_table_id_++;
    
    // initialize corresponding indexes_names_ map for this table
    index_names_[table_name] = {};

    // this page is for table metadata
    page_id_t this_table_page;
    Page *p = buffer_pool_manager_->NewPage(this_table_page);
    catalog_meta_->table_meta_pages_[this_table_id]  = this_table_page;
    tm->SerializeTo(p->GetData());
    buffer_pool_manager_->UnpinPage(this_table_page, true);
  }
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/* GetTable
 * Input: given table_name
 * Output: corresponding table_info and return dberr_t
 */ 

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if(table_names_.find(table_name) == table_names_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  else
  {
    auto it = table_names_.find(table_name);
    table_info = tables_.find(it->second)->second;
  }
  return DB_SUCCESS;
}

/* GetTables
 * Input: None
 * Output: all table_info in this database and return dberr_t
 */ 

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for(auto it:table_names_) //遍历整个table map，输出key及其对应的value值(即tableinfo)
  {
    tables.push_back(tables_.find(it.second)->second);
  }
  return DB_SUCCESS;
}

/* CreateIndex
 * Input: given table_name, index_name, index_keys and index_info pointer for null
 * Output: corresponding index_info and return dberr_t
 */ 

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) 
{
  // Finding corresponding table
  auto it_table = index_names_.find(table_name);
  if(it_table == index_names_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  else
  {
    // Testing the index has existed
    if(it_table->second.find(index_name) != it_table->second.end())
    {
      if(it_table->second.size() != 0)
      {
        return DB_INDEX_ALREADY_EXIST;
      }
    }
    else
    {
      // Real step for create index
      IndexMetadata *im;
      TableInfo *ti = tables_.find(table_names_.find(table_name)->second)->second;
      Schema *sm = ti->GetSchema();
      std::vector<uint32_t> this_key_map_;

      for(int i = 0; i < index_keys.size(); i ++)
      {
        uint32_t col_id;
        // Testing the column has existed
        enum dberr_t flag = sm->GetColumnIndex(index_keys[i], col_id);
        this_key_map_.push_back(col_id);
        if(flag != DB_SUCCESS)
        {
          return flag;
        }
      }
      uint32_t this_index_id = next_index_id_.load();
      it_table->second[index_name] = this_index_id;
      im = IndexMetadata::Create(this_index_id, index_name, table_names_.find(table_name)->second, this_key_map_, heap_);
      index_info = index_info->Create(heap_);
      index_info->Init(im, ti, buffer_pool_manager_);
      indexes_[this_index_id] = index_info;
      next_index_id_++;
      // This page for index metadata
      page_id_t this_index_page;
      Page *p = buffer_pool_manager_->NewPage(this_index_page);
      catalog_meta_->index_meta_pages_[this_index_id]  = this_index_page;

      im->SerializeTo(p->GetData());
      buffer_pool_manager_->UnpinPage(this_index_page, true);
    }
  }
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/* CetIndex
 * Input: given table_name, index_name and index_info pointer for null
 * Output: corresponding index_info and return dberr_t
 */ 

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const 
{
  auto it_table = index_names_.find(table_name);
  if(it_table == index_names_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  else
  {
    if(it_table->second.find(index_name) == it_table->second.end())
    {
      return DB_INDEX_NOT_FOUND;
    }
    else
    {
      index_info = indexes_.find(it_table->second.find(index_name)->second)->second;
    }
  }
  return DB_SUCCESS;
}

/* CetIndex
 * Input: given table_name and vector of index_info pointer
 * Output: corresponding all index_info in this table and return dberr_t
 */ 

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const
{
  auto it_table = index_names_.find(table_name);
  if(it_table == index_names_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  else
  {
    for(auto it:it_table->second) //遍历整个index map，输出key及其对应的value值(即indexinfo)
    {
      indexes.push_back(indexes_.find(it.second)->second);
    }
  }
  return DB_SUCCESS;
}


/* DropTable
 * Input: given table_name
 * Output: return dberr_t
 * Role: clear related data for this table in catlaog level 
 *       including indexes in this table
 *       Do not consider specific data in this table
 */ 

dberr_t CatalogManager::DropTable(const string &table_name)
{
  auto it_table = table_names_.find(table_name);
  if(it_table == table_names_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  else
  {
    auto it_table_all_indexes = index_names_.find(table_name)->second;
    //遍历该table对应的indexes map，并调用dropindex
    for(auto x:it_table_all_indexes)
    {
      DropIndex(table_name, x.first);
    }
    index_names_.erase(table_name);

    // 对应map进行删除即可
    table_id_t drop_table_id = it_table->second;
    delete tables_.find(drop_table_id)->second;
    tables_.erase(drop_table_id);
    table_names_.erase(table_name);

    buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_.find(it_table->second)->second);
    catalog_meta_->table_meta_pages_.erase(drop_table_id);
  }
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/* DropIndex
 * Input: given table_name and index_name
 * Output: return dberr_t
 * Role: clear related data for this index in catlaog level 
 *       Do not consider specific index data in it(mission for b+ tree)
 */ 

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name)
{
  auto it_table = index_names_.find(table_name);
  if(it_table == index_names_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  else
  {
    if(it_table->second.find(index_name) == it_table->second.end())
    {
      return DB_INDEX_NOT_FOUND;
    }
    else
    {
      auto it_table_index = it_table->second.find(index_name);
      index_id_t drop_index_id = it_table_index->second;

      //indexes_.find(drop_index_id)->second->GetIndex()->Destroy();
      delete indexes_.find(drop_index_id)->second;
      indexes_.erase(drop_index_id);
      it_table->second.erase(index_name);

      buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_.find(drop_index_id)->second);
      catalog_meta_->index_meta_pages_.erase(drop_index_id);
    }
  }
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/* FlushCatalogMetaPage
 * Role: FlushMetaPage when update table and indexes
 */ 
dberr_t CatalogManager::FlushCatalogMetaPage() const
{
  Page *p = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(p->GetData());
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);    
  return DB_SUCCESS;
}

/* LoadTable
 * Input: table_id and corresponding page_id for metadata 
 * Role: tablemetadata deserialize from given page_id and push_back map
 *       tableheap create from recovered tablemetadata which has heap root_page_id information
 */ 

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id)
{
  Page *p = buffer_pool_manager_->FetchPage(page_id);
  TableMetadata *tm;
  tm->DeserializeFrom(p->GetData(), tm, heap_);


  TableHeap *th;
  th = th->Create(buffer_pool_manager_, tm->GetFirstPageId(), tm->GetSchema(), log_manager_, lock_manager_, heap_);

  TableInfo *ti;
  ti = ti->Create(heap_);
  ti->Init(tm, th);
  table_names_[tm->GetTableName()] = table_id;
  tables_[table_id] = ti;
  index_names_[tm->GetTableName()] = {};
  return DB_SUCCESS;
}

/* LoadIndex
 * Input: index_id and corresponding page_id for metadata 
 * Role: indexmetadata deserialize from given page_id and push_back map
 *       index create from recovered indexmetadata which has all nformation it need
 */ 

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id)
{
  Page *p = buffer_pool_manager_->FetchPage(page_id);
  IndexMetadata *imd;
  imd->DeserializeFrom(p->GetData(), imd, heap_);

  IndexInfo *in  = IndexInfo::Create(heap_);
  TableInfo *ti;
  GetTable(imd->GetTableId(), ti);
  in->Init(imd, ti, buffer_pool_manager_);
  index_names_.find(ti->GetTableName())->second[imd->GetIndexName()] = index_id;
  indexes_[index_id] = in;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info)
{
  if(tables_.find(table_id) == tables_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  else
  {
    table_info = tables_.find(table_id)->second;
  }
  return DB_SUCCESS;
}
