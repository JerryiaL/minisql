#include "executor/execute_engine.h"
#include "glog/logging.h"
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"
#include <algorithm>
#include <fstream>
#include <time.h>
#include <chrono>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h> 
extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"

}
void getAllDatabase(string path, vector<string>& files) 
{
  DIR *dp; //创建一个指向root路径下每个文件的指针
  struct dirent *dirp;
  if((dp = opendir(path.c_str()))==NULL)
  {
    mkdir(path.c_str(),S_IRUSR | S_IWUSR | S_IXUSR | S_IRWXG | S_IRWXO);
    return;
  }
  int i = 0;
  while((dirp = readdir(dp)) != NULL){
    i++;
    if (
      (dirp->d_name[0] == '.' && dirp->d_name[1] == '\0') || 
      (dirp->d_name[0] == '.' && dirp->d_name[1] == '.' && dirp->d_name[2] == '\0')
    )
      continue;
    else 
      files.push_back(dirp->d_name);
  }
}

extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"

}
ExecuteEngine::ExecuteEngine() 
{
  //从文件中加载所有已存在的Database
  vector<string> dbname;
  getAllDatabase("./database/", dbname);
  for (size_t i = 0; i < dbname.size(); i++) {
    DBStorageEngine * newdb = new DBStorageEngine("./database/"+dbname[i], false);
    dbs_.insert({dbname[i], newdb});
  }
}


bool RowId_compare(RowId x, RowId y){
  //对RowId进行比较
  if(x.GetPageId() < y.GetPageId())return true;
  else if(x.GetPageId() == y.GetPageId()){
    if(x.GetSlotNum() < y.GetSlotNum())return true;
  }
  return false;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
  std::chrono::high_resolution_clock::time_point beginTime = std::chrono::high_resolution_clock::now();
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  pSyntaxNode NodePointer = ast;
  NodePointer = NodePointer->child_;
  string db_name = (string)NodePointer->val_;

  // 检查数据库名是否被占用
  if(dbs_.find(db_name) != dbs_.end()) {
    cout<<"This Database has already existed."<<endl;
    return DB_FAILED;
  }

  // 创建新数据库
  DBStorageEngine * newdb = new DBStorageEngine("./database/"+db_name);
  dbs_.insert({db_name, newdb});
  current_db_ = db_name;

  std::chrono::high_resolution_clock::time_point endTime = std::chrono::high_resolution_clock::now();
  std::chrono::microseconds timeInterval = std::chrono::duration_cast <std::chrono::microseconds>(endTime - beginTime);
  std::cout << "Time: " << timeInterval.count() << "us" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
  std::chrono::high_resolution_clock::time_point beginTime = std::chrono::high_resolution_clock::now();
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  pSyntaxNode NodePointer = ast;
  NodePointer = NodePointer->child_;
  string db_name = (string)NodePointer->val_;

  // 检查有无对应数据库
  std::unordered_map<std::string, DBStorageEngine *>::iterator it = dbs_.find(db_name);
  if(it == dbs_.end())return DB_FAILED;

  // 删除对应数据库
  DBStorageEngine *deletedDB = it->second;
  delete deletedDB;
  dbs_.erase(db_name);
  remove(("./database/"+db_name).c_str());
  if(db_name == current_db_)current_db_ = "";

  std::chrono::high_resolution_clock::time_point endTime = std::chrono::high_resolution_clock::now();
  std::chrono::microseconds timeInterval = std::chrono::duration_cast <std::chrono::microseconds>(endTime - beginTime);
  std::cout << "Time: " << timeInterval.count() << "us" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
  std::chrono::high_resolution_clock::time_point beginTime = std::chrono::high_resolution_clock::now();
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif

  // 如果当前没有数据库
  if(dbs_.size() == 0){
    cout<<"No Database yet."<<endl;
    return DB_SUCCESS;
  }

  // 输出每个数据库的名称
  std::unordered_map<std::string, DBStorageEngine *>::iterator it = dbs_.begin();
  while(it!=dbs_.end()){
    cout << it->first << endl;
    it++;
  }
  std::chrono::high_resolution_clock::time_point endTime = std::chrono::high_resolution_clock::now();
  std::chrono::microseconds timeInterval = std::chrono::duration_cast <std::chrono::microseconds>(endTime - beginTime);
  std::cout << "Time: " << timeInterval.count() << "us" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
  std::chrono::high_resolution_clock::time_point beginTime = std::chrono::high_resolution_clock::now();
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  pSyntaxNode NodePointer = ast;
  NodePointer = NodePointer->child_;
  std::string db_name = (std::string)NodePointer->val_;

  // 确认是否存在该数据库
  if(dbs_.find(db_name) == dbs_.end()){
    cout<<"No such Database."<<endl;
    return DB_FAILED;
  }

  // 设置当前数据库名称
  current_db_ = db_name;

  std::chrono::high_resolution_clock::time_point endTime = std::chrono::high_resolution_clock::now();
  std::chrono::microseconds timeInterval = std::chrono::duration_cast <std::chrono::microseconds>(endTime - beginTime);
  std::cout << "Time: " << timeInterval.count() << "us" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
  std::chrono::high_resolution_clock::time_point beginTime = std::chrono::high_resolution_clock::now();
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  std::vector<TableInfo *> tables;

  // 获取所有Table
  db->catalog_mgr_->GetTables(tables);

  // 输出所有Table名
  for(int i = 0; i < (int)tables.size() ; i++){
    cout << tables[i]->GetTableName() << endl;
  }

  std::chrono::high_resolution_clock::time_point endTime = std::chrono::high_resolution_clock::now();
  std::chrono::microseconds timeInterval = std::chrono::duration_cast <std::chrono::microseconds>(endTime - beginTime);
  std::cout << "Time: " << timeInterval.count() << "us" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
  std::chrono::high_resolution_clock::time_point beginTime = std::chrono::high_resolution_clock::now();
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  pSyntaxNode NodePointer = ast;

// 获取Table名
  NodePointer = NodePointer->child_;
  std::string table_name = (std::string)NodePointer->val_;

// 确认该表是否存在
  TableInfo* tableinfo = NULL;
  db->catalog_mgr_->GetTable(table_name, tableinfo);
  if(tableinfo != NULL) return DB_TABLE_ALREADY_EXIST;

  NodePointer = NodePointer->next_->child_;
  std::vector<Column* > columns;
  uint32_t columnindex = 0;

  // 获取Primary Key
  std::vector<std::string> primary_key;
  pSyntaxNode primaryPointer = NodePointer;
  while(primaryPointer->next_ != NULL)primaryPointer=primaryPointer->next_;
  primaryPointer=primaryPointer->child_;
  if(primaryPointer == NULL){
    cout<<"Error: there are no primary key."<<endl;
    return DB_FAILED;
  }
  while(primaryPointer != NULL){
    primary_key.push_back((std::string)primaryPointer->val_);
    primaryPointer = primaryPointer->next_;
  }

  // 获取每个Column信息
  while(NodePointer->type_ != kNodeColumnList){
    int length = 0;
    bool unique = false;
    bool Nullable = true;
    if(NodePointer->val_ != NULL && (string)NodePointer->val_ == "unique")unique = true;
    pSyntaxNode pChild = NodePointer->child_;
    std::string coloum_name = (std::string)pChild->val_;
    if(std::count(primary_key.begin(), primary_key.end(), coloum_name)){
      Nullable = false;
      unique = true;
    }
    pChild = pChild->next_;
    std::string type_name = (std::string)pChild->val_;
    TypeId typeid_;
    if(type_name == "int")typeid_ = kTypeInt;
    else if (type_name == "float") typeid_ = kTypeFloat;
    else if(type_name == "char")typeid_ = kTypeChar;
    else return DB_FAILED;
    if(typeid_ == kTypeChar){
      pChild = pChild->child_;
      if(pChild != NULL){
        const char* length_s = (const char*)pChild->val_;
        if(strchr(length_s, '.') == NULL && strchr(length_s, '-') == NULL){
          length = atoi(pChild->val_);
        }
        else{
          cout << "Char type need length!" << endl;
          return DB_FAILED;
          }
      }
    }
    Column* new_column;
    if(typeid_ != kTypeChar) new_column = new Column(coloum_name, typeid_, columnindex, Nullable, unique);
    else{
        new_column = new Column(coloum_name, typeid_, length, columnindex, Nullable, unique);
    }
    columns.push_back(new_column);

    NodePointer = NodePointer->next_; 
    columnindex++;
  }

  // 创建Schema
  Schema* table_schema = new Schema(columns);
  // 创建Table
  db->catalog_mgr_->CreateTable(table_name, table_schema, NULL, tableinfo);

  // 为Unique属性创建Index
  for(auto i = columns.begin(); i != columns.end(); i++){
    if((*i)->IsUnique()){
      std::vector<std::string> unique_key;
      IndexInfo* index_info;
      unique_key.push_back((*i)->GetName());
      db->catalog_mgr_->CreateIndex(table_name, "Unique_"+(*i)->GetName(), unique_key, NULL, index_info);
    }
  }
  // 为Primary Key创建Index
  IndexInfo* index_info = NULL;
  db->catalog_mgr_->CreateIndex(table_name, "PRIMARY", primary_key, NULL, index_info);
  std::chrono::high_resolution_clock::time_point endTime = std::chrono::high_resolution_clock::now();
  std::chrono::microseconds timeInterval = std::chrono::duration_cast <std::chrono::microseconds>(endTime - beginTime);
  std::cout << "Time: " << timeInterval.count() << "us" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
  std::chrono::high_resolution_clock::time_point beginTime = std::chrono::high_resolution_clock::now();
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  pSyntaxNode NodePointer = ast;
  NodePointer = NodePointer->child_;
  std::string table_name = NodePointer->val_;
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  
  // 确认该表是否存在
  TableInfo* table_info = NULL;
  db->catalog_mgr_->GetTable(table_name, table_info);
  if(table_info == NULL){
    cout<<"Error: No Such Table."<<endl;
    return DB_FAILED;
  }

  // 删除表
  db->catalog_mgr_->DropTable(table_name);

  std::chrono::high_resolution_clock::time_point endTime = std::chrono::high_resolution_clock::now();
  std::chrono::microseconds timeInterval = std::chrono::duration_cast <std::chrono::microseconds>(endTime - beginTime);
  std::cout << "Time: " << timeInterval.count() << "us" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
  std::chrono::high_resolution_clock::time_point beginTime = std::chrono::high_resolution_clock::now();
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif

// 获取当前数据库所有表
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  std::vector<TableInfo *> tables;
  db->catalog_mgr_->GetTables(tables);

  // 获取每个表的所有Index，依次输出
  for(auto i = tables.begin(); i != tables.end() ; i++){
    std::vector<IndexInfo *> indexes;
    db->catalog_mgr_->GetTableIndexes((*i)->GetTableName(), indexes);
    for(int j = 0; j < (int)indexes.size(); j++){
      cout << "| " << (*i)->GetTableName() << " | " << indexes[j]->GetIndexName() << " | " ;//should be more here
      cout<<endl;
    }
  }

  std::chrono::high_resolution_clock::time_point endTime = std::chrono::high_resolution_clock::now();
  std::chrono::microseconds timeInterval = std::chrono::duration_cast <std::chrono::microseconds>(endTime - beginTime);
  std::cout << "Time: " << timeInterval.count() << "us" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
   std::chrono::high_resolution_clock::time_point beginTime = std::chrono::high_resolution_clock::now();
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  pSyntaxNode NodePointer = ast;

  // 获取Index名
  NodePointer = NodePointer->child_;
  std::string index_name = (std::string)NodePointer->val_;

// 获取表名和表
  NodePointer = NodePointer->next_;
  std::string table_name = (std::string)NodePointer->val_;
  TableInfo* table_info;
  db->catalog_mgr_->GetTable(table_name, table_info);

// 确认该Index是否存在
  IndexInfo *index_info = NULL;
  db->catalog_mgr_->GetIndex(table_name, index_name, index_info);
  if(index_info != NULL){
    cout<<"Error: Index already exists."<<endl;
    return DB_FAILED;
  }

// 获取Keys信息
  NodePointer = NodePointer->next_;
  std::vector<std::string> keys;
  pSyntaxNode childpointer = NodePointer->child_;
  while(childpointer != NULL){
    //only unique key can create index
    uint32_t idx;
    table_info->GetSchema()->GetColumnIndex((std::string)childpointer->val_,idx);
    if(!table_info->GetSchema()->GetColumn(idx)->IsUnique()){
      cout<<"Error: Index can only be create on unique keys."<<endl;
      return DB_FAILED;
    }
    keys.push_back((std::string)childpointer->val_);
    childpointer = childpointer->next_;
  }

  // 创建Index
  IndexInfo* New_index_info = NULL;
  db->catalog_mgr_->CreateIndex(table_name, index_name, keys, NULL, New_index_info);
  vector<uint32_t>index_column_num;
  for(auto i = keys.begin(); i != keys.end(); i++){
    uint32_t idx;
    table_info->GetSchema()->GetColumnIndex(*i, idx);
    index_column_num.push_back(idx);
  }

  // 把当前表中数据传入Index
  for(auto i = table_info->GetTableHeap()->Begin(NULL); i != table_info->GetTableHeap()->End(); i++){
    vector<Field> index_fields;
    Row *temp_row = new Row(i->GetRowId());
    table_info->GetTableHeap()->GetTuple(temp_row, NULL);
    for(auto j=index_column_num.begin(); j != index_column_num.end(); j++){
      index_fields.push_back(*(temp_row->GetField(*j)));
    }
    Row index_row(index_fields);
    New_index_info->GetIndex()->InsertEntry(index_row, i->GetRowId(), NULL);
  }

  std::chrono::high_resolution_clock::time_point endTime = std::chrono::high_resolution_clock::now();
  std::chrono::microseconds timeInterval = std::chrono::duration_cast <std::chrono::microseconds>(endTime - beginTime);
  std::cout << "Time: " << timeInterval.count() << "us" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
   std::chrono::high_resolution_clock::time_point beginTime = std::chrono::high_resolution_clock::now();
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
// 获取Index名
  pSyntaxNode NodePointer = ast;
  NodePointer = NodePointer->child_;
  string index_name = NodePointer->val_;

  //获取所有表名
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  std::vector<TableInfo *> tables;
  db->catalog_mgr_->GetTables(tables);
  IndexInfo* info = NULL;

  // 寻找对应名称的index
  int i;
  for(i = 0; i < (int)tables.size() ; i++){
    db->catalog_mgr_->GetIndex(tables[i]->GetTableName(), index_name, info);
    if(info != NULL)break;
  }
  if(info == NULL){
    cout<<"Error: Index Not Found!"<<endl;
    return DB_INDEX_NOT_FOUND;
    }

  //删除对应Index
  db->catalog_mgr_->DropIndex(tables[i]->GetTableName(), index_name);
  std::chrono::high_resolution_clock::time_point endTime = std::chrono::high_resolution_clock::now();
  std::chrono::microseconds timeInterval = std::chrono::duration_cast <std::chrono::microseconds>(endTime - beginTime);
  std::cout << "Time: " << timeInterval.count() << "us" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
  std::chrono::high_resolution_clock::time_point beginTime = std::chrono::high_resolution_clock::now();
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  pSyntaxNode NodePointer = ast->child_;
  std::vector<std::string> columnList;
  TableInfo* table_info = NULL;
  std::string table_name;

  // 获取需要选取的Column名称
  if(NodePointer->type_ == kNodeAllColumns){
    //LOG(INFO) << "ExecuteSelect check0.1" << std::endl;
    NodePointer = NodePointer->next_;
    table_name = (std::string)NodePointer->val_;
    db->catalog_mgr_->GetTable(table_name, table_info);
    if(table_info == NULL)
    {
      cout << "table not exist" << endl;
      return DB_TABLE_NOT_EXIST;
    }
    uint32_t cnt = table_info->GetSchema()->GetColumnCount();
    for(uint32_t i = 0; i < cnt; i++){
      columnList.push_back(table_info->GetSchema()->GetColumn(i)->GetName());
    }
  }
  else{
    pSyntaxNode childs = NodePointer->child_;
    while(childs!=NULL){
      columnList.push_back((std::string)childs->val_);
      childs = childs->next_;
    }  
    NodePointer = NodePointer->next_;
    table_name = (std::string)NodePointer->val_;
    db->catalog_mgr_->GetTable(table_name, table_info);
  }

  // 根据条件筛选对应Row
  std::vector<RowId> res;
  NodePointer = NodePointer->next_;
  if(NodePointer != NULL){
    res = Condition(NodePointer->child_, table_name);
  }
  else{
    TableIterator Iterator = table_info->GetTableHeap()->Begin(NULL);
    TableIterator End = table_info->GetTableHeap()->End();
    while(Iterator != End){
      res.push_back(Iterator->GetRowId());
      ++Iterator;
      }
  }

  // 输出每个Row的内容
  int i;
  for(i = 0; i < (int)res.size(); i++){
    if(res[i].GetPageId() == -1)break;
    Row* row = new Row(res[i]);
    table_info->GetTableHeap()->GetTuple(row, NULL);
    for(int j = 0; j < (int)columnList.size(); j++){
      uint32_t idx;
      table_info->GetSchema()->GetColumnIndex(columnList[j], idx);
      cout<<" "<<row->GetField(idx)->GetData()<<" ";
    }
    cout<<endl;
  }

  cout<<"Selected Row Number : "<<i<<endl;
  std::chrono::high_resolution_clock::time_point endTime = std::chrono::high_resolution_clock::now();
  std::chrono::microseconds timeInterval = std::chrono::duration_cast <std::chrono::microseconds>(endTime - beginTime);
  std::cout << "Time: " << timeInterval.count() << "us" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
  std::chrono::high_resolution_clock::time_point beginTime = std::chrono::high_resolution_clock::now();
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
// 获取表和Columns
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  pSyntaxNode NodePointer = ast->child_;
  std::string table_name = (std::string)NodePointer->val_;
  TableInfo *table_info = NULL;
  db->catalog_mgr_->GetTable(table_name,table_info);
  std::vector<Column*> columns = table_info->GetSchema()->GetColumns();

  // 获取插入的Value，创建每个Field
  NodePointer = NodePointer->next_;
  NodePointer = NodePointer->child_;
  std::vector<Field> fields;
  int cnt = 0;
  while(NodePointer != NULL){
    // 如果是Unique，检查是否重复
    if(columns[cnt]->IsUnique()){
      IndexInfo* index_info;
      db->catalog_mgr_->GetIndex(table_name, "Unique_"+columns[cnt]->GetName(), index_info);
      std::vector<Field> fields;
      if(columns[cnt]->GetType() == kTypeChar)fields.push_back(Field(columns[cnt]->GetType(), NodePointer->val_, std::string(NodePointer->val_).size(), false));
      else if(columns[cnt]->GetType() == kTypeInt)fields.push_back(Field(columns[cnt]->GetType(), atoi(NodePointer->val_)));
      else if(columns[cnt]->GetType() == kTypeFloat)fields.push_back(Field(columns[cnt]->GetType(), (float)atof((const char*)NodePointer->val_)));
      Row key(fields);
      std::vector<RowId> result;
      result.clear();
      index_info->GetIndex()->ScanKey(key, result, NULL);
      if(!result.empty()){
        cout<<"Error: Unique Constraints Conflict!"<<endl;
        return DB_FAILED;
      }
    }

    if(NodePointer->type_ == kNodeNumber){
      if(columns[cnt]->GetType() == kTypeInt)fields.push_back(Field(kTypeInt, atoi(NodePointer->val_)));
      else if(columns[cnt]->GetType() == kTypeFloat)fields.push_back(Field(kTypeFloat, (float)atof((const char*)NodePointer->val_)));
      else return DB_FAILED;
    }
    else if(NodePointer->type_ == kNodeString){
      if(columns[cnt]->GetType() == kTypeChar)fields.push_back(Field((kTypeChar), (char*)NodePointer->val_, std::string(NodePointer->val_).size(), true));
      else return DB_FAILED;
    }
    else if(NodePointer->type_ == kNodeNull){
      if(!columns[cnt]->IsNullable()){
        cout<<"Error: this column not Nullable."<<endl;
      }
      fields.push_back(Field(columns[cnt]->GetType()));
    }
    cnt++;
    NodePointer = NodePointer->next_;
  }

  // 创建并插入Row
  Row row(fields);
  table_info->GetTableHeap()->InsertTuple(row,NULL);
  RowId rid(row.GetRowId());
  
  // 在该表的每个Index插入Entry
  vector<IndexInfo*> index_infos;
  db->catalog_mgr_->GetTableIndexes(table_name, index_infos);
  for(auto i = index_infos.begin(); i != index_infos.end(); i++){
    vector<Column*> key_columns = (*i)->GetIndexKeySchema()->GetColumns();
    vector<uint32_t> column_indexes;
    for(auto j = key_columns.begin(); j != key_columns.end();j++){
      uint32_t idx;
      table_info->GetSchema()->GetColumnIndex((*j)->GetName(), idx);
      column_indexes.push_back(idx);
    }
    vector<Field> index_fields;
    for(auto j=column_indexes.begin(); j != column_indexes.end(); j++){
      index_fields.push_back(*(row.GetField(*j)));
    }
    Row index_row(index_fields);
    (*i)->GetIndex()->InsertEntry(index_row, row.GetRowId(), NULL);
  }

  std::chrono::high_resolution_clock::time_point endTime = std::chrono::high_resolution_clock::now();
  std::chrono::microseconds timeInterval = std::chrono::duration_cast <std::chrono::microseconds>(endTime - beginTime);
  std::cout << "Time: " << timeInterval.count() << "us" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
  std::chrono::high_resolution_clock::time_point beginTime = std::chrono::high_resolution_clock::now();
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
// 获取表
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  pSyntaxNode NodePointer = ast->child_;
  std::string table_name = (std::string)NodePointer->val_;
  TableInfo *table_info = NULL;
  db->catalog_mgr_->GetTable(table_name,table_info);
  if(table_info == NULL)
  {
    cout << "table not exist" << endl;;
    return DB_TABLE_NOT_EXIST;
  }

  // 根据条件筛选对应的Row
  NodePointer = NodePointer->next_;
  std::vector<RowId> res;
  if(NodePointer!=NULL)res = Condition(NodePointer->child_, table_name);
  else{
    TableIterator Iterator = table_info->GetTableHeap()->Begin(NULL);
    while(Iterator != table_info->GetTableHeap()->End()){
      res.push_back(Iterator->GetRowId());
      ++Iterator;
      }
  }
  cout<<"Deleted Row Num : "<<res.size()<<endl;

  // 在该表的所有Index中删除对应的Entry
  vector<IndexInfo*> index_infos;
  db->catalog_mgr_->GetTableIndexes(table_name, index_infos);
  vector<Row> rows;
  for(auto i = res.begin(); i!= res.end(); i++){
    Row *temp_row = new Row((*i));
    table_info->GetTableHeap()->GetTuple(temp_row, NULL);
    temp_row->SetRowId(*i);
    rows.push_back(*temp_row);
  }
  for(auto i = index_infos.begin(); i != index_infos.end(); i++){
    vector<Column*> key_columns = (*i)->GetIndexKeySchema()->GetColumns();
    vector<uint32_t> column_indexes;
    for(auto j = key_columns.begin(); j != key_columns.end();j++){
      uint32_t idx;
      table_info->GetSchema()->GetColumnIndex((*j)->GetName(), idx);
      column_indexes.push_back(idx);
    }
    for(auto m = rows.begin(); m!= rows.end(); m++){
      Row *row = new Row(*m);
      vector<Field> index_fields;
      table_info->GetTableHeap()->GetTuple(row, NULL);
      row->SetRowId((*m).GetRowId());
      for(auto j=column_indexes.begin(); j != column_indexes.end(); j++){
        index_fields.push_back(*(row->GetField(*j)));
      }
      Row index_row(index_fields);
      (*i)->GetIndex()->RemoveEntry(index_row, row->GetRowId(), NULL);
    }
    
  }

  // 删除表中的记录
  for(int i = 0; i < (int)res.size(); i++)table_info->GetTableHeap()->ApplyDelete(res[i], NULL);

  std::chrono::high_resolution_clock::time_point endTime = std::chrono::high_resolution_clock::now();
  std::chrono::microseconds timeInterval = std::chrono::duration_cast <std::chrono::microseconds>(endTime - beginTime);
  std::cout << "Time: " << timeInterval.count() << "us" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
  std::chrono::high_resolution_clock::time_point beginTime = std::chrono::high_resolution_clock::now();
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
// 获取表
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  pSyntaxNode NodePointer = ast->child_;
  std::string table_name = (std::string)NodePointer->val_;
  TableInfo *table_info = NULL;
  db->catalog_mgr_->GetTable(table_name,table_info);
  if(table_info == NULL)
  {
    cout << "table not exist" << endl;
    return DB_TABLE_NOT_EXIST;
  }

  // 获取要更新的Column和value
  NodePointer = NodePointer->next_;
  pSyntaxNode ChildPointer = NodePointer->child_;
  std::vector<std::string> value_names;
  std::vector<std::string> values;
  while(ChildPointer != NULL){
    value_names.push_back((std::string)ChildPointer->child_->val_);
    values.push_back((std::string)ChildPointer->child_->next_->val_);
    ChildPointer = ChildPointer->next_;
  }

  // 根据条件筛选对应Row
  NodePointer = NodePointer->next_;
  std::vector<RowId> res;
  if(NodePointer != NULL)res = Condition(NodePointer->child_, table_name);
  else{
    TableIterator Iterator = table_info->GetTableHeap()->Begin(NULL);
    while(Iterator != table_info->GetTableHeap()->End()){
      res.push_back(Iterator->GetRowId());
      ++Iterator;
      }
  }
  cout<<"Updated Row Num : "<<res.size()<<endl;

  // 更新每个Row的值
  for(int i = 0; i < (int)res.size(); i++){
    Row *row = new Row(res[i]);
    table_info->GetTableHeap()->GetTuple(row, NULL);
    for(int j = 0; j < (int)values.size(); j++){
      std::string value_name = value_names[j];
      uint32_t idx;
      table_info->GetSchema()->GetColumnIndex(value_name, idx);
      TypeId type = table_info->GetSchema()->GetColumn(idx)->GetType();
      Field *field;
      if(type == kTypeInt)field = new Field(type, atoi(values[j].c_str()));
      else if(type == kTypeFloat)field = new Field(type, (float)atof(values[j].c_str()));
      else if(type == kTypeChar)field = new Field(type, (char*)values[j].c_str(), (uint32_t)values[j].size(), false);
      row->GetField(idx)->operator= (*field);
    }
    table_info->GetTableHeap()->UpdateTuple(*row, res[i], NULL);
  }

  std::chrono::high_resolution_clock::time_point endTime = std::chrono::high_resolution_clock::now();
  std::chrono::microseconds timeInterval = std::chrono::duration_cast <std::chrono::microseconds>(endTime - beginTime);
  std::cout << "Time: " << timeInterval.count() << "us" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
  std::chrono::high_resolution_clock::time_point beginTime = std::chrono::high_resolution_clock::now();
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  // 输入文件名并打开
  fstream file;
  pSyntaxNode NodePointer = ast;
  FILE *yyin;
  TreeFileManagers syntax_tree_file_mgr("syntax_tree_");
  [[maybe_unused]] uint32_t syntax_tree_id = 0;
  NodePointer = NodePointer->child_;
  const char* file_name = (const char*)NodePointer->val_;
  file.open(file_name, ios::in|ios::binary);
  yyin = fopen(file_name,"r");
  if(yyin == NULL){
      LOG(INFO) <<"Open failed!";
      return DB_FAILED;
  }
  // 从文件逐句输入命令并执行
  while(!feof(yyin)){
    int i = 0;
    char cmd[1024];
    while (cmd[i-1] != ';') {
      cmd[i++] = fgetc(yyin);
    }
    fgetc(yyin);
    cmd[i] = 0;
    LOG(INFO) << (string)cmd;
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    } else {
#ifdef ENABLE_PARSER_DEBUG
      printf("[INFO] Sql syntax parse ok!\n");
      SyntaxTreePrinter printer(MinisqlGetParserRootNode());
      printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
#endif
    }

    ExecuteContext context;
    Execute(MinisqlGetParserRootNode(), &context);
    //sleep(0.1);

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    // quit condition
    if (context.flag_quit_) {
      printf("bye!\n");
      break;
    }
  }

  std::chrono::high_resolution_clock::time_point endTime = std::chrono::high_resolution_clock::now();
  std::chrono::microseconds timeInterval = std::chrono::duration_cast <std::chrono::microseconds>(endTime - beginTime);
  std::cout << "Time: " << timeInterval.count() << "us" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}


std::vector<RowId> ExecuteEngine::Condition(pSyntaxNode ast, std::string table_name){
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  pSyntaxNode pointer = ast;
  std::vector<RowId> res;
  if(ast->type_ == kNodeConnector){
    if((std::string)ast->val_ == "and"){
      // 分别获取and两边条件符合的Row
      pointer = pointer->child_;
      std::vector<RowId> Rows1 = Condition(pointer, table_name);
      pointer = pointer->next_;
      std::vector<RowId> Rows2 = Condition(pointer, table_name);
      //取交集
      sort (Rows1.begin(),Rows1.end(), RowId_compare);
      sort (Rows2.begin(),Rows2.end(), RowId_compare);
      res.resize(min(Rows1.size(), Rows2.size()));
      set_intersection(Rows1.begin(), Rows1.end(), Rows2.begin(), Rows2.end(), res.begin(), RowId_compare);
      // 确保res的size正确
      int i = res.size()-1;
      while(res[i].GetPageId() == -1)i--;
      res.resize(i+1);
    }
    else if((std::string)ast->val_ == "or"){
      // 分别获取or两边条件符合的Row
      pointer = pointer->child_;
      std::vector<RowId> Rows1 = Condition(pointer, table_name);
      pointer = pointer->next_;
      std::vector<RowId> Rows2 = Condition(pointer, table_name);
      //取并集
      sort (Rows1.begin(),Rows1.end(), RowId_compare);
      sort (Rows2.begin(),Rows2.end(), RowId_compare);
      res.resize(Rows1.size()+Rows2.size());
      set_union(Rows1.begin(), Rows1.end(), Rows2.begin(), Rows2.end(), res.begin(), RowId_compare);
      // 确保res的size正确
      int i = res.size()-1;
      while(res[i].GetPageId() == -1)i--;
      res.resize(i+1);
    }
  }
  else if(ast->type_ == kNodeCompareOperator){
    if((std::string)ast->val_ == "="){
      // 获取要筛选的属性名和值、数据类型
      pointer = pointer->child_;
      std::string column_name = (std::string)pointer->val_;
      pointer = pointer->next_;
      TableInfo *table_info = NULL;
      db->catalog_mgr_->GetTable(table_name, table_info);
      uint32_t idx;
      table_info->GetSchema()->GetColumnIndex(column_name, idx);
      TypeId type = table_info->GetSchema()->GetColumn(idx)->GetType();

      //检查有无可用的Index
      std::vector<IndexInfo*> indexes;
      IndexInfo* chosen_index = NULL;
      db->catalog_mgr_->GetTableIndexes(table_name, indexes);
      for(auto i = indexes.begin(); i!=indexes.end(); i++){
        std::vector<Column*> columns = (*i)->GetIndexKeySchema()->GetColumns();
        for(auto j = columns.begin(); j != columns.end(); j++){
          if((*j)->GetName() == column_name && columns.size() == 1)chosen_index = (*i);
        }
        if(chosen_index != NULL)break;
      }
      // 若有Index可用，则用Index搜索
      if(chosen_index != NULL){
        std::vector<Field> fields;
        if(type == kTypeChar)fields.push_back(Field(type, pointer->val_, ((std::string)pointer->val_).size(), 0));
        else if(type == kTypeInt)fields.push_back(Field(type, atoi(pointer->val_)));
        else if(type == kTypeFloat)fields.push_back(Field(type, (float)atof(pointer->val_)));
        Row key(fields);
        std::vector<RowId> result;
        chosen_index->GetIndex()->ScanKey(key, result, NULL);
        return result;
      }
      // 若无Index可用，则用TableHeap逐条轮询
      TableIterator Iterator = table_info->GetTableHeap()->Begin(NULL);
      TableIterator End = table_info->GetTableHeap()->End();
      while(Iterator != End){
        Row* row = new Row(*Iterator);
        table_info->GetTableHeap()->GetTuple(row, NULL);
        if(type == kTypeFloat){
          Field field(type, (float)atof(pointer->val_));
          if(row->GetField(idx)->CompareEquals(field))res.push_back(Iterator->GetRowId());
        }
        else if(type == kTypeInt){
          Field field(type, atoi(pointer->val_));
          if(row->GetField(idx)->CompareEquals(field))res.push_back(Iterator->GetRowId());
        }
        else if(type == kTypeChar){
          Field field(type, pointer->val_, ((std::string)pointer->val_).size(), false);
          if(row->GetField(idx)->CompareEquals(field))res.push_back(Iterator->GetRowId());
        }
        ++Iterator;
        delete row;
      }
    }
    else if((std::string)ast->val_ == "not"){
      // 获取要筛选的属性名和值、数据类型
      pointer = pointer->child_;
      std::string column_name = (std::string)pointer->val_;
      pointer = pointer->next_;
      TableInfo *table_info = NULL;
      db->catalog_mgr_->GetTable(table_name, table_info);
      uint32_t idx;
      table_info->GetSchema()->GetColumnIndex(column_name, idx);
      // 轮询寻找符合条件的Row
      TableIterator Iterator = table_info->GetTableHeap()->Begin(NULL);
      TableIterator End = table_info->GetTableHeap()->End();
      while(Iterator != End){
        Row* row = new Row(*Iterator);
        table_info->GetTableHeap()->GetTuple(row, NULL);
        if(! row->GetField(idx)->IsNull())res.push_back(Iterator->GetRowId());
        ++Iterator;
        delete row;
      }
    }
    else if((std::string)ast->val_ == "is"){  //is NULL
    // 获取要筛选的属性名和值、数据类型
      pointer = pointer->child_;
      std::string column_name = (std::string)pointer->val_;
      pointer = pointer->next_;
      TableInfo *table_info = NULL;
      db->catalog_mgr_->GetTable(table_name, table_info);
      uint32_t idx;
      table_info->GetSchema()->GetColumnIndex(column_name, idx);
      // 轮询寻找符合条件的Row
      TableIterator Iterator = table_info->GetTableHeap()->Begin(NULL);
      TableIterator End = table_info->GetTableHeap()->End();
      while(Iterator != End){
        Row* row = new Row(*Iterator);
        table_info->GetTableHeap()->GetTuple(row, NULL);
        if(Iterator->GetField(idx)->IsNull())res.push_back(Iterator->GetRowId());
        ++Iterator;
        delete row;
      }
    }
    else if((std::string)ast->val_ == "<>"){
      // 获取要筛选的属性名和值、数据类型
      pointer = pointer->child_;
      std::string column_name = (std::string)pointer->val_;
      pointer = pointer->next_;
      TableInfo *table_info = NULL;
      db->catalog_mgr_->GetTable(table_name, table_info);
      uint32_t idx;
      table_info->GetSchema()->GetColumnIndex(column_name, idx);
      // 轮询寻找符合条件的Row
      TableIterator Iterator = table_info->GetTableHeap()->Begin(NULL);
      TableIterator End = table_info->GetTableHeap()->End();
      TypeId type = table_info->GetSchema()->GetColumn(idx)->GetType();
      Field *f;
      if(type == kTypeInt)
      {
        f = new Field(kTypeInt, atoi(pointer->val_));
      }
      else if(type == kTypeFloat)
      {
        f = new Field(kTypeFloat, float(atof(pointer->val_)));
      }
      else
      {
        f = new Field(kTypeChar, pointer->val_, string(pointer->val_).size(), false); 
      }
      while(Iterator != End){
        Row* row = new Row(*Iterator);
        table_info->GetTableHeap()->GetTuple(row, NULL);
        if(row->GetField(idx)->CompareNotEquals(*f) == kTrue)res.push_back(Iterator->GetRowId());
        ++Iterator;
        delete row;
      }
      delete f;
    }
    else if((std::string)ast->val_ == ">"){
      // 获取要筛选的属性名和值、数据类型
      pointer = pointer->child_;
      std::string column_name = (std::string)pointer->val_;
      pointer = pointer->next_;
      TableInfo *table_info = NULL;
      db->catalog_mgr_->GetTable(table_name, table_info);
      uint32_t idx;
      table_info->GetSchema()->GetColumnIndex(column_name, idx);
      // 轮询寻找符合条件的Row
      TableIterator Iterator = table_info->GetTableHeap()->Begin(NULL);
      TableIterator End = table_info->GetTableHeap()->End();
      TypeId type = table_info->GetSchema()->GetColumn(idx)->GetType();
      Field *f;
      if(type == kTypeInt)
      {
        f = new Field(kTypeInt, atoi(pointer->val_));
      }
      else if(type == kTypeFloat)
      {
        f = new Field(kTypeFloat, float(atof(pointer->val_)));
      }
      else
      {
        f = new Field(kTypeChar, pointer->val_, string(pointer->val_).size(), false); 
      }
      while(Iterator != End){
        Row* row = new Row(*Iterator);
        table_info->GetTableHeap()->GetTuple(row, NULL);
        if(row->GetField(idx)->CompareGreaterThan(*f) == kTrue)res.push_back(Iterator->GetRowId());
        ++Iterator;
        delete row;
      }
      delete f;
    }
    else if((std::string)ast->val_ == "<"){
       // 获取要筛选的属性名和值、数据类型
      pointer = pointer->child_;
      std::string column_name = (std::string)pointer->val_;
      pointer = pointer->next_;
      TableInfo *table_info = NULL;
      db->catalog_mgr_->GetTable(table_name, table_info);
      uint32_t idx;
      table_info->GetSchema()->GetColumnIndex(column_name, idx);
      // 轮询寻找符合条件的Row
      TableIterator Iterator = table_info->GetTableHeap()->Begin(NULL);
      TableIterator End = table_info->GetTableHeap()->End();
      TypeId type = table_info->GetSchema()->GetColumn(idx)->GetType();
      Field *f;
      if(type == kTypeInt)
      {
        f = new Field(kTypeInt, atoi(pointer->val_));
      }
      else if(type == kTypeFloat)
      {
        f = new Field(kTypeFloat, float(atof(pointer->val_)));
      }
      else
      {
        f = new Field(kTypeChar, pointer->val_, string(pointer->val_).size(), false); 
      }
      while(Iterator != End){
        Row* row = new Row(*Iterator);
        table_info->GetTableHeap()->GetTuple(row, NULL);
        if(row->GetField(idx)->CompareLessThan(*f) == kTrue)res.push_back(Iterator->GetRowId());
        ++Iterator;
        delete row;
      }
      delete f;
    }
    else if((std::string)ast->val_ == ">="){
       // 获取要筛选的属性名和值、数据类型
      pointer = pointer->child_;
      std::string column_name = (std::string)pointer->val_;
      pointer = pointer->next_;
      TableInfo *table_info = NULL;
      db->catalog_mgr_->GetTable(table_name, table_info);
      uint32_t idx;
      table_info->GetSchema()->GetColumnIndex(column_name, idx);
      // 轮询寻找符合条件的Row
      TableIterator Iterator = table_info->GetTableHeap()->Begin(NULL);
      TableIterator End = table_info->GetTableHeap()->End();
      TypeId type = table_info->GetSchema()->GetColumn(idx)->GetType();
      Field *f;
      if(type == kTypeInt)
      {
        f = new Field(kTypeInt, atoi(pointer->val_));
      }
      else if(type == kTypeFloat)
      {
        f = new Field(kTypeFloat, float(atof(pointer->val_)));
      }
      else
      {
        f = new Field(kTypeChar, pointer->val_, string(pointer->val_).size(), false); 
      }
      while(Iterator != End){
        Row* row = new Row(*Iterator);
        table_info->GetTableHeap()->GetTuple(row, NULL);
        if(row->GetField(idx)->CompareGreaterThanEquals(*f) == kTrue)res.push_back(Iterator->GetRowId());
        ++Iterator;
        delete row;
      }
      delete f;
    }
    else if((std::string)ast->val_ == "<="){
       // 获取要筛选的属性名和值、数据类型
      pointer = pointer->child_;
      std::string column_name = (std::string)pointer->val_;
      pointer = pointer->next_;
      TableInfo *table_info = NULL;
      db->catalog_mgr_->GetTable(table_name, table_info);
      uint32_t idx;
      table_info->GetSchema()->GetColumnIndex(column_name, idx);
      // 轮询寻找符合条件的Row
      TableIterator Iterator = table_info->GetTableHeap()->Begin(NULL);
      TableIterator End = table_info->GetTableHeap()->End();
      TypeId type = table_info->GetSchema()->GetColumn(idx)->GetType();
      Field *f;
      if(type == kTypeInt)
      {
        f = new Field(kTypeInt, atoi(pointer->val_));
      }
      else if(type == kTypeFloat)
      {
        f = new Field(kTypeFloat, float(atof(pointer->val_)));
      }
      else
      {
        f = new Field(kTypeChar, pointer->val_, string(pointer->val_).size(), false); 
      }
      while(Iterator != End){
        Row* row = new Row(*Iterator);
        table_info->GetTableHeap()->GetTuple(row, NULL);
        if(row->GetField(idx)->CompareLessThanEquals(*f) == kTrue)res.push_back(Iterator->GetRowId());
        ++Iterator;
        delete row;
      }
      delete f;
    }
  }
  return res;
}