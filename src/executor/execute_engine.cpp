#include "executor/execute_engine.h"
#include "glog/logging.h"
#include <algorithm>
#include <fstream>
extern "C" {
int yyparse(void);

#include "parser/minisql_lex.h"
#include "parser/parser.h"

}
ExecuteEngine::ExecuteEngine() {

}



bool RowId_compare(RowId x, RowId y){
  if(x.GetSlotNum() < y.GetSlotNum())return true;
  else return false;
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
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  pSyntaxNode NodePointer = ast;
  NodePointer = NodePointer->child_;
  string db_name = (string)NodePointer->val_;
  DBStorageEngine * newdb = new DBStorageEngine(db_name);
  dbs_.insert({db_name, newdb});
  current_db_ = db_name;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  pSyntaxNode NodePointer = ast;
  NodePointer = NodePointer->child_;
  string db_name = (string)NodePointer->val_;
  std::unordered_map<std::string, DBStorageEngine *>::iterator it = dbs_.find(db_name);
  if(it == dbs_.end())return DB_FAILED;
  DBStorageEngine *deletedDB = it->second;
  delete deletedDB;
  dbs_.erase(db_name);
  if(db_name == current_db_)current_db_ = "";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  std::unordered_map<std::string, DBStorageEngine *>::iterator it = dbs_.begin();
  while(it!=dbs_.end()){
    cout << it->first << endl;
    it++;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  pSyntaxNode NodePointer = ast;
  NodePointer = NodePointer->child_;
  string db_name = NodePointer->val_;
  current_db_ = db_name;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  std::vector<TableInfo *> tables;
  db->catalog_mgr_->GetTables(tables);
  for(int i = 0; i < (int)tables.size() ; i++){
    cout << tables[i]->GetTableName() << endl;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  //haven't deal with memheap
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  pSyntaxNode NodePointer = ast;

  NodePointer = NodePointer->child_;
  std::string table_name = (std::string)NodePointer->val_;

  TableInfo* tableinfo = NULL;
  db->catalog_mgr_->GetTable(table_name, tableinfo);
  if(tableinfo != NULL) return DB_TABLE_ALREADY_EXIST;

  NodePointer = NodePointer->next_->child_;
  std::vector<Column* > columns;
  uint32_t columnindex = 0;

  std::vector<std::string> primary_key;
  pSyntaxNode primaryPointer = NodePointer;
  while(primaryPointer->next_ != NULL)primaryPointer=primaryPointer->next_;
  primaryPointer=primaryPointer->child_;
  while(primaryPointer != NULL){
    primary_key.push_back((std::string)primaryPointer->val_);
    primaryPointer = primaryPointer->next_;
  }

  while(NodePointer->type_ != kNodeColumnList){
    int length = 0;
    bool unique = false;
    bool Nullable = true;
    if(NodePointer->val_ != NULL && (string)NodePointer->val_ == "unique")unique = true;
    pSyntaxNode pChild = NodePointer->child_;
    std::string coloum_name = (std::string)pChild->val_;
    if(std::count(primary_key.begin(), primary_key.end(), coloum_name))Nullable = false;
    pChild = pChild->next_;
    LOG(INFO) << "ExecuteCreateTable check1" << std::endl;
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
          LOG(INFO) << "ExecuteCreateTable ILLEGAL!" << std::endl;
          return DB_FAILED;
          }
      }
    }
    
    LOG(INFO) << "ExecuteCreateTable check3 type: "<< typeid_ << std::endl;
    Column* new_column;
    if(typeid_ != kTypeChar) new_column = new Column(coloum_name, typeid_, columnindex, Nullable, unique);
    else{
        new_column = new Column(coloum_name, typeid_, length, columnindex, Nullable, unique);
    }
    columns.push_back(new_column);
    
    NodePointer = NodePointer->next_; 
    columnindex++;
  }
  LOG(INFO) << "ExecuteCreateTable check4" << std::endl;
  Schema* table_schema = new Schema(columns);
  db->catalog_mgr_->CreateTable(table_name, table_schema, NULL, tableinfo);
  IndexInfo* index_info = NULL;
  db->catalog_mgr_->CreateIndex(table_name, "PRIMARY", primary_key, NULL, index_info);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  pSyntaxNode NodePointer = ast;
  NodePointer = NodePointer->child_;
  string table_name = NodePointer->val_;
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  db->catalog_mgr_->DropTable(table_name);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  std::vector<TableInfo *> tables;
  db->catalog_mgr_->GetTables(tables);
  for(int i = 0; i < (int)tables.size() ; i++){
    std::vector<IndexInfo *> indexes;
    db->catalog_mgr_->GetTableIndexes(tables[i]->GetTableName(), indexes);
    for(int j = 0; j < (int)indexes.size(); i++){
      cout << "| " << tables[i]->GetTableName() << " | " << indexes[j]->GetIndexName() << " | " ;//should be more here
      cout<<endl;
    }
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  //haven't deal with data structure
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  pSyntaxNode NodePointer = ast;

  NodePointer = NodePointer->child_;
  std::string index_name = (std::string)NodePointer->val_;

  NodePointer = NodePointer->next_;
  std::string table_name = (std::string)NodePointer->val_;

  NodePointer = NodePointer->next_;
  std::vector<std::string> keys;
  pSyntaxNode childpointer = NodePointer->child_;
  while(childpointer != NULL){
    keys.push_back((std::string)childpointer->val_);
    childpointer = childpointer->next_;
  }
  IndexInfo* index_info = NULL;
  db->catalog_mgr_->CreateIndex(table_name, index_name, keys, NULL, index_info);

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  pSyntaxNode NodePointer = ast;
  NodePointer = NodePointer->child_;
  string index_name = NodePointer->val_;
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  std::vector<TableInfo *> tables;
  db->catalog_mgr_->GetTables(tables);
  IndexInfo* info = NULL;
  int i;
  for(i = 0; i < (int)tables.size() ; i++){
    db->catalog_mgr_->GetIndex(tables[i]->GetTableName(), index_name, info);
    if(info != NULL)break;
  }
  if(info == NULL)return DB_INDEX_NOT_FOUND;
  db->catalog_mgr_->DropIndex(tables[i]->GetTableName(), index_name);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  pSyntaxNode NodePointer = ast->child_;
  std::vector<std::string> columnList;
  TableInfo* table_info = NULL;
  std::string table_name;
  if(NodePointer->type_ == kNodeAllColumns){
    LOG(INFO) << "ExecuteSelect check0.1" << std::endl;
    NodePointer = NodePointer->next_;
    table_name = (std::string)NodePointer->val_;
    db->catalog_mgr_->GetTable(table_name, table_info);
    uint32_t cnt = table_info->GetSchema()->GetColumnCount();
    for(uint32_t i = 0; i < cnt; i++){
      columnList.push_back(table_info->GetSchema()->GetColumn(i)->GetName());
    }
  }
  else{
    LOG(INFO) << "ExecuteSelect check0.2" << std::endl;
    pSyntaxNode childs = NodePointer->child_;
    while(childs!=NULL){
      columnList.push_back((std::string)childs->val_);
      childs = childs->next_;
    }
  }
  LOG(INFO) << "ExecuteSelect check0" << std::endl;
  std::vector<RowId> res;
  NodePointer = NodePointer->next_;
  if(NodePointer != NULL){
    res = Condition(NodePointer, table_name);
  }
  LOG(INFO) << "ExecuteSelect check1" << std::endl;
  for(int i = 0; i < (int)res.size(); i++){
    Row* row = new Row(res[i]);
    table_info->GetTableHeap()->GetTuple(row, NULL);
    for(int j = 0; j < (int)row->GetFieldCount(); j++){
      cout<<" "<<row->GetField(j)<<" ";
    }
    cout<<endl;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  pSyntaxNode NodePointer = ast->child_;
  std::string table_name = (std::string)NodePointer->val_;
  TableInfo *table_info = NULL;
  db->catalog_mgr_->GetTable(table_name,table_info);
  LOG(INFO) << "ExecuteInsert check1" << std::endl;
  std::vector<Column*> columns = table_info->GetSchema()->GetColumns();
  LOG(INFO) << "ExecuteInsert check0" << std::endl;
  NodePointer = NodePointer->next_;
  NodePointer = NodePointer->child_;
  LOG(INFO) << "ExecuteInsert check2" << std::endl;
  std::vector<Field> fields;
  int cnt = 0;
  while(NodePointer != NULL){
    if(NodePointer->type_ == kNodeNumber){
      if(columns[cnt]->GetType() == kTypeInt)fields.push_back(Field(kTypeInt, atoi(NodePointer->val_)));
      else if(columns[cnt]->GetType() == kTypeFloat)fields.push_back(Field(kTypeFloat, (float)atof((const char*)NodePointer->val_)));
      else return DB_FAILED;
    }
    else if(NodePointer->type_ == kNodeString){
      if(columns[cnt]->GetType() == kTypeChar)fields.push_back(Field((kTypeChar), (char*)NodePointer->val_, ((std::string)NodePointer->val_).size(), true));
      else return DB_FAILED;
    }
    else if(NodePointer->type_ == kNodeNull){
      fields.push_back(Field(columns[cnt]->GetType()));
    }
    cnt++;
    NodePointer = NodePointer->next_;
  }
  LOG(INFO) << "ExecuteInsert check3" << std::endl;
  Row row(fields);
  LOG(INFO) << "ExecuteInsert check4" << std::endl;
  table_info->GetTableHeap()->InsertTuple(row,NULL);
  RowId rid(row.GetRowId());
  IndexInfo* index_info = NULL;
  LOG(INFO) << "ExecuteInsert check5" << std::endl;
  db->catalog_mgr_->GetIndex(table_name, "PRIMARY", index_info);
  index_info->GetIndex()->InsertEntry(row, rid, NULL);
  
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  pSyntaxNode NodePointer = ast->child_;
  std::string table_name = (std::string)NodePointer->val_;
  TableInfo *table_info = NULL;
  db->catalog_mgr_->GetTable(table_name,table_info);
  NodePointer = NodePointer->next_;
  std::vector<RowId> res;
  if(NodePointer!=NULL)res = Condition(NodePointer, table_name);
  for(int i = 0; i < (int)res.size(); i++)table_info->GetTableHeap()->ApplyDelete(res[i], NULL);
  IndexInfo* index_info = NULL;
  db->catalog_mgr_->GetIndex(table_name, "PRIMARY", index_info);
  //for(int i = 0; i < res.size(); i++){
    //index_info->GetIndex()->RemoveEntry(key, res[i], NULL);
  //}
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  DBStorageEngine* db = dbs_.find(current_db_)->second;
  pSyntaxNode NodePointer = ast->child_;
  std::string table_name = (std::string)NodePointer->val_;
  TableInfo *table_info = NULL;
  db->catalog_mgr_->GetTable(table_name,table_info);
  NodePointer = NodePointer->next_;
  pSyntaxNode ChildPointer = NodePointer->child_;
  std::vector<std::string> value_names;
  std::vector<std::string> values;
  while(ChildPointer != NULL){
    value_names.push_back((std::string)ChildPointer->child_->val_);
    values.push_back((std::string)ChildPointer->child_->next_->val_);
    ChildPointer = ChildPointer->next_;
  }
  NodePointer = NodePointer->next_;
  std::vector<RowId> res;
  if(NodePointer != NULL)res = Condition(NodePointer, table_name);
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
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  fstream file;
  pSyntaxNode NodePointer = ast;
  FILE *yyin;
  NodePointer = NodePointer->child_;
  const char* file_name = (const char*)NodePointer->val_;
  file.open(file_name, ios::in|ios::binary);
  yyin = fopen(file_name,"r");
  if(yyin == NULL){
      LOG(INFO) <<"Open failed!";
      return DB_FAILED;
  }
  while(!feof(yyin)){
    int i = 0;
    char ch;
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
    sleep(1);

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
      pointer = pointer->child_;
      std::vector<RowId> Rows1 = Condition(pointer, table_name);
      pointer = pointer->next_;
      std::vector<RowId> Rows2 = Condition(pointer, table_name);
      //取交集
      sort (Rows1.begin(),Rows1.end(), RowId_compare);
      sort (Rows2.begin(),Rows2.end(), RowId_compare);
      set_intersection(Rows1.begin(), Rows1.end(), Rows2.begin(), Rows2.end(), res.begin(), RowId_compare);
    }
    else if((std::string)ast->val_ == "or"){
      pointer = pointer->child_;
      std::vector<RowId> Rows1 = Condition(pointer, table_name);
      pointer = pointer->next_;
      std::vector<RowId> Rows2 = Condition(pointer, table_name);
      //取并集
      sort (Rows1.begin(),Rows1.end(), RowId_compare);
      sort (Rows2.begin(),Rows2.end(), RowId_compare);
      set_union(Rows1.begin(), Rows1.end(), Rows2.begin(), Rows2.end(), res.begin(), RowId_compare);
    }
  }
  else if(ast->type_ == kNodeCompareOperator){
    if((std::string)ast->val_ == "="){
      std::vector<RowId> res;
      pointer = pointer->child_;
      std::string column_name = (std::string)pointer->val_;
      pointer = pointer->next_;
      TableInfo *table_info = NULL;
      db->catalog_mgr_->GetTable(table_name, table_info);
      uint32_t idx;
      table_info->GetSchema()->GetColumnIndex(column_name, idx);
      TableIterator Iterator = table_info->GetTableHeap()->Begin(NULL);
      TableIterator End = table_info->GetTableHeap()->End();
      while(Iterator != End){
        if((string)(Iterator->GetField(idx)->GetData()) == (string)pointer->val_ )res.push_back(Iterator->GetRowId());
        Iterator++;
      }
    }
    else if((std::string)ast->val_ == "not"){
      pointer = pointer->child_;
      std::string column_name = (std::string)pointer->val_;
      pointer = pointer->next_;
      TableInfo *table_info = NULL;
      db->catalog_mgr_->GetTable(table_name, table_info);
      uint32_t idx;
      table_info->GetSchema()->GetColumnIndex(column_name, idx);
      TableIterator Iterator = table_info->GetTableHeap()->Begin(NULL);
      TableIterator End = table_info->GetTableHeap()->End();
      while(Iterator != End){
        if((string)(Iterator->GetField(idx)->GetData()) != (string)pointer->val_ )res.push_back(Iterator->GetRowId());
        Iterator++;
      }
    }
    else if((std::string)ast->val_ == "is"){  //is NULL
      pointer = pointer->child_;
      std::string column_name = (std::string)pointer->val_;
      pointer = pointer->next_;
      TableInfo *table_info = NULL;
      db->catalog_mgr_->GetTable(table_name, table_info);
      uint32_t idx;
      table_info->GetSchema()->GetColumnIndex(column_name, idx);
      TableIterator Iterator = table_info->GetTableHeap()->Begin(NULL);
      TableIterator End = table_info->GetTableHeap()->End();
      while(Iterator != End){
        if(Iterator->GetField(idx)->IsNull())res.push_back(Iterator->GetRowId());
        Iterator++;
      }
    }
  }
  return res;
}