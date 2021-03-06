#include <unordered_map>
#include <vector>

#include "common/instance.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "storage/table_heap.h"
#include "storage/table_iterator.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  DBStorageEngine engine(db_file_name);
  SimpleMemHeap heap;
  const int row_nums = 1000;
  // create schema
  std::vector<Column *> columns = {ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
                                   ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
                                   ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  TableHeap *table_heap = TableHeap::Create(engine.bpm_, schema.get(), nullptr, nullptr, nullptr, &heap);
  LOG(INFO) << "Check" << endl;
  engine.bpm_->CheckAllUnpinned();
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    table_heap->InsertTuple(row, nullptr);
    row_values[row.GetRowId().Get()] = fields;
    delete[] characters;
  }
  engine.bpm_->CheckAllUnpinned();
  ASSERT_EQ(row_nums, row_values.size());
  for (auto row_kv : row_values) {
    Row row(RowId(row_kv.first));
    // LOG(INFO) << "test page id: " << row.GetRowId().GetPageId()  << endl;
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    // delete row_kv.second;
  }
  LOG(INFO) << table_heap->GetFirstPageId() << std::endl;
  auto page = reinterpret_cast<TablePage *>(engine.bpm_->FetchPage(table_heap->GetFirstPageId()));
  auto rid = new RowId();
  page->WLatch();
  page->GetFirstTupleRid(rid);
  page->WUnlatch();
  engine.bpm_->UnpinPage(page->GetPageId(), true);

  LOG(INFO) << rid->GetPageId() << " " << rid->GetSlotNum() << std::endl;
  auto it(table_heap->Begin(nullptr));
  LOG(INFO) << "begin: " << it->GetRowId().GetPageId() << " " << it->GetRowId().GetSlotNum() << std::endl;
  it = table_heap->End();
  LOG(INFO) << "end: " << it->GetRowId().GetPageId() << " " << it->GetRowId().GetSlotNum() << std::endl;

  int loop_cnt = 100;
  for (auto it = table_heap->Begin(nullptr); it != table_heap->End() && loop_cnt; it++, loop_cnt--) {
    Row row = *it;
    // LOG(INFO) << "RowId: " << row.GetRowId().GetPageId() << ", " << row.GetRowId().GetSlotNum() << std::endl;
    table_heap->GetTuple(&row, nullptr);
    std::vector<Field *> fields = row.GetFields();
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    // LOG(INFO) << "fields count: " << schema.get()->GetColumnCount() << ", " << row.GetFields().size() << std::endl;
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(*fields[j]));
      ASSERT_EQ(0, strcmp(row.GetField(j)->GetData(), fields[j]->GetData()));
      // LOG(INFO) << "j = " << j << ", " << row.GetField(j)->GetData() << ", " << fields[j]->GetData() << std::endl;
    }
  }
  
  for (int i = 0; i < 20; i++) {
    RowId rid(2, i);
    Row row(rid);
    table_heap->GetTuple(&row, nullptr);
    table_heap->MarkDelete(rid, nullptr);
    table_heap->ApplyDelete(rid, nullptr);
  }

  loop_cnt = 20;
  for (auto it = table_heap->Begin(nullptr); it != table_heap->End() && loop_cnt; it++, loop_cnt--) {
    Row row = *it;
    LOG(INFO) << "RowId: " << row.GetRowId().GetPageId() << ", " << row.GetRowId().GetSlotNum() << std::endl;
    table_heap->GetTuple(&row, nullptr);
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      LOG(INFO) << row.GetField(j)->GetData();
    }
    LOG(INFO) << std::endl;
  }
}