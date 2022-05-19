#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"
#include "glog/logging.h"

TableIterator::TableIterator(TableHeap *table_heap, Row row) : table_heap_(table_heap) {
  row_ = new Row(row.GetRowId());
  LOG(INFO) << "it construct: " << row_->GetRowId().GetPageId() << " " << row_->GetRowId().GetSlotNum() << std::endl;
}

TableIterator::TableIterator(const TableIterator &other) : table_heap_(other.table_heap_) {
  row_ = new Row(other.row_->GetRowId());
  LOG(INFO) << "it copy: " << row_->GetRowId().GetPageId() << " " << row_->GetRowId().GetSlotNum() << std::endl;
}

TableIterator::~TableIterator() {}

bool TableIterator::operator == (const TableIterator &itr) const {
  if (table_heap_ == itr.table_heap_ && row_->GetRowId() == itr.row_->GetRowId()) 
    return true;
  return false;
}

bool TableIterator::operator != (const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  return *row_;
}

Row *TableIterator::operator->() {
  return row_;
}

TableIterator &TableIterator::operator++() {
  RowId cur_rid = row_->GetRowId();
  RowId *next_rid = new RowId();
  TablePage* page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(cur_rid.GetPageId()));
  page->GetNextTupleRid(cur_rid, next_rid);
  if (next_rid->GetPageId() == INVALID_PAGE_ID) {
    auto next_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
    next_page->GetFirstTupleRid(next_rid);
  }
  row_->SetRowId(*next_rid);
  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator ret_val = *this;
  ++(*this);
  return ret_val;
  // return TableIterator();
}
