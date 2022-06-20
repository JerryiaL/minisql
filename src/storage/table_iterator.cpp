#include "storage/table_iterator.h"
#include "common/macros.h"
#include "glog/logging.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(TableHeap* table_heap, Row row) : table_heap_(table_heap) {
  row_ = new Row(row.GetRowId());
}

TableIterator::TableIterator(const TableIterator& other) : table_heap_(other.table_heap_) {
  row_ = new Row(other.row_->GetRowId());
}

TableIterator::~TableIterator() {}

bool TableIterator::operator==(const TableIterator& itr) const {
  if (table_heap_ == itr.table_heap_ && row_->GetRowId() == itr.row_->GetRowId())
    return true;
  return false;
}

bool TableIterator::operator!=(const TableIterator& itr) const { return !(*this == itr); }

const Row& TableIterator::operator*() { return *row_; }

Row* TableIterator::operator->() { return row_; }

TableIterator& TableIterator::operator++() {
  // 1. Try to get next tuple
  RowId cur_rid = row_->GetRowId();
  RowId* next_rid = new RowId();
  TablePage* page = reinterpret_cast<TablePage*>(table_heap_->buffer_pool_manager_->FetchPage(cur_rid.GetPageId()));
  page->WLatch();
  page->GetNextTupleRid(cur_rid, next_rid);
  page->WUnlatch();
  table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);

  // 2. If that page is iterate over, fetch next page, unpin this page
  if (next_rid->GetPageId() == INVALID_PAGE_ID) {
    auto next_page_id = page->GetNextPageId();
    // If no more page, point to invalid row, then return
    if (next_page_id == INVALID_FRAME_ID) {
      row_->SetRowId(INVALID_ROWID);
      return *this;
    }
    auto next_page = reinterpret_cast<TablePage*>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
    next_page->WLatch();
    next_page->GetFirstTupleRid(next_rid);
    next_page->WUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(next_page->GetTablePageId(), true);
  }

  row_->SetRowId(*next_rid);
  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator ret_val = *this;
  ++(*this);
  return ret_val;
}
