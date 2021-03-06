#include "storage/table_heap.h"
#include "glog/logging.h"

bool TableHeap::InsertTuple(Row& row, Transaction* txn) {
  // start from the first page
  page_id_t page_id = first_page_id_;
  auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
  if (page == nullptr) {
    return false;
  }
  page->WLatch();
  // f record the result of the insert
  bool f = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  if (f == true) {
    return true;
  }
  else {
    // judge whether existed page have space to insert
    while (page->GetNextPageId() != -1) {
      page_id = page->GetNextPageId();
      page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
      if (page == nullptr) {
        return false;
      }
      page->WLatch();
      bool f = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
      if (f == true) {
        return true;
      }
    }
    // no space int all existed pages
    page_id_t pre_id = page_id;
    // make a new page as next page
    page = reinterpret_cast<TablePage*>(buffer_pool_manager_->NewPage(page_id));
    page->Init(page_id, pre_id, log_manager_, txn);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    // update the pre_page
    page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(pre_id));
    page->WLatch();
    page->SetNextPageId(page_id);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    if (page == nullptr) {
      return false;
    }
    // insert the tuple into the new page
    page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
    page->WLatch();
    f = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return f;
  }
}

bool TableHeap::MarkDelete(const RowId& rid, Transaction* txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(Row& row, const RowId& rid, Transaction* txn) {
  // use rid to find the page of old record
  auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false;
  }
  Row old_row(rid);
  page->WLatch();
  UpdateTablePageStatus f = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  if (f == UpdateTablePageStatus::completed) // update success
    return true;
  else if (f == UpdateTablePageStatus::too_much_data) { // new data is too much
    page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    page->WLatch();
    page->ApplyDelete(rid, txn, log_manager_); // delete old record
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true); // insert new record
    return InsertTuple(row, txn);
  }
  else
    return false;
}

void TableHeap::ApplyDelete(const RowId& rid, Transaction* txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  // buffer_pool_manager_->DeletePage(page->GetTablePageId());
}

void TableHeap::RollbackDelete(const RowId& rid, Transaction* txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  delete buffer_pool_manager_;
  delete schema_;
}

bool TableHeap::GetTuple(Row* row, Transaction* txn) {
  page_id_t page_id = row->GetRowId().GetPageId();
  auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
  if (page == nullptr) {
    return false;
  }
  bool f;
  page->WLatch();
  f = page->GetTuple(row, schema_, txn, lock_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return f;
}

TableIterator TableHeap::Begin(Transaction* txn) {
  // iterator point to first_page_id_'s first row
  auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(first_page_id_));
  auto rid = new RowId();
  page->WLatch();
  page->GetFirstTupleRid(rid);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return TableIterator(this, Row(*rid));
}

TableIterator TableHeap::End() {
  // iterator point to invalid row
  auto rid = new RowId(INVALID_ROWID);
  return TableIterator(this, Row(*rid));
}
