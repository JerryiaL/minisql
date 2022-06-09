#include "storage/table_heap.h"
#include "glog/logging.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  page_id_t page_id = first_page_id_;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if (page == nullptr) {
    return false;
  }
  page->WLatch();
  bool f = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  if (f == true) {
    return true;
  } else {
    while (page->GetNextPageId() != -1) {
      page_id = page->GetNextPageId();
      page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
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
    page_id_t pre_id = page_id;
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(page_id));
    page->Init(page_id, pre_id, log_manager_, txn);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(pre_id));
    page->WLatch();
    page->SetNextPageId(page_id);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    if (page == nullptr) {
      return false;
    }
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    page->WLatch();
    f = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return f;
  }
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
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

bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false;
  }
  Row old_row(rid);
  page->WLatch();
  UpdateTablePageStatus f = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  if (f == UpdateTablePageStatus::completed)
    return true;
  else if(f == UpdateTablePageStatus::too_much_data){
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    page->WLatch();
    page->ApplyDelete(rid,txn,log_manager_);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return InsertTuple(row, txn);
  }
  else
    return false;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  // buffer_pool_manager_->DeletePage(page->GetTablePageId());
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
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

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  // cout << "GET" << endl;
  page_id_t page_id = row->GetRowId().GetPageId();
  // cout << "get page id: " << page_id << endl;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
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

TableIterator TableHeap::Begin(Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  auto rid = new RowId();
  page->WLatch();
  page->GetFirstTupleRid(rid);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  // LOG(INFO) << "begin: " << rid->GetPageId() << " " << rid->GetSlotNum() << std::endl;
  // LOG(INFO) << "begin: " << Row(*rid).GetRowId().GetPageId() << " " << Row(*rid).GetRowId().GetSlotNum() <<
  // std::endl;
  return TableIterator(this, Row(*rid));
}

TableIterator TableHeap::End() {
  auto rid = new RowId(INVALID_ROWID);
  // LOG(INFO) << "end: " << rid->GetPageId() << " " << rid->GetSlotNum() << std::endl;
  // LOG(INFO) << "end: " << Row(*rid).GetRowId().GetPageId() << " " << Row(*rid).GetRowId().GetSlotNum() << std::endl;
  return TableIterator(this, Row(*rid));
}
