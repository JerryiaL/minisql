#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
  : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_]; // pages (buffer pool) is empty
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  } // all the pages are free
}

BufferPoolManager::~BufferPoolManager() {
  FlushAllPages();
  delete[] pages_;
  delete replacer_;
}

Page* BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  auto result = page_table_.find(page_id);
  // Pinned and unpinned pages probably exist in page table
  if (result != page_table_.end()) {
    frame_id_t frame_id = result->second;
    Page* p = pages_ + frame_id; // find the page
    p->pin_count_++;
    replacer_->Pin(frame_id); // pin the page, because it might be unpinned
    return p;
  }

  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  frame_id_t frame_id = INVALID_FRAME_ID;
  Page* r = nullptr;
  // get it from free_list
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
    assert(frame_id >= 0 && frame_id < static_cast<int>(pool_size_));
  }
  // get it from replacer
  else {
    bool isReplacer = replacer_->Victim(&frame_id); // that unpinned page in replacer has been deleted
    if (!isReplacer) {
      return nullptr;
    }
    assert(frame_id >= 0 && frame_id < static_cast<int>(pool_size_));
  }
  r = pages_ + frame_id;
  r->pin_count_ = 0;

  // 2.     If R is dirty, write it back to the disk.
  if (r->IsDirty()) {
    disk_manager_->WritePage(r->GetPageId(), r->GetData());
    r->is_dirty_ = false;
  }

  // 3.     Delete R from the page table and insert P.
  page_table_.erase(r->GetPageId());
  page_table_[page_id] = frame_id;

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  Page* p = r; // always operate that page address, with page_id changed, data restored
  p->page_id_ = page_id;
  p->ResetMemory();
  disk_manager_->ReadPage(page_id, p->GetData());
  p->pin_count_++;
  return p;
}

Page* BufferPoolManager::NewPage(page_id_t& page_id) {
  // 0.   Make sure you call AllocatePage!
  page_id_t page_id_allocate = AllocatePage();
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  bool isAllPinned = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ == 0) {
      isAllPinned = false;
      break;
    }
  }
  if (isAllPinned) {
    DeallocatePage(page_id_allocate);
    return nullptr;
  }

  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frame_id = INVALID_FRAME_ID;
  Page* p = nullptr;
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
    assert(frame_id >= 0 && frame_id < static_cast<int>(pool_size_));
  }
  // get it from replacer
  else {
    bool isReplacer = replacer_->Victim(&frame_id);
    if (!isReplacer) {
      return nullptr;
    }
    assert(frame_id >= 0 && frame_id < static_cast<int>(pool_size_));
  }
  p = pages_ + frame_id;
  p->pin_count_ = 0;
  // If P is dirty, write it back to the disk.
  if (p->IsDirty()) {
    disk_manager_->WritePage(p->GetPageId(), p->GetData());
    p->is_dirty_ = false;
  }

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  page_table_.erase(p->GetPageId());
  p->page_id_ = page_id_allocate;
  page_table_[page_id_allocate] = frame_id;
  p->ResetMemory();
  p->pin_count_++;

  // 4.   Set the page ID output parameter. Return a pointer to P.
  page_id = page_id_allocate;
  return p;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  DeallocatePage(page_id);
  // 1.   Search the page table for the requested page (P).
  //      If P does not exist, return true.
  auto result = page_table_.find(page_id);
  if (result == page_table_.end()) {
    return true;
  }

  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  else {
    frame_id_t frame_id = result->second;
    Page* p = pages_ + frame_id; // find the page
    if (p->pin_count_ != 0) {
      return false;
    }
    else {
      page_table_.erase(page_id);
      p->pin_count_ = 0;
      p->is_dirty_ = false;
      p->page_id_ = INVALID_PAGE_ID;
      p->ResetMemory();
      free_list_.push_back(frame_id);
      return true;
    }
  }
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  // Process is_dirty lazily when that page be victimized by replacer
  auto result = page_table_.find(page_id);
  if (result == page_table_.end()) {
    return false;
  }
  else {
    frame_id_t frame_id = result->second;
    Page* p = pages_ + frame_id;
    if (p->pin_count_ > 0) p->pin_count_--;

    // Only call replacer's unpin when pin_count = 0
    if (p->pin_count_ == 0) {
      replacer_->Unpin(frame_id);
      if (is_dirty) {
        disk_manager_->WritePage(page_id, p->GetData());
        p->is_dirty_ = false;
      }
    }
    return true;
  }
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  auto result = page_table_.find(page_id);
  if (result == page_table_.end()) {
    return false;
  }
  else {
    frame_id_t frame_id = result->second;
    Page* p = pages_ + frame_id;
    disk_manager_->WritePage(page_id, p->GetData());
    return true;
  }
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug and test
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}

bool BufferPoolManager::FlushAllPages() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
}