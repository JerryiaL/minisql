#include <stdexcept>
#include <sys/stat.h>

#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"


DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
  A = new DiskFileMetaPage;
}


void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

page_id_t DiskManager::AllocatePage() {
  page_id_t logical_id = 0;
  if(A->num_extents_ == 0) A->num_extents_ = 1;
  //printf("b1\n");
  while(!IsPageFree(logical_id)){
    logical_id++;
  }//printf("b2\n");
  if(A->num_extents_ < logical_id/BITMAP_SIZE + 1) 
  A->num_extents_= logical_id/BITMAP_SIZE + 1;
  A->num_allocated_pages_ = A->num_allocated_pages_ + 1;
  A->extent_used_page_[logical_id/BITMAP_SIZE] = logical_id%BITMAP_SIZE + 1;
  // printf("logical_id = %d\n",logical_id);
  // printf("A->extent_used_page_[logical_id/BITMAP_SIZE] = %d\n",A->extent_used_page_[logical_id/BITMAP_SIZE]);
  uint32_t extent_id = logical_id / BITMAP_SIZE ,index = logical_id % BITMAP_SIZE ;
  B[extent_id].AllocatePage(index);
  //write bitmap page
  char temp_char[PAGE_SIZE];
  uint32_t page_allocated_ = B[extent_id].Get_page_allocated_();
  temp_char[0] = page_allocated_; page_allocated_ = page_allocated_ >> 8;
  temp_char[1] = page_allocated_; page_allocated_ = page_allocated_ >> 8;
  temp_char[2] = page_allocated_; page_allocated_ = page_allocated_ >> 8;
  temp_char[3] = page_allocated_;
  uint32_t next_free_page_ = B[extent_id].Get_next_free_page_();
  temp_char[4] = next_free_page_; next_free_page_ = next_free_page_ >> 8;
  temp_char[5] = next_free_page_; next_free_page_ = next_free_page_ >> 8;
  temp_char[6] = next_free_page_; next_free_page_ = next_free_page_ >> 8;
  temp_char[7] = next_free_page_;
  for (uint32_t i = 0; i < PAGE_SIZE- 2 * sizeof(uint32_t) ; i++){
    temp_char[i + 2 * sizeof(uint32_t)] = B[extent_id].IsPageFree(i + 2 * sizeof(uint32_t));
  }
  WritePhysicalPage(1 + extent_id*(BITMAP_SIZE + 1), temp_char);
  //write meta page
  u_int32_t a;
  a = A->num_allocated_pages_;
  meta_data_[0] = a; a = a >> 8;
  meta_data_[1] = a; a = a >> 8;
  meta_data_[2] = a; a = a >> 8;
  meta_data_[3] = a; 

  a = A->num_extents_;
  meta_data_[4] = a; a = a >> 8;
  meta_data_[5] = a; a = a >> 8;
  meta_data_[6] = a; a = a >> 8;
  meta_data_[7] = a;
  for(uint32_t i = 0 ; i < A->num_extents_; i++){
    a = A->extent_used_page_[i];
    meta_data_[8+i*4] = a; a = a >> 8;
    meta_data_[9+i*4] = a; a = a >> 8;
    meta_data_[10+i*4] = a; a = a >> 8;
    meta_data_[11+i*4] = a;
  }
  WritePhysicalPage(META_PAGE_ID, meta_data_);  
  return logical_id;
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  page_id_t physical_page_id = MapPageId(logical_page_id);
  // bool j = ((physical_page_id + 1)*PAGE_SIZE <= GetFileSize(file_name_));
  // ASSERT(j, "Not invalid logical_page_id.");

  uint32_t extent_id = logical_page_id / BITMAP_SIZE ,index = logical_page_id % BITMAP_SIZE ;
  char page_data[PAGE_SIZE];
  memset(page_data, 0, PAGE_SIZE);
  WritePhysicalPage(physical_page_id,page_data);
  B[extent_id].DeAllocatePage(index);
  A->extent_used_page_[extent_id] -= 1;
  A->num_allocated_pages_ = A->num_allocated_pages_ - 1;
  //write bitmap page  
  char temp_char[PAGE_SIZE];
  uint32_t page_allocated_ = B[extent_id].Get_page_allocated_();
  temp_char[0] = page_allocated_; page_allocated_ = page_allocated_ >> 8;
  temp_char[1] = page_allocated_; page_allocated_ = page_allocated_ >> 8;
  temp_char[2] = page_allocated_; page_allocated_ = page_allocated_ >> 8;
  temp_char[3] = page_allocated_;
  uint32_t next_free_page_ = B[extent_id].Get_next_free_page_();
  temp_char[4] = next_free_page_; next_free_page_ = next_free_page_ >> 8;
  temp_char[5] = next_free_page_; next_free_page_ = next_free_page_ >> 8;
  temp_char[6] = next_free_page_; next_free_page_ = next_free_page_ >> 8;
  temp_char[7] = next_free_page_;
  for (uint32_t i = 0; i < PAGE_SIZE- 2 * sizeof(uint32_t) ; i++){
    temp_char[i + 2 * sizeof(uint32_t)] = B[extent_id].IsPageFree(i + 2 * sizeof(uint32_t));
  }
  WritePhysicalPage(1 + extent_id*(BITMAP_SIZE + 1), temp_char);
  //write meta page
  u_int32_t a;
  a = A->num_allocated_pages_;
  meta_data_[0] = a; a = a >> 8;
  meta_data_[1] = a; a = a >> 8;
  meta_data_[2] = a; a = a >> 8;
  meta_data_[3] = a; 

  a = A->num_extents_;
  meta_data_[4] = a; a = a >> 8;
  meta_data_[5] = a; a = a >> 8;
  meta_data_[6] = a; a = a >> 8;
  meta_data_[7] = a;
  for(uint32_t i = 0 ; i < A->num_extents_; i++){
    a = A->extent_used_page_[i];
    meta_data_[8+i*4] = a; a = a >> 8;
    meta_data_[9+i*4] = a; a = a >> 8;
    meta_data_[10+i*4] = a; a = a >> 8;
    meta_data_[11+i*4] = a;
  }
  WritePhysicalPage(META_PAGE_ID, meta_data_);
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  //printf("IsPageFree(logical_page_id=%d)",logical_page_id);
  uint32_t extent_id = 0,index = logical_page_id % BITMAP_SIZE ;
  uint32_t n = A->GetExtentNums();
  extent_id = logical_page_id / BITMAP_SIZE ;
  if(extent_id >= n){
    if(extent_id > MAX_EXTENT){
      printf("over the range ");
      return false;
    }
    A->num_extents_ += 1;
    extent_id++; 
  }
  return B[extent_id].IsPageFree(index);
  
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  page_id_t physical_page_id = 0;
  uint32_t extent_id = logical_page_id / BITMAP_SIZE ;
  uint32_t index = logical_page_id % BITMAP_SIZE ;
  physical_page_id = extent_id*(BITMAP_SIZE + 1) + index + 2;

  return physical_page_id;
  

}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}


char *DiskManager::GetMetaData() {
  u_int32_t a;
  a = A->num_allocated_pages_;
  meta_data_[0] = a; a = a >> 8;
  meta_data_[1] = a; a = a >> 8;
  meta_data_[2] = a; a = a >> 8;
  meta_data_[3] = a; 

  a = A->num_extents_;
  meta_data_[4] = a; a = a >> 8;
  meta_data_[5] = a; a = a >> 8;
  meta_data_[6] = a; a = a >> 8;
  meta_data_[7] = a;
  for(uint32_t i = 0 ; i < A->num_extents_; i++){
    a = A->extent_used_page_[i];
    meta_data_[8+i*4] = a; a = a >> 8;
    meta_data_[9+i*4] = a; a = a >> 8;
    meta_data_[10+i*4] = a; a = a >> 8;
    meta_data_[11+i*4] = a;
  }
  
  return meta_data_;
}