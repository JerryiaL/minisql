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
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_); 
  uint32_t extent_id = 0;
  for(uint32_t i = 0 ; i < MAX_EXTENT;i++){
    if(meta_page->extent_used_page_[i] < BITMAP_SIZE){
      meta_page->extent_used_page_[i]++;
      meta_page->num_allocated_pages_++;
      extent_id = i;
      break;
    }
  }meta_page->num_extents_ = extent_id+1;
  char temp_char[PAGE_SIZE];  
  if(meta_page->extent_used_page_[extent_id] == 0){
    //create a bitmap_page
    memset(temp_char, 0, PAGE_SIZE);
    WritePhysicalPage(extent_id*(BITMAP_SIZE+1)+1,temp_char);   
  }
  //read bitmap page  
  ReadPhysicalPage(extent_id*(BITMAP_SIZE+1)+1,temp_char);
  BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(temp_char);
  uint32_t index ;
  bitmap_page->AllocatePage(index);
  //std::cout << "index = " << index << std::endl;
  logical_id = extent_id*BITMAP_SIZE + index;
  //write bitmap page
  memcpy(temp_char,bitmap_page,PAGE_SIZE);
  WritePhysicalPage(1 + extent_id*(BITMAP_SIZE + 1), temp_char);
  //store meta_page
  memcpy(meta_data_,meta_page,PAGE_SIZE);
  return logical_id;
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  page_id_t physical_page_id = MapPageId(logical_page_id);
  uint32_t extent_id = logical_page_id / BITMAP_SIZE ,index = logical_page_id % BITMAP_SIZE ;
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_); 
  if(meta_page->extent_used_page_[extent_id] > 0){
    meta_page->extent_used_page_[extent_id]-- ;
    meta_page->num_allocated_pages_--;    
  }else{
    return ;
  }
  char page_data[PAGE_SIZE];
  memset(page_data, 0, PAGE_SIZE);
  WritePhysicalPage(physical_page_id,page_data);
  //read bitmap page  
  char temp_char[PAGE_SIZE];
  ReadPhysicalPage(1 + extent_id*(BITMAP_SIZE + 1),temp_char);
  BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(temp_char);  
  bitmap_page->DeAllocatePage(index);
  //write bitmap page
  memcpy(temp_char,bitmap_page,PAGE_SIZE);
  WritePhysicalPage(1 + extent_id*(BITMAP_SIZE + 1), temp_char);
  //store meta_page
  memcpy(meta_data_,meta_page,PAGE_SIZE);
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  char page_data[PAGE_SIZE];
  uint32_t extent_id = logical_page_id / BITMAP_SIZE, index = logical_page_id % BITMAP_SIZE ;;
  if(extent_id > MAX_EXTENT){
    printf("Extent_nums: over the range ");
    return false;
  }
  //read bitmap page  
  ReadPhysicalPage(1 + extent_id*(BITMAP_SIZE + 1), page_data);
  BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(page_data);

  return bitmap_page->IsPageFree(index);
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
  return meta_data_;
}