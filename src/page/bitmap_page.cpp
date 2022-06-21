#include "page/bitmap_page.h"
#include "glog/logging.h"
template<size_t PageSize>
#define FILENAME "data"
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  // 如果已分配满
  if (page_allocated_ == 8*MAX_CHARS) {
    return false;
  }
  // 分配next free page
  int byteindex = next_free_page_/8;
  int bitindex = next_free_page_%8;
  int tmp = 1<<bitindex;
  bytes[byteindex] |= tmp;
  page_allocated_++;
  page_offset = next_free_page_;
  // 重新设置next free page 
  if (page_allocated_ < 8*MAX_CHARS) {
    for (size_t i = next_free_page_; i < 8*MAX_CHARS; i++) {
      byteindex = i/8;
      bitindex = i%8;
      tmp = 1<<bitindex;
      if ((bytes[byteindex] & tmp) == 0){
        next_free_page_ = i;
        break;
	  }
	}
  }
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  int byteindex = page_offset/8;
  int bitindex = page_offset%8;
  int tmp = 1<<bitindex;
  // 如果已经空闲，则返回false
  if ((bytes[byteindex] & tmp) == 0) return false;
  // 回收页
  tmp = ~tmp;
  bytes[byteindex] &= tmp;
  // 更新metadata
  page_allocated_--;
  next_free_page_ = std::min(next_free_page_, page_offset);
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  // 转换计算地址
  int byteindex = page_offset/8;
  int bitindex = page_offset%8;
  int tmp = 1<<bitindex;
  // 检查是否空闲
  if ((bytes[byteindex] & tmp) == 0)
    return true;
  else
    return false;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  // 检查是否空闲
  int tmp = 1<<bit_index;
  if ((bytes[byte_index] & tmp) == 0)
    return true;
  else
    return false;
}

template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;