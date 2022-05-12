#include "page/bitmap_page.h"
#include "glog/logging.h"
template<size_t PageSize>
#define FILENAME "data"
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (page_allocated_ == 8*MAX_CHARS) {
    return false;
  }
  int byteindex = next_free_page_/8;
  int bitindex = next_free_page_%8;
  int tmp = 1<<bitindex;
  bytes[byteindex] |= tmp;
  page_allocated_++;
  page_offset = next_free_page_;
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
  if ((bytes[byteindex] & tmp) == 0) return false;
  tmp = ~tmp;
  bytes[byteindex] &= tmp;
  page_allocated_--;
  next_free_page_ = std::min(next_free_page_, page_offset);
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  
  int byteindex = page_offset/8;
  int bitindex = page_offset%8;
  int tmp = 1<<bitindex;
  if ((bytes[byteindex] & tmp) == 0)
    return true;
  else
    return false;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
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