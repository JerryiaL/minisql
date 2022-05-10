#include "page/bitmap_page.h"
template<size_t PageSize>
#define FILENAME "data"
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (page_allocated_ == MAX_CHARS) {
    return false;
  }
  bytes[next_free_page_] = 1;
  page_allocated_++;
  page_offset = next_free_page_;
  if (page_allocated_ < MAX_CHARS) {
    for (size_t i = next_free_page_; i < MAX_CHARS; i++) {
      if (bytes[i] == 0){
        next_free_page_ = i;
        break;
	  }
	}
  }
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (bytes[page_offset] == 0) return false;
  bytes[page_offset] = 0;
  page_allocated_--;
  next_free_page_ = std::min(next_free_page_, page_offset);
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if (bytes[page_offset] == 0)
    return true;
  else
    return false;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  int offset = byte_index * 8 + bit_index;
  if (bytes[offset] == 0)
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