#include "page/bitmap_page.h"
#define FILEPATH "data"
template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  char PageBuffer[PageSize];
  lseek(FILEPATH, 0, SEEK_SET);
  read(FILEPATH, PageBuffer, PageSize);
  int index = *((int *)PageBuffer);
  if (index == 0) {
    index = lseek(FILEPATH, 0, SEEK_END)/PageSize;
	memset(PageBuffer, 0, sizeof(char));
    int *allocated = (int *)PageBuffer + 1;
    *allocated = 1;
	write(FILEPATH, PageBuffer, PageSize);
  } else {
    lseek(FILEPATH, index * PageSize, SEEK_SET);
    read(FILEPATH, PageBuffer, PageSize);
    int *allocated = (int *)PageBuffer + 1;
    *allocated = 1;
    lseek(FILEPATH, 0, SEEK_SET);
    write(FILEPATH, PageBuffer, PageSize);
  }
  
  page_offset = index;
  
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  char PageBuffer[PageSize];
  lseek(FILEPATH, 0, SEEK_SET);
  read(FILEPATH, PageBuffer, PageSize);
  int *allocated = (int *)PageBuffer + 1;
  *allocated = 0;
  lseek(FILEPATH, page_offset * PageSize, SEEK_SET);
  write(FILEPATH, PageBuffer, PageSize);
  int *index = (int *)PageBuffer;
  *index = page_offset;
  lseek(FILEPATH, 0, SEEK_SET);
  write(FILEPATH, PageBuffer, PageSize);
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  char PageBuffer[PageSize];
  lseek(FILEPATH, page_offset * PageSize, SEEK_SET);
  read(FILEPATH, PageBuffer, PageSize);
  int allocated = *((int *)PageBuffer * +1);
  if (allocated == 0) return true;
  return false;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
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