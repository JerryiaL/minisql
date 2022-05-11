#ifndef MINISQL_BUFFER_POOL_MANAGER_H
#define MINISQL_BUFFER_POOL_MANAGER_H

#include <list>
#include <mutex>
#include <unordered_map>

#include "buffer/lru_replacer.h"
#include "page/page.h"
#include "page/disk_file_meta_page.h"
#include "storage/disk_manager.h"

using namespace std;

class BufferPoolManager {
public:
  explicit BufferPoolManager(size_t pool_size, DiskManager *disk_manager);

  ~BufferPoolManager();

  /**
   * @brief 根据逻辑页号获取对应的数据页，如果该数据页不在内存中，则需要从磁盘中进行读取
   * 如果空闲页列表（free_list_）中没有可用的页面并且没有可以被替换的数据页，则应返回 nullptr。
   * FlushPage操作应该将页面内容转储到磁盘中，无论其是否被固定。
   * 
   * @param page_id 
   * @return Page* 
   */
  Page *FetchPage(page_id_t page_id);

  /**
   * @brief 取消固定一个数据页
   * 
   * @param page_id 
   * @param is_dirty 
   */
  bool UnpinPage(page_id_t page_id, bool is_dirty);

  /**
   * @brief 将数据页转储到磁盘中
   * 
   * @param page_id
   */
  bool FlushPage(page_id_t page_id);

  /**
   * @brief 分配一个新的数据页，并将逻辑页号于page_id中返回
   * 
   * @param page_id 
   * @return Page* 
   */
  Page *NewPage(page_id_t &page_id);

  /**
   * @brief 释放一个数据页
   * 
   * @param page_id 
   */
  bool DeletePage(page_id_t page_id);

  bool IsPageFree(page_id_t page_id);

  bool CheckAllUnpinned();

  /**
   * @brief 将所有的页面都转储到磁盘中
   */
  bool FlushAllPages();

private:
  /**
   * Allocate new page (operations like create index/table) For now just keep an increasing counter
   */
  page_id_t AllocatePage();

  /**
   * Deallocate page (operations like drop index/table) Need bitmap in header page for tracking pages
   */
  void DeallocatePage(page_id_t page_id);


private:
  size_t pool_size_;                                        // number of pages in buffer pool
  Page *pages_;                                             // array of pages
  DiskManager *disk_manager_;                               // pointer to the disk manager.
  std::unordered_map<page_id_t, frame_id_t> page_table_;    // to keep track of pages
  Replacer *replacer_;                                      // to find an unpinned page for replacement
  std::list<frame_id_t> free_list_;                         // to find a free page for replacement
  recursive_mutex latch_;                                   // to protect shared data structure
};

#endif  // MINISQL_BUFFER_POOL_MANAGER_H
