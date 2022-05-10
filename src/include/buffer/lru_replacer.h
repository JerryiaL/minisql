#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <list>
#include <mutex>
#include <unordered_set>
#include <vector>
#include <unordered_map>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

class doubleLinkedListNode{
  public:
    doubleLinkedListNode();
    doubleLinkedListNode(frame_id_t frame_id);
    ~doubleLinkedListNode();
    friend class LRUReplacer;

  private:
    doubleLinkedListNode* prior;
    doubleLinkedListNode* next;
    frame_id_t data;
};

/**
 * @brief LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
public:
  /**
   * @brief Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  /**
   * @brief 替换（即删除）与所有被跟踪的页相比最近最少被访问的页，
   * 将其页帧号（即数据页在Buffer Pool的Page数组中的下标）存储在输出参数frame_id中输出并返回true，
   * 如果当前没有可以替换的元素则返回false
   * 
   * @param frame_id 
   */
  bool Victim(frame_id_t *frame_id) override;

  /**
   * @brief 将数据页固定使之不能被Replacer替换，
   * 即从lru_list_中移除该数据页对应的页帧。
   * Pin函数应当在一个数据页被Buffer Pool Manager固定时被调用
   * 
   * @param frame_id 
   */
  void Pin(frame_id_t frame_id) override;

  /**
   * @brief 将数据页解除固定，放入lru_list_中，使之可以在必要时被Replacer替换掉。
   * Unpin函数应当在一个数据页的引用计数变为0时被Buffer Pool Manager调用，
   * 使页帧对应的数据页能够在必要时被替换；
   * 
   * @param frame_id 
   */
  void Unpin(frame_id_t frame_id) override;

  /**
   * @brief 此方法返回当前LRUReplacer中能够被替换的数据页的数量
   * 
   * @return size_t 
   */
  size_t Size() override;

private:
  // add your own private member variables here
  size_t max_pages;
  doubleLinkedListNode* head;
  doubleLinkedListNode* tail;
  std::unordered_map<frame_id_t, doubleLinkedListNode*> lru_map_;
};

#endif  // MINISQL_LRU_REPLACER_H
