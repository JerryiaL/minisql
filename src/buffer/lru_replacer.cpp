#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {
  max_pages = num_pages;
  head = new doubleLinkedListNode();
  tail = new doubleLinkedListNode();
  head->next = tail;
  tail->prior = head;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (lru_map_.empty()) {
    return false;
  }
  auto last = tail->prior;
  tail->prior = last->prior;
  last->prior->next = tail;
  *frame_id = last->data;
  lru_map_.erase(last->data);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (lru_map_.find(frame_id) != lru_map_.end()) {
    auto cur = lru_map_[frame_id];
    cur->prior->next = cur->next;
    cur->next->prior = cur->prior;
    lru_map_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  doubleLinkedListNode* cur;
  if (lru_map_.find(frame_id) != lru_map_.end()) {} 
  else 
  {
    cur = new doubleLinkedListNode(frame_id);
    head->next->prior = cur;
    cur->next = head->next;
    head->next = cur;
    cur->prior = head;
    lru_map_[frame_id] = cur;
  }
}

size_t LRUReplacer::Size() {
  return lru_map_.size();
}

doubleLinkedListNode::doubleLinkedListNode() {
	prior = NULL;
	next = NULL;
}

//结点的有参构造函数，初始化指针域和数据域
doubleLinkedListNode::doubleLinkedListNode(frame_id_t _data) {
	prior = NULL;
	data = _data;//初始化数据域
	next = NULL;
}

doubleLinkedListNode::~doubleLinkedListNode() {
	prior = NULL;
	next = NULL;
}
