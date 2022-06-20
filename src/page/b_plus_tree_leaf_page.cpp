#include <algorithm>
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_leaf_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

 /**
  * Init method after creating a new leaf page
  * Including set page type, set current size to zero, set page id/parent id, set
  * next page id and set max size
  */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetLSN();
  SetSize(0);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType& key, const KeyComparator& comparator) const {
  /*
  for (int i = 0; i < GetMaxSize(); i++)
    if (comparator(array_[i].first, key) >= 0)
      return i;
  return 0;
  */
  int start = 0, end = GetSize(), mid;
  while (start < end) {
    mid = start + (end - start) / 2;
    if (comparator(array_[mid].first, key) < 0)
      start = mid + 1;
    else end = mid;
  }
  return start;
}

/**
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  ASSERT(0 <= index && index < GetMaxSize(), "index invalid");
  KeyType key{ array_[index].first };
  return key;
}

/**
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType& B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  ASSERT(0 <= index && index < GetMaxSize(), "index invalid");
  return array_[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
 /**
  * Insert key & value pair into leaf page ordered by key
  * @return page size after insertion
  */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType& key, const ValueType& value, const KeyComparator& comparator) {
  int key_index = KeyIndex(key, comparator);
  if (key_index == GetSize()) {
    array_[key_index].first = key;
    array_[key_index].second = value;
    this->IncreaseSize(1);
    return this->GetSize();
  }
  if (comparator(array_[key_index].first, key) == 0) {
    return this->GetSize();
  }

  for (int i = GetSize() - 1; i >= key_index; i--) {
    array_[i + 1].first = array_[i].first;
    array_[i + 1].second = array_[i].second;
  }
  array_[key_index].first = key;
  array_[key_index].second = value;
  this->IncreaseSize(1);
  return this->GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
 /**
  * Remove half of key & value pairs from this page to "recipient" page
  * NOTE: Without process next_page
  */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage* recipient) {
  recipient->CopyNFrom(array_ + this->GetMinSize(), GetMaxSize() - GetMinSize());
  this->IncreaseSize(GetMinSize() - GetMaxSize());
}

/**
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType* items, int size) {
  int curr_size = this->GetSize();
  this->IncreaseSize(size);
  ASSERT(array_ + curr_size >= items + size || array_ + curr_size + size <= items, "address should not overlapped");
  for (int i = 0; i < size; i++) {
    this->array_[curr_size + i].first = items[i].first;
    this->array_[curr_size + i].second = items[i].second;
  }
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
 /**
  * For the given key, check to see whether it exists in the leaf page. If it
  * does, then store its corresponding value in input "value" and return true.
  * If the key does not exist, then return false
  */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType& key, ValueType& value, const KeyComparator& comparator) const {
  int start = 0, end = GetSize() - 1;
  while (start <= end) {
    int mid = start + (end - start) / 2;
    if (comparator(key, array_[mid].first) == 0) {
      value = array_[mid].second;
      return true;
    }
    else if (comparator(key, array_[mid].first) < 0)
      end = mid - 1;
    else start = mid + 1;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
 /**
  * First look through leaf page to see whether delete key exist or not. If
  * exist, perform deletion, otherwise return immediately.
  * NOTE: ensure all key & value pairs store in continuous storage.
  * @return node size after deletion
  */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType& key, const KeyComparator& comparator) {
  int key_index = KeyIndex(key, comparator);
  if (key_index == GetSize() || comparator(key, KeyAt(key_index)) != 0) {
    ASSERT(false, "Leaf::RemoveAndDeleteRecord: not found! ");
  }
  else {
    for (int i = key_index; i < GetSize() - 1; i++) {
      array_[i].first = array_[i + 1].first;
      array_[i].second = array_[i + 1].second;
    }
    IncreaseSize(-1);
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
 /**
  * Remove all of key & value pairs from this page to "recipient" page. Don't forget
  * to update the next_page id in the sibling page
  * NOTE: Should process next page id !!
  */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage* recipient) {
  recipient->CopyNFrom(array_, GetSize());
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
 /**
  * Remove the first key & value pair from this page to end of "recipient" page.
  *
  */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage* recipient) {
  recipient->CopyLastFrom(array_[0]);
  for (int i = 0; i < GetSize() - 1; i++) {
    array_[i].first = array_[i + 1].first;
    array_[i].second = array_[i + 1].second;
  }
  this->IncreaseSize(-1);
}

/**
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType& item) {
  IncreaseSize(1);
  array_[GetSize() - 1].first = item.first;
  array_[GetSize() - 1].second = item.second;
}

/**
 * Remove the last key & value pair from this page to front of "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage* recipient) {
  recipient->CopyFirstFrom(array_[GetSize() - 1]);
  this->IncreaseSize(-1);
}

/**
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType& item) {
  IncreaseSize(1);
  for (int i = GetSize() - 1; i > 0; i--) {
    array_[i].first = array_[i - 1].first;
    array_[i].second = array_[i - 1].second;
  }
  array_[0].first = item.first;
  array_[0].second = item.second;
}

template
class BPlusTreeLeafPage<int, int, BasicComparator<int>>;

template
class BPlusTreeLeafPage<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTreeLeafPage<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTreeLeafPage<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTreeLeafPage<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTreeLeafPage<GenericKey<64>, RowId, GenericComparator<64>>;