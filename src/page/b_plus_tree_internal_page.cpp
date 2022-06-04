#include "page/b_plus_tree_internal_page.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
 /*
  * Init method after creating a new internal page
  * Including set page type, set current size, set page id, set parent id and set
  * max page size
  */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetLSN();
  SetSize(0);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
  array_[0].first = INVALID_PAGE_ID;
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  ASSERT(0 <= index && index < GetMaxSize(), "index invalid");
  return this->array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType& key) {
  ASSERT(0 <= index && index < GetMaxSize(), "index invalid");
  this->array_[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType& value) const {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return 0;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  ASSERT(0 <= index && index < GetMaxSize(), "index invalid");
  return array_[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
 /*
  * Find and return the child pointer(page_id) which points to the child page
  * that contains input "key"
  * Start the search from the second key(the first key should always be invalid)
  */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType& key, const KeyComparator& comparator) const {
  int i;
  for (i = 1; i < GetMaxSize(); i++) {
    if (comparator(array_[i].first, key) > 0) {
      break;
    }
  }
  ValueType val = array_[i - 1].second;
  return val;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
 /*
  * Populate new root page with old_value + new_key & new_value
  * When the insertion cause overflow from leaf page all the way upto the root
  * page, you should create a new root page and populate its elements.
  * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
  */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType& old_value, const KeyType& new_key,
  const ValueType& new_value) {
  array_[0].second = old_value;
  array_[1].first = new_key;
  array_[1].second = new_value;
  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType& old_value, const KeyType& new_key,
  const ValueType& new_value) {
  int index = ValueIndex(old_value);
  IncreaseSize(1);
  for (int i = GetSize() - 1; i > index + 1; i--) {
    array_[i].first = array_[i - 1].first;
    array_[i].second = array_[i - 1].second;
  }
  array_[index + 1].first = new_key;
  array_[index + 1].second = new_value;

  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
 /*
  * Remove half of key & value pairs from this page to "recipient" page
  */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage* recipient,
  BufferPoolManager* buffer_pool_manager) {
  recipient->CopyNFrom(array_ + this->GetMinSize(), GetSize() - GetMinSize(), buffer_pool_manager);
  this->IncreaseSize(GetMinSize() - GetSize());
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType* items, int size, BufferPoolManager* buffer_pool_manager) {
  int curr_size = GetSize();
  IncreaseSize(size);

  for (int i = 0; i < size; i++) {
    this->array_[curr_size + i].first = items[i].first;
    this->array_[curr_size + i].second = items[i].second;
    page_id_t page_id = items[i].second;
    auto* page = buffer_pool_manager->FetchPage(page_id);
    if (page != nullptr) {
      auto* node = reinterpret_cast<BPlusTreePage*>(page->GetData());
      node->SetParentPageId(this->GetPageId());
      buffer_pool_manager->UnpinPage(page_id, true);
    }
  }

}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
 /*
  * Remove the key & value pair in internal page according to input index(a.k.a
  * array offset)
  * NOTE: store key&value pair continuously after deletion
  */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for (int i = index; i < GetSize() - 1; i++) {
    array_[i].first = array_[i + 1].first;
    array_[i].second = array_[i + 1].second;
  }
  array_[0].first = INVALID_PAGE_ID;
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  ValueType val{ array_[0].second };
  SetSize(0);
  return val;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
 /*
  * Remove all of key & value pairs from this page to "recipient" page.
  * The middle_key is the separation key you should get from the parent. You need
  * to make sure the middle key is added to the recipient to maintain the invariant.
  * You also need to use BufferPoolManager to persist changes to the parent page id for those
  * pages that are moved to the recipient
  */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage* recipient, const KeyType& middle_key,
  BufferPoolManager* buffer_pool_manager) {
  SetKeyAt(0, middle_key);
  recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
 /*
  * Remove the first key & value pair from this page to tail of "recipient" page.
  *
  * The middle_key is the separation key you should get from the parent. You need
  * to make sure the middle key is added to the recipient to maintain the invariant.
  * You also need to use BufferPoolManager to persist changes to the parent page id for those
  * pages that are moved to the recipient
  */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage* recipient, const KeyType& middle_key,
  BufferPoolManager* buffer_pool_manager) {
  SetKeyAt(0, middle_key);
  recipient->CopyLastFrom(array_[0], buffer_pool_manager);
  Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType& pair, BufferPoolManager* buffer_pool_manager) {
  IncreaseSize(1);
  array_[GetSize()].first = pair.first;
  array_[GetSize()].second = pair.second;

  page_id_t page_id = pair.second;
  auto* page = buffer_pool_manager->FetchPage(page_id);
  if (page != nullptr) {
    auto* node = reinterpret_cast<BPlusTreePage*>(page->GetData());
    node->SetParentPageId(this->GetPageId());
    buffer_pool_manager->UnpinPage(page_id, true);
  }
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage* recipient, const KeyType& middle_key,
  BufferPoolManager* buffer_pool_manager) {
  recipient->SetKeyAt(0, middle_key);
  recipient->CopyFirstFrom(array_[GetSize() - 1], buffer_pool_manager);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType& pair, BufferPoolManager* buffer_pool_manager) {
  IncreaseSize(1);
  for (int i = GetSize() - 1; i > 0; i--) {
    array_[i].first = array_[i - 1].first;
    array_[i].second = array_[i - 1].second;
  }
  array_[0].second = pair.second;
  //the key used to be invalid and now not
  page_id_t page_id = array_[1].second;
  auto* page = buffer_pool_manager->FetchPage(page_id);
  if (page != nullptr) {
    auto* node = reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());
    array_[1].first = node->array_[1].first;
    buffer_pool_manager->UnpinPage(page_id, true);
  }
  //'adopt' pair by changing its parent page id
  page_id = pair.second;
  page = buffer_pool_manager->FetchPage(page_id);
  if (page != nullptr) {
    auto* node = reinterpret_cast<BPlusTreePage*>(page->GetData());
    node->SetParentPageId(this->GetPageId());
    buffer_pool_manager->UnpinPage(page_id, true);
  }
}

template class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;