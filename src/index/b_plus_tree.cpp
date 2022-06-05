#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager* buffer_pool_manager, const KeyComparator& comparator,
  int leaf_max_size, int internal_max_size)
  : index_id_(index_id),
  root_page_id_(INVALID_PAGE_ID), 
  buffer_pool_manager_(buffer_pool_manager),
  comparator_(comparator),
  leaf_max_size_(leaf_max_size),
  internal_max_size_(internal_max_size) {

}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
  Page* root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage* root_node = reinterpret_cast<BPlusTreePage*>(root_page->GetData());
  if (root_node->IsLeafPage()) {
    buffer_pool_manager_->DeletePage(root_page_id_);
  }
  else {
    for (auto page_id : destroy_internal_pages_id) {
      buffer_pool_manager_->DeletePage(page_id);
    }
  }
  buffer_pool_manager_->UnpinPage(root_page->GetPageId(), false);

  KeyType key;
  Page* leaf_page = FindLeafPage(key, true);
  LeafPage* leaf_node = reinterpret_cast<LeafPage*>(leaf_page->GetData());
  assert(leaf_node->IsLeafPage());
  page_id_t next_id = leaf_node->GetNextPageId();
  while (next_id != INVALID_PAGE_ID) {
    buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

    leaf_page = buffer_pool_manager_->FetchPage(next_id);
    leaf_node = reinterpret_cast<LeafPage*>(leaf_page->GetData());
    assert(leaf_node->IsLeafPage());
    next_id = leaf_node->GetNextPageId();
  }
  buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  UpdateRootPageId(2);
  root_page_id_ = INVALID_PAGE_ID;
  destroy_internal_pages_id.clear();
}

/**
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
 /**
  * Return the only value that associated with input key
  * This method is used for point query
  * @return : true means key exists
  */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType& key, std::vector<ValueType>& result, Transaction* transaction) {
  // ASSERT(root_page_id_ != INVALID_PAGE_ID, "root_page_id is INVALID_PAGE_ID");
  // Page* page = buffer_pool_manager_->FetchPage(root_page_id_);
  // BPlusTreePage* node = reinterpret_cast<BPlusTreePage*>(page->GetData());
  // ASSERT(node->IsRootPage(), "Not Root Page");

  // while (!node->IsLeafPage()) {
  //   InternalPage* internal_node = reinterpret_cast<InternalPage*>(page->GetData());
  //   page_id_t next_page_id = internal_node->Lookup(key, comparator_);
  //   buffer_pool_manager_->UnpinPage(internal_node->GetPageId(), false);
  //   page = buffer_pool_manager_->FetchPage(next_page_id);
  //   node = reinterpret_cast<BPlusTreePage*>(page->GetData());
  // }

  // LeafPage* leaf_node = reinterpret_cast<LeafPage*>(page->GetData());
  // ValueType value;
  // bool ret = leaf_node->Lookup(key, value, comparator_);
  // result.push_back(value);
  // buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  // return ret;
  LeafPage* leaf_node = reinterpret_cast<LeafPage*>(FindLeafPage(key,false)->GetData());
  ValueType value;
  bool ret = leaf_node->Lookup(key, value, comparator_);
  result.push_back(value);
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
  return ret;  
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
 /**
  * Insert constant key & value pair into b+ tree
  * if current tree is empty, start new tree, update root page id and insert
  * entry, otherwise insert into leaf page.
  * @return: since we only support unique key, if user try to insert duplicate
  * keys return false, otherwise return true.
  */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType& key, const ValueType& value, Transaction* transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/**
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager (
 * NOTICE: throw an "out of memory" exception if returned value is nullptr),
 * then update b+ tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType& key, const ValueType& value) { 
  Page* new_page = buffer_pool_manager_->NewPage(root_page_id_);
  ASSERT(new_page != nullptr, "out of memory");

  LeafPage* leaf_node = reinterpret_cast<LeafPage*>(new_page->GetData());
  leaf_node->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  assert(leaf_node->IsLeafPage());
  UpdateRootPageId(1);
  // root page is the leaf page.
  leaf_node->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/**
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType& key, const ValueType& value, Transaction* transaction) {
  LeafPage* leaf_node = reinterpret_cast<LeafPage*>(FindLeafPage(key, false)->GetData());
  assert(leaf_node->IsLeafPage());
  ValueType lookup_value;
  if (leaf_node->Lookup(key, lookup_value, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    return false;
  }
  if (leaf_node->GetSize() < leaf_node->GetMaxSize()) {
    leaf_node->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
    return true;
  }
  
  LeafPage* new_leaf = Split<LeafPage>(leaf_node);
  if (comparator_(key, new_leaf->KeyAt(0)) < 0) 
    leaf_node->Insert(key, value, comparator_);
  else 
    new_leaf->Insert(key, value, comparator_);
  
  // process next_page
  if (comparator_(leaf_node->KeyAt(0), new_leaf->KeyAt(0)) < 0) {
    new_leaf->SetNextPageId(leaf_node->GetNextPageId());
    leaf_node->SetNextPageId(new_leaf->GetPageId());
  }
  else {
    new_leaf->SetNextPageId(leaf_node->GetPageId());
  }
  InsertIntoParent(leaf_node, new_leaf->KeyAt(0), new_leaf, transaction);

  buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
  return true;
}

/**
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager (
 * NOTICE: throw an "out of memory" exception if returned value is nullptr),
 * then move half of key & value pairs from input page to newly created page
 * NOTE: Without process next_page
 * NOTE: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N* BPLUSTREE_TYPE::Split(N* node) {
  page_id_t new_page_id;
  Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
  ASSERT(new_page != nullptr, "out of memory");

  N* ret_recipient = nullptr;
  if (node->IsLeafPage()) {
    assert(node->IsLeafPage());
    LeafPage* leaf_node = reinterpret_cast<LeafPage *>(node);
    assert(leaf_node->IsLeafPage());

    LeafPage* recipient = reinterpret_cast<LeafPage *>(new_page->GetData());
    recipient->Init(new_page_id, leaf_node->GetParentPageId(), leaf_max_size_);
    leaf_node->MoveHalfTo(recipient);
    ret_recipient = reinterpret_cast<N *>(recipient);
  }
  else {
    assert(!node->IsLeafPage());
    InternalPage* internal_node = reinterpret_cast<InternalPage *>(node);
    assert(!internal_node->IsLeafPage());

    InternalPage* recipient = reinterpret_cast<InternalPage *>(new_page->GetData());
    recipient->Init(new_page_id, internal_node->GetParentPageId(), internal_max_size_);
    internal_node->MoveHalfTo(recipient, buffer_pool_manager_);
    ret_recipient = reinterpret_cast<N *>(recipient);
  }
  return ret_recipient;
}

/**
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage* old_node, const KeyType& key, BPlusTreePage* new_node, Transaction* transaction) {
  if (old_node->IsRootPage()) {
    page_id_t new_page_id;
    Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
    ASSERT(new_page != nullptr, "out of memory");
    assert(new_page->GetPinCount() == 1);

    InternalPage* root = reinterpret_cast<InternalPage*>(new_page->GetData());
    root->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
    root_page_id_ = new_page_id;
  
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    return;
  }

  page_id_t parent_page_id = old_node->GetParentPageId();
  Page* parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  ASSERT(parent_page != nullptr, "out of memory");
  assert(parent_page->GetPinCount() == 1);
  InternalPage* parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  if (parent->GetSize() < parent->GetMaxSize()) {
    parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    new_node->SetParentPageId(parent_page_id);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    return;
  }

  // parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  
  InternalPage* new_parent = Split<InternalPage>(parent); 
  if (comparator_(key, new_parent->KeyAt(0)) < 0) {
    parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    new_node->SetParentPageId(parent->GetPageId());
  }
  else {
    new_parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    new_node->SetParentPageId(new_parent->GetPageId());
    // old_node->SetParentPageId(new_parent->GetPageId());
  }
  InsertIntoParent(parent, new_parent->KeyAt(0), new_parent, transaction);
  
  buffer_pool_manager_->UnpinPage(new_parent->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
 /**
  * Delete key & value pair associated with input key
  * If current tree is empty, return immediately.
  * If not, User needs to first find the right leaf page as deletion target, then
  * delete entry from leaf page. Remember to deal with redistribute or merge if
  * necessary.
  */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType& key, Transaction* transaction) {
  if (root_page_id_ == INVALID_PAGE_ID) return;
  Page* leaf_page = FindLeafPage(key, false);
  LeafPage* leaf_node = reinterpret_cast<LeafPage*>(leaf_page->GetData());
  assert(leaf_node->IsLeafPage());
  ValueType lookup_value;
  if (leaf_node->Lookup(key, lookup_value, comparator_) == false) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return;
  }
  int leaf_size = leaf_node->RemoveAndDeleteRecord(key, comparator_);
  if (leaf_size < leaf_node->GetMinSize()) {
    CoalesceOrRedistribute(leaf_node, transaction);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
}

/**
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N* node, Transaction* transaction) {
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }
  page_id_t parent_id, prev_id, next_id;
  N* prev_node;
  N* next_node;
  Page* prev_page;
  Page* next_page;
  parent_id = node->GetParentPageId();
  Page* parent_page = buffer_pool_manager_->FetchPage(parent_id);
  InternalPage* parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());
  int node_index = parent_node->ValueIndex(node->GetPageId());
  if (node_index > 0) {
    prev_id = parent_node->ValueAt(node_index - 1);
    prev_page = buffer_pool_manager_->FetchPage(prev_id);
    prev_node = reinterpret_cast<N*>(prev_page->GetData());
    if (prev_node->GetSize() > prev_node->GetMinSize()) {
      Redistribute(prev_node, node, 1);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      buffer_pool_manager_->UnpinPage(prev_id, true);
      return false;
    }
  }
  if (node_index != parent_node->GetSize() - 1) {
    next_id = parent_node->ValueAt(node_index + 1);
    next_page = buffer_pool_manager_->FetchPage(next_id);
    next_node = reinterpret_cast<N*>(next_page->GetData());
    if (next_node->GetSize() > next_node->GetMinSize()) {
      Redistribute(next_node, node, 0);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      buffer_pool_manager_->UnpinPage(next_id, true);
      if (node_index > 0) {
        buffer_pool_manager_->UnpinPage(prev_id, false);
      }
      return false;
    }
  }
  if (prev_id != INVALID_PAGE_ID) {
    Coalesce(&prev_node, &node, &parent_node, node_index, transaction);
    buffer_pool_manager_->UnpinPage(parent_id, true);
    buffer_pool_manager_->UnpinPage(prev_id, true);
    if (next_id != INVALID_PAGE_ID) {
      buffer_pool_manager_->UnpinPage(next_id, false);
    }
    return true;
  }
  else {
    Coalesce(&node, &next_node, &parent_node, node_index + 1, transaction);
    buffer_pool_manager_->UnpinPage(parent_id, true);
    buffer_pool_manager_->UnpinPage(next_id, true);
    return false;
  }
}

/**
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N** neighbor_node, N** node,
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>** parent, int index,
  Transaction* transaction) {
  if ((*node)->IsLeafPage()) {
    LeafPage* op_node = reinterpret_cast<LeafPage*>(*node);
    LeafPage* op_neighbor_node = reinterpret_cast<LeafPage*>(*neighbor_node);
    assert(op_node->IsLeafPage());
    assert(op_neighbor_node->IsLeafPage());
    op_node->MoveAllTo(op_neighbor_node);
  }
  else {
    InternalPage* op_node = reinterpret_cast<InternalPage*>(*node);
    InternalPage* op_neighbor_node = reinterpret_cast<InternalPage*>(*neighbor_node);
    KeyType middle_key = (*parent)->KeyAt(index);
    op_node->MoveAllTo(op_neighbor_node, middle_key, buffer_pool_manager_);
  }
  (*parent)->Remove(index);
  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    return CoalesceOrRedistribute(*parent, transaction);
  }
  return false;
}

/**
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N* neighbor_node, N* node, int index) {
  page_id_t parent_page_id = node->GetParentPageId();
  Page* parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  InternalPage* parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());
  if (node->IsLeafPage()) {
    LeafPage* op_node = reinterpret_cast<LeafPage*>(node);
    LeafPage* op_neighbor_node = reinterpret_cast<LeafPage*>(neighbor_node);
    assert(op_node->IsLeafPage());
    assert(op_neighbor_node->IsLeafPage());
    if (index == 0) {
      op_neighbor_node->MoveFirstToEndOf(op_node);
      int node_index = parent_node->ValueIndex(op_neighbor_node->GetPageId());
      parent_node->SetKeyAt(node_index, op_neighbor_node->KeyAt(0));
    }
    else {
      op_neighbor_node->MoveLastToFrontOf(op_node);
      int node_index = parent_node->ValueIndex(op_node->GetPageId());
      parent_node->SetKeyAt(node_index, op_node->KeyAt(0));
    }
  }
  else {
    InternalPage* op_node = reinterpret_cast<InternalPage*>(node);
    InternalPage* op_neighbor_node = reinterpret_cast<InternalPage*>(neighbor_node);
    if (index == 0) {
      int node_index = parent_node->ValueIndex(op_neighbor_node->GetPageId());
      KeyType middle_key = parent_node->KeyAt(node_index);
      KeyType next_middle_key = op_neighbor_node->KeyAt(1);
      op_neighbor_node->MoveFirstToEndOf(op_node, middle_key, buffer_pool_manager_);
      parent_node->SetKeyAt(node_index, next_middle_key);
    }
    else {
      int node_index = parent_node->ValueIndex(op_node->GetPageId());
      KeyType middle_key = parent_node->KeyAt(node_index);
      KeyType next_middle_key = op_neighbor_node->KeyAt(op_neighbor_node->GetSize() - 1);
      op_neighbor_node->MoveLastToFrontOf(op_node, middle_key, buffer_pool_manager_);
      parent_node->SetKeyAt(node_index, next_middle_key);
    }
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

/**
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage* old_root_node) {
  if (old_root_node->GetSize() > 1) {
    return false;
  }
  page_id_t new_root_id;
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() == 1) {
      return false;
    }
    new_root_id = INVALID_PAGE_ID;
  }
  else {
    InternalPage* old_root_internal_node = reinterpret_cast<InternalPage*>(old_root_node);
    new_root_id = old_root_internal_node->RemoveAndReturnOnlyChild();
    Page* new_root_page = buffer_pool_manager_->FetchPage(new_root_id);
    InternalPage* new_root_node = reinterpret_cast<InternalPage*>(new_root_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(new_root_id, true);
  }
  root_page_id_ = new_root_id;
  UpdateRootPageId(0);

  return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
 /**
  * Input parameter is void, find the left most leaf page first, then construct
  * index iterator
  * @return : index iterator
  */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  KeyType key;
  Page* leaf_page = FindLeafPage(key, true); // Pinned !!!
  ASSERT(leaf_page, "leaf page is nullptr");
  LeafPage* leaf_node = reinterpret_cast<LeafPage*>(leaf_page->GetData());
  assert(leaf_node->IsLeafPage());
  return INDEXITERATOR_TYPE(leaf_node, 0, buffer_pool_manager_);
}

/**
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType& key) {
  Page* leaf_page = FindLeafPage(key, false); // Pinned !!!
  ASSERT(leaf_page, "leaf page is nullptr");
  LeafPage* leaf_node = reinterpret_cast<LeafPage*>(leaf_page->GetData());
  assert(leaf_node->IsLeafPage());
  int index = leaf_node->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(leaf_node, index, buffer_pool_manager_);
}

/**
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
 /**
  * Find leaf page containing particular key, if leftMost flag == true, find
  * the left most leaf page
  * NOTE: the leaf page is pinned, you need to unpin it after use.
  */
INDEX_TEMPLATE_ARGUMENTS
Page* BPLUSTREE_TYPE::FindLeafPage(const KeyType& key, bool leftMost) {
  ASSERT(root_page_id_ != INVALID_PAGE_ID, "root_page_id is INVALID_PAGE_ID");
  Page* page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage* node = reinterpret_cast<BPlusTreePage*>(page->GetData());
  ASSERT(node->IsRootPage(), "Not Root Page");

  while (!node->IsLeafPage()) {
    InternalPage* internal_node = reinterpret_cast<InternalPage*>(page->GetData());
    page_id_t next_page_id = leftMost ? internal_node->ValueAt(0) : internal_node->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(internal_node->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(next_page_id);
    node = reinterpret_cast<BPlusTreePage*>(page->GetData());
  }

  // LeafPage* leaf_node = reinterpret_cast<LeafPage*>(page->GetData());
  // buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return page;
}

/**
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  Page* page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  ASSERT(page != nullptr, "root fetch fail");
  IndexRootsPage* node = reinterpret_cast<IndexRootsPage*>(page->GetData());
  if (!insert_record) { // update
    bool update_status = node->Update(index_id_, root_page_id_);
    ASSERT(update_status, "update root fail");
  }
  else if (insert_record == 1) { // insert
    bool insert_status = node->Insert(index_id_, root_page_id_);
    ASSERT(insert_status, "insert root fail");
  }
  else { // delete
    bool delete_status = node->Delete(index_id_);
    ASSERT(delete_status, "delete root fail");
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage* page, BufferPoolManager* bpm, std::ofstream& out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto* leaf = reinterpret_cast<LeafPage*>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
      << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
      << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
      << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
        << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
        << leaf->GetPageId() << ";\n";
    }
  }
  else {
    auto* inner = reinterpret_cast<InternalPage*>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
      << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
      << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
      << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      }
      else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
        << internal_prefix
        << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage*>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage*>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
            << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage* page, BufferPoolManager* bpm) const {
  if (page->IsLeafPage()) {
    auto* leaf = reinterpret_cast<LeafPage*>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
      << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  }
  else {
    auto* internal = reinterpret_cast<InternalPage*>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
      << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage*>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;
