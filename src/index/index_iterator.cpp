#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(LeafPage* leaf, int index, BufferPoolManager* bpm) :
  leaf_(leaf), index_(index), bpm_(bpm) {}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
  bpm_->FetchPage(leaf_->GetPageId())->RUnlatch();
  bpm_->UnpinPage(leaf_->GetPageId(), false);
  bpm_->UnpinPage(leaf_->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS const MappingType& INDEXITERATOR_TYPE::operator*() {
  ASSERT(!(
    leaf_ == nullptr || 
    (index_ == leaf_->GetSize() && leaf_->GetNextPageId() == INVALID_PAGE_ID)
    ), "out of range");
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE& INDEXITERATOR_TYPE::operator++() {
  index_++;
  if (index_ == leaf_->GetSize() && leaf_->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_t next_page_id = leaf_->GetNextPageId();

    Page* page = bpm_->FetchPage(next_page_id);
    ASSERT(page, "IndexIterator(operator++): all page are pinned");

    page->RLatch();
    bpm_->FetchPage(leaf_->GetPageId())->RUnlatch();
    bpm_->UnpinPage(leaf_->GetPageId(), false);
    bpm_->UnpinPage(leaf_->GetPageId(), false);

    LeafPage* next_leaf = reinterpret_cast<LeafPage *>(page->GetData());
    assert(next_leaf->IsLeafPage());
    index_ = 0;
    leaf_ = next_leaf;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE INDEXITERATOR_TYPE::operator++(int) {
  IndexIterator ret_val = *this;
  ++(*this);
  return ret_val;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator& itr) const {
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator& itr) const {
  return false;
}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
