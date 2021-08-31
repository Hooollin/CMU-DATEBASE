/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(Page *left_most_page, int k, BufferPoolManager *buffer_pool_manager) {
  this->curr_page_ = left_most_page;
  this->k_ = k;
  this->buffer_pool_manager_ = buffer_pool_manager;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (this->curr_page_ != nullptr) {
    this->buffer_pool_manager_->UnpinPage(this->curr_page_->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return this->curr_page_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(this->curr_page_->GetData())->GetItem(this->k_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (!isEnd()) {
    this->k_++;
    if (this->k_ == reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(this->curr_page_->GetData())->GetSize()) {
      page_id_t next_page =
          reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(this->curr_page_->GetData())->GetNextPageId();
      // current page is no more needed in iteration
      this->buffer_pool_manager_->UnpinPage(this->curr_page_->GetPageId(), true);
      if (next_page != INVALID_PAGE_ID) {
        this->curr_page_ = this->buffer_pool_manager_->FetchPage(next_page);
      } else {
        this->curr_page_ = nullptr;
      }
      this->k_ = 0;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
