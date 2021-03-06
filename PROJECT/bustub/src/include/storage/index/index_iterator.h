//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();

  IndexIterator(Page *left_most_page, int k, BufferPoolManager *buffer_pool_manager);

  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const { return this->curr_page_ == itr.curr_page_ && this->k_ == itr.k_; }

  bool operator!=(const IndexIterator &itr) const {
    return !(this->curr_page_ == itr.curr_page_ && this->k_ == itr.k_);
  }

 private:
  Page *curr_page_;
  int k_;
  BufferPoolManager *buffer_pool_manager_;
  // add your own private member variables here
};

}  // namespace bustub
