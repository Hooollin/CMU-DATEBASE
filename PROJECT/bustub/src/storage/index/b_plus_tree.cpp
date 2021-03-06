//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return this->root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  this->rwlatch_.RLock();
  Page *target_page = FindLeafPage(key, false);
  if (target_page != nullptr) {
    ValueType val;
    if (reinterpret_cast<LeafPage *>(target_page->GetData())->Lookup(key, &val, this->comparator_)) {
      result->push_back(val);
      this->rwlatch_.RUnlock();
      return true;
    } else {
      this->rwlatch_.RUnlock();
    }
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  this->rwlatch_.WLock();
  if (this->IsEmpty()) {
    this->StartNewTree(key, value);
    this->rwlatch_.WUnlock();
    return true;
  } else {
    bool rev = this->InsertIntoLeaf(key, value, transaction);
    this->rwlatch_.WUnlock();
    return rev;
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  Page *root_page = this->buffer_pool_manager_->NewPage(&this->root_page_id_);
  if (root_page == nullptr) {
    throw new Exception(ExceptionType::OUT_OF_MEMORY, "out of memory!");
  }
  UpdateRootPageId(1);
  LeafPage *page = reinterpret_cast<LeafPage *>(root_page->GetData());
  page->Init(this->root_page_id_, INVALID_PAGE_ID, this->leaf_max_size_);
  page->Insert(key, value, this->comparator_);
  this->buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  page_id_t curr_page_id = this->root_page_id_;
  while (curr_page_id != INVALID_PAGE_ID) {
    BPlusTreePage *curr_page_ptr =
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(curr_page_id)->GetData());
    if (curr_page_ptr->IsLeafPage()) {
      LeafPage *leaf_page_ptr = reinterpret_cast<LeafPage *>(curr_page_ptr);
      ValueType v;
      // key already exists
      if (leaf_page_ptr->Lookup(key, &v, this->comparator_)) {
        return false;
      }
      int size = leaf_page_ptr->Insert(key, value, this->comparator_);
      // overflow
      if (size > leaf_page_ptr->GetMaxSize()) {
        BPlusTreePage *new_leaf_page_ptr = Split(curr_page_ptr);
        this->InsertIntoParent(curr_page_ptr, reinterpret_cast<LeafPage *>(new_leaf_page_ptr)->KeyAt(0),
                               new_leaf_page_ptr, transaction);
        this->buffer_pool_manager_->UnpinPage(new_leaf_page_ptr->GetPageId(), true);
      }
      this->buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), true);
      return true;
    } else {
      InternalPage *internal_page_ptr = reinterpret_cast<InternalPage *>(curr_page_ptr);
      curr_page_id = internal_page_ptr->Lookup(key, this->comparator_);
      this->buffer_pool_manager_->UnpinPage(curr_page_ptr->GetPageId(), false);
    }
  }
  return false;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id;
  Page *new_page_ptr = this->buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page_ptr == nullptr) {
    throw new Exception(ExceptionType::OUT_OF_MEMORY, "out of memory!");
  }
  if (node->IsLeafPage()) {
    LeafPage *recipient = reinterpret_cast<LeafPage *>(new_page_ptr->GetData());
    recipient->Init(new_page_id, node->GetParentPageId(), node->GetMaxSize());

    reinterpret_cast<LeafPage *>(node)->MoveHalfTo(recipient);
    reinterpret_cast<LeafPage *>(node)->SetNextPageId(new_page_id);

    return recipient;
  } else {
    InternalPage *recipient = reinterpret_cast<InternalPage *>(new_page_ptr->GetData());
    recipient->Init(new_page_id, node->GetParentPageId(), node->GetMaxSize());

    reinterpret_cast<InternalPage *>(node)->MoveHalfTo(recipient, this->buffer_pool_manager_);
    return recipient;
  }
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // ??????old_node???root page
  if (old_node->GetPageId() == this->root_page_id_) {
    page_id_t new_root_page_id;
    Page *new_root_page = this->buffer_pool_manager_->NewPage(&new_root_page_id);
    if (new_root_page == nullptr) {
      throw new Exception(ExceptionType::OUT_OF_MEMORY, "out of memory!");
    }
    this->root_page_id_ = new_root_page_id;
    UpdateRootPageId(0);

    InternalPage *new_root_tree_page =
        reinterpret_cast<InternalPage *>(this->buffer_pool_manager_->FetchPage(new_root_page_id)->GetData());
    new_root_tree_page->Init(new_root_page_id, INVALID_PAGE_ID, this->internal_max_size_);
    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    new_root_tree_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    this->buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    return;
  }
  page_id_t parent_id = old_node->GetParentPageId();
  BPlusTreePage *parent_page =
      reinterpret_cast<BPlusTreePage *>(this->buffer_pool_manager_->FetchPage(parent_id)->GetData());
  int size =
      reinterpret_cast<InternalPage *>(parent_page)->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  if (size > parent_page->GetMaxSize()) {
    BPlusTreePage *new_split_page = this->Split(parent_page);
    this->InsertIntoParent(parent_page, reinterpret_cast<InternalPage *>(new_split_page)->KeyAt(0), new_split_page,
                           transaction);

    this->buffer_pool_manager_->UnpinPage(new_split_page->GetPageId(), true);
  }
  this->buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  this->rwlatch_.WLock();
  if (this->IsEmpty()) {
    return;
  }
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, false));
  if (leaf_page == nullptr) {
    this->rwlatch_.WUnlock();
    return;
  }
  int size_after_deletion = leaf_page->RemoveAndDeleteRecord(key, this->comparator_);
  // underflow happends
  if (size_after_deletion < leaf_page->GetMinSize()) {
    if (CoalesceOrRedistribute(leaf_page, transaction)) {
      this->buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
    }
  }
  this->rwlatch_.WUnlock();
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // std::cout << "coalesce Or Redistribute: " << node->GetPageId() << std::endl;
  if (node->IsRootPage()) {
    return this->AdjustRoot(node);
  }
  page_id_t parent_page_id = node->GetParentPageId();
  InternalPage *parent_page =
      reinterpret_cast<InternalPage *>(this->buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
  int pos = parent_page->ValueIndex(node->GetPageId());

  page_id_t left_subling_id = INVALID_PAGE_ID, right_subling_id = INVALID_PAGE_ID;
  N *left_subling_page = nullptr, *right_subling_page = nullptr;

  if (pos - 1 >= 0) {
    left_subling_id = parent_page->ValueAt(pos - 1);
    left_subling_page = reinterpret_cast<N *>(this->buffer_pool_manager_->FetchPage(left_subling_id)->GetData());
  }
  if (pos + 1 < parent_page->GetSize()) {
    right_subling_id = parent_page->ValueAt(pos + 1);
    right_subling_page = reinterpret_cast<N *>(this->buffer_pool_manager_->FetchPage(right_subling_id)->GetData());
  }

  N *to_coalesce_subling_page = nullptr;
  if (left_subling_page != nullptr) {
    if (left_subling_page->GetSize() + node->GetSize() > node->GetMaxSize()) {
      this->Redistribute(left_subling_page, node, 1);
      this->buffer_pool_manager_->UnpinPage(left_subling_id, true);
      this->buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
      return false;
    }
    to_coalesce_subling_page = left_subling_page;
  } else if (right_subling_page != nullptr) {
    if (right_subling_page->GetSize() + node->GetSize() > node->GetMaxSize()) {
      this->Redistribute(right_subling_page, node, 0);
      this->buffer_pool_manager_->UnpinPage(right_subling_id, true);
      this->buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
      return false;
    }
    to_coalesce_subling_page = right_subling_page;
  }
  if (this->Coalesce(&to_coalesce_subling_page, &node, &parent_page, pos, transaction)) {
    this->buffer_pool_manager_->DeletePage(parent_page_id);
  }
  // coalesce happened, unpin used pages
  if (to_coalesce_subling_page == left_subling_page) {
    if (left_subling_id != INVALID_PAGE_ID) {
      this->buffer_pool_manager_->UnpinPage(left_subling_id, true);
    }
  } else {
    if (right_subling_id != INVALID_PAGE_ID) {
      this->buffer_pool_manager_->UnpinPage(right_subling_id, true);
    }
  }
  this->buffer_pool_manager_->UnpinPage(parent_page_id, true);
  return true;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  if ((*node)->IsLeafPage()) {
    LeafPage *n = reinterpret_cast<LeafPage *>(*node), *nn = reinterpret_cast<LeafPage *>(*neighbor_node);
    n->MoveAllTo(nn);
  } else {
    InternalPage *n = reinterpret_cast<InternalPage *>(*node), *nn = reinterpret_cast<InternalPage *>(*neighbor_node);
    n->MoveAllTo(nn, (*parent)->KeyAt(index), this->buffer_pool_manager_);
  }
  (*parent)->Remove(index);
  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    return CoalesceOrRedistribute(*parent, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if (index == 0) {
    InternalPage *parent_page = reinterpret_cast<InternalPage *>(
        this->buffer_pool_manager_->FetchPage(neighbor_node->GetParentPageId())->GetData());
    int pos = parent_page->ValueIndex(neighbor_node->GetPageId());
    if (node->IsLeafPage()) {
      reinterpret_cast<LeafPage *>(neighbor_node)->MoveFirstToEndOf(reinterpret_cast<LeafPage *>(node));
      parent_page->SetKeyAt(pos, reinterpret_cast<LeafPage *>(neighbor_node)->KeyAt(0));
    } else {
      reinterpret_cast<InternalPage *>(neighbor_node)
          ->MoveFirstToEndOf(reinterpret_cast<InternalPage *>(node), parent_page->KeyAt(pos),
                             this->buffer_pool_manager_);
      parent_page->SetKeyAt(pos, reinterpret_cast<InternalPage *>(neighbor_node)->KeyAt(1));
    }
    this->buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  } else {
    InternalPage *parent_page =
        reinterpret_cast<InternalPage *>(this->buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
    int pos = parent_page->ValueIndex(node->GetPageId());
    if (node->IsLeafPage()) {
      reinterpret_cast<LeafPage *>(neighbor_node)->MoveLastToFrontOf(reinterpret_cast<LeafPage *>(node));
      parent_page->SetKeyAt(pos, reinterpret_cast<LeafPage *>(node)->KeyAt(0));
    } else {
      reinterpret_cast<InternalPage *>(neighbor_node)
          ->MoveLastToFrontOf(reinterpret_cast<InternalPage *>(node),
                              parent_page->KeyAt(parent_page->ValueIndex(node->GetPageId())),
                              this->buffer_pool_manager_);
      parent_page->SetKeyAt(pos, reinterpret_cast<InternalPage *>(node)->KeyAt(1));
    }
    this->buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() == 0) {
      this->root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
      return true;
    }
    return false;
  } else {
    if (old_root_node->GetSize() > 1) {
      return false;
    }
    page_id_t new_page_id = reinterpret_cast<InternalPage *>(old_root_node)->RemoveAndReturnOnlyChild();
    this->root_page_id_ = new_page_id;
    reinterpret_cast<BPlusTreePage *>(this->buffer_pool_manager_->FetchPage(new_page_id))
        ->SetParentPageId(INVALID_PAGE_ID);
    UpdateRootPageId(0);
    return true;
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  return INDEXITERATOR_TYPE(this->FindLeafPage(KeyType(), true), 0, this->buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *page = this->FindLeafPage(key, false);
  int k = reinterpret_cast<LeafPage *>(page->GetData())->KeyIndex(key, this->comparator_);
  return INDEXITERATOR_TYPE(page, k, this->buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(nullptr, 0, this->buffer_pool_manager_); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  page_id_t curr_page_id = this->root_page_id_;
  while (curr_page_id != INVALID_PAGE_ID) {
    Page *curr_page = this->buffer_pool_manager_->FetchPage(curr_page_id);
    if (reinterpret_cast<BPlusTreePage *>(curr_page->GetData())->IsLeafPage()) {
      if (leftMost) {
        return curr_page;
      } else {
        ValueType val;
        if (reinterpret_cast<LeafPage *>(curr_page->GetData())->Lookup(key, &val, this->comparator_)) {
          return curr_page;
        } else {
          return nullptr;
        }
      }
    } else {
      if (leftMost) {
        curr_page_id = reinterpret_cast<InternalPage *>(curr_page->GetData())->ValueAt(0);
      } else {
        curr_page_id = reinterpret_cast<InternalPage *>(curr_page->GetData())->Lookup(key, this->comparator_);
      }
      this->rwlatch_.RUnlock();
      this->buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
    }
  }
  return nullptr;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
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
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
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
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << " size:" << internal->GetSize() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
