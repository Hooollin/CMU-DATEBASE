//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>
#include "common/logger.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);

  Page *P = nullptr;
  frame_id_t target_frame_id = -1;
  if(this->page_table_.find(page_id) == this->page_table_.end()){// P not exists
    if(!HasFreePage()){// no free pages
      bool try_find_victim = this->replacer_->Victim(&target_frame_id);
      if(try_find_victim){// has victim frame
        Page *R = &this->pages_[target_frame_id];
        if(R->IsDirty()){
          disk_manager_->WritePage(R->page_id_, R->data_);
        }// victim page is not dirty, do nothing
        page_table_.erase(R->page_id_);
     }else return nullptr;  // no vimctim frame, should return nullptr
    }else{// has free pages
      target_frame_id = this->free_list_.front();
      this->free_list_.pop_front();
    }
    // once we find spare or substitute frame, fetch the page from disk and update frame's metadata
    this->page_table_[page_id] = target_frame_id;
    P = &this->pages_[target_frame_id];
    P->page_id_ = page_id;
    P->pin_count_ = 1;
    P->is_dirty_ = false;
    this->disk_manager_->ReadPage(P->page_id_, P->data_);
  }else{// P exists
    target_frame_id = page_table_[page_id];
    P = &this->pages_[target_frame_id];
    P->pin_count_++;
  }
  // this frame is certainly in use
  this->replacer_->Pin(target_frame_id);
  return P;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> guard(latch_);

  // unpin doens't mean remove it from buffer pool!!!
  if(this->page_table_.find(page_id) == this->page_table_.end()){
    return false;
  }
  Page* P = &this->pages_[this->page_table_[page_id]];
  P->is_dirty_ |= is_dirty;
  if(P->pin_count_ <= 0){
    return false;
  }
  P->pin_count_--;
  if(P->pin_count_ == 0){
    this->replacer_->Unpin(this->page_table_[page_id]);
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> guard(latch_);

  if(page_id == INVALID_PAGE_ID || this->page_table_.find(page_id) == this->page_table_.end()){
    return false;
  }
  frame_id_t target_frame_id = this->page_table_[page_id];
  Page *P = &this->pages_[target_frame_id];
  if(P->is_dirty_){
    this->disk_manager_->WritePage(page_id, P->data_);
  }
  P->is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);

  Page *P = nullptr;
  frame_id_t target_frame_id = -1;
  if(this->HasFreePage()){
    target_frame_id = this->free_list_.front();
    this->free_list_.pop_front();
  }else{
    bool try_find_victim = this->replacer_->Victim(&target_frame_id);
    if(!try_find_victim){
      return nullptr;// no victim can be found, return nullptr
    }
  }
  P = &this->pages_[target_frame_id];
  if(P->is_dirty_){
    disk_manager_->WritePage(P->page_id_, P->data_);
  }

  // remove old mapping
  this->page_table_.erase(P->page_id_);
  P->ResetMemory();

  page_id_t new_page_id = this->disk_manager_->AllocatePage();
  P->page_id_ = new_page_id;
  P->pin_count_ = 1;
  P->is_dirty_ = false;
  this->page_table_[new_page_id] = target_frame_id;
  this->replacer_->Pin(target_frame_id);

  *page_id = new_page_id;
  return P;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> guard(latch_);

  if(this->page_table_.find(page_id) == this->page_table_.end()){
    return true;
  }else{
    Page *P = &this->pages_[this->page_table_[page_id]];
    if(P->GetPinCount() > 0){
      return false;
    }else{
      this->disk_manager_->DeallocatePage(page_id);
      P->ResetMemory();
      this->free_list_.push_back(page_table_[page_id]);
      // remove the mapping from page_table
      this->page_table_.erase(page_id);
    }
  }
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  std::lock_guard<std::mutex> guard(latch_);
  
  for(auto &p : page_table_){
    page_id_t page_id = p.first;
    frame_id_t frame_id = p.second;
    Page *P = &this->pages_[frame_id];
    if(P->is_dirty_){
      this->disk_manager_->WritePage(page_id, P->data_);
      P->is_dirty_ = false;
    }
  }
}

bool BufferPoolManager::HasFreePage(){
  return static_cast<int>(this->free_list_.size()) > 0;
}

}  // namespace bustub
