//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  this->num_pages_ = num_pages;
  this->victim_size_ = 0;
}

LRUReplacer::~LRUReplacer() {}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(lru_mutex);

  if (this->victim_size_ == 0) {
    frame_id = nullptr;
    return false;
  } else {
    *frame_id = frame_list_.front();
    frame_list_.pop_front();
    frame_id_node_.erase(*frame_id);
    this->victim_size_--;
    return true;
  }
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(lru_mutex);

  if (frame_id_node_.find(frame_id) != frame_id_node_.end()) {
    auto it = frame_id_node_[frame_id];
    frame_list_.erase(it);
    frame_id_node_.erase(frame_id);
    this->victim_size_--;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(lru_mutex);

  if (frame_id_node_.find(frame_id) == frame_id_node_.end()) {
    frame_list_.push_back(frame_id);
    frame_id_node_[frame_id] = --frame_list_.end();
    this->victim_size_++;
  } else {
    LOG_INFO("frame_id is already maintained by lru_replacer, do nothing.");
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock(lru_mutex);
  return this->victim_size_;
}

}  // namespace bustub
