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

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { capacity_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::unique_lock<std::mutex> lock(mtx_);
  if (lru_map_.empty()) {
    return false;
  }
  // select the last object and delete
  frame_id_t last_frame = lru_list_.back();
  lru_map_.erase(last_frame);
  lru_list_.pop_back();
  // no bug, But I think last_frame may be destroyed.
  *frame_id = last_frame;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(mtx_);
  if (lru_map_.find(frame_id) != lru_map_.end()) {
    lru_list_.erase(lru_map_[frame_id]);
    lru_map_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(mtx_);
  if (lru_map_.find(frame_id) != lru_map_.end()) {
    return;
  }
  while (Size() >= capacity_) {
    frame_id_t to_del = lru_list_.back();
    lru_list_.pop_back();
    lru_map_.erase(to_del);
  }
  // insert
  lru_list_.push_front(frame_id);
  lru_map_[frame_id] = lru_list_.begin();
}

size_t LRUReplacer::Size() { return lru_list_.size(); }

}  // namespace bustub
