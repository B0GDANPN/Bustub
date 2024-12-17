//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  if (curr_size_ == 0) {
    return std::nullopt;
  }
  size_t max_distance = 0;
  frame_id_t frame;
  for (const auto &it_node : node_store_) {
    const LRUKNode &node = it_node.second;
    if (!node.is_evictable_) continue;

    size_t tmp_distance;
    if (node.history_.size() < node.k_)
      tmp_distance = SIZE_MAX;
    else
      tmp_distance = current_timestamp_ - node.history_.back();

    if (tmp_distance > max_distance) {
      max_distance = tmp_distance;
      frame = node.fid_;
    }
    if (tmp_distance == SIZE_MAX && node.history_.back() < node_store_[frame].history_.back()) {
      frame = node.fid_;
    }
  }
  return frame;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "frame_id out of range");
  if (node_store_.find(frame_id) == node_store_.end()) {
    LRUKNode node;
    node.k_ = k_;
    node.fid_ = frame_id;
    node_store_[frame_id] = node;
  }
  LRUKNode node = node_store_[frame_id];

  node.history_.push_back(current_timestamp_++);
  if (node.history_.size() > k_) {
    node.history_.pop_front();
  }
  node_store_[frame_id] = node;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "frame_id out of range");
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  LRUKNode node = node_store_[frame_id];
  if (set_evictable && !node.is_evictable_) {
    curr_size_++;
    node.is_evictable_ = true;
  } else if (!set_evictable && node.is_evictable_) {
    curr_size_--;
    node.is_evictable_ = false;
  }
  node_store_[frame_id] = node;
  current_timestamp_++;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "frame_id out of range");
  bool debug=node_store_.find(frame_id) != node_store_.end();
  if (!debug){
    int hh=99;
    hh++;
  }
  BUSTUB_ASSERT(node_store_.find(frame_id) != node_store_.end(), "frame_id not found in node_store");
  LRUKNode node = node_store_[frame_id];
  BUSTUB_ASSERT(node.is_evictable_, "frame_id is not evictable");
  curr_size_--;
  node_store_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }
void LRUKReplacer::Print_info() {
  std::cout << "replacer size: " << replacer_size_ << std::endl;
  std::cout << "current timestamp: " << current_timestamp_ << std::endl;
  std::cout << "k: " << k_ << std::endl;
  std::cout << "current size of evicred pages: " << curr_size_ << std::endl;
  for (auto it = node_store_.begin(); it != node_store_.end(); it++) {
    frame_id_t frame_id = it->first;
    LRUKNode node = it->second;
    if (node.is_evictable_) {
      std::cout << "frame_id: " << frame_id;
    }
    std::cout << std::endl;
  }
  std::cout << "node_store: " << std::endl;
  for (auto it = node_store_.begin(); it != node_store_.end(); it++) {
    frame_id_t frame_id = it->first;
    LRUKNode node = it->second;
    std::cout << "frame_id: " << frame_id << " history: ";
    for (auto it2 = node.history_.begin(); it2 != node.history_.end(); it2++) {
      std::cout << *it2 << " ";
    }
    std::cout << std::endl;
  }
}
}  // namespace bustub