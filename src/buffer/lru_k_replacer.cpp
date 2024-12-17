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
  frame_id_t frame;
  /// get the max distance
  if (!less_k_history_.empty()) {
    frame = less_k_history_.top().fid_;
    less_k_history_.pop();
  } else {
    frame = k_history_.top().fid_;
    k_history_.pop();
  }
  curr_size_--;
  cache_deleted_pages.insert({current_timestamp_++, node_store_[frame]});
  if (cache_deleted_pages.size() > size_cache_deleted_pages_) {
    cache_deleted_pages.erase(cache_deleted_pages.begin());
  }
  node_store_.erase(frame);

  return frame;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "frame_id out of range");
  if (node_store_.find(frame_id) == node_store_.end()) {
    LRUKNode node;
    bool in_cache_deleted_pages = false;
    for (auto it = cache_deleted_pages.begin(); it != cache_deleted_pages.end(); it++) {
      if (it->second.fid_ == frame_id) {
        node = it->second;
        node.history_.clear();
        node.is_evictable_ = false;
        cache_deleted_pages.erase(it);
        in_cache_deleted_pages = true;
        break;
      }
    }
    if (!in_cache_deleted_pages) {
      node.k_ = k_;
      node.fid_ = frame_id;
    }
    node_store_[frame_id] = node;
  }
  LRUKNode node = node_store_[frame_id];

  node.history_.push_back(current_timestamp_++);
  if (node.history_.size() > k_) {
    node.history_.pop_front();
    if (node.is_evictable_) {
      std::priority_queue<LRUKNode, std::vector<LRUKNode>, CompareKNode> new_k_history_;
      while (k_history_.top().fid_ != frame_id) {
        new_k_history_.push(k_history_.top());
        k_history_.pop();
      }
      k_history_.pop();
      while (!new_k_history_.empty()) {
        k_history_.push(new_k_history_.top());
        new_k_history_.pop();
      }
      k_history_.push(node);
    }
  }
  if (node.history_.size() == k_ && node.is_evictable_) {  // just care from less_k to k_history
    k_history_.push(node);
    std::priority_queue<LRUKNode, std::vector<LRUKNode>, CompareLessKNode> new_less_k_history_;
    while (less_k_history_.top().fid_ != frame_id) {
      new_less_k_history_.push(less_k_history_.top());
      less_k_history_.pop();
    }
    //debug
    if(less_k_history_.size()==0){
      int uu=85;
      uu++;
    }
    //debug
    less_k_history_.pop();
    while (!new_less_k_history_.empty()) {
      less_k_history_.push(new_less_k_history_.top());
      new_less_k_history_.pop();
    }
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
    if (node.history_.size() < k_) {  // add to heaps
      less_k_history_.push(node);
    } else {
      k_history_.push(node);
    }
  } else if (!set_evictable && node.is_evictable_) {
    curr_size_--;
    node.is_evictable_ = false;
    // need to remove from heaps
    std::priority_queue<LRUKNode, std::vector<LRUKNode>, CompareKNode> new_k_history_;
    std::priority_queue<LRUKNode, std::vector<LRUKNode>, CompareLessKNode> new_less_k_history_;
    bool in_k_history = false;
    while (!k_history_.empty()) {
      if (k_history_.top().fid_ == frame_id) {
        in_k_history = true;
        k_history_.pop();
        break;
      }
      new_k_history_.push(k_history_.top());
      k_history_.pop();
    }
    while (!new_k_history_.empty()) {
      k_history_.push(new_k_history_.top());
      new_k_history_.pop();
    }
    if (!in_k_history) {
      while (less_k_history_.top().fid_ != frame_id) {
        new_less_k_history_.push(less_k_history_.top());
        less_k_history_.pop();
      }
      less_k_history_.pop();
      while (!new_less_k_history_.empty()) {
        less_k_history_.push(new_less_k_history_.top());
        new_less_k_history_.pop();
      }
    }
  }
  node_store_[frame_id] = node;
  current_timestamp_++;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "frame_id out of range");
  if (node_store_.find(frame_id) != node_store_.end()) {
    LRUKNode node = node_store_[frame_id];
    BUSTUB_ASSERT(node.is_evictable_, "frame_id is not evictable");
    cache_deleted_pages.insert({current_timestamp_++, node});
    curr_size_--;
    if (cache_deleted_pages.size() > size_cache_deleted_pages_) {
      cache_deleted_pages.erase(cache_deleted_pages.begin());
    }
    node_store_.erase(frame_id);
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }
void LRUKReplacer::Print_info() {
  std::cout << "replacer size: " << replacer_size_ << std::endl;
  std::cout << "current timestamp: " << current_timestamp_ << std::endl;
  std::cout << "k: " << k_ << std::endl;
  std::cout << "current size of evicted pages: " << curr_size_ << std::endl;
  for (auto it = node_store_.begin(); it != node_store_.end(); it++) {
    frame_id_t frame_id = it->first;
    LRUKNode node = it->second;
    if (node.is_evictable_) {
      std::cout << "frame_id: " << frame_id;
    }
    std::cout << std::endl;
  }
  std::cout << "cache_deleted_pages size: " << cache_deleted_pages.size() << std::endl;
  std::cout << "cache_deleted_pages: " << std::endl;
  for (auto it = cache_deleted_pages.begin(); it != cache_deleted_pages.end(); it++) {
    size_t timestamp = it->first;
    LRUKNode node = it->second;
    std::cout << "timestamp: " << timestamp << " frame_id: " << node.fid_ << " history: ";
    for (auto it2 = node.history_.begin(); it2 != node.history_.end(); it2++) {
      std::cout << *it2 << " ";
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
  std::vector<LRUKNode> v1, v2;
  std::cout << "k_history: " << std::endl;
  while (!k_history_.empty()) {
    std::cout << k_history_.top().fid_ << " ";
    v1.push_back(k_history_.top());
    k_history_.pop();
  }
  std::cout << std::endl;
  std::cout << "less_k_history: " << std::endl;
  while (!less_k_history_.empty()) {
    std::cout << less_k_history_.top().fid_ << " ";
    v2.push_back(less_k_history_.top());
    less_k_history_.pop();
  }
  std::cout << std::endl;
  for (auto it = v1.begin(); it != v1.end(); it++) {
    k_history_.push(*it);
  }
  for (auto it = v2.begin(); it != v2.end(); it++) {
    less_k_history_.push(*it);
  }
  std::cout << "------------------------------------" << std::endl;
}
}  // namespace bustub