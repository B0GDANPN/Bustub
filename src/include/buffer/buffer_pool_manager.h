//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

class BufferPoolManager;
class ReadPageGuard;
class WritePageGuard;
template <typename A, typename B>
class ThreadSafeMapWrapper;
template <typename A>
class ThreadSafeVectorWrapper;
template <typename A>
class ThreadSafeListWrapper;
/**
 * @brief A helper class for `BufferPoolManager` that manages a frame of memory and related metadata.
 *
 * This class represents headers for frames of memory that the `BufferPoolManager` stores pages of data into. Note that
 * the actual frames of memory are not stored directly inside a `FrameHeader`, rather the `FrameHeader`s store pointer
 * to the frames and are stored separately them.
 *
 * ---
 *
 * Something that may (or may not) be of interest to you is why the field `data_` is stored as a vector that is
 * allocated on the fly instead of as a direct pointer to some pre-allocated chunk of memory.
 *
 * In a traditional production buffer pool manager, all memory that the buffer pool is intended to manage is allocated
 * in one large contiguous array (think of a very large `malloc` call that allocates several gigabytes of memory up
 * front). This large contiguous block of memory is then divided into contiguous frames. In other words, frames are
 * defined by an offset from the base of the array in page-sized (4 KB) intervals.
 *
 * In BusTub, we instead allocate each frame on its own (via a `std::vector<char>`) in order to easily detect buffer
 * overflow with address sanitizer. Since C++ has no notion of memory safety, it would be very easy to cast a page's
 * data pointer into some large data type and start overwriting other pages of data if they were all contiguous.
 *
 * If you would like to attempt to use more efficient data structures for your buffer pool manager, you are free to do
 * so. However, you will likely benefit significantly from detecting buffer overflow in future projects (especially
 * project 2).
 */
class FrameHeader {
  friend class BufferPoolManager;
  friend class ReadPageGuard;
  friend class WritePageGuard;

 public:
  explicit FrameHeader(frame_id_t frame_id);

 private:
  auto GetData() const -> const char *;
  auto GetDataMut() -> char *;
  void Reset();

  /** @brief The frame ID / index of the frame this header represents. */
  const frame_id_t frame_id_;

  /** @brief The readers / writer latch for this frame. */
  std::shared_mutex rwlatch_;

  /** @brief The number of pins on this frame keeping the page in memory. */
  std::atomic<size_t> pin_count_;

  /** @brief The dirty flag. */
  bool is_dirty_;

  /**
   * @brief A pointer to the data of the page that this frame holds.
   *
   * If the frame does not hold any page data, the frame contains all null bytes.
   */
  std::vector<char> data_;

  /**
   * TODO(P1): You may add any fields or helper functions under here that you think are necessary.
   *
   * One potential optimization you could make is storing an optional page ID of the page that the `FrameHeader` is
   * currently storing. This might allow you to skip searching for the corresponding (page ID, frame ID) pair somewhere
   * else in the buffer pool manager...
   */
};

/**
 * @brief The declaration of the `BufferPoolManager` class.
 *
 * As stated in the writeup, the buffer pool is responsible for moving physical pages of data back and forth from
 * buffers in main memory to persistent storage. It also behaves as a cache, keeping frequently used pages in memory for
 * faster access, and evicting unused or cold pages back out to storage.
 *
 * Make sure you read the writeup in its entirety before attempting to implement the buffer pool manager. You also need
 * to have completed the implementation of both the `LRUKReplacer` and `DiskManager` classes.
 */
template <typename A, typename B>
class ThreadSafeMapWrapper {

public:
  ThreadSafeMapWrapper(std::unordered_map<A, B>& map,std::shared_ptr<std::mutex> latch) : map_(map), latch_(latch) {}
  ThreadSafeMapWrapper(const ThreadSafeMapWrapper& other);
  ThreadSafeMapWrapper& operator=(const ThreadSafeMapWrapper& other){
    if (this != &other) {
      map_ = other.map_;
      latch_ = other.latch_;
    }
    return *this;
  };
  B& operator[](const A& key) {
    std::scoped_lock latch(*latch_);
    return map_[key];
  }
  B get(const A& key) const {
    std::scoped_lock latch(*latch_);
    return map_.at(key);
  }

  bool find(const A& key) const {
    std::scoped_lock latch(*latch_);
    return map_.find(key) != map_.end();
  }

  void erase(const A& key) {
    std::scoped_lock latch(*latch_);
    map_.erase(key);
  }

private:
  std::unordered_map<A, B>& map_;
  std::shared_ptr<std::mutex> latch_;
};
template <typename A>
class ThreadSafeVectorWrapper {
  public:
  ThreadSafeVectorWrapper(std::vector<A>& vector,std::shared_ptr<std::mutex> latch) : vector_(vector), latch_(latch) {}
  ThreadSafeVectorWrapper(const ThreadSafeVectorWrapper& other);
  ThreadSafeVectorWrapper& operator=(const ThreadSafeVectorWrapper& other){
    if (this != &other) {
      vector_ = other.vector_;
      latch_ = other.latch_;
    }
    return *this;
  };
  A& operator[](const size_t index) {
    std::scoped_lock latch(*latch_);
    return vector_[index];
  };
private:
  std::vector<A>& vector_;
  std::shared_ptr<std::mutex> latch_;
};

template <typename A>
class ThreadSafeListWrapper{
  public:
  ThreadSafeListWrapper(std::list<A>& list,std::shared_ptr<std::mutex> latch) : list_(list), latch_(latch) {}
  ThreadSafeListWrapper(const ThreadSafeListWrapper& other);
  ThreadSafeListWrapper& operator=(const ThreadSafeListWrapper& other){
    if (this != &other) {
      list_ = other.vector_;
      latch_ = other.latch_;
    }
    return *this;
  };
  void push_back(const A& value) {
    std::scoped_lock latch(*latch_);
    list_.push_back(value);
  }
  bool empty() const {
    std::scoped_lock latch(*latch_);
    return list_.empty();
  }
  A front() const {
    std::scoped_lock latch(*latch_);
    return list_.front();
  }
  void pop_front() {
    std::scoped_lock latch(*latch_);
    list_.pop_front();
  }
  private:
  std::list<A>& list_;
  std::shared_ptr<std::mutex> latch_;
};
class BufferPoolManager {
 public:
  BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist = LRUK_REPLACER_K,
                    LogManager *log_manager = nullptr);
  ~BufferPoolManager();

  auto Size() const -> size_t;
  auto NewPage() -> page_id_t;
  auto DeletePage(page_id_t page_id) -> bool;
  auto CheckedWritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown)
      -> std::optional<WritePageGuard>;
  auto CheckedReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> std::optional<ReadPageGuard>;
  auto WritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> WritePageGuard;
  auto ReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> ReadPageGuard;
  auto FlushPage(page_id_t page_id) -> bool;
  void FlushAllPages();
  auto GetPinCount(page_id_t page_id) -> std::optional<size_t>;
 private:
  /** @brief The number of frames in the buffer pool. */
  const size_t num_frames_;

  /** @brief The next page ID to be allocated.  */
  std::atomic<page_id_t> next_page_id_;

  /**
   * @brief The latch protecting the buffer pool's inner data structures.
   *
   * TODO(P1) We recommend replacing this comment with details about what this latch actually protects.
   */
  std::shared_ptr<std::mutex> bpm_latch_;

  /** @brief The frame headers of the frames that this buffer pool manages. */
  std::vector<std::shared_ptr<FrameHeader>> frames_;
  ThreadSafeVectorWrapper<std::shared_ptr<FrameHeader>> safe_frames_{frames_, bpm_latch_};
  /** @brief The page table that keeps track of the mapping between pages and buffer pool frames. */
  std::unordered_map<page_id_t, frame_id_t> page_table_;
  ThreadSafeMapWrapper<page_id_t, frame_id_t> safe_page_table_{page_table_, bpm_latch_};
  std::unordered_map<frame_id_t, page_id_t> frame_table_;
  ThreadSafeMapWrapper<frame_id_t, page_id_t> safe_frame_table_{frame_table_,bpm_latch_};

  /** @brief A list of free frames that do not hold any page's data. */
  std::list<frame_id_t> free_frames_;
  ThreadSafeListWrapper<frame_id_t> safe_free_frames_{free_frames_, bpm_latch_};

  /** @brief The replacer to find unpinned / candidate pages for eviction. */
  std::shared_ptr<LRUKReplacer> replacer_;

  /** @brief A pointer to the disk scheduler. */
  std::unique_ptr<DiskScheduler> disk_scheduler_;

  /**
   * @brief A pointer to the log manager.
   *
   * Note: Please ignore this for P1.
   */
  LogManager *log_manager_ __attribute__((__unused__));

  /**
   * TODO(P1): You may add additional private members and helper functions if you find them necessary.
   *
   * There will likely be a lot of code duplication between the different modes of accessing a page.
   *
   * We would recommend implementing a helper function that returns the ID of a frame that is free and has nothing
   * stored inside of it. Additionally, you may also want to implement a helper function that returns either a shared
   * pointer to a `FrameHeader` that already has a page's data stored inside of it, or an index to said `FrameHeader`.
   */
};

}  // namespace bustub
