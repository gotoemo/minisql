#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages): max_size_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  lock_guard<mutex> lock(latch_);
  if (lru_list_.empty()) {
    return false;
  }

  // 选择最近最少使用的帧（链表末尾）
  *frame_id = lru_list_.back();
  lru_list_.pop_back();
  lru_map_.erase(*frame_id);
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  lock_guard<mutex> lock(latch_);
  if (lru_map_.count(frame_id) == 0) {
    return;  // 帧不在replacer中
  }

  // 从LRU列表中移除该帧
  lru_list_.erase(lru_map_[frame_id]);
  lru_map_.erase(frame_id);
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  lock_guard<mutex> lock(latch_);
  if (lru_map_.count(frame_id) != 0) {
    return;  // 帧已经在replacer中
  }

  // 如果达到容量限制，需要先淘汰一个帧
  if (lru_list_.size() >= max_size_) {
    frame_id_t victim_id;
    if (!Victim(&victim_id)) {
      return;  // 没有可淘汰的帧
    }
  }

  // 将帧添加到LRU列表前端
  lru_list_.push_front(frame_id);
  lru_map_[frame_id] = lru_list_.begin();
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  lock_guard<mutex> lock(latch_);
  return lru_list_.size();
}