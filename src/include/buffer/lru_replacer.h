#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <list>
#include <mutex>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

private:
  mutable mutex latch_;  // 用于线程安全
  list<frame_id_t> lru_list_;  // 按访问时间排序的链表，最近访问的在前面
  unordered_map<frame_id_t, list<frame_id_t>::iterator> lru_map_;  // 快速查找帧在链表中的位置
  size_t max_size_;  // 最大容量
};

#endif  // MINISQL_LRU_REPLACER_H
