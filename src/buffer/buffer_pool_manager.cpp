#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  lock_guard<recursive_mutex> lock(latch_);

  // 1. 检查页面是否已经在缓冲池中
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];

    // 增加pin计数，标记为最近使用
    page->pin_count_++;
    replacer_->Pin(frame_id);
    return page;
  }

  // 2. 尝试获取空闲帧
  frame_id_t frame_id = TryToFindFreePage();
  if (frame_id == INVALID_FRAME_ID) {
    return nullptr;  // 没有可用帧
  }

  Page *replacement_page = &pages_[frame_id];

  // 2. If R is dirty, write it back to the disk
  if (replacement_page->IsDirty()) {
    disk_manager_->WritePage(replacement_page->GetPageId(),
                           replacement_page->GetData());
  }

  // 3. Delete R from the page table and insert P
  if (replacement_page->GetPageId() != INVALID_PAGE_ID) {
    page_table_.erase(replacement_page->GetPageId());
  }
  page_table_[page_id] = frame_id;

  // 4. Update P's metadata, read from disk, and return
  Page *new_page = replacement_page;
  new_page->page_id_ = page_id;
  new_page->pin_count_ = 1;
  new_page->is_dirty_ = false;
  disk_manager_->ReadPage(page_id, new_page->GetData());

  // 确保新页不会被立即替换
  replacer_->Pin(frame_id);

  return new_page;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  lock_guard<recursive_mutex> lock(latch_);

  // 0. Allocate a new page ID
  page_id = AllocatePage();

  // 1. Check if all pages are pinned
  frame_id_t frame_id = TryToFindFreePage();
  if (frame_id == INVALID_FRAME_ID) {
    DeallocatePage(page_id);  // Rollback page allocation
    return nullptr;
  }

  // 2. Get the victim page (already handled in TryToFindFreePage)
  Page* page = &pages_[frame_id];

  // 3. Update metadata and zero out memory
  if (page->GetPageId() != INVALID_PAGE_ID) {
    // If replacing an existing page, handle dirty page
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page_table_.erase(page->GetPageId());
  }

  // Initialize new page
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->ResetMemory();  // Zero out memory

  // 4. Add to page table and return
  page_table_[page_id] = frame_id;
  replacer_->Pin(frame_id);  // Mark as pinned

  return page;
  //return nullptr;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  lock_guard<recursive_mutex> lock(latch_);

  // 0. Always attempt to deallocate on disk
  DeallocatePage(page_id);

  // 1. Search the page table
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // 1.1 Page not in buffer pool
    return true;
  }

  // 2. Check pin count
  frame_id_t frame_id = it->second;
  Page* page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    // 2.1 Page is pinned
    return false;
  }

  // 3. Delete the page
  if (page->IsDirty()) {
    disk_manager_->WritePage(page_id, page->GetData());
  }

  // Reset page metadata
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->ResetMemory();

  // Remove from page table and add to free list
  page_table_.erase(it);
  free_list_.push_back(frame_id);

  return true;
  //return false;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  lock_guard<recursive_mutex> lock(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;  // 页面不在缓冲池中
  }

  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];

  if (page->pin_count_ <= 0) {
    return false;  // 页面已经unpin
  }

  // 减少pin计数
  if (--page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }

  // 更新脏页状态
  if (is_dirty) {
    page->is_dirty_ = true;
  }

  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  lock_guard<recursive_mutex> lock(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;  // 页面不在缓冲池中
  }

  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];

  // 写入磁盘
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;

  return true;
}

frame_id_t BufferPoolManager::TryToFindFreePage() {
  // 1. 首先检查空闲列表 (Note that pages are always found from the free list first)
  if (!free_list_.empty()) {
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();
    return frame_id;
  }

  // 2. 如果没有空闲帧，使用替换器
  frame_id_t frame_id;
  if (replacer_->Victim(&frame_id)) {
    return frame_id;
  }

  return INVALID_FRAME_ID;  // 没有可用帧
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}