#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
    std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);

    constexpr size_t BITMAP_SIZE = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
    constexpr size_t PAGES_PER_EXTENT = BITMAP_SIZE + 1;

    // 读取元数据页
    auto* meta_page = reinterpret_cast<DiskFileMetaPage*>(meta_data_);

    // 尝试在现有extent中分配页面
    for (uint32_t extent_id = 0; extent_id < meta_page->num_extents_; ++extent_id) {
        if (meta_page->extent_used_page_[extent_id] < BITMAP_SIZE) {
            // 计算位图页的物理页号
            page_id_t bitmap_physical_page = 1 + extent_id * PAGES_PER_EXTENT;

            // 读取位图页
            char bitmap_page[PAGE_SIZE];
            ReadPhysicalPage(bitmap_physical_page, bitmap_page);

            auto* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap_page);

            // 尝试分配页面
            uint32_t page_in_extent = 0;
            if (bitmap->AllocatePage(page_in_extent)) {
                // 更新元数据
                meta_page->extent_used_page_[extent_id]++;
                meta_page->num_allocated_pages_++;

                // 写回位图页
                WritePhysicalPage(bitmap_physical_page, bitmap_page);
                // 写回元数据页
                WritePhysicalPage(0, meta_data_);

                // 计算并返回逻辑页号
                return extent_id * BITMAP_SIZE + page_in_extent;
            }
        }
    }

    // 没有可用空间，需要创建新extent
    uint32_t new_extent_id = meta_page->num_extents_;
    meta_page->num_extents_++;

    // 扩展元数据页以容纳新的extent信息
    // 注意：这里需要确保meta_data_有足够空间存储extent_used_page_数组

    // 初始化新的位图页
    char new_bitmap_page[PAGE_SIZE] = {0};
    auto* new_bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(new_bitmap_page);

    // 分配第一个页面
    uint32_t page_in_extent = 0;
    new_bitmap->AllocatePage(page_in_extent);

    // 计算新位图页的物理位置
    page_id_t new_bitmap_physical_page = 1 + new_extent_id * PAGES_PER_EXTENT;

    // 写回新的位图页
    WritePhysicalPage(new_bitmap_physical_page, new_bitmap_page);

    // 更新元数据
    meta_page->extent_used_page_[new_extent_id] = 1;
    meta_page->num_allocated_pages_++;

    // 写回元数据页
    WritePhysicalPage(0, meta_data_);

    // 返回新分配的逻辑页号
    return new_extent_id * BITMAP_SIZE + page_in_extent;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  if (logical_page_id < 0) {
    return; // 不能释放元数据页或无效页
  }

  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);

  constexpr size_t BITMAP_SIZE = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  constexpr size_t PAGES_PER_EXTENT = BITMAP_SIZE + 1;

  // 计算extent ID和在extent中的偏移
  uint32_t extent_id = logical_page_id / BITMAP_SIZE;
  uint32_t page_in_extent = logical_page_id % BITMAP_SIZE;

  // 读取元数据页
  auto* meta_page = reinterpret_cast<DiskFileMetaPage*>(meta_data_);

  if (extent_id >= meta_page->num_extents_) {
    return; // 无效的extent ID
  }

  // 计算位图页的物理页号
  page_id_t bitmap_physical_page = 1 + extent_id * PAGES_PER_EXTENT;

  // 读取位图页
  char bitmap_page[PAGE_SIZE];
  ReadPhysicalPage(bitmap_physical_page, bitmap_page);

  auto* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap_page);

  // 释放页面
  if (bitmap->DeAllocatePage(page_in_extent)) {
    // 更新元数据
    meta_page->extent_used_page_[extent_id]--;
    meta_page->num_allocated_pages_--;

    // 写回位图页
    WritePhysicalPage(bitmap_physical_page, bitmap_page);
    // 写回元数据页
    WritePhysicalPage(0, meta_data_);
  }
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  if (logical_page_id < 0) {
    return false; // 元数据页不算空闲页
  }

  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);

  constexpr size_t BITMAP_SIZE = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  constexpr size_t PAGES_PER_EXTENT = BITMAP_SIZE + 1;

  // 计算extent ID和在extent中的偏移
  uint32_t extent_id = logical_page_id / BITMAP_SIZE;
  uint32_t page_in_extent = logical_page_id % BITMAP_SIZE;

  // 读取元数据页
  auto* meta_page = reinterpret_cast<DiskFileMetaPage*>(meta_data_);

  if (extent_id >= meta_page->num_extents_) {
    return true; // 未分配的extent中的页视为空闲
  }

  // 计算位图页的物理页号
  page_id_t bitmap_physical_page = 1 + extent_id * PAGES_PER_EXTENT;

  // 读取位图页
  char bitmap_page[PAGE_SIZE];
  ReadPhysicalPage(bitmap_physical_page, bitmap_page);

  auto* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap_page);

  // 检查页是否被分配
  return bitmap->IsPageFree(page_in_extent);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  constexpr size_t MAX_PAGES = BitmapPage<PAGE_SIZE>::MAX_PAGES;
  page_id_t group = logical_page_id / MAX_PAGES;
  page_id_t offset = logical_page_id % MAX_PAGES;
  return group * (MAX_PAGES + 1) + offset + 2;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}