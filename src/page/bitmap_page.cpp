#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  // 检查是否已满
  if (page_allocated_ >= GetMaxSupportedSize()) {
    return false;
  }

  // 从next_free_page_开始查找空闲页
  for (uint32_t i = next_free_page_; i < GetMaxSupportedSize(); ++i) {
    uint32_t byte_index = i / 8;
    uint8_t bit_index = i % 8;

    if (IsPageFreeLow(byte_index, bit_index)) {
      // 找到空闲页，设置对应位为1
      bytes[byte_index] |= (1 << bit_index);
      page_offset = i;
      page_allocated_++;

      // 更新next_free_page_，下次从这里开始查找
      next_free_page_ = i + 1;
      return true;
    }
  }

  // 如果从next_free_page_开始没找到，从头开始查找
  for (uint32_t i = 0; i < next_free_page_; ++i) {
    uint32_t byte_index = i / 8;
    uint8_t bit_index = i % 8;

    if (IsPageFreeLow(byte_index, bit_index)) {
      bytes[byte_index] |= (1 << bit_index);
      page_offset = i;
      page_allocated_++;
      next_free_page_ = i + 1;
      return true;
    }
  }

  // 没有找到空闲页
  return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  // 检查偏移量是否有效
  if (page_offset >= GetMaxSupportedSize()) {
    return false;
  }

  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;

  // 检查页面是否已被分配
  if (IsPageFreeLow(byte_index, bit_index)) {
    return false;  // 页面本来就是空闲的
  }

  // 释放页面（设置对应位为0）
  bytes[byte_index] &= ~(1 << bit_index);
  page_allocated_--;

  // 更新next_free_page_，指向更小的空闲位置
  if (page_offset < next_free_page_) {
    next_free_page_ = page_offset;
  }

  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  // 检查偏移量是否超出支持的最大范围
  if (page_offset >= GetMaxSupportedSize()) return false;

  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;

  return IsPageFreeLow(byte_index, bit_index);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  if (byte_index >= MAX_CHARS) return false;

  // 获取对应字节，然后检查特定位是否为0
  unsigned char byte = bytes[byte_index];
  return (byte & (1 << bit_index)) == 0;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;