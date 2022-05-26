//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}
frame_id_t BufferPoolManager::GetAvailableFrame() {
  frame_id_t frame_id;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    return frame_id;
  }

  if (replacer_->Victim(&frame_id)) {
    Page *page_ptr = GetPage(frame_id);
    page_id_t page_id = page_ptr->GetPageId();
    if (page_ptr->IsDirty()) {
      disk_manager_->WritePage(page_id, page_ptr->GetData());
      page_ptr->SetPinCount(0);
    }
    page_table_.erase(page_ptr->GetPageId());

    return frame_id;
  }
  return INVALID_FRAME_ID;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  assert(page_id != INVALID_PAGE_ID);
  // if (page_id == INVALID_PAGE_ID) {
  //   printf("1111\n");
  //   return nullptr;
  // }
  std::unique_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = GetFrame(page_id);
  Page *page_ptr = nullptr;
  if (frame_id != INVALID_FRAME_ID) {
    page_ptr = &pages_[frame_id];
    page_ptr->AddPinCount();
    replacer_->Pin(frame_id);
    return page_ptr;
  }

  frame_id = GetAvailableFrame();
  if (frame_id == INVALID_FRAME_ID) {
    return nullptr;
  }

  page_table_.insert({page_id, frame_id});
  page_ptr = &pages_[frame_id];
  disk_manager_->ReadPage(page_id, page_ptr->GetData());
  page_ptr->SetPageId(page_id);
  page_ptr->SetPinCount(1);
  page_ptr->SetDirty(false);
  replacer_->Pin(frame_id);

  return page_ptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::unique_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = GetFrame(page_id);
  if (frame_id == INVALID_FRAME_ID) {
    return false;
  }

  Page *page_ptr = GetPage(frame_id);
  if (is_dirty) {
    page_ptr->SetDirty(is_dirty);
  }
  int pin_count = page_ptr->GetPinCount();
  if (pin_count <= 0) {
    return false;
  }
  pin_count = page_ptr->SubPinCount();
  if (pin_count == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  assert(page_id != INVALID_PAGE_ID);
  std::unique_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = GetFrame(page_id);
  if (frame_id == INVALID_FRAME_ID) {
    return true;
  }
  Page *page_ptr = GetPage(frame_id);
  if (page_ptr->IsDirty()) {
    disk_manager_->WritePage(page_id, page_ptr->GetData());
  }

  page_ptr->ResetAll();
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  replacer_->Pin(frame_id);

  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::unique_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = GetAvailableFrame();
  if (frame_id == INVALID_FRAME_ID) {
    return nullptr;
  }

  Page *page_ptr = GetPage(frame_id);
  page_id_t new_page_id = disk_manager_->AllocatePage();
  page_ptr->ResetAll();
  page_ptr->SetPageId(new_page_id);
  page_ptr->SetPinCount(1);

  page_table_.insert({new_page_id, frame_id});
  replacer_->Pin(frame_id);

  disk_manager_->WritePage(page_ptr->GetPageId(), page_ptr->GetData());
  *page_id = new_page_id;
  return page_ptr;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  assert(page_id != INVALID_PAGE_ID);
  std::unique_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = GetFrame(page_id);
  if (frame_id == INVALID_FRAME_ID) {
    disk_manager_->DeallocatePage(page_id);
    return true;
  }

  Page *page_ptr = GetPage(frame_id);
  if (page_ptr->GetPinCount() > 0) {
    return false;
  }

  disk_manager_->DeallocatePage(page_id);
  page_table_.erase(page_id);
  page_ptr->ResetAll();
  replacer_->Pin(frame_id);
  free_list_.push_back(frame_id);

  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (const auto &pair : page_table_) {
    FlushPageImpl(pair.first);
  }
}

}  // namespace bustub
