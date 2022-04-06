/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int index, BufferPoolManager *bpm)
    : page_id_(page_id), index_(index), buffer_pool_manager_(bpm) {
  page_ptr_ = buffer_pool_manager_->FetchPage(page_id_);
  leaf_ptr_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_ptr_->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
  //     throw std::runtime_error("unimplemented");
  return page_id_ == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  // throw std::runtime_error("unimplemented");
  assert(leaf_ptr_ != nullptr);
  return leaf_ptr_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  //   throw std::runtime_error("unimplemented");
  if (leaf_ptr_ == nullptr) {
    return *this;
  }

  if (index_ == leaf_ptr_->GetSize() - 1) {
    page_id_t old_page_id = page_id_;
    // Page *old_page_ptr = page_ptr_;

    page_id_ = leaf_ptr_->GetNextPageId();
    leaf_ptr_ = GetLeafPage();

    buffer_pool_manager_->UnpinPage(old_page_id, false);
    index_ = 0;
  } else {
    index_++;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *INDEXITERATOR_TYPE::GetLeafPage() {
  if (page_id_ == INVALID_PAGE_ID) {
    return nullptr;
  }
  Page *page_ptr = buffer_pool_manager_->FetchPage(page_id_);
  if (page_ptr == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "INDEXITERATOR_TYPE::GetLeafPage() Out of memory in iterator");
  }
  return reniterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_ptr->GetData());
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
