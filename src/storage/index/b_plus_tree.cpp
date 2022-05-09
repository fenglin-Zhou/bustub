//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  if (IsEmpty()) {
    return false;
  }
  root_id_latch_.RLock();
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(nullptr);
  }
  Page *page_ptr = FindLeafPageRW(key, transaction, OpType::READ, false);
  // Page *page_ptr = FindLeafPage(key);
  page_id_t page_id = page_ptr->GetPageId();
  LeafPage *leaf_page_ptr = reinterpret_cast<LeafPage *>(page_ptr->GetData());

  RID rid;
  bool ret = leaf_page_ptr->Lookup(key, &rid, comparator_);
  if (transaction != nullptr) {
    ReleaseLatch(transaction, OpType::READ);
  } else {
    page_ptr->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, false);
  }
  if (!ret) {
    return false;
  }
  result->push_back(rid);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  assert(transaction != nullptr);
  bool ret;
  root_id_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);

  if (IsEmpty()) {
    StartNewTree(key, value);
    ret = true;
  } else {
    ret = InsertIntoLeaf(key, value, transaction);
  }
  ReleaseLatch(transaction, OpType::INSERT);
  return ret;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t root_page_id = INVALID_PAGE_ID;
  Page *page_ptr = buffer_pool_manager_->NewPage(&root_page_id);
  if (page_ptr == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "BPLUSTREE_TYPE::StartNewTree, buffer pool out of memory!");
  }
  root_page_id_ = root_page_id;
  UpdateRootPageId(1);
  LeafPage *leaf_page_ptr = reinterpret_cast<LeafPage *>(page_ptr->GetData());
  leaf_page_ptr->Init(root_page_id, INVALID_PAGE_ID, leaf_max_size_);
  leaf_page_ptr->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page *page_ptr = FindLeafPageRW(key, transaction, OpType::INSERT, false);
  // Page *page_ptr = FindLeafPage(key);
  if (page_ptr == nullptr) {
    return false;
  }
  LeafPage *leaf_page_ptr = reinterpret_cast<LeafPage *>(page_ptr->GetData());
  page_id_t page_id = page_ptr->GetPageId();
  // the key exists
  if (leaf_page_ptr->CheckDuplicated(key, comparator_)) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return false;
  }
  int size = leaf_page_ptr->Insert(key, value, comparator_);
  if (size == leaf_page_ptr->GetMaxSize()) {
    LeafPage *new_page_ptr = Split<LeafPage>(leaf_page_ptr);
    leaf_page_ptr->MoveHalfTo(new_page_ptr);
    new_page_ptr->SetNextPageId(leaf_page_ptr->GetNextPageId());
    // new_page_ptr->SetPrevPageId(leaf_page_ptr->GetPageId());
    leaf_page_ptr->SetNextPageId(new_page_ptr->GetPageId());

    InsertIntoParent(leaf_page_ptr, new_page_ptr->KeyAt(0), new_page_ptr, transaction);
    buffer_pool_manager_->UnpinPage(new_page_ptr->GetPageId(), true);
  }
  // buffer_pool_manager_->UnpinPage(page_id, true);
  page_ptr->SetDirty(true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id = INVALID_PAGE_ID;
  Page *page_ptr = GetNewPage(&page_id, "In func BPLUSTREE_TYPE::Split buffer pool out of memory!");

  N *new_leaf_page = reinterpret_cast<N *>(page_ptr->GetData());
  if (node->IsLeafPage()) {
    new_leaf_page->Init(page_id, node->GetParentPageId(), leaf_max_size_);
  } else {
    new_leaf_page->Init(page_id, node->GetParentPageId(), internal_max_size_);
  }
  return new_leaf_page;
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::GetPage(page_id_t page_id, const std::string log_string) {
  Page *page_ptr = buffer_pool_manager_->FetchPage(page_id);
  if (page_ptr == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, log_string);
  }
  return page_ptr;
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::GetNewPage(page_id_t *page_id, const std::string log_string) {
  Page *page_ptr = buffer_pool_manager_->NewPage(page_id);
  if (page_ptr == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, log_string);
  }
  return page_ptr;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    page_id_t new_root_page_id = INVALID_PAGE_ID;
    Page *new_root_page_ptr = nullptr;
    new_root_page_ptr = GetNewPage(&new_root_page_id, "BPLUSTREE_TYPE::InsertIntoParent buffer pool out of memory!");

    InternalPage *new_internal_ptr = reinterpret_cast<InternalPage *>(new_root_page_ptr->GetData());

    new_internal_ptr->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);

    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    new_internal_ptr->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    root_page_id_ = new_root_page_id;
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    return;
  }
  page_id_t parent_page_id = old_node->GetParentPageId();
  Page *page_ptr = GetPage(parent_page_id, "BPLUSTREE_TYPE::InsertIntoParent  buffer pool out of memory!");

  InternalPage *parent_internal_ptr = reinterpret_cast<InternalPage *>(page_ptr->GetData());
  int new_size = parent_internal_ptr->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  if (new_size == parent_internal_ptr->GetMaxSize()) {
    InternalPage *new_internal_ptr = Split<InternalPage>(parent_internal_ptr);
    parent_internal_ptr->MoveHalfTo(new_internal_ptr, buffer_pool_manager_);

    InsertIntoParent(parent_internal_ptr, new_internal_ptr->KeyAt(0), new_internal_ptr, transaction);

    buffer_pool_manager_->UnpinPage(new_internal_ptr->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  assert(transaction != nullptr);
  root_id_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);
  Page *leaf_page_ptr = FindLeafPageRW(key, transaction, OpType::DELETE, false);
  // Page *leaf_page_ptr = FindLeafPage(key);
  page_id_t leaf_page_id = leaf_page_ptr->GetPageId();
  LeafPage *leaf_ptr = reinterpret_cast<LeafPage *>(leaf_page_ptr->GetData());

  if (!leaf_ptr->CheckDuplicated(key, comparator_)) {
    ReleaseLatch(transaction, OpType::DELETE);
    return;
  }

  int index = leaf_ptr->KeyIndex(key, comparator_);
  leaf_ptr->RemoveAt(index);
  bool ret = false;
  if (leaf_ptr->GetSize() < leaf_ptr->GetMinSize()) {
    ret = CoalesceOrRedistribute<LeafPage>(leaf_ptr, transaction);
  }
  if (ret) {
    transaction->AddIntoDeletedPageSet(leaf_page_id);
  } else {
    leaf_page_ptr->SetDirty(true);
  }
  ReleaseLatch(transaction, OpType::DELETE);
  DeletePages(transaction);
  // buffer_pool_manager_->UnpinPage(leaf_page_id, true);
  // root_id_latch_.WUnlock();
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }
  page_id_t parent_page_id = INVALID_PAGE_ID;
  page_id_t pre_page_id = INVALID_PAGE_ID;
  page_id_t next_page_id = INVALID_PAGE_ID;
  Page *parent_page_ptr = nullptr;
  Page *pre_page_ptr = nullptr;
  Page *next_page_ptr = nullptr;
  InternalPage *parent_internal_tree_ptr = nullptr;
  N *pre_node = nullptr;
  N *next_node = nullptr;

  parent_page_id = node->GetParentPageId();
  parent_page_ptr = GetPage(parent_page_id, "BPLUSTREE_TYPE::CoalesceOrRedistribute : out of memory!");
  parent_internal_tree_ptr = reinterpret_cast<InternalPage *>(parent_page_ptr->GetData());

  int node_index = parent_internal_tree_ptr->ValueIndex(node->GetPageId());
  // borrow value from previous node
  if (node_index > 0) {
    pre_page_id = parent_internal_tree_ptr->ValueAt(node_index - 1);
    pre_page_ptr = GetPage(pre_page_id, "BPLUSTREE_TYPE::CoalesceOrRedistribute out of memory!");
    pre_node = reinterpret_cast<N *>(pre_page_ptr->GetData());

    // If the borrow is successful, move the value and return
    if (pre_node->GetSize() > pre_node->GetMinSize()) {
      Redistribute<N>(pre_node, node, 1);
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      buffer_pool_manager_->UnpinPage(pre_page_id, true);
      return false;
    }
  }
  // Borrow from later node
  if (node_index != parent_internal_tree_ptr->GetSize() - 1) {
    next_page_id = parent_internal_tree_ptr->ValueAt(node_index + 1);
    next_page_ptr = GetPage(next_page_id, "BPLUSTREE_TYPE::CoalesceOrRedistribute out of memory!");
    next_node = reinterpret_cast<N *>(next_page_ptr->GetData());

    // If the borrow is successful, move the value and return
    if (next_node->GetSize() > next_node->GetMinSize()) {
      Redistribute(next_node, node, 0);
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      if (node_index > 0) {
        // Unmodified previous node, unpin page
        buffer_pool_manager_->UnpinPage(pre_page_id, false);
      }
      buffer_pool_manager_->UnpinPage(next_page_id, true);
      return false;
    }
  }

  bool ret = false;
  if (pre_page_id != INVALID_PAGE_ID) {
    ret = Coalesce(&pre_node, &node, &parent_internal_tree_ptr, node_index, transaction);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    if (ret) {
      transaction->AddIntoDeletedPageSet(parent_page_id);
    }
    buffer_pool_manager_->UnpinPage(pre_page_id, true);
    if (next_page_id != INVALID_PAGE_ID) {
      buffer_pool_manager_->UnpinPage(next_page_id, false);
    }
    return true;
  }

  ret = Coalesce(&node, &next_node, &parent_internal_tree_ptr, node_index + 1, transaction);
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  buffer_pool_manager_->UnpinPage(next_page_id, true);
  transaction->AddIntoDeletedPageSet(next_page_id);
  if (ret) {
    transaction->AddIntoDeletedPageSet(parent_page_id);
  }

  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  if ((*node)->IsLeafPage()) {
    LeafPage *temp_node = reinterpret_cast<LeafPage *>(*node);
    LeafPage *temp_neighbor_node = reinterpret_cast<LeafPage *>(*neighbor_node);
    temp_node->MoveAllTo(temp_neighbor_node);
  } else {
    InternalPage *temp_node = reinterpret_cast<InternalPage *>(*node);
    InternalPage *temp_neighbor_node = reinterpret_cast<InternalPage *>(neighbor_node);
    KeyType middle_key = (*parent)->KeyAt(index);
    temp_node->MoveAllTo(temp_neighbor_node, middle_key, buffer_pool_manager_);
  }

  (*parent)->Remove(index);
  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    return CoalesceOrRedistribute(*parent, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  page_id_t parent_page_id = node->GetParentPageId();
  Page *parent_page_ptr = GetPage(parent_page_id, "BPLUSTREE_TYPE::Redistribute : out of memory!");
  InternalPage *parent_tree_ptr = reinterpret_cast<InternalPage *>(parent_page_ptr->GetData());
  // node is leaf
  if (node->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
    if (index == 0) {
      neighbor_leaf_node->MoveFirstToEndOf(leaf_node);
      int index_insert = parent_tree_ptr->ValueIndex(neighbor_leaf_node->GetPageId());
      parent_tree_ptr->SetKeyAt(index_insert, neighbor_leaf_node->KeyAt(0));
    } else {
      neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
      int index_insert = parent_tree_ptr->ValueIndex(leaf_node->GetPageId());
      parent_tree_ptr->SetKeyAt(index_insert, leaf_node->KeyAt(0));
    }
  } else {  // node is internal node
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
    if (index == 0) {
      int index_node = parent_tree_ptr->ValueIndex(neighbor_internal_node->GetPageId());
      KeyType middle_key = parent_tree_ptr->KeyAt(index_node);
      KeyType next_middle_key = neighbor_internal_node->KeyAt(1);
      neighbor_internal_node->MoveFirstToEndOf(internal_node, middle_key, buffer_pool_manager_);
      parent_tree_ptr->SetKeyAt(index_node, next_middle_key);
    } else {
      int index_node = parent_tree_ptr->ValueIndex(internal_node->GetPageId());
      KeyType middle_key = parent_tree_ptr->KeyAt(index_node);
      KeyType next_middle_key = neighbor_internal_node->KeyAt(neighbor_internal_node->GetSize() - 1);
      neighbor_internal_node->MoveLastToFrontOf(internal_node, middle_key, buffer_pool_manager_);
      parent_tree_ptr->SetKeyAt(index_node, next_middle_key);
    }
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->GetSize() > 1) {
    return false;
  }
  page_id_t new_root_id = INVALID_PAGE_ID;
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() == 1) {
      return false;
    }
  } else {
    InternalPage *old_internal_root_node = reinterpret_cast<InternalPage *>(old_root_node);
    new_root_id = old_internal_root_node->RemoveAndReturnOnlyChild();

    Page *new_root_page_ptr = GetPage(new_root_id, "BPLUSTREE_TYPE::AdjustRoot : out of memory!");
    BPlusTreePage *new_root_internal_ptr = reinterpret_cast<BPlusTreePage *>(new_root_page_ptr->GetData());
    new_root_internal_ptr->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(new_root_id, true);
  }
  root_page_id_ = new_root_id;
  UpdateRootPageId(0);
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  root_id_latch_.RLock();
  Page *left_page_ptr = FindLeafPageRW(KeyType(), nullptr, OpType::READ, true);
  return INDEXITERATOR_TYPE(left_page_ptr, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  root_id_latch_.RLock();
  Page *page_ptr = FindLeafPageRW(key, nullptr, OpType::READ, false);
  LeafPage *leaf_ptr = reinterpret_cast<LeafPage *>(page_ptr->GetData());
  int index = leaf_ptr->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(page_ptr, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  Page *page_ptr = buffer_pool_manager_->FetchPage(root_page_id_);
  page_id_t page_id = root_page_id_;
  page_id_t next_page_id = INVALID_PAGE_ID;

  BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *>(page_ptr->GetData());
  while (!tree_page->IsLeafPage()) {
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(tree_page);

    if (leftMost) {
      next_page_id = internal_page->ValueAt(0);
    } else {
      next_page_id = internal_page->Lookup(key, comparator_);
    }

    buffer_pool_manager_->UnpinPage(page_id, false);
    if (next_page_id == INVALID_PAGE_ID) {
      return nullptr;
    }
    page_ptr = buffer_pool_manager_->FetchPage(next_page_id);
    page_id = next_page_id;
    tree_page = reinterpret_cast<BPlusTreePage *>(page_ptr->GetData());
  }
  return page_ptr;
}
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * @parameter:op type of operation
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPageRW(const KeyType &key, Transaction *transaction, OpType op, bool leftMost) {
  Page *page_ptr = buffer_pool_manager_->FetchPage(root_page_id_);
  Page *old_page = nullptr;
  page_id_t page_id = root_page_id_;
  page_id_t old_page_id = INVALID_PAGE_ID;

  BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *>(page_ptr->GetData());
  while (true) {
    if (op == OpType::READ) {
      page_ptr->RLatch();
    } else {
      page_ptr->WLatch();
    }
    if (transaction == nullptr) {
      if (old_page != nullptr) {
        if (op == OpType::READ) {
          old_page->RUnlatch();
        } else {
          old_page->WUnlatch();
        }
        buffer_pool_manager_->UnpinPage(old_page_id, false);
      } else {
        if (op == OpType::READ) {
          root_id_latch_.RUnlock();
        } else {
          root_id_latch_.WUnlock();
        }
      }
    } else {
      if (CheckSafe(tree_page, op)) {
        ReleaseLatch(transaction, op);
      }
      transaction->AddIntoPageSet(page_ptr);
    }
    if (tree_page->IsLeafPage()) break;
    old_page = page_ptr;
    old_page_id = page_id;

    InternalPage *internal_ptr = reinterpret_cast<InternalPage *>(tree_page);
    page_id = leftMost ? internal_ptr->ValueAt(0) : internal_ptr->Lookup(key, comparator_);
    page_ptr = buffer_pool_manager_->FetchPage(page_id);
    tree_page = reinterpret_cast<BPlusTreePage *>(page_ptr->GetData());
  }
  return page_ptr;
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::CheckSafe(BPlusTreePage *tree_ptr, OpType op) {
  if (op == OpType::READ) {
    return true;
  }
  if (op == OpType::INSERT) {
    return tree_ptr->GetSize() < (tree_ptr->GetMaxSize() - 1);
  }
  // op == OpType::DELETE
  if (tree_ptr->IsRootPage()) {
    if (tree_ptr->IsLeafPage()) {
      return true;
    }
    return tree_ptr->GetSize() > 2;
  }
  return tree_ptr->GetSize() > tree_ptr->GetMinSize();
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeletePages(Transaction *transaction) {
  const auto set_ptr = transaction->GetDeletedPageSet();
  for (const auto &page_id : *set_ptr) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  set_ptr->clear();
}
/*
 * when the operation is done, call this function to release the latchs
 *
 * @parameter:op  type of operation
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatch(Transaction *transaction, OpType op) {
  if (transaction == nullptr) {
    return;
  }
  auto pages = transaction->GetPageSet();
  for (auto page_ptr : *pages) {
    // std::shared_ptr<std::deque<Page *>> deque_ptr = transaction->GetPageSet();
    // while (!deque_ptr->empty()) {
    //   Page *page_ptr = deque_ptr->front();
    //   deque_ptr->pop_front();
    if (page_ptr == nullptr) {
      if (op == OpType::READ) {
        root_id_latch_.RUnlock();
      } else {
        root_id_latch_.WUnlock();
      }
    } else {
      page_id_t page_id = page_ptr->GetPageId();
      if (op == OpType::READ) {
        page_ptr->RUnlatch();
      }
      if (op == OpType::INSERT || op == OpType::DELETE) {
        page_ptr->WUnlatch();
      }
      buffer_pool_manager_->UnpinPage(page_id, false);
    }
  }
  pages->clear();
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
