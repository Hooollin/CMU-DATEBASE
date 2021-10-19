//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
}

void IndexScanExecutor::Init() {
  Index *target_index = this->exec_ctx_->GetCatalog()->GetIndex(this->plan_->GetIndexOid())->index_.get();
  this->it_ =
      reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(target_index)->GetBeginIterator();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (!this->it_.isEnd()) {
    auto p = *this->it_;
    Tuple this_one{p.second};
    *tuple = this_one;
    *rid = p.second;
    if (this->plan_->GetPredicate() != nullptr) {
      if (this->plan_->GetPredicate()->Evaluate(tuple, this->GetOutputSchema()).GetAs<bool>()) {
        ++this->it_;
        return true;
      } else {
        ++this->it_;
      }
    } else {
      ++this->it_;
      return true;
    }
  }
  return false;
}
}  // namespace bustub
