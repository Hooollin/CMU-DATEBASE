//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
  this->child_executor_ = std::move(child_executor);
}

void DeleteExecutor::Init() {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->TableOid());
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (child_executor_->Next(tuple, rid)) {
    if (!this->table_info_->table_->MarkDelete(*rid, this->exec_ctx_->GetTransaction())) {
      throw bustub::Exception("shit happens when deleting tuples!");
    }
    for (auto &index_info : this->exec_ctx_->GetCatalog()->GetTableIndexes(this->table_info_->name_)) {
      index_info->index_->DeleteEntry(
          tuple->KeyFromTuple(this->table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          *rid, this->exec_ctx_->GetTransaction());
    }
    return true;
  }
  return false;
}

}  // namespace bustub
