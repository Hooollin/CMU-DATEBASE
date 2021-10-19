//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
  this->child_executor_ = std::move(child_executor_);
}

void UpdateExecutor::Init() {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  this->child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (this->child_executor_->Next(tuple, rid)) {
    Tuple updated_tuple = this->GenerateUpdatedTuple(*tuple);
    this->table_info_->table_->UpdateTuple(updated_tuple, updated_tuple.GetRid(), this->exec_ctx_->GetTransaction());
    for (auto &index_info : this->exec_ctx_->GetCatalog()->GetTableIndexes(this->table_info_->name_)) {
      index_info->index_->DeleteEntry(
          tuple->KeyFromTuple(this->table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          *rid, this->exec_ctx_->GetTransaction());
      index_info->index_->InsertEntry(updated_tuple.KeyFromTuple(this->table_info_->schema_, index_info->key_schema_,
                                                                 index_info->index_->GetKeyAttrs()),
                                      updated_tuple.GetRid(), this->exec_ctx_->GetTransaction());
    }
    return true;
  }
  return false;
}
}  // namespace bustub
