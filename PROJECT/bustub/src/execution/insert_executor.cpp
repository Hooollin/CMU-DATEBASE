//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
  this->child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() {
  this->target_table_ = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->TableOid())->table_.get();
  this->target_table_metadata_ = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->TableOid());

  if (this->plan_->IsRawInsert()) {
    this->it_ = this->plan_->RawValues().begin();
    this->it_begin_ = this->plan_->RawValues().begin();
    this->it_end_ = this->plan_->RawValues().end();
  } else {
    this->child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple to_insert_tuple;
  RID to_insert_rid;
  if (this->plan_->IsRawInsert()) {
    if (this->it_ != this->it_end_) {
      Tuple to_insert_tuple{*this->it_, &this->target_table_metadata_->schema_};
      to_insert_rid = to_insert_tuple.GetRid();
      if (this->target_table_->InsertTuple(to_insert_tuple, &to_insert_rid, this->exec_ctx_->GetTransaction())) {
        for (auto &index_info : this->exec_ctx_->GetCatalog()->GetTableIndexes(this->target_table_metadata_->name_)) {
          index_info->index_->InsertEntry(
              to_insert_tuple.KeyFromTuple(this->target_table_metadata_->schema_, index_info->key_schema_,
                                           index_info->index_->GetKeyAttrs()),
              to_insert_rid, this->exec_ctx_->GetTransaction());
        }
        this->it_++;
        return true;
      }
    }
    return false;
  } else {
    if (this->child_executor_->Next(&to_insert_tuple, &to_insert_rid)) {
      if (this->target_table_->InsertTuple(to_insert_tuple, &to_insert_rid, this->exec_ctx_->GetTransaction())) {
        for (auto &index_info : this->exec_ctx_->GetCatalog()->GetTableIndexes(this->target_table_metadata_->name_)) {
          index_info->index_->InsertEntry(
              to_insert_tuple.KeyFromTuple(this->target_table_metadata_->schema_, index_info->key_schema_,
                                           index_info->index_->GetKeyAttrs()),
              to_insert_rid, this->exec_ctx_->GetTransaction());
        }
        return true;
      }
    }
    return false;
  }
}

}  // namespace bustub
