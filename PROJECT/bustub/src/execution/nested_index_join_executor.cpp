//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  this->exec_ctx_ = exec_ctx_;
  this->plan_ = plan;
  this->child_executor_ = std::move(child_executor);
}

void NestIndexJoinExecutor::Init() {
  this->child_executor_->Init();
  this->index_info_ = this->exec_ctx_->GetCatalog()->GetIndex(
      this->plan_->GetIndexName(), this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetInnerTableOid())->name_);
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetInnerTableOid());
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple left_tuple, right_to_search_tuple;
  RID left_tuple_RID, right_to_search_RID;
  while (this->child_executor_->Next(&right_to_search_tuple, &right_to_search_RID)) {
    left_tuple = right_to_search_tuple.KeyFromTuple(*this->plan_->OuterTableSchema(), this->index_info_->key_schema_,
                                                    this->index_info_->index_->GetKeyAttrs());
    std::vector<RID> result;
    this->index_info_->index_->ScanKey(left_tuple, &result, this->exec_ctx_->GetTransaction());
    if (result.size() > 0) {
      assert(result.size() == 1);
      Tuple t;
      this->table_info_->table_->GetTuple(result[0], &t, this->exec_ctx_->GetTransaction());
      if (this->plan_->Predicate()
              ->EvaluateJoin(&t, this->plan_->InnerTableSchema(), &right_to_search_tuple,
                             this->plan_->OuterTableSchema())
              .GetAs<bool>()) {
        std::vector<Value> values;
        for (auto &col : GetOutputSchema()->GetColumns()) {
          values.push_back(col.GetExpr()->EvaluateJoin(&t, this->plan_->InnerTableSchema(), &right_to_search_tuple,
                                                       this->plan_->OuterTableSchema()));
        }
        *tuple = Tuple(values, GetOutputSchema());
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
