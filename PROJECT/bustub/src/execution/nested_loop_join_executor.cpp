//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  this->exec_ctx_ = exec_ctx;
  this->plan_ = plan;
  this->left_executor_ = std::move(left_executor);
  this->right_executor_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() { this->left_executor_.get()->Init(); }

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple left_tuple, right_tuple;
  RID left_tuple_rid, right_tuple_rid;
  while (this->left_executor_->Next(&left_tuple, &left_tuple_rid)) {
    this->right_executor_.get()->Init();
    while (this->right_executor_->Next(&right_tuple, &right_tuple_rid)) {
      if (this->plan_->Predicate()
              ->EvaluateJoin(&left_tuple, this->plan_->GetLeftPlan()->OutputSchema(), &right_tuple,
                             this->plan_->GetRightPlan()->OutputSchema())
              .GetAs<bool>()) {
        std::vector<Value> values;
        for (auto &col : GetOutputSchema()->GetColumns()) {
          values.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, this->left_executor_->GetOutputSchema(),
                                                       &right_tuple, this->right_executor_->GetOutputSchema()));
        }
        *tuple = Tuple(values, GetOutputSchema());
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
