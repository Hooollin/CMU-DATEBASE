//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan), 
    child_executor_(std::move(child_executor)) {
        this->exec_ctx_ = exec_ctx_;
        this->counter_ = 0;
        this->offset_ = 0;
    }

void LimitExecutor::Init() {
    this->child_executor_->Init();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
    while(this->counter_ < this->plan_->GetLimit() && this->child_executor_->Next(tuple, rid)){
        if(this->offset_ < this->plan_->GetOffset()){
            this->offset_++;
            continue;
        }
        this->counter_++;
        return true;
    }
    return false;
}

}  // namespace bustub
