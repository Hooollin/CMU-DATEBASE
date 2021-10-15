//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
    this->plan_ = plan;
}

void SeqScanExecutor::Init() {
    TableHeap *target_table = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetTableOid())->table_.get();
    this->it_ = target_table->Begin(this->exec_ctx_->GetTransaction());
    this->it_end_ = target_table->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) { 
    if(this->it_ == this->it_end_){
        return false;
    }
    while(this->it_ != this->it_end_){
        *tuple = *this->it_;
        *rid = this->it_->GetRid();
        if(this->plan_->GetPredicate()->Evaluate(tuple, this->GetOutputSchema()).GetAs<bool>()){
            ++this->it_;
            return true;
        }
        ++this->it_;
    }
    return false;
}

}  // namespace bustub
