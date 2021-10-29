//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), 
    plan_(plan), 
    child_(std::move(child)),
    aht_{this->plan_->GetAggregates(), this->plan_->GetAggregateTypes()},
    aht_iterator_(aht_.Begin()){
        this->exec_ctx_ = exec_ctx;
    }

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
    Tuple tuple;
    RID rid;
    this->child_->Init();
    while(this->child_->Next(&tuple, &rid)){
        this->aht_.InsertCombine(this->MakeKey(&tuple), this->MakeVal(&tuple));
    }
    this->aht_iterator_ = this->aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
    while(this->aht_iterator_ != this->aht_.End()){
        if(this->plan_->GetHaving() == nullptr || this->plan_->GetHaving()->EvaluateAggregate(this->aht_iterator_.Key().group_bys_, this->aht_iterator_.Val().aggregates_).GetAs<bool>()){
            std::vector<Value> values;
            for(const auto &col : this->GetOutputSchema()->GetColumns()){
                values.push_back(col.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_));
            }
            *tuple = Tuple(values, this->GetOutputSchema());
            ++this->aht_iterator_;
            return true;
        }
        ++this->aht_iterator_;
    }
    return false;
}

}  // namespace bustub
