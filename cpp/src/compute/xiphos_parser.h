#pragma once
#include <arrow/record_batch.h>
#include <memory>
#include <string>
#include "compute/substrait_utils.h"



namespace gazellejni {
namespace compute {


class XiphosResultIterator : public ResultIterator<arrow::RecordBatch> 
{ 
public:
    XiphosResultIterator(const std::string &_execution_id) :
        execution_id(_execution_id)
    {
    }
    virtual bool HasNext() override;
    virtual arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) override; 
private:
    std::string execution_id;
};

class XiphosParser {
public:
    XiphosParser();
    virtual ~XiphosParser();
    void ParsePlan(const substrait::Plan & plan);
    std::shared_ptr<ResultIterator<arrow::RecordBatch>> getResIter();
private:
    std::string execution_id;
    std::unordered_map<uint64_t, std::string> functions_map_;
};

}
}
