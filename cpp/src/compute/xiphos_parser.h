#pragma once
#include <arrow/record_batch.h>
#include <memory>
#include <string>
#include "daxl/Pipeline.h"
#include "daxl/SubTreeExecutor.h"
#include "daxl/SubTree.h"
#include "daxl/Daxl.h"
#include "compute/substrait_utils.h"
#include <string>
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"


namespace gazellejni {
namespace compute {


class XiphosResultIterator : public ResultIterator<arrow::RecordBatch> 
{ 
public:
    XiphosResultIterator(const std::string &);
    virtual bool HasNext() override;
    virtual arrow::Status Next(std::shared_ptr<arrow::RecordBatch>* out) override; 
private:
    std::string execution_id_;
    bool is_plan_executed_ = false;
    daxl::SubTreeExecutor execution_;
    std::shared_ptr<arrow::RecordBatch> batch_;
    bool is_consumed_;
};

class XiphosParser {
public:
    XiphosParser();
    virtual ~XiphosParser();
    void Init();
    void ParsePlan(const substrait::Plan & plan);
    std::shared_ptr<ResultIterator<arrow::RecordBatch>> getResIter();
private:
    std::string execution_id_;
    void ParseRelRoot(const substrait::RelRoot& sroot, daxl::Pipeline & );
    void ParseRel(const substrait::Rel&, daxl::Pipeline &);
    void ParseReadRel(const substrait::ReadRel& sread, daxl::Pipeline &pipeline);
};

}
}

