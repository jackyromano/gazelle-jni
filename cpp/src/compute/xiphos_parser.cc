#include "xiphos_parser.h"
#include <arrow/pretty_print.h>
using namespace daxl;
static bool verbose = true;

gazellejni::compute::XiphosParser::XiphosParser()
{
  std::cout << "Constructor\n";
}

gazellejni::compute::XiphosParser::~XiphosParser()
{
}

void gazellejni::compute::XiphosParser::Init(){
  std::cout << "Init\n";
  std::string confPath;
  if (getenv("DAXL_CONFIG_FILE") != nullptr) {
      confPath = std::string(getenv("DAXL_CONFIG_FILE"));
  } else {
      std::cerr << "DAXL_CONFIG_FILE env variable is not defined..\n please define it to point to DAXL configuration file\n";
  }

  Daxl::getInstance()->init(confPath, daxl::MASTER);
}


void gazellejni::compute::XiphosParser::ParsePlan(const substrait::Plan &splan)
{

    daxl::SubTree subtree;
    //daxl::Pipeline pipeline("lineitem", "p1");
    daxl::Pipeline pipeline("ints", "p1");
#if 0
  for (auto& sextension : splan.extensions()) {
    if (!sextension.has_extension_function()) {
      continue;
    }
    auto& sfmap = sextension.extension_function();
    execution_id = sfmap.function_anchor();
    auto name = sfmap.name();
    resultIterator(execution_id);
    break;
   // functions_map_[id] = name;
   // if (verbose) std::cout << "Function id: " << id << ", name: " << name << std::endl;
  }
#endif
  assert(splan.size() == 1);
  for (auto& srel : splan.relations()) {
    if (srel.has_root()) {
      ParseRelRoot(srel.root(), pipeline);
    }
    if (srel.has_rel()) {
      ParseRel(srel.rel(), pipeline);
    }
  }
  std::cout << "Stream To Host" << std::endl;
  pipeline.streamToHost();
  std::cout << "Add pipeline to tree" << std::endl;
  subtree.addPipe(pipeline);
  std::cout << "Optimize" << std::endl;
  Status stat = subtree.optimize();
  if (stat.isError()) {
      std::cout << "Optimize failed with \"" << stat.getErrorMessage() << "\"" << std::endl;
  }
  std::cout << "getId: " << subtree.getId() << std::endl;
  execution_id_ = subtree.getId();
}

gazellejni::compute::XiphosResultIterator::XiphosResultIterator(const std::string &_execution_id):
        execution_id_( _execution_id),
        batch_(nullptr)
{
}


std::shared_ptr<ResultIterator<arrow::RecordBatch>> gazellejni::compute::XiphosParser::getResIter()
{
  std::cout << "getResIterator" << std::endl;
	return std::make_shared<XiphosResultIterator>(execution_id_);
}


bool gazellejni::compute::XiphosResultIterator::HasNext()  {

    std::cout << ">> Xiphos Result Iterator HasNext\n";
    if(!is_plan_executed_){
        std::cout << "Execute: " << execution_id_ << std::endl;
        execution_.execute(execution_id_);
        is_plan_executed_ = true;
        batch_ = nullptr;
        is_consumed_ = false;
    }
    std::cout << "Get Next Chunk1" << std::endl;

    if (!is_consumed_ && batch_ == nullptr) {
        batch_ = execution_.getNextRowChunk();
    }
    std::cout << "hasNext has a batch\n";
    if (batch_ != nullptr) {
        std::cout << "batch is valid\n";
    } else {
        is_consumed_ = true;
        std::cout << "XiphosResultIterator consumed\n";
    }
    return batch_ != nullptr;
}

arrow::Status gazellejni::compute::XiphosResultIterator::Next(std::shared_ptr<arrow::RecordBatch>* out) {
    std::cout << ">> Next batch: " << batch_ << std::endl;
    assert(batch != nullptr);

    if(batch_){
        std::cout << "getResIterator" << std::endl;
        *out = batch_;
        std::cout << "out assigned\n";
        arrow::PrettyPrint(*out->get(), 0, &std::cout); 

        batch_ = nullptr;
        return arrow::Status::OK();
    }
    return arrow::Status::Invalid("Invalid Status");
}

void gazellejni::compute::XiphosParser::ParseRel(const substrait::Rel& srel, daxl::Pipeline& pipeline) {
  if (srel.has_aggregate() && false) {
    if (verbose) std::cout << "ParseRel Aggregate" << std::endl;
    //ParseAggregateRel(srel.aggregate());
  } else if (srel.has_project() && false) {
    if (verbose) std::cout << "ParseRel Project" << std::endl;
    //ParseProjectRel(srel.project());
  } else if (srel.has_filter() && false) {
    if (verbose) std::cout << "ParseRel Filter" << std::endl;
    //ParseFilterRel(srel.filter());
  } else if (srel.has_read()) {
    if (verbose) std::cout << "ParseRel Read" << std::endl;
    ParseReadRel(srel.read(), pipeline);
  } else {
    if (verbose) std::cout << "not supported" << std::endl;
    return;
  }
}

void gazellejni::compute::XiphosParser::ParseRelRoot(const substrait::RelRoot& sroot, daxl::Pipeline & pipeline) {
  if (verbose) std::cout << "ParseRelRoot" << std::endl;
  if (sroot.has_input()) {
    auto& srel = sroot.input();
    ParseRel(srel, pipeline);
  }
  if (verbose) {
    auto& snames = sroot.names();
    if (snames.size() == 0) {
      std::cout << "ParseRelRoot has no names" << std::endl;
    } else {
      for (auto const& n : snames) {
        std::cout << "ParseRelRoot : " << n << std::endl;
      }
    }
  }
}

void gazellejni::compute::XiphosParser::ParseReadRel(const substrait::ReadRel& sread, daxl::Pipeline &pipeline)
{
    std::string table_name;
	if (sread.has_local_files()) {
        auto & files = sread.local_files().items();
        assert(files.size() == 1);
        std::cout << "files.size=1" << std::endl;
        for (auto & f: files) {
            table_name = f.uri_file();
        }
        std::cout << "table_name" << table_name << std::endl;

        //pipeline.setSourceTable(table_name);
	}
    /* XXX - Todo - handle filter and base_schema **/
}

