#include "xiphos_parser.h"
#include <arrow/pretty_print.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <memory>
#include <unistd.h>

using namespace daxl;
static bool verbose = false;

using namespace std;
gazellejni::compute::XiphosParser::XiphosParser() {
}

gazellejni::compute::XiphosParser::~XiphosParser() {
}

void gazellejni::compute::XiphosParser::Init() {
  string confPath;
  if (getenv("DAXL_CONFIG_FILE") != nullptr) {
    confPath = string(getenv("DAXL_CONFIG_FILE"));
  } else {
    cerr << "DAXL_CONFIG_FILE env variable is not defined..\n please define it to point to DAXL configuration file\n";
  }

  Daxl::getInstance()->init(confPath, daxl::MASTER);
}


void gazellejni::compute::XiphosParser::ParsePlan(const substrait::Plan &splan) {
  daxl::SubTree subtree;
  daxl::Pipeline pipeline("strs1", "p1");
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
    // if (verbose) cout << "Function id: " << id << ", name: " << name << endl;
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
  pipeline.streamToHost();
  subtree.addPipe(pipeline);
  Status stat = subtree.optimize();
  if (stat.isError()) {
    cerr << "Optimize failed with \"" << stat.getErrorMessage() << "\"" << endl;
  }
  execution_id_ = subtree.getId();
}

gazellejni::compute::XiphosResultIterator::XiphosResultIterator(const string &_execution_id):
  execution_id_( _execution_id),
  batch_(nullptr) {
}


shared_ptr<ResultIterator<arrow::RecordBatch>> gazellejni::compute::XiphosParser::getResIter() {
  return make_shared<XiphosResultIterator>(execution_id_);
}


static void print(ArrowSchema * schema, int indent = 0) {

  cout << string(indent * 4, ' ') << "Name: " << schema->name << endl;
  cout << string(indent * 4, ' ') << "Format: " << schema->format << endl;
  cout << string(indent * 4, ' ') << "Metadata: " << schema->metadata << endl;
  cout << string(indent * 4, ' ') << "Flags: " << schema->flags << endl;
  cout << string(indent * 4, ' ') << "n_children: " << schema->n_children << endl;
  for (int i = 0; i < schema->n_children; i++) {
    print(schema->children[i], indent + 1);
  }
}

bool gazellejni::compute::XiphosResultIterator::HasNext() {

  if(!is_plan_executed_) {
    execution_.execute(execution_id_);
    is_plan_executed_ = true;
    batch_ = nullptr;
    is_consumed_ = false;
  }

  if (getenv("GLUTEN_JNI_DEBUG") != nullptr) {
    const int wait_time = 20;
    cout << "Please attach your debugger to procoess " << getpid() << endl;
    cout << "Waiting for " << wait_time << "seconds\n";
    sleep(wait_time);
    cout << "continuing\n";
  }

  if (!is_consumed_ && batch_ == nullptr) {
    ArrowArray res_array;
    ArrowSchema res_schema;

    if (verbose) cout << "Read new Xiphos chunk\n";

    if (execution_.getNextRowChunk(&res_array, &res_schema)) {
      arrow::Result<shared_ptr<arrow::RecordBatch>> res = arrow::ImportRecordBatch(&res_array, &res_schema);
      if (res.ok()) {
        batch_ = res.ValueOrDie();
        if (verbose) cout << "Got new batch from Xiphos\n";
      } else {
        cerr << "ImportRecordBatch failed\n";
      }
    } else {
      // no more batches for this pipeline;
      batch_ = nullptr;
    }
  }

  if (batch_ == nullptr) {
    is_consumed_ = true;
    if (verbose) cout << "XiphosResultIterator consumed\n";
  }
  return batch_ != nullptr;
}

arrow::Status gazellejni::compute::XiphosResultIterator::Next(shared_ptr<arrow::RecordBatch>* out) {
  assert(batch != nullptr);

  if(batch_){
    *out = batch_;
    batch_ = nullptr;
    return arrow::Status::OK();
  }
  return arrow::Status::Invalid("Invalid Status");
}

void gazellejni::compute::XiphosParser::ParseRel(const substrait::Rel& srel, daxl::Pipeline& pipeline) {
  if (srel.has_aggregate() && false) {
    if (verbose) cout << "ParseRel Aggregate" << endl;
    //ParseAggregateRel(srel.aggregate());
  } else if (srel.has_project() && false) {
    if (verbose) cout << "ParseRel Project" << endl;
    //ParseProjectRel(srel.project());
  } else if (srel.has_filter() && false) {
    if (verbose) cout << "ParseRel Filter" << endl;
    //ParseFilterRel(srel.filter());
  } else if (srel.has_read()) {
    if (verbose) cout << "ParseRel Read" << endl;
    ParseReadRel(srel.read(), pipeline);
  } else {
    cerr << "Rel operation not supported" << endl;
    return;
  }
}

void gazellejni::compute::XiphosParser::ParseRelRoot(const substrait::RelRoot& sroot, daxl::Pipeline & pipeline) {
  if (verbose) cout << "ParseRelRoot" << endl;
  if (sroot.has_input()) {
    auto& srel = sroot.input();
    ParseRel(srel, pipeline);
  }
  if (verbose) {
    auto& snames = sroot.names();
    if (snames.size() == 0) {
      cout << "ParseRelRoot has no names" << endl;
    } else {
      for (auto const& n : snames) {
        cout << "ParseRelRoot : " << n << endl;
      }
    }
  }
}

void gazellejni::compute::XiphosParser::ParseReadRel(const substrait::ReadRel& sread, daxl::Pipeline &pipeline)
{
  string table_name;
  if (sread.has_local_files()) {
    auto & files = sread.local_files().items();
    assert(files.size() == 1);
    for (auto & f: files) {
      table_name = f.uri_file();
    }
    if (verbose) cout << "ReadRel table_name" << table_name << endl;

    //pipeline.setSourceTable(table_name);
  }
  /* XXX - Todo - handle filter and base_schema **/
}

