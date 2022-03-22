#include "xiphos_parser.h"

static bool verbose = true;

gazellejni::compute::XiphosParser::XiphosParser()
{
}

gazellejni::compute::XiphosParser::~XiphosParser()
{
}

void gazellejni::compute::XiphosParser::ParsePlan(const substrait::Plan &splan)
{
#if 0
  for (auto& sextension : splan.extensions()) {
    if (!sextension.has_extension_function()) {
      continue;
    }
    auto& sfmap = sextension.extension_function();
    auto id = sfmap.function_anchor();
    auto name = sfmap.name();
    functions_map_[id] = name;
    if (verbose) std::cout << "Function id: " << id << ", name: " << name << std::endl;
  }
  for (auto& srel : splan.relations()) {
    if (srel.has_root()) {
      ParseRelRoot(srel.root());
    }
    if (srel.has_rel()) {
      ParseRel(srel.rel());
    }
  }
#endif
}

std::shared_ptr<ResultIterator<arrow::RecordBatch>> gazellejni::compute::XiphosParser::getResIter()
{

}

