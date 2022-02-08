#include <arrow/pretty_print.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <exception>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <vector>

#include "proto/protobuf_utils.h"
#include "proto/substrait_utils.h"

using namespace std;

vector<uint8_t> ImportPlan(const string& filename) {
  ifstream ifs(filename, ios::binary);
  if (!ifs) throw runtime_error("Failed to open " + filename);

  vector<uint8_t> data;
  data.insert(data.begin(), istreambuf_iterator<char>(ifs), istreambuf_iterator<char>());
  return data;
}

int main(int argc, char* argv[]) {
  if (argc != 2) {
    printf("Usage: %s <substrait plan>\n", argv[0]);
    exit(1);
  }

  vector<uint8_t> plan_bytes = ImportPlan(argv[1]);
  cout << "Plan: " << argv[1] << " size: " << plan_bytes.size() << endl;
  substrait::Plan ws_plan;
  if (!ParseProtobuf(plan_bytes.data(), plan_bytes.size(), &ws_plan)) {
    cout << "Failed to parse plan\n";
    exit(1);
  }
  auto parser = std::make_shared<SubstraitParser>();
  parser->ParsePlan(ws_plan);

  shared_ptr<ResultIterator<arrow::RecordBatch>> out_iter = parser->getResIter();

  cout << "============= RESULTS batches ==================" << endl;
  while (out_iter->HasNext()) {
    shared_ptr<arrow::RecordBatch> record;
    if (out_iter->Next(&record) != arrow::Status::OK()) {
      cout << "Ooop, error" << endl;
      break;
    }
    arrow::PrettyPrint(*record, 0, &cout);
  }
  cout << "===============================================" << endl;
  return 0;
}
