#include <iostream>
#include <fstream>
#include <filesystem>

#include "planAndExecute.h"
#include "daxl/Pipeline.h"
#include "daxl/SubTreeExecutor.h"
#include "daxl/SubTree.h"
#include "daxl/Daxl.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"

using namespace daxl;
void getResults(std::string id, std::shared_ptr<arrow::Schema> &schema){
    auto execution = daxl::SubTreeExecutor();

    execution.execute(id);


    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

    while(auto batch = execution.getNextRowChunk()){
        if(batch){
            batches.push_back(batch);
        }   
    }

    if(!batches.empty()){
        auto t = arrow::Table::FromRecordBatches(schema, batches);
        std::shared_ptr<arrow::Table> table = std::move(t).ValueOrDie();
        std::cout << "Table data: \n" << table->ToString() << "\n";
    }
}

std::string planStrs(std::shared_ptr<arrow::Schema> &schema){

    //planner thread
    daxl::SubTree subTree;
    daxl::Pipeline pipline("strs", "p1");
    pipline.streamToHost();
    subTree.addPipe(pipline);
    subTree.optimize(); //the optimize  
    std::string id = subTree.getId();
    schema = subTree.getResultSchema();

    return id;

}


int main(int argc, char *argv[]){

    std::string pathStr;
    if(argc < 2){
        pathStr = std::filesystem::current_path().string() + 
            "/config.yaml";
    }else{
        pathStr = std::string(argv[1]);
    }

    Daxl::getInstance()->init(pathStr, MASTER);

    std::cout << "============== Planning and executing Query 0 ================\n";
    std::shared_ptr<arrow::Schema> schemaInts;
    auto idStrs = planStrs(schemaInts);
    getResults(idStrs,schemaInts);
    return 0;
}
