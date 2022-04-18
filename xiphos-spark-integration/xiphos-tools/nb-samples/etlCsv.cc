#include "etlCsv.h"
#include <iostream>
#include <fstream>
#include <cmath>
#include <iostream>
#include <filesystem>

#include "arrow/flight/api.h"

#include "csv/CSVFileDataSource.h"
#include "csv/CSVtoArrowConverter.h"
#include "daxl/Status.h"
#include "daxl/Daxl.h"

#define STATUS_OK(to_call)                                                  \
do {                                                                        \
    Status _s = (to_call);                                                  \
    if(!_s.isOk()) {                                                        \
        throw std::runtime_error(_s.getErrorMessage());                     \
    }                                                                       \
} while (false)

const char CSV_DELIMITER = '|';
const int DEFAULT_CSV_BATCH_SIZE = 10000;

using namespace std;

std::shared_ptr<ArrowSourceReader<CSVRowBatch>> resigterDataSouces(
    std::string tableName, std::string csvFilePath, Session *session, int batchSize){

    TableManager *tableManager = TableManager::getInstance();

    Status status;

    DiskTable table(tableName);

    status = tableManager->findTable(tableName, &table);

    if(status.getErrorCode() == TABLE_NOT_FOUND){
        throw std::runtime_error("table " + tableName + " not found");
    }

    auto csvDataSource = std::make_shared<CSVFileDataSource>(csvFilePath, batchSize);
    auto arrowConverter = std::make_shared<CSVtoArrowConverter>(CSV_DELIMITER, table);
    return session->registerDatasource<CSVRowBatch>(tableName +"-csv-reader", csvDataSource, arrowConverter);
}

int importSingleFile(std::string tableName, std::shared_ptr<ArrowSourceReader<CSVRowBatch>> csvDataSourceReader, Session *session){

    cout << "importSingleFile for " << tableName << endl;
    auto tableWriter = session->getTableWriter(tableName);

    for (int i = 0; csvDataSourceReader->hasData(); i++) {

        cout << "Reading batch " << i << endl;
        std::shared_ptr<arrow::RecordBatch> batch = csvDataSourceReader->getNextBatch();

        tableWriter->writeBatch(batch.get());

        // Periodically save progress
        if (i % 5 == 0)
            session->checkpoint();
    }
    return 0;
}

int main(int argc, char *argv[]){
    
    auto confPath = std::filesystem::current_path().string() +
                "/config.yaml";

    Daxl::getInstance()->init(confPath, Role::DATA_IO);

    if(argc < 2){
        return EXIT_FAILURE;
    }

    int batchSize = DEFAULT_CSV_BATCH_SIZE;
    if(argc > 2){
        batchSize = std::atoi(argv[2]);
    }

    auto pathStr = std::string(argv[1]);
    auto path = std::filesystem::path(pathStr);
    Session session("session");

    if(path.filename().extension() == ".csv"){

        std::string tableName = path.filename().stem();
        std::string csvFilePath = path.c_str();

        cout << "create datasource with table " << tableName << " path: " << csvFilePath << "batchSize: " << batchSize << endl;
        auto ds = resigterDataSouces(tableName, csvFilePath, &session, batchSize);

        session.start();

        importSingleFile(tableName, ds, &session);

        session.commit();
        session.end();

        return 0;
    }

    if(!std::filesystem::is_directory(path)){
        return EXIT_FAILURE;
    }


    std::unordered_map<std::string, std::shared_ptr<ArrowSourceReader<CSVRowBatch>>> map;

    for (const auto & entry : std::filesystem::directory_iterator(pathStr)){

        if(entry.path().filename().extension() != ".csv"){
            continue;
        }
        std::string tableName = entry.path().filename().stem();
        std::string csvFilePath = entry.path().c_str();

        auto ds = resigterDataSouces(tableName, csvFilePath, &session, batchSize);
        map[tableName] = ds;
    }

    session.start();

    for(auto it = map.begin(); it != map.end(); it++) {
        importSingleFile(it->first, it->second, &session);
    }

    //should not insert data
    for(auto it = map.begin(); it != map.end(); it++) {
        importSingleFile(it->first, it->second, &session);
    }

    session.commit();
    session.end();

}
