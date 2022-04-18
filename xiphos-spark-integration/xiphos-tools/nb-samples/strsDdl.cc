#include "etlCsv.h"
#include <iostream>
#include <fstream>
#include <cmath>
#include <iostream>
#include "arrow/flight/api.h"
#include "daxl/Daxl.h"
#include "csv/CSVFileDataSource.h"
#include "csv/CSVtoArrowConverter.h"
#include "daxl/Status.h"
#include "daxl/NBStringDataType.h"
#include <filesystem>

#define STATUS_OK(to_call)                                                  \
do {                                                                        \
    Status _s = (to_call);                                                  \
    if(!_s.isOk()) {                                                        \
        throw std::runtime_error(_s.getErrorMessage());                     \
    }                                                                       \
} while (false)

int main(int argc, char *argv[]){
    
    if (argc != 2) {
        std::cout << "Usage: " << argv[0] << " <table-name>\n";
        exit(1);
    }
     std::string tableName = std::string(argv[1]);
    auto confPath = std::filesystem::current_path().string() +
                "/config.yaml";

    Daxl::getInstance()->init(confPath, Role::DATA_IO);

    DiskTable table(tableName);

    table.addColumn(Column(std::make_shared<NBStringDataType>(8, ASCII), "nb_str_1", false));
    table.addColumn(Column(std::make_shared<NBStringDataType>(8, ASCII), "nb_str_2", false));
    table.addColumn(Column(std::make_shared<NBStringDataType>(8, ASCII), "nb_str_3", false));
    TableManager::getInstance()->createTable(table);

}
