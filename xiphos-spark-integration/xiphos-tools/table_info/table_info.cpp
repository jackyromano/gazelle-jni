#include <daxl/SubTree.h>
#include <arrow/record_batch.h>
#include <daxl/DiskTable.h>
#include <daxl/TableWriter.h>
#include <iostream>
#include <daxl/Daxl.h>
#include <daxl/Pipeline.h>
#include <daxl/SubTree.h>
#include <daxl/SubTreeExecutor.h>
#include <daxl/TableManager.h>
#include <daxl/Session.h>
#include <arrow/api.h>
#include <arrow/record_batch.h>
#include <string>
#include <memory>
#include <vector>

using namespace std;

void printTableInfo(const string & tableName)
{
    daxl::dataIO::TableManager *tableManger = daxl::dataIO::TableManager::getInstance();
    daxl::DiskTable diskTable(tableName);
    
    daxl::Status status = tableManger->findTable(tableName, &diskTable);
    if (status.isOk()) {
        cout << "OK\n";
    } else {
        cout << "findTable " << tableName << " failed with << " <<  status.getErrorMessage() << endl;
    }
    
    auto columns = diskTable.getColumns();
    for (auto const & c : columns) {
        cout << "name: " << c.getName() << " type: " << *c.getType() << " scale: " << c.getScale() << " prec: " << c.getPrecision() << endl;
    }
}

void printTablesInfo()
{
    vector<daxl::DiskTable> tables;
    daxl::dataIO::TableManager * mgr = daxl::dataIO::TableManager::getInstance();

    if (!mgr->getAllTables(&tables).isOk()) {
        cerr << "Failed to get all tables\n";
        return;
    }

    cout << "Got " << tables.size() << " tables\n";

    for (auto const & t : tables) {
        cout << t.getName() << ":\n";
        printTableInfo(t.getName());
    }

}

int main(int argc, char *argv[]) 
{
    
    string confFile;
    if (getenv("DAXL_CONFIG_FILE") == nullptr) {
        cerr << "DAXL_CONFIG_FILE is not defined..\n Please define it to point to your DAXL configuration file\n";
    } else {
        confFile = string(getenv("DAXL_CONFIG_FILE"));
    }

    if (argc > 1 && (string(argv[1]) == "-h" || string(argv[1]) == "--help")) {
        cout << "Usage: " << argv[0] << " [table name]" << endl;
        return 0;
    }

    daxl::Daxl::getInstance()->init(confFile, daxl::Role::MASTER);

    if (argc < 2) {
        printTablesInfo();
    } else {
        printTableInfo(argv[1]);
    }

    return 0;
}
