#include <iostream>

#include <arrow/csv/writer.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include "arrow/io/file.h"
#include "arrow/csv/api.h"
#include "arrow/filesystem/api.h"

#include "daxl/ArrowConverter.h"
#include "daxl/Session.h"
#include "daxl/DataSource.h"
#include "daxl/ArrowSourceReader.h"
#include "daxl/Table.h"
#include "daxl/ColumnBase.h"
#include "daxl/Column.h"
#include "daxl/TableManager.h"
#include "daxl/DiskTable.h"
#include "daxl/TableWriter.h"

using namespace daxl;
using namespace daxl::dataIO;

enum class CSVState {
    UnquotedField,
    QuotedField,
    QuotedQuote
};

DiskTable tpchSchemaTable(std::string tableName);
void scanAndPrintTable(DiskTable table);
