#pragma once

#include "CSVRowBatch.h"
#include "arrow/csv/api.h"
#include "daxl/ArrowConverter.h"
#include "daxl/DiskTable.h"

using namespace daxl;
using namespace daxl::dataIO;

class CSVtoArrowConverter : public ArrowConverter<CSVRowBatch> {
public:
    /// implement.
    CSVtoArrowConverter(char seperator, DiskTable schema);
    std::shared_ptr<arrow::RecordBatch> convert(const CSVRowBatch &batch) override;

private:
    //std::shared_ptr<arrow::DataType> getArrowDataType(arrow::DataType type, int precision, int scale);

    char seperator = '|';
    DiskTable schema;
    arrow::csv::ConvertOptions convertOptions;
    arrow::csv::ReadOptions readOptions;
};
