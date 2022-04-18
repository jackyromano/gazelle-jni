#pragma once

#include <iostream>

#include <arrow/api.h>

class CSVRowBatch {
public:
    CSVRowBatch(std::shared_ptr<arrow::io::InputStream> inputStream, int batchSize, long chunkSize);
    std::shared_ptr<arrow::io::InputStream> getInputStream() const;
    int batchSize;
    long chunkSize;
private:
    std::shared_ptr<arrow::io::InputStream> inputStream;
};
