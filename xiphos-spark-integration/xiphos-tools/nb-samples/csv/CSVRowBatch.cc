#include "CSVRowBatch.h"

CSVRowBatch::CSVRowBatch(std::shared_ptr<arrow::io::InputStream> inputStream, int batchSize, long chunkSize)
:batchSize(batchSize), chunkSize(chunkSize), inputStream(inputStream){
}

std::shared_ptr<arrow::io::InputStream> CSVRowBatch::getInputStream() const{
    return inputStream;
}
