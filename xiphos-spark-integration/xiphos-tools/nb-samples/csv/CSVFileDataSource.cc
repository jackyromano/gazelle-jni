#include "CSVFileDataSource.h"
#include "arrow/io/api.h"
#include "arrow/filesystem/api.h"
#include <iostream>

CSVRowBatch CSVFileDataSource::getNextBatch(){
    int nextBatchInd = getNextBatchInd(progress);
    long startingPoint = offsets.at(nextBatchInd);
    long howMuchToRead = offsets.at(nextBatchInd + 1) - offsets.at(nextBatchInd);

    std::cout << "Reading stream from " << startingPoint << " howMuchToRead: " << howMuchToRead << std::endl;
    std::shared_ptr<arrow::io::InputStream> is = arrow::io::RandomAccessFile::GetStream(raf, startingPoint, howMuchToRead);
    setNextBatchInd(++nextBatchInd);
    return CSVRowBatch(is, batchSize, howMuchToRead);
}

bool CSVFileDataSource::hasData(){
    uint nextBatchInd = getNextBatchInd(progress);
    if(offsets.size() <= (nextBatchInd + 1)){
        return false;
    }
    return offsets.at(nextBatchInd + 1) <= raf->GetSize().ValueOrDie();
}

CSVFileDataSource::CSVFileDataSource(const std::string & csvFileLocation, long batchSize) 
:batchSize(batchSize){
    
    std::cout << " creating csv data source from file " << csvFileLocation << std::endl;

    std::shared_ptr<arrow::fs::FileSystem> fileS = arrow::fs::FileSystemFromUri("file:///home").ValueOrDie();
    raf = fileS->OpenInputFile(csvFileLocation).ValueOrDie();

    long fileSize = raf->GetSize().ValueOrDie();
    std::cout << "Opened " << csvFileLocation << " size: " << fileSize << std::endl;

    std::shared_ptr<arrow::Buffer> buff = raf->Read(fileSize).ValueOrDie();

    offsets.push_back(0);   
    long rowNum=0;
    for(long i=0 ; i<fileSize ; i++ ){
        if((*buff)[i] == '\n'){
            rowNum++;
            if(rowNum % batchSize == 0){
                offsets.push_back(++i);   
            } else if(i == fileSize - 1){
                offsets.push_back(fileSize);   
            }
        }else if(i == fileSize - 1){
            offsets.push_back(fileSize);   
        }
    }
}

void CSVFileDataSource::setProgress(Progress &progress){
    this->progress = progress;
}

Progress& CSVFileDataSource::getProgress(){
    return progress;
}

int CSVFileDataSource::getNextBatchInd(Progress progress){
    if(progress.size() == 0){
        return 0;
    }
    return std::atoi(progress["batchInd"].c_str());
}

void CSVFileDataSource::setNextBatchInd(int ind){
    progress["batchInd"] = std::to_string(ind);
}
