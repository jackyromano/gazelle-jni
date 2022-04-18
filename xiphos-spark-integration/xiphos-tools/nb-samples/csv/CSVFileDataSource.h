#pragma once

 #include "CSVRowBatch.h"
 #include "daxl/DataSourceWithProgress.h"

using namespace daxl::dataIO;


class CSVFileDataSource : public DataSourceWithProgress<CSVRowBatch> {
public:
    CSVFileDataSource(const std::string & csvFileLocation, long batchSize);

    /// implement.
    CSVRowBatch getNextBatch() override;

    bool hasData() override;

    void setProgress(Progress &progress) override;

    Progress &getProgress() override;

private:

    long batchSize;
    std::shared_ptr<arrow::io::RandomAccessFile> raf;
    std::vector<long> offsets;

    int getNextBatchInd(Progress progress);
    void setNextBatchInd(int ind);
};

