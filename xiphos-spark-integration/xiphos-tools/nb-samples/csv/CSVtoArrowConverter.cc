#include "CSVtoArrowConverter.h"
#include "arrow/io/api.h"
#include "arrow/csv/api.h"

CSVtoArrowConverter::CSVtoArrowConverter(char seperator, DiskTable schema)
:seperator(seperator), schema(schema) {

    convertOptions = arrow::csv::ConvertOptions::Defaults();
    readOptions = arrow::csv::ReadOptions::Defaults();

   readOptions.column_names = std::vector<std::string>(schema.getColumns().size(),"");
   for(uint i = 0 ; i < schema.getColumns().size() ; i++){
       auto colName = schema.getColumns().at(i).getName();
       auto colType = schema.getColumns().at(i).getType();

       readOptions.column_names.at(i) = colName;
       convertOptions.column_types[colName] = colType;
   }
}

std::shared_ptr<arrow::RecordBatch> CSVtoArrowConverter::convert(const CSVRowBatch &batch){


    arrow::io::IOContext io_context = arrow::io::default_io_context();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    parse_options.delimiter = seperator;

    // Instantiate TableReader from input stream and options
    std::shared_ptr<arrow::io::InputStream> input = batch.getInputStream();
    readOptions.block_size = batch.chunkSize;
    auto maybe_reader =
        arrow::csv::TableReader::Make(io_context,
                                    input,
                                    readOptions,
                                    parse_options,
                                    convertOptions);
    if (!maybe_reader.ok()) {
        throw std::runtime_error("falied initialize csv file reader");
    }

    std::shared_ptr<arrow::csv::TableReader> reader = *maybe_reader;

    // Read table from CSV file
    auto maybe_table = reader->Read();

    if (!maybe_table.ok()) {
        throw std::runtime_error("falied to read csv file ");
    }

    std::shared_ptr<arrow::Table> table = *maybe_table;
    arrow::TableBatchReader tableBatchReader = arrow::TableBatchReader(*table);
    tableBatchReader.set_chunksize(batch.batchSize);
    std::shared_ptr<arrow::RecordBatch> recB;
    auto status = tableBatchReader.ReadNext(&recB);
    if(!status.ok()){
        throw std::runtime_error("unable to read next batch from table ");
    }

    return recB;

}