#include "arrow/filesystem/api.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/flight/api.h"
#include "arrow/io/api.h"
#include "daxl/Daxl.h"
#include "daxl/DiskTable.h"
#include "daxl/NBStringDataType.h"
#include "daxl/Session.h"
#include "daxl/Status.h"
#include "daxl/TableManager.h"
#include "parquet/arrow/reader.h"
#include <arrow/array/array_primitive.h>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <list>
#include <stack>
#include <string>

using namespace daxl;
using namespace daxl::dataIO;
using namespace boost::gregorian;

template <typename Base, typename T> inline bool instanceof (const T *) {
  return std::is_base_of<Base, T>::value;
}

#define STATUS_OK(to_call)                                                     \
  do {                                                                         \
    Status _s = (to_call);                                                     \
    if (!_s.isOk()) {                                                          \
      throw std::runtime_error(_s.getErrorMessage());                          \
    }                                                                          \
  } while (false)

const int DEFAULT_CSV_BATCH_SIZE = 10000;

std::unique_ptr<parquet::arrow::FileReader> openFile(std::string filePath) {
  arrow::MemoryPool *pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::io::RandomAccessFile> input =
      arrow::io::ReadableFile::Open(filePath).ValueOrDie();

  // Attach an  arrow::io::ReadableFile to a Parquet file reader:
  std::unique_ptr<parquet::arrow::FileReader> reader;
  parquet::arrow::OpenFile(input, pool, &reader);

  return reader;
}

daxl::Status findDiskTable(std::string tableName, DiskTable *table) {
  TableManager *tableManager = TableManager::getInstance();

  return tableManager->findTable(tableName, table);
}

daxl::Status createNewTable(
    DiskTable *table, std::unique_ptr<parquet::arrow::FileReader> *reader,
    std::list<std::tuple<std::string, std::shared_ptr<arrow::DataType>,
                         std::string>> const &partitions) {
  std::shared_ptr<::arrow::Schema> schema;

  reader->get()->GetSchema(&schema);
  for (uint i = 0; i < schema->num_fields(); i++) {
    std::shared_ptr<arrow::Field> current_field = schema->field(i);
    std::string name = current_field->name();
    bool nullable = current_field->nullable();
    std::shared_ptr<arrow::DataType> type = current_field->type();
    std::string type_name = type->name();
    int scale = 0;
    int precision = 0;
    bool is_dictionary_encoded = false;
    if (type == arrow::int32()) {
      type = arrow::int64();
    } else if (type_name.rfind("decimal", 0) == 0) {
      auto decimal_type = std::static_pointer_cast<arrow::DecimalType>(type);
      scale = decimal_type->scale();
      precision = decimal_type->precision();
      type = arrow::decimal128(precision, scale);
    } else if (type == arrow::utf8()) {
      type = std::make_shared<NBStringDataType>(8, ASCII);
      is_dictionary_encoded = true;
    } else if (type_name == "timestamp") {
      type = arrow::date64();
    }
    table->addColumn(
        Column(type, precision, scale, name, is_dictionary_encoded));
  }

  for (auto &tup : partitions) {
    auto column_name = std::get<0>(tup);
    auto data_type = std::get<1>(tup);
    table->addColumn(Column(data_type, column_name));
  }

  return TableManager::getInstance()->createTable(*table);
}

time_t to_time_t(const boost::gregorian::date &date) {
  using namespace boost::posix_time;
  static ptime epoch(boost::gregorian::date(1970, 1, 1));
  time_duration::sec_type secs =
      (ptime(date, seconds(0)) - epoch).total_seconds();
  return time_t(secs);
}

std::shared_ptr<arrow::Array>
createAdditionalColumnData(std::shared_ptr<arrow::DataType> datatype,
                           std::string strValue, int rowNumber) {
  std::shared_ptr<arrow::Array> result;
  if (datatype == arrow::int64() || datatype == arrow::int32()) {
    arrow::Int64Builder builder;
    auto value = std::stoi(strValue);
    for (int i = 0; i < rowNumber; i++) {
      builder.Append(value);
    }
    result = builder.Finish().ValueOrDie();
  } else if (datatype == arrow::date64()) {
    arrow::Date64Builder builder;
    auto date = from_simple_string(strValue);
    auto timestamp = to_time_t(date);
    for (int i = 0; i < rowNumber; i++) {
      builder.Append(timestamp);
    }
    result = builder.Finish().ValueOrDie();
  } else {
    arrow::StringBuilder builder;
    auto value = strValue;
    for (int i = 0; i < rowNumber; i++) {
      builder.Append(value);
    }
    result = builder.Finish().ValueOrDie();
  }

  return result;
}

void insert_data_into_table(
    std::string tableName, std::unique_ptr<parquet::arrow::FileReader> *reader,
    DiskTable *table, int batch_size, Session *session,
    std::list<std::tuple<std::string, std::shared_ptr<arrow::DataType>,
                         std::string>> const &partitions) {

  auto tableWriter = session->getTableWriter(tableName);

  int row_group_num = reader->get()->num_row_groups();

  for (int group_index = 0; group_index < row_group_num; group_index++) {
    std::shared_ptr<arrow::Table> table;

    //  Fill the table with data from a single row group:
    arrow::Status status;
    status = reader->get()->ReadRowGroup(group_index, &table);

    if (!status.ok()) {
      throw std::runtime_error("falied to read parquet file ");
    }

    arrow::TableBatchReader tableBatchReader = arrow::TableBatchReader(*table);
    tableBatchReader.set_chunksize(batch_size);
    bool hasNextBatch;
    hasNextBatch = true;
    while (hasNextBatch) {
      std::shared_ptr<arrow::RecordBatch> recB;

      status = tableBatchReader.ReadNext(&recB);
      if (!status.ok()) {
        throw std::runtime_error("unable to read next batch from table ");
      }
      if (recB != NULL && recB.get() != NULL) {
        int rows_number = recB->num_rows();
        int colum_number = recB->num_columns();
        int j = 0;
        for (auto &tup : partitions) {
          auto value = std::get<2>(tup); // std::stoi(std::get<2>(tup));
          auto fieldName = std::get<0>(tup);
          auto dataType = std::get<1>(tup);
          auto array = createAdditionalColumnData(dataType, value, rows_number);
          recB =
              recB->AddColumn(colum_number + j, fieldName, array).ValueOrDie();
          j++;
        }

        tableWriter->writeBatch(recB.get());
      } else {
        hasNextBatch = false;
      }
    }
    session->checkpoint();
  }
}

int importSingleFile(
    std::filesystem::path filePath, int batch_size, Session *session,
    std::list<std::tuple<std::string, std::shared_ptr<arrow::DataType>,
                         std::string>> const &partitions,
    std::string tableName = "") {

  if (tableName == "") {
    tableName = filePath.filename().stem();
  }

  std::string parquetFilePath = filePath.c_str();

  auto reader = openFile(parquetFilePath);

  DiskTable table(tableName);

  daxl::Status status = findDiskTable(tableName, &table);

  if (status.getErrorCode() == TABLE_NOT_FOUND) {
    status = createNewTable(&table, &reader, partitions);
  }

  if (status.isOk()) {
    insert_data_into_table(tableName, &reader, &table, batch_size, session,
                           partitions);
  } else {
    throw std::runtime_error(status.getErrorMessage());
  }
  return 0;
}

std::string createPartitionValues(
    std::filesystem::path filePath,
    std::list<std::tuple<std::string, std::shared_ptr<arrow::DataType>>> const
        &partitions,
    std::list<std::tuple<std::string, std::shared_ptr<arrow::DataType>,
                         std::string>> &partitionsWithValues) {
  std::filesystem::path parent = filePath.parent_path();
  std::stack<std::string> stackPartitionValues;

  for (int i = 0; i < partitions.size(); i++) {

    stackPartitionValues.push(parent.filename());
    parent = parent.parent_path();
  }

  for (auto it = partitions.begin();
       it != partitions.end() && !stackPartitionValues.empty(); ++it) {
    std::string partition_wtth_value = stackPartitionValues.top();
    stackPartitionValues.pop();
    std::string strValue;
    std::string strPartitionName;
    size_t delimiter_pos = partition_wtth_value.find("=");
    if (delimiter_pos != std::string::npos) {
      strPartitionName = partition_wtth_value.substr(0, delimiter_pos);
      strValue = partition_wtth_value.erase(0, delimiter_pos + 1);
    } else {
      strValue = partition_wtth_value;
      strPartitionName = std::get<0>(*it);
    }
    std::shared_ptr<arrow::DataType> dataType = std::get<1>(*it);

    auto tuple = std::make_tuple(strPartitionName, dataType, strValue);
    partitionsWithValues.push_back(tuple);
  }
  return (partitions.empty()) ? filePath.filename() : parent.filename();
}

void importSingleFileWithPartitions(
    std::filesystem::path filePath, int batch_size, Session *session,
    std::list<std::tuple<std::string, std::shared_ptr<arrow::DataType>>> const
        &partitions) {
  std::list<
      std::tuple<std::string, std::shared_ptr<arrow::DataType>, std::string>>
      partitionsWithValue;
  std::string table_name = filePath.filename();
  if (!partitions.empty()) {
    table_name =
        createPartitionValues(filePath, partitions, partitionsWithValue);
  }
  importSingleFile(filePath, batch_size, session, partitionsWithValue,
                   table_name);
}

void importDirectory(
    std::filesystem::path const &filePath, int batch_size, Session *session,
    std::list<std::tuple<std::string, std::shared_ptr<arrow::DataType>>>
        partitions) {
  for (auto const &entry :
       std::filesystem::recursive_directory_iterator(filePath)) {
    if (std::filesystem::is_regular_file(entry) &&
        entry.path().extension() == ".parquet") {
      importSingleFileWithPartitions(entry.path(), batch_size, session,
                                     partitions);
    }
  }
}

void importByPath(
    std::filesystem::path const &filePath, int batch_size,
    std::list<std::tuple<std::string, std::shared_ptr<arrow::DataType>>>
        partitions) {
  Session session("session");
  session.start();

  if (std::filesystem::exists(filePath) &&
      std::filesystem::is_directory(filePath)) {
    importDirectory(filePath, batch_size, &session, partitions);
  } else {
    importSingleFileWithPartitions(filePath, batch_size, &session, partitions);
  }

  session.commit();
  session.end();
}

std::shared_ptr<arrow::DataType> parseDataType(std::string strDataType) {

  std::shared_ptr<arrow::DataType> datatype;
  if (strDataType == "i") {
    datatype = arrow::int64();
  } else if (strDataType == "d") {
    datatype = arrow::timestamp(arrow::TimeUnit::SECOND);
  } else {
    datatype = std::make_shared<NBStringDataType>(8, ASCII);
  }
  return datatype;
}

std::tuple<std::string, std::shared_ptr<arrow::DataType>>
parsePartition(std::string strPartition) {
  std::string delimiter = ":";
  size_t pos = strPartition.find(delimiter);
  std::string partitionName = strPartition.substr(0, pos);
  strPartition.erase(0, pos + delimiter.length());
  std::string strDataType = strPartition;
  auto dataType = parseDataType(strDataType);
  return std::make_tuple(partitionName, dataType);
}

std::list<std::tuple<std::string, std::shared_ptr<arrow::DataType>>>
parsePartitionsParameter(std::string strPartitions) {
  std::string delimiter = "/";
  size_t pos = 0;
  std::string partition;
  std::list<std::tuple<std::string, std::shared_ptr<arrow::DataType>>> result;
  while ((pos = strPartitions.find(delimiter)) != std::string::npos) {
    partition = strPartitions.substr(0, pos);
    auto tuple = parsePartition(partition);
    result.push_back(tuple);
    strPartitions.erase(0, pos + delimiter.length());
  }
  auto tuple = parsePartition(strPartitions);
  result.push_back(tuple);
  return result;
}

int main(int argc, char *argv[]) {

  if (argc < 2) {
    std::cerr << "Missing parquet file - Usage: " << argv[0] << " <parquet file or directory> [batch size]\n";
    return EXIT_FAILURE;
  }

  std::string confPath;
  if (getenv("DAXL_CONFIG_FILE") != nullptr) {
      confPath = std::string(getenv("DAXL_CONFIG_FILE"));
  } else {
      std::cerr << "DAXL_CONFIG_FILE env variable is not defined..\n please define it to point to DAXL configuration file\n";
  }

  Daxl::getInstance()->init(confPath, Role::DATA_IO);

  int batch_size = DEFAULT_CSV_BATCH_SIZE;
  if (argc > 2) {
    batch_size = std::atoi(argv[2]);
  }

  auto pathStr = std::string(argv[1]);
  auto path = std::filesystem::path(pathStr);

  std::list<std::tuple<std::string, std::shared_ptr<arrow::DataType>>>
      partitions;

  if (argc > 3) {
    auto strPartitions = argv[3];
    partitions = parsePartitionsParameter(strPartitions);
  }
  importByPath(pathStr, batch_size, partitions);

  return EXIT_SUCCESS;
}
