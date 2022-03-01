#include <iostream>
#include <daxl/Daxl.h>
#include "daxl/DiskTable.h"
#include "daxl/Status.h"
#include "daxl/TableManager.h"


using namespace std;
using namespace daxl;
using namespace daxl::dataIO;

const std::string ARROW_DECIMAL_TYPE = "decimal128";
const std::string SPARK_DECIMAL_TYPE = "decimal";

class XiphosUtils {
public:
   /* DiskTable findTable(std::string tableName);
    std::string serialize_column(daxl::Column column);
    std::string serialize_column(std::string col_name, std::string col_type, bool is_nullable);
    std::string bool_to_str(bool val);
*/

        static DiskTable findTable(std::string tableName)
         {
             TableManager *tableManager = TableManager::getInstance();

                daxl::Status status;

                DiskTable table(tableName);

                status = tableManager->findTable(tableName, &table);

                if (status.getErrorCode() == TABLE_NOT_FOUND)
                {
                    throw std::runtime_error("table " + tableName + " not found");
                }
                return table;
         }

         static std::string serialize_column(daxl::Column column) {
            auto col_name = column.getName();
            auto col_type = column.getType()->ToString();
            bool is_nullable = true;
            return serialize_column(col_name, col_type, is_nullable = true);
         }

         static std::string serialize_column(std::string col_name, std::string col_type, bool is_nullable = true){
            if(col_type == "int32"){
                col_type = "integer";
            } else if(col_type == "int64"){
                col_type = "long";
            }else if(col_type.rfind(ARROW_DECIMAL_TYPE, 0) == 0){
                col_type = col_type.replace(0, ARROW_DECIMAL_TYPE.length(), SPARK_DECIMAL_TYPE );
            } else if(col_type.rfind("date", 0) == 0){
                col_type = "date";
            }
            return "{\"name\":\"" + col_name + "\", \"type\":\""+col_type+"\", \"nullable\": " + bool_to_str(is_nullable) + " ,\"metadata\":{}}";
         }

        static std::string bool_to_str(bool val)
        {
            return val? "true" : "false";
        }

};