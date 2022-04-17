#include "com_intel_dbio_sources_datasourcev2_xiphosv2_XiphosJniImp.h"
#include <iostream>
#include <string>
#include <algorithm>
#include <daxl/Daxl.h>
#include "daxl/DiskTable.h"
#include  "xiphosUtils.h"

using namespace std;
using namespace daxl;


static bool verbose = false;

jint throwNoClassDefError( JNIEnv *env, const std::string & message ) {
  jclass exClass;
  const char *className = "java/lang/NoClassDefFoundError";

  exClass = env->FindClass(className);
  // we don't have much to do if this exception does not exists
  // hence we'll let the thing crash

  return env->ThrowNew(exClass, message.c_str());
}

jint throwException(JNIEnv *env, const string & exName, const string & message) {

  string className = exName;
  replace(className.begin(), className.end(), '.', '/');

  jclass exClass = env->FindClass(className.c_str());
  if (exClass == nullptr) {
    throwNoClassDefError(env, "class name " + exName + " doesn't exists");
  }
  return env->ThrowNew(exClass, message.c_str());
}


JNIEXPORT jboolean JNICALL Java_com_intel_dbio_sources_datasourcev2_xiphosv2_XiphosJniImp_init(JNIEnv *, jobject) {

  if (getenv("DAXL_CONFIG_FILE") == nullptr) {
    std::cerr << "ERROR: DAXL_CONFIG_FILE env variable is not defined !!" << std::endl;
    return false;
  }
  std::string configPath(getenv("DAXL_CONFIG_FILE"));

  daxl::Daxl::getInstance()->init(configPath, daxl::Role::WORKER);
  if (verbose) {
    std::cout << "DAXL initialized\n";
  }
  return true;
}

JNIEXPORT jstring JNICALL Java_com_intel_dbio_sources_datasourcev2_xiphosv2_XiphosJniImp_getSchemaDesc(JNIEnv *env, jobject obj, jstring _tableName) {

  string tableName = string(env->GetStringUTFChars(_tableName, nullptr));

  if (verbose) cout << "getSchemaDesc for " << tableName << endl;

  string retval = "{\"type\":\"struct\",\"fields\":[";
  if (tableName == string("test_table_1")) {
    retval += XiphosUtils::serialize_column("id", "integer");
  } else if (tableName == string("test_table_2")) {
    retval += XiphosUtils::serialize_column("id", "integer", false);
    retval += "," + XiphosUtils::serialize_column("first_value", "string", false);
    retval += "," + XiphosUtils::serialize_column("second_value", "integer", true);
  } else {
    DiskTable diskTable = XiphosUtils::findTable(tableName);
    for (uint i = 0; i < diskTable.getColumns().size(); i++)
    {
      retval += XiphosUtils::serialize_column(diskTable.getColumns().at(i));
      if(i != diskTable.getColumns().size() -1)
      {
        retval += ",";
      }
    }
  }
  retval += "]}";
  return env->NewStringUTF(retval.c_str());
}

