#include "com_intel_dbio_sources_datasourcev2_xiphosv2_XiphosJniImp.h"
#include <iostream>
#include <string>
#include <algorithm>

using namespace std;

static bool verbose = false;

jint throwNoClassDefError( JNIEnv *env, const std::string & message )
{
    jclass exClass;
    const char *className = "java/lang/NoClassDefFoundError";

    exClass = env->FindClass(className);
    // we don't have much to do if this exception does not exists
    // hence we'll let the thing crash

    return env->ThrowNew(exClass, message.c_str());
}

jint throwException(JNIEnv *env, const string & exName, const string & message) 
{
    //const char *className = "java/io/FileNotFoundException";

    string className = exName;
    replace(className.begin(), className.end(), '.', '/');

    jclass exClass = env->FindClass(className.c_str());
    if (exClass == nullptr) {
        throwNoClassDefError(env, "class name " + exName + " doesn't exists");
    }
    return env->ThrowNew(exClass, message.c_str());
}


JNIEXPORT jboolean JNICALL 
    Java_com_intel_dbio_sources_datasourcev2_xiphosv2_XiphosJniImp_init (JNIEnv *env, jobject obj, jstring initString)
{
    std::cout << "Hello from JNI\n";
    std::cout << "Init string: " << env->GetStringUTFChars(initString, nullptr)  << std::endl;
    return true;
}

JNIEXPORT jstring JNICALL 
    Java_com_intel_dbio_sources_datasourcev2_xiphosv2_XiphosJniImp_getSchemaDesc (JNIEnv *env, jobject obj, jstring _tableName)
{
    string tableName = string(env->GetStringUTFChars(_tableName, nullptr));
    if (verbose) cout << "getSchemaDesc for " << tableName << endl;

    string retval = "";

    if (tableName == string("test_table_1")) {
        retval += string("id:i:false value:s:false");
    } else if (tableName == string("test_table_2")) {
        retval += string("id:i:false") + " ";
        retval += string("first_value:s:false") + " ";
        retval += string("second_value:i:true") + " ";
    } else {
        // TODO - get the data from Xiphos
        // crash for now
        throwException(env, "java.io.fileNoFoundException", "Xiphos is not supported for now"); 
    }
    return env->NewStringUTF(retval.c_str());
}
