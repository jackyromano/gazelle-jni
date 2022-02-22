package com.intel.dbio.sources.datasourcev2.xiphosv2;

public class XiphosJniImp {
    static {
        // leaving these debug printout below for future debug of adding the JNI .so file
        // into runtime path
        if (true) {
            String path = System.getProperty("java.library.path");
            System.out.println("Current path is: " + path);
            String cwd = System.getProperty("user.dir");
            System.out.println("Working Directory = " + cwd);
            path = path + ":" + cwd + "/cpp/build";
            System.setProperty("java.library.path", path);
            System.out.println("java.library.path: " + System.getProperty("java.library.path"));
        }
        System.loadLibrary("xiphosJNI");
    }

    /**
     * Initialize Xiphos appliance
     * @param str
     * @return success status
     */
    public native boolean  init();

    /**
     * Get schema description string. The string is space delimited pairs of name:type:isNullable where type is one of
     * "s" : string
     * "i" : Integer
     * "d" : data
     * @param tableName
     * @return schema description string
     */
    public native String getSchemaDesc(String tableName);
}
