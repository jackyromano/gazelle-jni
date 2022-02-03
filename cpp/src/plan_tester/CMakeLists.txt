cmake_minimum_required(VERSION 3.16)
project(spark_columnar_jni)

set(SRC
    main.cc
    )

add_executable(plan_tester ${SRC})
target_link_libraries(plan_tester spark_columnar_jni)
