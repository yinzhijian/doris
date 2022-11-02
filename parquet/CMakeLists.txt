#cmake_minimum_required(VERSION 2.6)
cmake_minimum_required(VERSION 3.19.2)
project(lexical_cast)

set(BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(THIRDPARTY_DIR "${BASE_DIR}/../thirdparty/installed/")

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set (COMPILER_GCC 1)
elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set (COMPILER_CLANG 1)
endif ()

#add_definitions(-std=c++11)
set(CMAKE_CXX_FLAGS " -static -static-libgcc -static-libstdc++ -lresolv -lstdc++fs")

set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wall -Wno-sign-compare -pthread -Werror")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fstrict-aliasing -fno-omit-frame-pointer")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -std=gnu++17 -D__STDC_FORMAT_MACROS")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_SYSTEM_NO_DEPRECATED")
# Enable the cpu and heap profile of brpc
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBRPC_ENABLE_CPU_PROFILER")

if (USE_LIBCPP AND COMPILER_CLANG)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -stdlib=libc++")
    if (NOT OS_MACOSX)
        set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -lstdc++")
    endif()
    add_definitions(-DUSE_LIBCPP)
endif()
if (COMPILER_GCC)
    # Avoid GCC 11 false alarm
    # https://stackoverflow.com/questions/67584073/gcc-11-false-array-subscript-is-partly-outside-array-bounds-warning
    # https://stackoverflow.com/questions/69426070/gcc-11-order-of-arguments-triggers-false-positive-wstringop-overflow-is-this-bu
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-array-bounds -Wno-stringop-overread")
    add_compile_options(-Wno-stringop-overflow -fdiagnostics-color=always)
endif ()

#include_directories("/usr/local/include" "/usr/include")
include_directories(
    SYSTEM
    ${THIRDPARTY_DIR}/include
)
link_directories("/usr/local/lib" "/usr/lib/x86_64-linux-gnu/")

if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    set (OS_LINUX 1)
    add_definitions(-D OS_LINUX)
elseif (CMAKE_SYSTEM_NAME MATCHES "Darwin")
    set (OS_MACOSX 1)
    add_definitions(-D OS_MACOSX)
endif ()
if (NOT OS_MACOSX)
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS}
        ${DORIS_DEPENDENCIES}
        -static-libstdc++
        -static-libgcc
        -lstdc++fs
        -lresolv
    )
else()
    set(DORIS_LINK_LIBS
        ${DORIS_LINK_LIBS}
        ${DORIS_DEPENDENCIES}
        -lapple_nghttp2
        -lresolv
        -liconv
    )
endif()

add_library(arrow STATIC IMPORTED)
set_target_properties(arrow PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libarrow.a)

add_library(parquet STATIC IMPORTED)
set_target_properties(parquet PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libparquet.a)

add_library(orc STATIC IMPORTED)
set_target_properties(orc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/liborc.a)
add_library(thrift STATIC IMPORTED)
set_target_properties(thrift PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libthrift.a)
add_library(snappy STATIC IMPORTED)
set_target_properties(snappy PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libsnappy.a)
add_library(thriftnb STATIC IMPORTED)
set_target_properties(thriftnb PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libthriftnb.a)
add_library(protobuf STATIC IMPORTED)
set_target_properties(protobuf PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libprotobuf.a)
add_library(gflags STATIC IMPORTED)
set_target_properties(gflags PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libgflags.a)

add_library(glog STATIC IMPORTED)
set_target_properties(glog PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libglog.a)

add_library(protoc STATIC IMPORTED)
set_target_properties(protoc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libprotoc.a)
file( GLOB APP_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
add_executable(main main.cpp)
#foreach( sourcefile ${APP_SOURCES} )
#    file(RELATIVE_PATH filename ${CMAKE_CURRENT_SOURCE_DIR} ${sourcefile})
#    string(REPLACE ".cpp" "" file ${filename})
#    add_executable(${file} ${sourcefile})
target_link_libraries(main pthread arrow parquet thrift protobuf orc glog protoc gflags snappy)
#target_link_libraries(main boost_filesystem boost_thread boost_system boost_serialization pthread boost_chrono arrow parquet)
#endforeach( sourcefile ${APP_SOURCES} )
