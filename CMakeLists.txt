cmake_minimum_required(VERSION 3.22)
project(KVRaftCpp)

# ====================== Feature Switches ======================
# Each feature can be switched independently from the CMake command line:
#   cmake -DENABLE_READ_INDEX=OFF ..
#   cmake -DENABLE_KEY_TTL=OFF ..
#   cmake -DENABLE_LRU_CACHE=OFF ..
#   cmake -DENABLE_LSM_TREE=ON ..
option(ENABLE_READ_INDEX   "Enable ReadIndex read optimization"      ON)
option(ENABLE_KEY_TTL      "Enable key TTL expiration"               ON)
option(ENABLE_TTL_ACTIVE_EXPIRE "Enable non-mutating TTL active-expire observer" OFF)
option(ENABLE_THREAD_POOL  "Enable RPC worker thread pool"           ON)
option(ENABLE_METRICS      "Enable runtime metrics"                  ON)
option(ENABLE_LRU_CACHE    "Enable in-memory LRU cache for KV reads" ON)
option(ENABLE_LSM_TREE     "Use minimal LSM-tree KV engine"          OFF)

# Pass feature switches to the compiler. config.h provides defaults when a
# definition is not set here.
if(NOT ENABLE_READ_INDEX)
  add_compile_definitions(ENABLE_READ_INDEX=0)
endif()
if(NOT ENABLE_KEY_TTL)
  add_compile_definitions(ENABLE_KEY_TTL=0)
endif()
if(ENABLE_TTL_ACTIVE_EXPIRE)
  add_compile_definitions(ENABLE_TTL_ACTIVE_EXPIRE=1)
endif()
if(NOT ENABLE_THREAD_POOL)
  add_compile_definitions(ENABLE_THREAD_POOL=0)
endif()
if(NOT ENABLE_METRICS)
  add_compile_definitions(ENABLE_METRICS=0)
endif()
if(NOT ENABLE_LRU_CACHE)
  add_compile_definitions(ENABLE_LRU_CACHE=0)
endif()
if(ENABLE_LSM_TREE)
  add_compile_definitions(ENABLE_LSM_TREE=1)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_BUILD_TYPE "Debug")

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

include_directories(${PROJECT_SOURCE_DIR}/src/common/include)
include_directories(${PROJECT_SOURCE_DIR}/src/fiber/include)
include_directories(${PROJECT_SOURCE_DIR}/src/rpc/include)
include_directories(${PROJECT_SOURCE_DIR}/example)
include_directories(${PROJECT_SOURCE_DIR}/src/raftCore/include)
include_directories(${PROJECT_SOURCE_DIR}/src/raftRpcPro/include)
include_directories(${PROJECT_SOURCE_DIR}/src/raftClerk/include)
include_directories(${PROJECT_SOURCE_DIR}/src/skipList/include)
include_directories(${PROJECT_SOURCE_DIR}/src/storage/include)

link_directories(${PROJECT_SOURCE_DIR}/lib)

enable_testing()

add_subdirectory(src)
add_subdirectory(example)

add_library(skip_list_on_raft STATIC
        ${src_rpc}
        ${src_fiber}
        ${rpc_example}
        ${raftsource}
        ${src_raftCore}
        ${src_raftRpcPro})
target_link_libraries(skip_list_on_raft muduo_net muduo_base pthread dl)

add_executable(storage_cache_test test/storage_cache_test.cpp)
add_test(NAME storage_cache_test COMMAND storage_cache_test)

add_executable(storage_lsm_test test/storage_lsm_test.cpp)
add_test(NAME storage_lsm_test COMMAND storage_lsm_test)

add_custom_target(format
        COMMAND bash ${PROJECT_SOURCE_DIR}/format.sh
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        COMMAND echo "format done!"
)
