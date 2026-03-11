# 设置cmake的最低版本和项目名称
cmake_minimum_required(VERSION 3.22)
project(KVRaftCpp)

# ====================== 【扩展功能开关】 ======================
# 每个扩展均可通过cmake命令行独立开关，例如：
#   cmake -DENABLE_READ_INDEX=OFF ..    关闭ReadIndex读优化
#   cmake -DENABLE_KEY_TTL=OFF ..       关闭Key TTL过期机制
#   cmake -DENABLE_THREAD_POOL=OFF ..   关闭RPC业务线程池
#   cmake -DENABLE_METRICS=OFF ..       关闭运行时监控指标
# 默认全部开启
option(ENABLE_READ_INDEX   "启用ReadIndex读优化"        ON)
option(ENABLE_KEY_TTL      "启用Key TTL过期删除机制"     ON)
option(ENABLE_THREAD_POOL  "启用RPC业务线程池"           ON)
option(ENABLE_METRICS      "启用运行时监控指标"           ON)

# 将开关状态传递给编译器（覆盖config.h中的默认值）
if(NOT ENABLE_READ_INDEX)
  add_compile_definitions(ENABLE_READ_INDEX=0)
endif()
if(NOT ENABLE_KEY_TTL)
  add_compile_definitions(ENABLE_KEY_TTL=0)
endif()
if(NOT ENABLE_THREAD_POOL)
  add_compile_definitions(ENABLE_THREAD_POOL=0)
endif()
if(NOT ENABLE_METRICS)
  add_compile_definitions(ENABLE_METRICS=0)
endif()


set(CMAKE_CXX_STANDARD 20)
# 生成debug版本，可以进行gdb调试
set(CMAKE_BUILD_TYPE "Debug")

# 设置项目可执行文件输出的路径
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
# 设置项目库文件输出的路径
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

# 设置项目编译头文件搜索路径 -I
# 目前可能存在路径污染的问题，需要进一步解决
include_directories(${PROJECT_SOURCE_DIR}/src/common/include)
include_directories(${PROJECT_SOURCE_DIR}/src/fiber/include)
include_directories(${PROJECT_SOURCE_DIR}/src/rpc/include)
include_directories(${PROJECT_SOURCE_DIR}/example)
include_directories(${PROJECT_SOURCE_DIR}/src/raftCore/include)
include_directories(${PROJECT_SOURCE_DIR}/src/raftRpcPro/include)
include_directories(${PROJECT_SOURCE_DIR}/src/raftClerk/include)
include_directories(${PROJECT_SOURCE_DIR}/src/skipList/include)


# 设置项目库文件搜索路径 -L
link_directories(${PROJECT_SOURCE_DIR}/lib)

# src包含了所有的相关代码
add_subdirectory(src)
# example包含了使用的示例代码
add_subdirectory(example)

add_library(skip_list_on_raft STATIC  ${src_rpc} ${src_fiber} ${rpc_example} ${raftsource} ${src_raftCore} ${src_raftRpcPro})
target_link_libraries(skip_list_on_raft muduo_net muduo_base pthread dl)
# 添加格式化目标 start
# from : https://blog.csdn.net/guotianqing/article/details/121661067

add_custom_target(format
        COMMAND bash ${PROJECT_SOURCE_DIR}/format.sh
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        COMMAND echo "format done!"
)


# 添加格式化目标 end