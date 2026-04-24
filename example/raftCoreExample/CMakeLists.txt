set(SRC_LIST raftKvDB.cpp)


add_executable(raftCoreRun ${SRC_LIST})
target_link_libraries(raftCoreRun skip_list_on_raft rpc_lib protobuf muduo_net muduo_base pthread )

#################################

set(SRC_LIST2 caller.cpp)
add_executable(callerMain ${src_raftClerk} ${SRC_LIST2}  ${src_common})
target_link_libraries(callerMain skip_list_on_raft  protobuf boost_serialization )

set(BENCH_SRC_LIST benchMain.cpp)
add_executable(benchMain ${src_raftClerk} ${BENCH_SRC_LIST} ${src_common})
target_link_libraries(benchMain skip_list_on_raft protobuf boost_serialization pthread)
