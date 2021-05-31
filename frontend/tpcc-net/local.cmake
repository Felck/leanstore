# TPCC over network

set(TPCC_SHARED_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/shared)

file(GLOB_RECURSE TPCC-SERVER ${CMAKE_CURRENT_LIST_DIR}/server/**.hpp ${CMAKE_CURRENT_LIST_DIR}/server/**.cpp ${NET_FILES})
add_executable(tpcc-server ${CMAKE_CURRENT_LIST_DIR}/server/tpcc-server.cpp ${TPCC-SERVER})
target_link_libraries(tpcc-server leanstore Threads::Threads)
target_include_directories(tpcc-server PRIVATE ${SHARED_INCLUDE_DIRECTORY} ${TPCC_SHARED_INCLUDE_DIR} ${NET_INCLUDE_DIR})

file(GLOB_RECURSE TPCC-CLIENT ${CMAKE_CURRENT_LIST_DIR}/client/**.hpp ${CMAKE_CURRENT_LIST_DIR}/client/**.cpp)
add_executable(tpcc-client ${CMAKE_CURRENT_LIST_DIR}/client/tpcc-client.cpp ${TPCC-CLIENT})
target_link_libraries(tpcc-client leanstore Threads::Threads)
target_include_directories(tpcc-client PRIVATE ${SHARED_INCLUDE_DIRECTORY} ${TPCC_SHARED_INCLUDE_DIR} ${NET_INCLUDE_DIR})
