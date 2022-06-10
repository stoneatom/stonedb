SET(BOOST_ROOT /usr/local/stonedb-boost)
AUX_SOURCE_DIRECTORY(. DIR_SRCS)

ADD_DEFINITIONS(-DDISABLE_USED_FOR_STONEDB)

SET(ASYNC_ROOT "${CMAKE_SOURCE_DIR}/storage/stonedb/")
INCLUDE_DIRECTORIES(${BOOST_ROOT}/include)
INCLUDE_DIRECTORIES(SYSTEM ${BOOST_ROOT}/include)

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/storage/stonedb/base)

INCLUDE_DIRECTORIES(${ASYNC_ROOT})
INCLUDE_DIRECTORIES(${ASYNC_ROOT}/base)
INCLUDE_DIRECTORIES(${BOOST_ROOT}/include)

AUX_SOURCE_DIRECTORY(. DIR_SRCS)
AUX_SOURCE_DIRECTORY(${CMAKE_SOURCE_DIR}/storage/stonedb/base/core SOURCE_async_core)
AUX_SOURCE_DIRECTORY(${CMAKE_SOURCE_DIR}/storage/stonedb/base/fmt SOURCE_async_fmt)
AUX_SOURCE_DIRECTORY(${CMAKE_SOURCE_DIR}/storage/stonedb/base/net SOURCE_async_net)
AUX_SOURCE_DIRECTORY(${CMAKE_SOURCE_DIR}/storage/stonedb/base/util SOURCE_async_util)

SET(ASYNC_SOURCES ${SOURCE_async_core}
                  ${SOURCE_async_fmt}
                  ${SOURCE_async_net}
                  ${SOURCE_async_util}
                  ${DIR_SRCS})

SET(DEPENDED_LIBS rt pthread dl ${BOOST_ROOT}/lib/libboost_system.a
${BOOST_ROOT}/lib/libboost_thread.a)

LINK_LIBRARIES(${DEPENDED_LIBS})

ADD_EXECUTABLE(test_prog ${ASYNC_SOURCES})

TARGET_LINK_LIBRARIES(test_prog ${DEPENDED_LIBS})
