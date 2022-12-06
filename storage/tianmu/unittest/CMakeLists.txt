MESSAGE(STATUS "Building unittest")
MESSAGE(STATUS "CMAKE_SOURCE_DIR ${CMAKE_SOURCE_DIR}")
MESSAGE(STATUS "TEST ${INCLUDE_DIRECTORIES}")


INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/storage/tianmu
		    ${CMAKE_SOURCE_DIR}
		    ${INCLUDE_DIRECTORIES}/include
)

# link success, but can not be used as push_warning() is undefined reference, we'll support it in the future.
#ADD_LIBRARY(common_definitions.a STATIC ${CMAKE_SOURCE_DIR}/storage/tianmu/common/common_definitions.cpp)

SET(LINK_LIBS  gtest pthread)

ADD_EXECUTABLE(testgmock testgmock.cpp)
TARGET_LINK_LIBRARIES(testgmock ${LINK_LIBS})

ADD_EXECUTABLE(testcommon test_common.cpp)
TARGET_LINK_LIBRARIES(testcommon ${LINK_LIBS})
