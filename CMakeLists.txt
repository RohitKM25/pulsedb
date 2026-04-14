cmake_minimum_required(VERSION 3.20)

project(PulseDB
    VERSION 0.1.0
    DESCRIPTION "Lightweight Embedded Time-Series DB for IoT"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LZ4 REQUIRED liblz4)

file(GLOB LIB_HEADERS CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/include/pulsedb/*.hpp")

add_library(pulsedb STATIC
    src/lsm.cpp
    src/page.cpp
    src/table.cpp
    src/table_builder.cpp
    src/table_pager.cpp
    ${LIB_HEADERS}
)

target_include_directories(pulsedb
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${LZ4_INCLUDE_DIRS}
)

target_link_libraries(pulsedb PUBLIC ${LZ4_LIBRARIES})

target_compile_options(pulsedb PRIVATE
    -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion -Werror -g
)

add_executable(pldbsh apps/cli/main.cpp)
target_link_libraries(pldbsh PRIVATE pulsedb)
target_include_directories(pldbsh PRIVATE ${CMAKE_SOURCE_DIR}/include)

target_compile_options(pldbsh PRIVATE
    -Wall -Wextra -Wpedantic -g
)