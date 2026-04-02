project(simpleclient)


####################################
# src
####################################
file(GLOB_RECURSE simpleclient_sources
  "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")

####################################
# create executable target
####################################
add_executable(simpleclient
  ${simpleclient_sources})


####################################
# include directories
####################################
target_include_directories(simpleclient
  PRIVATE
    $<BUILD_INTERFACE:${lucene++_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${lucene++_SOURCE_DIR}/include/lucene++>
    ${Boost_INCLUDE_DIRS}
    ${Shenango_INCLUDE_DIRS}
    ${Breakwater_INCLUDE_DIRS})


####################################
# dependencies
####################################
target_link_libraries(simpleclient
  PRIVATE
    Boost::boost
    Boost::date_time
    Boost::filesystem
    Boost::iostreams
    Boost::regex
    Boost::system
    Boost::thread
    ZLIB::ZLIB
    lucene++::lucene++
    lucene++::lucene++-contrib
    ${Shenango_RUNTIME_LIBS}
    ${Breakwater_LIBS})

set_target_properties(simpleclient
  PROPERTIES
    LINK_FLAGS "-T ${Shenango_PATH}/base/base.ld")
