message(STATUS "linking connectorx")

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/connectorx")

add_library(ch_contrib::jvm SHARED IMPORTED GLOBAL)
set_property(TARGET ch_contrib::jvm PROPERTY IMPORTED_LOCATION "/usr/lib/jvm/zulu-17-amd64/lib/server/libjvm.so")

# add_library(_connectorx STATIC IMPORTED GLOBAL)
add_library(_connectorx SHARED IMPORTED GLOBAL)
# set_property(TARGET _connectorx PROPERTY IMPORTED_LOCATION "/root/connector-x/target/release/libconnectorx_cpp.so")
set_property(TARGET _connectorx PROPERTY IMPORTED_LOCATION "/root/connector-x/target/debug/libconnectorx_cpp.so")
set_property(TARGET _connectorx PROPERTY INTERFACE_INCLUDE_DIRECTORIES "${LIBRARY_DIR}/include")

add_library(ch_contrib::connectorx ALIAS _connectorx)


if (TARGET ch_contrib::jvm)
    message(STATUS "added ch_contrib::jvm")
else()
    message(STATUS "not added ch_contrib::jvm")
endif()