set(CXTEST_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/cxtest")

add_library(ch_contrib::jvm SHARED IMPORTED GLOBAL)
set_property(TARGET ch_contrib::jvm PROPERTY IMPORTED_LOCATION "/usr/lib/jvm/zulu-17-amd64/lib/server/libjvm.so")

add_library(_cxtest SHARED IMPORTED GLOBAL)
set_property(TARGET _cxtest PROPERTY IMPORTED_LOCATION "${ClickHouse_SOURCE_DIR}/../j4rs-sanitizer-example/target/debug/libj4rs_sanitizer_example.so")
set_property(TARGET _cxtest PROPERTY INTERFACE_INCLUDE_DIRECTORIES "${CXTEST_SOURCE_DIR}/include")

add_library(ch_contrib::cxtest ALIAS _cxtest)
