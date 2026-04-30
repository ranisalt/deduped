find_package(GTest CONFIG REQUIRED)

enable_testing()
include(GoogleTest)

add_executable(unit_tests
    unit/test_types.cpp
    unit/test_hasher.cpp
    unit/test_metadata.cpp
    unit/test_repository.cpp
    unit/test_engine.cpp
    unit/test_event_handlers.cpp
)

target_link_libraries(unit_tests
    PRIVATE
        deduped_lib
        GTest::gtest
        GTest::gmock
        GTest::gtest_main
)

gtest_discover_tests(unit_tests LABELS unit)

add_executable(integration_tests
    integration/test_scan_and_dedupe.cpp
    integration/test_apply_hardlink.cpp
    integration/test_binaries.cpp
    integration/test_docker_entrypoint.cpp
    integration/test_crash_recovery.cpp
    integration/test_scanner.cpp
    integration/test_watcher.cpp
    integration/test_multi_root.cpp
)

target_link_libraries(integration_tests
    PRIVATE
        deduped_lib
        cli_impl
        daemon_impl
        GTest::gtest
        GTest::gtest_main
)

target_compile_definitions(integration_tests
    PRIVATE
        DEDUPED_CLI_PATH="$<TARGET_FILE:deduped-cli>"
        DEDUPED_DAEMON_PATH="$<TARGET_FILE:deduped>"
        DEDUPED_RUN_SCRIPT_PATH="${CMAKE_SOURCE_DIR}/root/etc/s6-overlay/s6-rc.d/svc-deduped/run"
)

gtest_discover_tests(integration_tests LABELS integration)

option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)

# Apply sanitizers to the library too, so bugs in library code are caught when
# running the test suite with sanitizers enabled.
foreach(tgt deduped_lib unit_tests integration_tests)
    if(ENABLE_ASAN)
        target_compile_options(${tgt} PRIVATE -fsanitize=address -fno-omit-frame-pointer)
        target_link_options(${tgt} PRIVATE -fsanitize=address)
    endif()
    if(ENABLE_UBSAN)
        target_compile_options(${tgt} PRIVATE -fsanitize=undefined -fno-omit-frame-pointer)
        target_link_options(${tgt} PRIVATE -fsanitize=undefined)
    endif()
endforeach()

option(ENABLE_COVERAGE "Enable coverage flags" OFF)
if(ENABLE_COVERAGE)
    foreach(tgt unit_tests integration_tests)
        if(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
            target_compile_options(${tgt} PRIVATE --coverage)
            target_link_options(${tgt} PRIVATE --coverage)
        elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
            target_compile_options(${tgt} PRIVATE -fprofile-instr-generate -fcoverage-mapping)
            target_link_options(${tgt} PRIVATE -fprofile-instr-generate)
        endif()
    endforeach()
endif()
