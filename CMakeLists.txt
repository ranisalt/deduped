cmake_minimum_required(VERSION 4.1.3)

set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

# Ensure to pick up the default triplet from the environment if any. This helps
# driving the vcpkg triplet in the same way either when starting vcpkg directly,
# or when letting CMake start vcpkg at configure/generate time.
# Note: this logic must happen before PROJECT command.
if(DEFINED ENV{VCPKG_DEFAULT_TRIPLET} AND NOT DEFINED VCPKG_TARGET_TRIPLET)
    set(VCPKG_TARGET_TRIPLET "$ENV{VCPKG_DEFAULT_TRIPLET}" CACHE STRING "The vcpkg triplet")
endif()

# Disable modules support, this is broken with clang-tidy at the moment
cmake_policy(SET CMP0155 OLD)

set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(deduped CXX)

# Build tests only when explicitly requested by the builder/preset.
option(BUILD_TESTING "Build tests" OFF)
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(ENABLE_COVERAGE "Enable coverage flags" OFF)
option(ENABLE_UNITY_BUILD "Enable CMake Unity (jumbo) builds" OFF)
option(ENABLE_LTO "Enable link-time optimization (IPO/LTO)" OFF)
include(CTest)

if(ENABLE_LTO)
    include(CheckIPOSupported)
    check_ipo_supported(RESULT IPO_SUPPORTED OUTPUT IPO_ERROR LANGUAGES CXX)
    if(IPO_SUPPORTED)
        set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ON)
    else()
        message(WARNING "ENABLE_LTO requested, but IPO/LTO is not supported: ${IPO_ERROR}")
    endif()
endif()

if(ENABLE_UNITY_BUILD)
    set(CMAKE_UNITY_BUILD ON)
endif()

if(NOT WIN32)
    add_compile_options(-Wall -Wextra -Wshadow -Wnon-virtual-dtor -pedantic -pipe -fvisibility=hidden)
endif()

if(ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()

if(ENABLE_UBSAN)
    add_compile_options(-fsanitize=undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=undefined)
endif()

if(ENABLE_COVERAGE)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
        add_compile_options(--coverage)
        add_link_options(--coverage)
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        add_compile_options(-fprofile-instr-generate -fcoverage-mapping)
        add_link_options(-fprofile-instr-generate)
    endif()
endif()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

add_subdirectory(lib)
add_subdirectory(cli)
add_subdirectory(daemon)

if(BUILD_TESTING)
	add_subdirectory(tests)
endif()
