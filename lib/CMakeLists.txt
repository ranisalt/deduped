add_library(deduped_lib STATIC
    types.cpp
    hasher.cpp
    scanner.cpp
    repository.cpp
    engine.cpp
    watcher.cpp
)

find_package(Boost REQUIRED COMPONENTS asio container unordered)
find_package(BLAKE3 CONFIG REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(spdlog CONFIG REQUIRED)
pkg_check_modules(sqlite3 REQUIRED IMPORTED_TARGET sqlite3)

target_link_libraries(deduped_lib
    PUBLIC
        BLAKE3::blake3
        PkgConfig::sqlite3
        spdlog::spdlog
    PRIVATE
        Boost::asio
        Boost::container
        Boost::unordered
)
