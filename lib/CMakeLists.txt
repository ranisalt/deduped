add_library(deduped_lib STATIC
    types.cpp
    hasher.cpp
    fs_walk.cpp
    scanner.cpp
    repository.cpp
    engine.cpp
    scan_coordinator.cpp
    watcher.cpp
    signal_watcher.cpp
    lock_file.cpp
    dedup_session.cpp
)

find_package(Boost REQUIRED COMPONENTS asio container unordered)
find_package(BLAKE3 CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

if(USE_SYSTEM_SQLITE3)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(SQLITE3 REQUIRED IMPORTED_TARGET sqlite3)
    set(_deduped_sqlite_target PkgConfig::SQLITE3)
else()
    find_package(unofficial-sqlite3 CONFIG REQUIRED)
    set(_deduped_sqlite_target unofficial::sqlite3::sqlite3)
endif()

target_link_libraries(deduped_lib
    PUBLIC
        BLAKE3::blake3
        ${_deduped_sqlite_target}
        spdlog::spdlog
        Boost::asio
    PRIVATE
        Boost::container
        Boost::unordered
)
