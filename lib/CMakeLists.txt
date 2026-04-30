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
find_package(unofficial-sqlite3 CONFIG REQUIRED)

target_link_libraries(deduped_lib
    PUBLIC
        BLAKE3::blake3
        unofficial::sqlite3::sqlite3
        spdlog::spdlog
        Boost::asio
    PRIVATE
        Boost::container
        Boost::unordered
)
