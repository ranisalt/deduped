find_package(Boost REQUIRED COMPONENTS asio)
find_package(CLI11 CONFIG REQUIRED)

add_library(daemon_impl daemon_impl.cpp)
target_link_libraries(daemon_impl
    PUBLIC
        deduped_lib
    PRIVATE
        Boost::asio
)

add_executable(deduped main.cpp)

target_link_libraries(deduped
    PRIVATE
        daemon_impl
        CLI11::CLI11
)
