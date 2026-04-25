find_package(CLI11 CONFIG REQUIRED)

add_library(cli_impl cli_impl.cpp)
target_link_libraries(cli_impl
    PUBLIC
        deduped_lib
)

add_executable(deduped-cli main.cpp)

target_link_libraries(deduped-cli
    PRIVATE
        cli_impl
        CLI11::CLI11
)
