#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

std::string get_db_path() { return (std::filesystem::current_path() / "pulsedb_data").string(); }

void ensure_db() {
    std::string db = get_db_path();
    if (!std::filesystem::exists(db)) {
        std::filesystem::create_directory(db);
    }
}

void clear_db() {
    std::string db = get_db_path();
    if (std::filesystem::exists(db)) {
        std::filesystem::remove_all(db);
    }
    std::cout << "Cleared: " << db << "\n";
}

void demo() {
    std::string db = get_db_path();
    std::cout << "DB path: " << db << "\n";

    ensure_db();
    std::filesystem::create_directory(db + "/test");
    std::filesystem::create_directory(db + "/timeseries");
    std::filesystem::create_directory(db + "/timeseries/level_0");
    std::filesystem::create_directory(db + "/timeseries/level_1");
    std::filesystem::create_directory(db + "/timeseries/level_2");

    std::ofstream f(db + "/test/tbl.def", std::ios::binary);
    uint16_t cols = 2;
    uint64_t var = 6;
    f.write((char*)&cols, 2);
    f.write((char*)&var, 8);
    f.write("\0\2id", 4);
    f.write("\1\4name", 6);
    f.close();

    std::ofstream sst(db + "/timeseries/level_0/sst_0", std::ios::binary);
    sst.write("test sst data", 12);
    sst.close();

    std::cout << "Files created:\n";
    for (auto& d : std::filesystem::recursive_directory_iterator(db)) {
        if (d.is_regular_file()) {
            std::cout << "  " << d.path() << " (" << d.file_size() << ")\n";
        } else {
            std::cout << "  " << d.path() << "/\n";
        }
    }
}

int main(int argc, char* argv[]) {
    if (argc > 1) {
        if (std::stoi(argv[1]) == 1)
            clear_db();
        return 0;
    }

    std::cout << "PulseDB CLI\n";
    std::cout << "Run from project root, or data is in ./pulsedb_data\n";
    std::cout << "Use ./pldbsh 1 to clear\n";

    std::string cmd;
    while (std::cin >> cmd) {
        if (cmd == "q" || cmd == "quit")
            break;
        if (cmd == "clear")
            clear_db();
        if (cmd == "demo")
            demo();
    }
    return 0;
}