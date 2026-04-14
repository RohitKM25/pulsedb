#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_map>

#include "pulsedb/lsm.hpp"
#include "pulsedb/table.hpp"
#include "pulsedb/table_builder.hpp"

std::string get_db_path() { return (std::filesystem::current_path() / "pulsedb_data").string(); }

void ensure_db_folder() {
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
    std::filesystem::create_directory(db);
    std::cout << "DB cleared!\n";
}

void demo_all_features() {
    std::string db = get_db_path();
    std::cout << "\n=== TABLE DEMO ===\n";

    TableBuilder()
        .add_column(ColumnType::INTEGER, "id")
        .add_column(ColumnType::STRING, "name")
        .add_column(ColumnType::INTEGER, "value")
        .create(db, "test_table");

    Table tbl("test_table", db);
    for (int i = 1; i <= 5; i++) {
        std::unordered_map<std::string, void*> row = {
            {"id", new int(i)},
            {"name", new std::string("item_" + std::to_string(i))},
            {"value", new int(i * 10)}};
        tbl.insert(row);
    }

    std::cout << "Inserted 5 rows\n";
    auto all = tbl.select();
    std::cout << "Select all: " << all.size() << " rows\n";
    for (auto& r : all) {
        std::cout << "  row @" << r.timestamp << " cols=" << r.values.size() << "\n";
    }

    auto latest = tbl.select_latest(3);
    std::cout << "Select latest(3): " << latest.size() << " rows\n";
    for (auto& r : latest) {
        std::cout << "  row @" << r.timestamp << " cols=" << r.values.size() << "\n";
    }

    std::cout << "\n=== LSM DEMO ===\n";
    LSM lsm("timeseries", db, 1024, 3);
    for (int i = 0; i < 3; i++) {
        lsm.put("sensor_" + std::to_string(i), "value_" + std::to_string(i));
    }

    std::cout << "Put 3 entries\n";
    auto val = lsm.get("sensor_0");
    std::cout << "Get sensor_0: " << (val ? *val : "null") << "\n";

    auto range = lsm.get_range(0, UINT64_MAX);
    std::cout << "Get range: " << range.size() << " entries\n";

    std::cout << "\n=== SSTABLE FLUSH ===\n";
    for (int i = 0; i < 5; i++) {
        lsm.put("device_" + std::to_string(i), "data_" + std::to_string(i));
    }
    std::cout << "Put 5 more entries\n";

    auto after_flush = lsm.select_since(0);
    std::cout << "Total entries: " << after_flush.count() << "\n";

    std::cout << "\n=== UNIQUE KEYS ===\n";
    auto unique = after_flush.unique_keys();
    std::cout << "Unique keys: " << unique.size() << "\n";
    std::cout << "Demo done!\n";
}

int main(int argc, char* argv[]) {
    ensure_db_folder();

    if (argc > 1) {
        int mode = std::stoi(argv[1]);
        if (mode == 1) {
            clear_db();
        } else if (mode == 2) {
            demo_all_features();
        }
        return 0;
    }

    std::string db = get_db_path();
    std::string tbl_name, lsm_name, key, value;
    int id, val;

    std::cout << "PulseDB CLI - type 'help' for commands\n";

    while (true) {
        std::cout << "\n> ";
        std::string cmd;
        std::cin >> cmd;

        if (cmd == "quit" || cmd == "exit" || cmd == "q") {
            break;
        }
        if (cmd == "clear") {
            clear_db();
            continue;
        }
        if (cmd == "demo") {
            demo_all_features();
            continue;
        }
        if (cmd == "help") {
            std::cout << "Commands:\n"
                      << "  tbl create <name> - Create table\n"
                      << "  tbl insert <name> <id> <name> <val> - Insert row\n"
                      << "  tbl select <name> - Select all\n"
                      << "  tbl latest <name> <n> - Select latest N\n"
                      << "  lsm create <name> - Create LSM\n"
                      << "  lsm put <name> <key> <val> - Put value\n"
                      << "  lsm get <name> <key> - Get value\n"
                      << "  lsm range <name> - Get all entries\n"
                      << "  lsm since <name> <ts> - Select since timestamp\n"
                      << "  clear - Clear DB\n"
                      << "  demo - Run demo\n"
                      << "  quit - Exit\n";
            continue;
        }

        if (cmd == "tbl") {
            std::string sub;
            std::cin >> sub;
            if (sub == "create") {
                std::cin >> tbl_name;
                TableBuilder()
                    .add_column(ColumnType::INTEGER, "id")
                    .add_column(ColumnType::STRING, "name")
                    .add_column(ColumnType::INTEGER, "value")
                    .create(db, tbl_name);
                std::cout << "Created table: " << tbl_name << "\n";
            } else if (sub == "insert") {
                std::cin >> tbl_name >> id >> key >> val;
                Table t(tbl_name, db);
                std::unordered_map<std::string, void*> row = {
                    {"id", new int(id)}, {"name", new std::string(key)}, {"value", new int(val)}};
                t.insert(row);
                std::cout << "Inserted\n";
            } else if (sub == "select") {
                std::cin >> tbl_name;
                Table t(tbl_name, db);
                auto rows = t.select();
                std::cout << "Rows: " << rows.size() << "\n";
                for (auto& r : rows) {
                    std::cout << "  ts=" << r.timestamp;
                    for (auto& v : r.values) {
                        std::cout << " " << v.first << "=(" << v.second << ")";
                    }
                    std::cout << "\n";
                }
            } else if (sub == "latest") {
                std::cin >> tbl_name >> val;
                Table t(tbl_name, db);
                auto rows = t.select_latest(val);
                std::cout << "Rows: " << rows.size() << "\n";
                for (auto& r : rows) {
                    std::cout << "  ts=" << r.timestamp;
                    for (auto& v : r.values) {
                        std::cout << " " << v.first << "=(" << v.second << ")";
                    }
                    std::cout << "\n";
                }
            }
            continue;
        }

        if (cmd == "lsm") {
            std::string sub;
            std::cin >> sub;
            if (sub == "create") {
                std::cin >> lsm_name;
                LSM lsm(lsm_name, db);
                std::cout << "Created LSM: " << lsm_name << "\n";
            } else if (sub == "put") {
                std::cin >> lsm_name >> key >> value;
                LSM lsm(lsm_name, db);
                lsm.put(key, value);
                std::cout << "Put\n";
            } else if (sub == "get") {
                std::cin >> lsm_name >> key;
                LSM lsm(lsm_name, db);
                auto val = lsm.get(key);
                std::cout << (val ? *val : "null") << "\n";
            } else if (sub == "range") {
                std::cin >> lsm_name;
                LSM lsm(lsm_name, db);
                auto entries = lsm.get_range(0, UINT64_MAX);
                std::cout << "Entries: " << entries.size() << "\n";
                for (auto& e : entries) {
                    std::cout << "  " << e.key << "=" << e.value << " @" << e.timestamp << "\n";
                }
            } else if (sub == "since") {
                std::cin >> lsm_name >> val;
                LSM lsm(lsm_name, db);
                auto result = lsm.select_since(val);
                std::cout << "Count: " << result.count() << "\n";
                for (auto& e : result.entries()) {
                    std::cout << "  " << e.key << "=" << e.value << " @" << e.timestamp << "\n";
                }
            }
            continue;
        }

        std::cout << "Unknown command. Type 'help'.\n";
    }
    return 0;
}