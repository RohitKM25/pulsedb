#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "pulsedb/table_pager.hpp"

enum ColumnType : uint8_t { INTEGER, STRING };

struct ColumnDef {
    ColumnType type;
    std::string name;
};

struct Row {
    uint64_t timestamp;
    std::unordered_map<std::string, void*> values;
    std::unordered_map<std::string, ColumnType> types;
};

class Table {
  private:
    std::string name_;
    std::string path_;
    std::vector<ColumnDef> cols_;
    TablePager pager_;

  public:
    Table(std::string name, std::string path);
    void insert(std::unordered_map<std::string, void*> values);

    std::vector<Row> select();
    std::vector<Row> select_since(uint64_t timestamp);
    std::vector<Row> select_between(uint64_t start, uint64_t end);
    std::vector<Row> select_latest(uint64_t count);
    std::optional<Row> select_one();
};