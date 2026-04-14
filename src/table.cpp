#include "pulsedb/table.hpp"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <ios>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "pulsedb/page.hpp"

Table::Table(std::string name, std::string path)
    : name_(name), path_(path + "/" + name), pager_(path + "/" + name) {
    auto table_file = std::fstream(path_ + "/tbl.def", std::ios_base::in | std::ios_base::binary);

    uint16_t cols_len;
    uint64_t var_len;
    table_file.read(reinterpret_cast<char*>(&cols_len), sizeof(uint16_t));
    table_file.read(reinterpret_cast<char*>(&var_len), sizeof(uint64_t));

    for (uint16_t i = 0; i < cols_len; ++i) {
        ColumnType type;
        table_file.read(reinterpret_cast<char*>(&type), sizeof(ColumnType));
        uint8_t name_len;
        table_file.read(reinterpret_cast<char*>(&name_len), sizeof(uint8_t));
        char* buf = new char[name_len];
        table_file.read(buf, name_len);
        std::string col_name = std::string(buf);
        cols_.push_back({type, col_name});
        delete[] buf;
    }
}

void Table::insert(std::unordered_map<std::string, void*> values) {
    auto buf_size = sizeof(uint16_t);
    char* buf = (char*)malloc(buf_size);
    uint16_t* col_map = reinterpret_cast<uint16_t*>(buf);
    *col_map = 0;
    for (uint16_t i = 0; i < cols_.size(); ++i) {
        const auto& it = values.find(cols_[i].name);
        if (it == values.end())
            continue;
        *col_map |= 1 << i;
        uint64_t data_size = 0;
        switch (cols_[i].type) {
        case ColumnType::INTEGER: {
            data_size = sizeof(uint64_t);
            break;
        }
        case ColumnType::STRING: {
            uint64_t s_len = static_cast<std::string*>(it->second)->size();
            data_size = sizeof(uint64_t);
            char* data = new char[data_size + s_len];
            memcpy(data, reinterpret_cast<char*>(&s_len), data_size);
            memcpy(data + data_size, static_cast<std::string*>(it->second)->data(), s_len);
            data_size += s_len;
            free(it->second);
            it->second = data;
            break;
        }
        }
        auto buf_size_inc = sizeof(ColumnType) + data_size;
        buf = (char*)realloc(buf, buf_size + buf_size_inc);
        memcpy(buf + buf_size, reinterpret_cast<char*>(&(cols_[i].type)), sizeof(ColumnType));
        memcpy(buf + buf_size + sizeof(ColumnType), reinterpret_cast<char*>(it->second), data_size);

        free(it->second);
        buf_size += buf_size_inc;
    }
    auto f = std::fstream(path_ + "/data.bin",
                          std::ios_base::out | std::ios_base::binary | std::ios_base::app);
    f.write(buf, static_cast<std::streamsize>(buf_size));

    pager_.write_row(buf, buf_size);

    free(buf);
}

std::vector<Row> Table::select() { return select_since(0); }

std::vector<Row> Table::select_since(uint64_t) {
    std::vector<Row> results;

    auto entries = pager_.read_all_entries();

    for (uint64_t i = 0; i < entries.size(); ++i) {
        auto* entry = pager_.get_page(i);
        if (!entry)
            continue;

        Page page(path_, entry->page_id);

        for (uint64_t s = 0; s < page.get_slots_count(); s++) {
            char buf[1024];
            uint64_t size = 0;
            if (page.read_slot(s, buf, &size)) {
                Row row;
                row.timestamp = s * 1000;

                char* ptr = buf;
                for (const auto& col : cols_) {
                    if (ptr >= buf + size)
                        break;

                    ColumnType type = *reinterpret_cast<ColumnType*>(ptr);
                    ptr += sizeof(ColumnType);
                    row.types[col.name] = type;

                    if (type == INTEGER) {
                        int64_t val = *reinterpret_cast<int64_t*>(ptr);
                        row.values[col.name] = new int64_t(val);
                        ptr += sizeof(int64_t);
                    } else if (type == STRING) {
                        uint64_t s_len = *reinterpret_cast<uint64_t*>(ptr);
                        ptr += sizeof(uint64_t);
                        std::string str(ptr, s_len);
                        row.values[col.name] = new std::string(str);
                        ptr += s_len;
                    }
                }
                results.push_back(row);
            }
        }
    }

    return results;
}

std::vector<Row> Table::select_between(uint64_t, uint64_t) { return select_since(0); }

std::vector<Row> Table::select_latest(uint64_t count) {
    auto all = select_since(0);
    if (count >= all.size()) {
        return all;
    }
    auto start_it = all.end() - static_cast<int>(count);
    return std::vector<Row>(start_it, all.end());
}

std::optional<Row> Table::select_one() {
    auto results = select_latest(1);
    if (results.empty()) {
        return std::nullopt;
    }
    return results[0];
}
