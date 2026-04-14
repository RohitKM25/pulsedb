#include "pulsedb/table_builder.hpp"

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iostream>
#include <string>
#include <vector>

TableBuilder& TableBuilder::add_column(ColumnType type, std::string name) {
  defs_.push_back({type, name});
  var_len_ += name.size();
  return *this;
}

void TableBuilder::create(std::string db_path, std::string t_name) {
  uint16_t defs_len = static_cast<uint16_t>(defs_.size());
  uint64_t buf_size = defs_len * sizeof(ColumnType) +
                      defs_len * sizeof(uint8_t) + var_len_ + sizeof(defs_len) +
                      sizeof(var_len_);
  char* buf = new char[buf_size];
  memcpy(buf, &defs_len, sizeof(uint16_t));
  memcpy(buf + sizeof(uint16_t), &var_len_, sizeof(uint64_t));
  char* curr = buf + sizeof(uint16_t) + sizeof(uint64_t);
  for (const auto& def : defs_) {
    memcpy(curr, &def.type, sizeof(ColumnType));
    curr += sizeof(ColumnType);
    auto curr_len = def.name.size();
    memcpy(curr, &curr_len, sizeof(uint8_t));
    curr += sizeof(uint8_t);
    memcpy(curr, def.name.data(), curr_len);
    curr += def.name.size();
  }
  auto tf_path = db_path + "/" + t_name;
  std::filesystem::create_directory(tf_path);
  auto table_file = std::fstream(tf_path + "/tbl.def",
                                 std::ios_base::out | std::ios_base::binary);
  table_file.write(buf, static_cast<std::streamsize>(buf_size));
}
