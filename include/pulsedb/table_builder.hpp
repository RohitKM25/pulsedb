#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "pulsedb/table.hpp"

class TableBuilder {
 private:
  std::vector<ColumnDef> defs_;
  uint64_t var_len_ = 0;

 public:
  TableBuilder& add_column(ColumnType type, std::string name);
  void create(std::string db_path, std::string t_name);
};