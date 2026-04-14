#pragma once

#include <cstdint>
#include <string>
#include <vector>

struct PageEntry {
    uint64_t page_id;
    uint64_t slot_id;
    uint64_t free_size;
};

class TablePager {
  private:
    std::string path_;
    uint64_t pages_len_;
    PageEntry* pages_;

  public:
    TablePager(std::string path);
    ~TablePager();
    void write_row(const char* data, uint64_t size);
    std::vector<PageEntry> read_all_entries();
    uint64_t get_pages_count() const { return pages_len_; }
    const PageEntry* get_page(uint64_t idx) const;
};