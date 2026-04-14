#pragma once

#include <cstdint>
#include <iostream>
#include <ostream>

const uint64_t PAGE_SIZE = 512;

struct PageHeader {
    uint64_t slots_length, data_offset, start_time, end_time;
};

struct Slot {
    uint64_t offset;
    uint64_t size;
    bool is_dead;
};

class Page {
  private:
    uint64_t id_;
    std::string path_;
    char* buf_;
    Slot* slots_;
    PageHeader* header_;

    uint64_t get_timestamp();

  public:
    Page(std::string path, uint64_t page_id);
    ~Page();
    uint64_t append_row(const char* data, uint64_t len);
    uint64_t get_free_space();
    uint64_t get_slots_count() const { return header_->slots_length; }
    bool read_slot(uint64_t slot_id, char* out_buf, uint64_t* out_size);
    friend std::ostream& operator<<(std::ostream& os, Page pg);
};