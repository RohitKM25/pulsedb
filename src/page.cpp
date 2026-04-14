#include "pulsedb/page.hpp"

#include <chrono>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iostream>
#include <ostream>

Page::Page(std::string path, uint64_t page_id) : id_(page_id), path_(path) {
    buf_ = new char[PAGE_SIZE];
    header_ = reinterpret_cast<PageHeader*>(buf_);
    slots_ = reinterpret_cast<Slot*>(buf_ + sizeof(PageHeader));
    char file_name[10];
    snprintf(file_name, 10, "page%lu", id_);
    if (!std::filesystem::exists(path_ + "/" + file_name)) {
        header_->slots_length = 0;
        header_->data_offset = PAGE_SIZE;
    } else {
        auto page_file =
            std::fstream(path_ + "/" + file_name, std::ios_base::in | std::ios_base::binary);
        page_file.read(buf_, PAGE_SIZE);
    }
}

Page::~Page() {
    char file_name[10];
    snprintf(file_name, 10, "page%lu", id_);
    auto page_file =
        std::fstream(path_ + "/" + file_name, std::ios_base::out | std::ios_base::binary);
    page_file.write(buf_, PAGE_SIZE);
}

uint64_t Page::get_timestamp() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count());
}

uint64_t Page::append_row(const char* data, uint64_t len) {
    if (header_->data_offset - len < header_->slots_length * sizeof(uint16_t) + sizeof(PageHeader))
        return UINT64_MAX;
    header_->data_offset -= len;
    std::memcpy(buf_ + header_->data_offset, data, len);
    if (header_->slots_length == 1)
        header_->start_time = get_timestamp();
    slots_[header_->slots_length] = {header_->data_offset, len, false};
    header_->slots_length++;
    return header_->slots_length - 1;
}

uint64_t Page::get_free_space() {
    return header_->data_offset - header_->slots_length * sizeof(uint16_t) + sizeof(PageHeader);
}

bool Page::read_slot(uint64_t slot_id, char* out_buf, uint64_t* out_size) {
    if (slot_id >= header_->slots_length) {
        return false;
    }
    auto& slot = slots_[slot_id];
    if (slot.is_dead || slot.offset == 0) {
        return false;
    }
    std::memcpy(out_buf, buf_ + slot.offset, slot.size);
    *out_size = slot.size;
    return true;
}

std::ostream& operator<<(std::ostream& os, Page pg) {
    return os << "(" << pg.id_ << ") Slots Length: " << pg.header_->slots_length
              << ", Data Offset: " << pg.header_->data_offset << "\n";
}
