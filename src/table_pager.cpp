#include "pulsedb/table_pager.hpp"

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <ios>
#include <string>

#include "pulsedb/page.hpp"

TablePager::TablePager(std::string path) : path_(path) {
    auto pager_path = path_ + "/pager";
    if (!std::filesystem::exists(pager_path)) {
        pages_len_ = 0;
        pages_ = (PageEntry*)malloc(sizeof(PageEntry));
    } else {
        auto page_file = std::fstream(pager_path, std::ios_base::in | std::ios_base::binary);
        page_file.read(reinterpret_cast<char*>(&pages_len_), sizeof(uint64_t));
        pages_ = (PageEntry*)malloc(sizeof(PageEntry) * pages_len_);
        page_file.read(reinterpret_cast<char*>(pages_),
                       static_cast<std::streamsize>(pages_len_ * sizeof(PageEntry)));
    }
}

TablePager::~TablePager() {
    auto page_file = std::fstream(path_ + "/pager", std::ios_base::out | std::ios_base::binary);
    page_file.write(reinterpret_cast<char*>(&pages_len_), sizeof(uint64_t));
    page_file.write(reinterpret_cast<char*>(pages_),
                    static_cast<std::streamsize>(pages_len_ * sizeof(PageEntry)));
    free(pages_);
}

void TablePager::write_row(const char* data, uint64_t size) {
    uint64_t page_id = 0;
    bool new_page = false;

    if (pages_len_ == 0) {
        new_page = true;
    } else {
        auto pg_c = pages_[pages_len_ - 1];
        if (pg_c.free_size < size) {
            new_page = true;
            pages_ = (PageEntry*)realloc(pages_, sizeof(PageEntry) * (pages_len_ + 1));
        }
    }

    if (new_page) {
        page_id = pages_len_;
        pages_len_++;
    } else {
        page_id = pages_len_ - 1;
    }

    auto pg = Page(path_, page_id);
    auto slot_id = pg.append_row(data, size);

    pages_[pages_len_ - 1].page_id = page_id;
    pages_[pages_len_ - 1].slot_id = slot_id;
    pages_[pages_len_ - 1].free_size = pg.get_free_space();
}

std::vector<PageEntry> TablePager::read_all_entries() {
    std::vector<PageEntry> entries;
    for (uint64_t i = 0; i < pages_len_; ++i) {
        entries.push_back(pages_[i]);
    }
    return entries;
}

const PageEntry* TablePager::get_page(uint64_t idx) const {
    if (idx < pages_len_) {
        return &pages_[idx];
    }
    return nullptr;
}
