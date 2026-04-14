#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <lz4.h>
#include <lz4frame.h>

#include "pulsedb/page.hpp"

const uint64_t CHUNK_MAX_SIZE = 64 * 1024;

struct ChunkMeta {
    uint64_t chunk_id;
    uint64_t offset;
    uint64_t compressed_size;
    uint64_t uncompressed_size;
    uint64_t start_time;
    uint64_t end_time;
    uint32_t crc;
};

struct SSTEntry {
    uint64_t start_time;
    uint64_t end_time;
    uint64_t page_id;
    std::string file_path;
    uint64_t chunk_count;
    std::vector<ChunkMeta> chunks;
};

struct MemTableEntry {
    uint64_t timestamp;
    std::string key;
    std::string value;
};

enum class TimeUnit : uint8_t { MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS };

struct TimeRange {
    uint64_t start;
    uint64_t end;
    TimeUnit unit;

    TimeRange(uint64_t s, uint64_t e, TimeUnit u = TimeUnit::MILLISECONDS)
        : start(s), end(e), unit(u) {}
};

class QueryResult {
  private:
    std::vector<MemTableEntry> entries_;
    uint64_t total_count_;
    uint64_t scanned_chunks_;

  public:
    QueryResult() : total_count_(0), scanned_chunks_(0) {}

    void add_entry(const MemTableEntry& entry) {
        entries_.push_back(entry);
        total_count_++;
    }

    void add_entries(const std::vector<MemTableEntry>& entries) {
        for (const auto& e : entries) {
            entries_.push_back(e);
        }
        total_count_ = entries_.size();
    }

    const std::vector<MemTableEntry>& entries() const { return entries_; }
    uint64_t count() const { return total_count_; }
    uint64_t scanned_chunks() const { return scanned_chunks_; }
    void set_scanned_chunks(uint64_t n) { scanned_chunks_ = n; }

    void sort_by_time() {
        std::sort(entries_.begin(), entries_.end(),
                  [](const MemTableEntry& a, const MemTableEntry& b) {
                      return a.timestamp < b.timestamp;
                  });
    }

    std::vector<MemTableEntry> unique_keys() const {
        std::unordered_map<std::string, MemTableEntry> latest;
        for (const auto& e : entries_) {
            auto it = latest.find(e.key);
            if (it == latest.end() || e.timestamp > it->second.timestamp) {
                latest[e.key] = e;
            }
        }
        std::vector<MemTableEntry> result;
        for (const auto& kv : latest) {
            result.push_back(kv.second);
        }
        return result;
    }
};

class MemTable {
  private:
    std::vector<MemTableEntry> entries_;
    uint64_t size_limit_;

  public:
    MemTable(uint64_t size_limit = 4096);
    void put(uint64_t timestamp, const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key);
    std::vector<MemTableEntry> get_range(uint64_t start_time, uint64_t end_time);
    bool should_flush() const;
    void clear();
    const std::vector<MemTableEntry>& entries() const;
};

class SSTable {
  private:
    std::string path_;
    std::vector<SSTEntry> ssts_;
    uint64_t current_id_;
    void load_index();
    void save_index();
    std::string compress_chunk(const char* data, size_t size, size_t* compressed_size);
    std::string decompress_chunk(const char* data, size_t size, size_t uncompressed_size);
    uint32_t crc32(const char* data, size_t size);

  public:
    SSTable(std::string path);
    void write(const std::vector<MemTableEntry>& entries);
    std::optional<std::string> get(const std::string& key);
    std::vector<MemTableEntry> get_range(uint64_t start_time, uint64_t end_time) const;
    const std::vector<SSTEntry>& ssts() const;
    void merge_ssts(const std::vector<SSTEntry>& from_ssts);
};

class Compactor {
  public:
    static std::vector<MemTableEntry>
    merge(const std::vector<std::vector<MemTableEntry>>& sorted_lists);
};

class LSM {
  private:
    std::string path_;
    std::string name_;
    uint64_t level_size_;
    uint64_t max_levels_;
    MemTable mem_table_;
    std::vector<SSTable> levels_;
    std::function<void(const std::vector<MemTableEntry>&)> flush_callback_;

    void flush();
    void compact_level(uint64_t level);
    uint64_t convert_time(uint64_t value, TimeUnit from, TimeUnit to) const;
    std::vector<SSTEntry> find_ssts_in_range(uint64_t start_time, uint64_t end_time);

  public:
    LSM(std::string name, std::string path, uint64_t level_size = 1024 * 1024,
        uint64_t max_levels = 4);
    void put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key);
    std::vector<MemTableEntry> get_range(uint64_t start_time, uint64_t end_time);
    void set_flush_callback(std::function<void(const std::vector<MemTableEntry>&)> callback);

    QueryResult select(const TimeRange& time_range);
    QueryResult select(const TimeRange& time_range, const std::string& key_pattern);
    QueryResult select_between(uint64_t start, uint64_t end);
    QueryResult select_since(uint64_t timestamp);
    QueryResult select_latest(const std::string& key, uint64_t count = 1);
    QueryResult select_aggregate(const TimeRange& time_range);
};