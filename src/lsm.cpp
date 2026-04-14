#include "pulsedb/lsm.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <sstream>

MemTable::MemTable(uint64_t size_limit) : size_limit_(size_limit) {}

void MemTable::put(uint64_t timestamp, const std::string& key, const std::string& value) {
    for (auto& entry : entries_) {
        if (entry.key == key) {
            entry.timestamp = timestamp;
            entry.value = value;
            return;
        }
    }
    entries_.push_back({timestamp, key, value});
}

std::optional<std::string> MemTable::get(const std::string& key) {
    for (auto it = entries_.rbegin(); it != entries_.rend(); ++it) {
        if (it->key == key) {
            return it->value;
        }
    }
    return std::nullopt;
}

std::vector<MemTableEntry> MemTable::get_range(uint64_t start_time, uint64_t end_time) {
    std::vector<MemTableEntry> result;
    for (const auto& entry : entries_) {
        if (entry.timestamp >= start_time && entry.timestamp <= end_time) {
            result.push_back(entry);
        }
    }
    return result;
}

bool MemTable::should_flush() const {
    uint64_t total_size = 0;
    for (const auto& e : entries_) {
        total_size += e.key.size() + e.value.size() + 16;
    }
    return total_size >= size_limit_;
}

void MemTable::clear() { entries_.clear(); }

const std::vector<MemTableEntry>& MemTable::entries() const { return entries_; }

SSTable::SSTable(std::string path) : path_(path), current_id_(0) { load_index(); }

void SSTable::load_index() {
    auto index_path = path_ + "/sst_index";
    if (!std::filesystem::exists(index_path)) {
        return;
    }
    std::ifstream file(index_path, std::ios::binary);
    uint64_t count;
    file.read(reinterpret_cast<char*>(&count), sizeof(uint64_t));
    for (uint64_t i = 0; i < count; ++i) {
        SSTEntry entry;
        file.read(reinterpret_cast<char*>(&entry.start_time), sizeof(uint64_t));
        file.read(reinterpret_cast<char*>(&entry.end_time), sizeof(uint64_t));
        file.read(reinterpret_cast<char*>(&entry.page_id), sizeof(uint64_t));
        uint8_t path_len;
        file.read(reinterpret_cast<char*>(&path_len), sizeof(uint8_t));
        char* path_buf = new char[path_len];
        file.read(path_buf, path_len);
        entry.file_path = std::string(path_buf);
        delete[] path_buf;
        ssts_.push_back(entry);
    }
}

void SSTable::save_index() {
    std::ofstream file(path_ + "/sst_index", std::ios::binary);
    uint64_t count = ssts_.size();
    file.write(reinterpret_cast<const char*>(&count), sizeof(uint64_t));
    for (const auto& entry : ssts_) {
        file.write(reinterpret_cast<const char*>(&entry.start_time), sizeof(uint64_t));
        file.write(reinterpret_cast<const char*>(&entry.end_time), sizeof(uint64_t));
        file.write(reinterpret_cast<const char*>(&entry.page_id), sizeof(uint64_t));
        uint8_t path_len = static_cast<uint8_t>(entry.file_path.size());
        file.write(reinterpret_cast<const char*>(&path_len), sizeof(uint8_t));
        file.write(entry.file_path.data(), path_len);
        file.write(reinterpret_cast<const char*>(&entry.chunk_count), sizeof(uint64_t));
        for (const auto& chunk : entry.chunks) {
            file.write(reinterpret_cast<const char*>(&chunk), sizeof(ChunkMeta));
        }
    }
}

uint32_t SSTable::crc32(const char* data, size_t size) {
    uint32_t crc = 0;
    for (size_t i = 0; i < size; ++i) {
        crc = (crc << 1) ^ static_cast<uint32_t>(data[i]);
    }
    return crc;
}

std::string SSTable::compress_chunk(const char* data, size_t size, size_t* compressed_size) {
    if (size > static_cast<size_t>(std::numeric_limits<int>::max())) {
        return std::string();
    }
    auto max_compressed = static_cast<size_t>(LZ4_compressBound(static_cast<int>(size)));
    char* compressed = new char[max_compressed];
    int cs = LZ4_compress_default(data, compressed, static_cast<int>(size),
                                  static_cast<int>(max_compressed));
    if (cs <= 0) {
        delete[] compressed;
        return std::string();
    }
    *compressed_size = static_cast<size_t>(cs);
    std::string result(compressed, static_cast<size_t>(cs));
    delete[] compressed;
    return result;
}

std::string SSTable::decompress_chunk(const char* data, size_t size, size_t uncompressed_size) {
    if (size > static_cast<size_t>(std::numeric_limits<int>::max()) ||
        uncompressed_size > static_cast<size_t>(std::numeric_limits<int>::max())) {
        return std::string();
    }
    char* decompressed = new char[uncompressed_size];
    int ds = LZ4_decompress_safe(data, decompressed, static_cast<int>(size),
                                 static_cast<int>(uncompressed_size));
    if (ds <= 0) {
        delete[] decompressed;
        return std::string();
    }
    std::string result(decompressed, static_cast<size_t>(ds));
    delete[] decompressed;
    return result;
}

void SSTable::write(const std::vector<MemTableEntry>& entries) {
    if (entries.empty()) {
        return;
    }

    auto sorted_entries = entries;
    std::sort(sorted_entries.begin(), sorted_entries.end(),
              [](const MemTableEntry& a, const MemTableEntry& b) { return a.key < b.key; });

    std::vector<MemTableEntry> chunk_entries;
    std::vector<ChunkMeta> chunks;
    uint64_t min_time = UINT64_MAX;
    uint64_t max_time = 0;
    size_t chunk_data_size = 0;
    uint64_t chunk_id = 0;
    uint64_t offset = 0;

    auto flush_chunk = [&](bool force = false) {
        if (chunk_entries.empty() && !force)
            return;

        std::stringstream ss;
        uint64_t count = chunk_entries.size();
        ss.write(reinterpret_cast<const char*>(&count), sizeof(uint64_t));
        for (const auto& e : chunk_entries) {
            auto key_len = static_cast<std::streamsize>(e.key.size());
            auto value_len = static_cast<std::streamsize>(e.value.size());
            ss.write(reinterpret_cast<const char*>(&key_len), sizeof(uint64_t));
            ss.write(e.key.data(), key_len);
            ss.write(reinterpret_cast<const char*>(&value_len), sizeof(uint64_t));
            ss.write(e.value.data(), value_len);
            ss.write(reinterpret_cast<const char*>(&e.timestamp), sizeof(uint64_t));
        }

        std::string raw_data = ss.str();
        size_t compressed_size;
        std::string compressed =
            compress_chunk(raw_data.c_str(), raw_data.size(), &compressed_size);

        if (compressed.empty()) {
            raw_data = ss.str();
            ChunkMeta meta{chunk_id++, offset, raw_data.size(), raw_data.size(), min_time,
                           max_time,   0};
            chunks.push_back(meta);
            offset += raw_data.size();
        } else {
            ChunkMeta meta{chunk_id++,
                           offset,
                           compressed_size,
                           raw_data.size(),
                           min_time,
                           max_time,
                           crc32(compressed.c_str(), compressed_size)};
            chunks.push_back(meta);
            offset += compressed_size;
        }

        chunk_entries.clear();
        min_time = UINT64_MAX;
        max_time = 0;
    };

    for (const auto& e : sorted_entries) {
        min_time = std::min(min_time, e.timestamp);
        max_time = std::max(max_time, e.timestamp);
        chunk_entries.push_back(e);

        if (chunk_data_size >= CHUNK_MAX_SIZE / 2) {
            flush_chunk();
            chunk_data_size = 0;
        } else {
            chunk_data_size += e.key.size() + e.value.size() + 32;
        }
    }
    flush_chunk(true);

    std::string sst_path = path_ + "/sst_" + std::to_string(current_id_++);
    std::ofstream out(sst_path, std::ios::binary);

    for (size_t i = 0; i < chunks.size(); ++i) {
        const auto& chunk = chunks[i];
        std::stringstream ss;
        uint64_t count = 0;
        size_t start_idx = 0;
        for (size_t j = 0; j < sorted_entries.size() && count == 0; ++j) {
            if (sorted_entries[j].timestamp >= chunk.start_time &&
                sorted_entries[j].timestamp <= chunk.end_time) {
                if (count == 0)
                    start_idx = j;
                ++count;
            }
        }

        ss.write(reinterpret_cast<const char*>(&count), sizeof(uint64_t));
        for (size_t j = start_idx; j < start_idx + count && j < sorted_entries.size(); ++j) {
            const auto& e = sorted_entries[j];
            auto key_len = static_cast<std::streamsize>(e.key.size());
            auto value_len = static_cast<std::streamsize>(e.value.size());
            ss.write(reinterpret_cast<const char*>(&key_len), sizeof(uint64_t));
            ss.write(e.key.data(), key_len);
            ss.write(reinterpret_cast<const char*>(&value_len), sizeof(uint64_t));
            ss.write(e.value.data(), value_len);
            ss.write(reinterpret_cast<const char*>(&e.timestamp), sizeof(uint64_t));
        }

        std::string raw_data = ss.str();
        size_t compressed_size;
        std::string compressed =
            compress_chunk(raw_data.c_str(), raw_data.size(), &compressed_size);

        if (compressed.empty() || compressed_size >= raw_data.size()) {
            out << raw_data;
        } else {
            out << compressed;
        }
    }

    SSTEntry entry;
    entry.start_time = chunks.empty() ? 0 : chunks.front().start_time;
    entry.end_time = chunks.empty() ? 0 : chunks.back().end_time;
    entry.page_id = 0;
    entry.file_path = sst_path;
    entry.chunk_count = chunks.size();
    entry.chunks = chunks;
    ssts_.push_back(entry);

    save_index();
}

std::optional<std::string> SSTable::get(const std::string& key) {
    for (auto it = ssts_.rbegin(); it != ssts_.rend(); ++it) {
        if (!std::filesystem::exists(it->file_path)) {
            continue;
        }
        std::ifstream file(it->file_path, std::ios::binary);
        uint64_t count;
        file.read(reinterpret_cast<char*>(&count), sizeof(uint64_t));
        for (uint64_t i = 0; i < count; ++i) {
            uint64_t key_len;
            file.read(reinterpret_cast<char*>(&key_len), sizeof(uint64_t));
            auto key_len_s = static_cast<std::streamsize>(key_len);
            char* key_buf = new char[key_len];
            file.read(key_buf, key_len_s);
            std::string read_key(key_buf, key_len);
            delete[] key_buf;
            uint64_t value_len;
            file.read(reinterpret_cast<char*>(&value_len), sizeof(uint64_t));
            auto value_len_s = static_cast<std::streamsize>(value_len);
            char* value_buf = new char[value_len];
            file.read(value_buf, value_len_s);
            if (read_key == key) {
                std::string result(value_buf, value_len);
                delete[] value_buf;
                return result;
            }
            delete[] value_buf;
            file.seekg(sizeof(uint64_t), std::ios::cur);
        }
    }
    return std::nullopt;
}

std::vector<MemTableEntry> SSTable::get_range(uint64_t start_time, uint64_t end_time) const {
    std::vector<MemTableEntry> result;

    for (const auto& sst : ssts_) {
        if (sst.start_time > end_time || sst.end_time < start_time) {
            continue;
        }
        if (!std::filesystem::exists(sst.file_path)) {
            continue;
        }
        std::ifstream file(sst.file_path, std::ios::binary);
        uint64_t count;
        file.read(reinterpret_cast<char*>(&count), sizeof(uint64_t));
        for (uint64_t i = 0; i < count; ++i) {
            uint64_t key_len;
            file.read(reinterpret_cast<char*>(&key_len), sizeof(uint64_t));
            auto key_len_s = static_cast<std::streamsize>(key_len);
            char* key_buf = new char[key_len];
            file.read(key_buf, key_len_s);
            std::string read_key(key_buf, key_len);
            delete[] key_buf;
            uint64_t value_len;
            file.read(reinterpret_cast<char*>(&value_len), sizeof(uint64_t));
            auto value_len_s = static_cast<std::streamsize>(value_len);
            char* value_buf = new char[value_len];
            file.read(value_buf, value_len_s);
            uint64_t ts;
            file.read(reinterpret_cast<char*>(&ts), sizeof(uint64_t));
            if (ts >= start_time && ts <= end_time) {
                result.push_back({ts, read_key, std::string(value_buf, value_len)});
            }
            delete[] value_buf;
        }
    }

    std::sort(result.begin(), result.end(), [](const MemTableEntry& a, const MemTableEntry& b) {
        return a.timestamp < b.timestamp;
    });
    return result;
}

const std::vector<SSTEntry>& SSTable::ssts() const { return ssts_; }

void SSTable::merge_ssts(const std::vector<SSTEntry>& from_ssts) {
    std::vector<std::vector<MemTableEntry>> all_entries;

    for (const auto& sst : from_ssts) {
        if (!std::filesystem::exists(sst.file_path)) {
            continue;
        }
        std::ifstream file(sst.file_path, std::ios::binary);
        uint64_t count;
        file.read(reinterpret_cast<char*>(&count), sizeof(uint64_t));
        std::vector<MemTableEntry> entries;
        for (uint64_t i = 0; i < count; ++i) {
            uint64_t key_len;
            file.read(reinterpret_cast<char*>(&key_len), sizeof(uint64_t));
            auto key_len_s = static_cast<std::streamsize>(key_len);
            char* key_buf = new char[key_len];
            file.read(key_buf, key_len_s);
            std::string read_key(key_buf, key_len);
            delete[] key_buf;
            uint64_t value_len;
            file.read(reinterpret_cast<char*>(&value_len), sizeof(uint64_t));
            auto value_len_s = static_cast<std::streamsize>(value_len);
            char* value_buf = new char[value_len];
            file.read(value_buf, value_len_s);
            uint64_t ts;
            file.read(reinterpret_cast<char*>(&ts), sizeof(uint64_t));
            entries.push_back({ts, read_key, std::string(value_buf, value_len)});
            delete[] value_buf;
        }
        all_entries.push_back(entries);
    }

    auto merged = Compactor::merge(all_entries);
    write(merged);
}

std::vector<MemTableEntry>
Compactor::merge(const std::vector<std::vector<MemTableEntry>>& sorted_lists) {
    if (sorted_lists.empty()) {
        return {};
    }

    std::unordered_map<std::string, MemTableEntry> latest;
    for (const auto& list : sorted_lists) {
        for (const auto& entry : list) {
            auto it = latest.find(entry.key);
            if (it == latest.end() || entry.timestamp > it->second.timestamp) {
                latest[entry.key] = entry;
            }
        }
    }

    std::vector<MemTableEntry> result;
    for (auto& kv : latest) {
        result.push_back(kv.second);
    }
    std::sort(result.begin(), result.end(),
              [](const MemTableEntry& a, const MemTableEntry& b) { return a.key < b.key; });
    return result;
}

LSM::LSM(std::string name, std::string path, uint64_t level_size, uint64_t max_levels)
    : path_(path), name_(name), level_size_(level_size), max_levels_(max_levels),
      mem_table_(level_size) {
    std::filesystem::create_directory(path_ + "/" + name_);

    for (uint64_t i = 0; i < max_levels_; ++i) {
        std::string level_path = path_ + "/" + name_ + "/level_" + std::to_string(i);
        std::filesystem::create_directory(level_path);
        levels_.push_back(SSTable(level_path));
    }
}

void LSM::put(const std::string& key, const std::string& value) {
    auto timestamp = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                               std::chrono::system_clock::now().time_since_epoch())
                                               .count());

    mem_table_.put(timestamp, key, value);

    if (mem_table_.should_flush()) {
        flush();
    }
}

void LSM::flush() {
    auto entries = mem_table_.entries();
    if (entries.empty()) {
        return;
    }

    if (flush_callback_) {
        flush_callback_(entries);
    }

    levels_[0].write(entries);
    mem_table_.clear();

    if (levels_[0].ssts().size() >= 2) {
        compact_level(0);
    }
}

void LSM::compact_level(uint64_t level) {
    if (level >= max_levels_ - 1) {
        return;
    }

    auto& current_sst = levels_[level];
    auto ssts = current_sst.ssts();

    if (ssts.size() < 2) {
        return;
    }

    std::vector<SSTEntry> to_merge;
    for (size_t i = 0; i < ssts.size() && i < 2; ++i) {
        to_merge.push_back(ssts[i]);
    }

    auto merged = Compactor::merge({levels_[level].get_range(0, UINT64_MAX)});
    levels_[level + 1].write(merged);

    for (const auto& sst : to_merge) {
        std::filesystem::remove(sst.file_path);
    }
}

std::optional<std::string> LSM::get(const std::string& key) {
    auto result = mem_table_.get(key);
    if (result) {
        return result;
    }

    for (auto it = levels_.rbegin(); it != levels_.rend(); ++it) {
        auto level_result = it->get(key);
        if (level_result) {
            return level_result;
        }
    }
    return std::nullopt;
}

std::vector<MemTableEntry> LSM::get_range(uint64_t start_time, uint64_t end_time) {
    auto result = mem_table_.get_range(start_time, end_time);

    for (auto it = levels_.rbegin(); it != levels_.rend(); ++it) {
        auto level_result = it->get_range(start_time, end_time);
        result.insert(result.end(), level_result.begin(), level_result.end());
    }

    std::sort(result.begin(), result.end(), [](const MemTableEntry& a, const MemTableEntry& b) {
        return a.timestamp < b.timestamp;
    });
    return result;
}

void LSM::set_flush_callback(std::function<void(const std::vector<MemTableEntry>&)> callback) {
    flush_callback_ = callback;
}

uint64_t LSM::convert_time(uint64_t value, TimeUnit from, TimeUnit to) const {
    uint64_t mult = 1;
    if (from > to) {
        for (auto u = from; u > to; u = static_cast<TimeUnit>(static_cast<uint8_t>(u) - 1)) {
            mult *= 1000;
        }
        return value * mult;
    } else if (from < to) {
        for (auto u = from; u < to; u = static_cast<TimeUnit>(static_cast<uint8_t>(u) + 1)) {
            mult *= 1000;
        }
        return value / mult;
    }
    return value;
}

std::vector<SSTEntry> LSM::find_ssts_in_range(uint64_t start_time, uint64_t end_time) {
    std::vector<SSTEntry> result;
    for (const auto& level : levels_) {
        for (const auto& sst : level.ssts()) {
            if (sst.start_time <= end_time && sst.end_time >= start_time) {
                result.push_back(sst);
            }
        }
    }
    return result;
}

QueryResult LSM::select(const TimeRange& time_range) {
    QueryResult result;
    auto start = convert_time(time_range.start, time_range.unit, TimeUnit::MILLISECONDS);
    auto end = convert_time(time_range.end, time_range.unit, TimeUnit::MILLISECONDS);

    auto mem_result = mem_table_.get_range(start, end);
    for (const auto& e : mem_result) {
        result.add_entry(e);
    }

    auto ssts = find_ssts_in_range(start, end);
    result.set_scanned_chunks(ssts.size());

    for (auto it = levels_.rbegin(); it != levels_.rend(); ++it) {
        auto level_result = it->get_range(start, end);
        for (const auto& e : level_result) {
            result.add_entry(e);
        }
    }

    result.sort_by_time();
    return result;
}

QueryResult LSM::select(const TimeRange& time_range, const std::string& key_pattern) {
    QueryResult result;
    auto start = convert_time(time_range.start, time_range.unit, TimeUnit::MILLISECONDS);
    auto end = convert_time(time_range.end, time_range.unit, TimeUnit::MILLISECONDS);

    auto mem_result = mem_table_.get_range(start, end);
    for (const auto& e : mem_result) {
        if (e.key.find(key_pattern) != std::string::npos) {
            result.add_entry(e);
        }
    }

    auto ssts = find_ssts_in_range(start, end);
    result.set_scanned_chunks(ssts.size());

    for (auto it = levels_.rbegin(); it != levels_.rend(); ++it) {
        auto level_result = it->get_range(start, end);
        for (const auto& e : level_result) {
            if (e.key.find(key_pattern) != std::string::npos) {
                result.add_entry(e);
            }
        }
    }

    result.sort_by_time();
    return result;
}

QueryResult LSM::select_between(uint64_t start, uint64_t end) {
    return select(TimeRange(start, end, TimeUnit::MILLISECONDS));
}

QueryResult LSM::select_since(uint64_t timestamp) {
    return select(TimeRange(timestamp, UINT64_MAX, TimeUnit::MILLISECONDS));
}

QueryResult LSM::select_latest(const std::string& key, uint64_t count) {
    auto all = select_since(0);
    std::vector<MemTableEntry> matching;
    for (const auto& e : all.entries()) {
        if (e.key == key) {
            matching.push_back(e);
        }
    }

    std::sort(matching.begin(), matching.end(), [](const MemTableEntry& a, const MemTableEntry& b) {
        return a.timestamp > b.timestamp;
    });

    QueryResult result;
    for (uint64_t i = 0; i < count && i < matching.size(); ++i) {
        result.add_entry(matching[i]);
    }
    return result;
}

QueryResult LSM::select_aggregate(const TimeRange& time_range) {
    QueryResult result = select(time_range);
    auto unique = result.unique_keys();
    result = QueryResult();
    for (const auto& e : unique) {
        result.add_entry(e);
    }
    return result;
}