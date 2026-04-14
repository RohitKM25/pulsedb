# PulseDB

Lightweight Embedded Time-Series Database for IoT

## Build

```bash
mkdir -p build && cd build
cmake ..
make
```

## CLI Usage

```bash
./pldbsh              # Interactive mode
./pldbsh 1           # Clear DB
./pldbsh 2           # Run demo
```

### Commands (interactive mode)

```
tbl create <name>       Create table (id, name, value columns)
tbl insert <name> <id> <name> <val>   Insert row
tbl select <name>       Select all rows
tbl latest <name> <n>  Select latest N rows
lsm create <name>       Create LSM store
lsm put <name> <key> <val>    Put key-value
lsm get <name> <key>   Get value by key
lsm range <name>       Get all entries
lsm since <name> <ts>  Select since timestamp
clear                  Clear DB
demo                   Run demo
quit                   Exit
```

## Data Storage Format

### Directory Structure

```
pulsedb_data/
├── <table_name>/           # Table storage
│   ├── tbl.def            # Table definition
│   ├── pager              # Page metadata
│   ├── page0              # Data page 0 (512 bytes)
│   ├── page1              # Data page 1 (512 bytes)
│   └── data.bin           # Raw row data
│
└── <lsm_name>/            # LSM store
    ├── level_0/
    │   ├── sst_index     # SSTable index
    │   └── sst_0         # SSTable file
    ├── level_1/
    └── level_2/
```

---

## Table Storage

### tbl.def (Table Definition)

Binary format for table schema:

| Offset | Size   | Field              | Description                      |
|--------|--------|--------------------|----------------------------------|
| 0      | 2      | cols_len           | Number of columns (uint16_t)     |
| 2      | 8      | var_len            | Variable data size (uint64_t)    |
| 10     | N      | columns            | Repeated column definitions      |

**Column Definition (repeated):**

| Offset | Size   | Field              | Description                      |
|--------|--------|--------------------|----------------------------------|
| 0      | 1      | type               | ColumnType: 0=INTEGER, 1=STRING |
| 1      | 1      | name_len           | Column name length (uint8_t)    |
| 2      | N      | name               | Column name (variable)           |

### pager (Page Metadata)

Binary format for page tracking:

| Offset | Size   | Field              | Description                      |
|--------|--------|--------------------|----------------------------------|
| 0      | 8      | pages_len          | Number of pages (uint64_t)       |
| 8      | 24*N   | page_entries       | Array of PageEntry               |

**PageEntry struct (24 bytes):**

```
struct PageEntry {
    uint64_t page_id;     // Page file ID
    uint64_t slot_id;    // Slot position in page
    uint64_t free_size;  // Available space in page
};
```

### page{N} (Data Pages)

Each page is 512 bytes with the following structure:

```
+------------------+---------------+
| PageHeader (24B) | Slot[0]       |
|                  | Slot[1]       |
|                  | ...           |
+------------------+---------------+
| Data Area (grows from end)       |
| Row 0                           |
| Row 1                           |
| ...                             |
+----------------------------------+
```

**PageHeader struct (24 bytes):**

| Offset | Size   | Field              | Description                      |
|--------|--------|--------------------|----------------------------------|
| 0      | 8      | slots_length       | Number of slots (uint64_t)       |
| 8      | 8      | data_offset        | Offset where data starts         |
| 16     | 8      | start_time         | First record timestamp           |

**Slot struct (24 bytes, grows from beginning):**

| Offset | Size   | Field              | Description                      |
|--------|--------|--------------------|----------------------------------|
| 0      | 8      | offset             | Offset to data in page           |
| 8      | 8      | size               | Size of record                   |
| 16     | 8      | is_dead            | Deleted flag (bool)              |

**Row Data Format (in data area):**

| Offset | Size   | Field              | Description                      |
|--------|--------|--------------------|----------------------------------|
| 0      | 1      | type               | ColumnType: 0=INTEGER, 1=STRING  |
| 1      | 8      | value              | int64_t for INTEGER              |
|        | 8+len  | string_len + data  | For STRING                       |

### data.bin

Append-only raw row data (grows from end). Each row encoded as:
- ColumnType (1 byte)
- Value (varies by type)

---

## LSM Storage

### level_N/sst_index

SSTable index file - tracks all SSTables in this level.

| Offset | Size   | Field              | Description                      |
|--------|--------|--------------------|----------------------------------|
| 0      | 8      | count              | Number of SSTables (uint64_t)    |
| 8      | N      | sst_entries        | Array of SSTEntry                |

**SSTEntry struct:**

| Offset | Size   | Field              | Description                      |
|--------|--------|--------------------|----------------------------------|
| 0      | 8      | start_time         | Min timestamp in SST             |
| 8      | 8      | end_time           | Max timestamp in SST             |
| 16     | 8      | page_id            | Internal page ID                 |
| 24     | 1      | path_len           | File path length                 |
| 25     | N      | file_path          | Path to SST file                 |
| 25+N   | 8      | chunk_count        | Number of chunks                 |
| ...    | N*16   | chunks             | Array of ChunkMeta               |

### level_N/sst_{N}

SSTable files - sorted key-value storage with compression.

**SSTable File Structure:**

```
┌─────────────────────────────────────┐
│ Header (16 bytes)                   │
│   magic: 0x50534442 ("PSDB")        │
│   version: uint32_t                 │
│   entry_count: uint32_t             │
│   chunk_count: uint32_t              │
├─────────────────────────────────────┤
│ Chunk 0                             │
│   ┌───────────────────────────────┐ │
│   │ ChunkMeta (16 bytes)           │ │
│   │   key_count: uint32            │ │
│   │   uncompressed_size: uint32    │ │
│   │   compressed_size: uint32     │ │
│   │   crc32: uint32               │ │
│   ├───────────────────────────────┤ │
│   │ Compressed Data (LZ4)         │ │
│   │   Sorted key-value pairs      │ │
│   │   Format:                     │ │
│   │   [key_len][key][ts][val_len] │
│   │   [value]...                  │ │
│   └───────────────────────────────┘ │
├─────────────────────────────────────┤
│ Chunk 1                             │
│ ...                                 │
└─────────────────────────────────────┘
```

**ChunkMeta struct (16 bytes):**

| Offset | Size   | Field              | Description                      |
|--------|--------|--------------------|----------------------------------|
| 0      | 4      | key_count         | Number of keys in chunk          |
| 4      | 4      | uncompressed_size | Original data size               |
| 8      | 4      | compressed_size   | Compressed size                 |
| 12     | 4      | crc32              | CRC32 checksum                  |

**Key-Value Entry in Chunk (uncompressed):**

```
[uint8_t key_len]
[key_len bytes: key]
[uint64_t timestamp]
[uint8_t val_len]
[val_len bytes: value]
```

### MemTable (In-Memory)

In-memory buffer before flush to SSTable:

```
struct MemTableEntry {
    uint64_t timestamp;
    std::string key;
    std::string value;
};
```

Flushed to SSTable when size exceeds `level_size` (default 4096 bytes).

---

## Demo

```bash
./pldbsh 2
```

Output:
```
=== TABLE DEMO ===
Inserted 5 rows
Select all: 5 rows
  row @0 cols=2
  row @1000 cols=2
  row @2000 cols=2
  row @3000 cols=2
  row @4000 cols=2
Select latest(3): 3 rows

=== LSM DEMO ===
Put 3 entries
Get sensor_0: value_0
Get range: 3 entries

=== SSTABLE FLUSH ===
Put 5 more entries
Total entries: 8

=== UNIQUE KEYS ===
Unique keys: 8
Demo done!
```