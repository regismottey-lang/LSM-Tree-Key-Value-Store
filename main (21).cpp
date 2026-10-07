// Project 10: Small LSM-tree key-value store: WAL + memtable + SSTables + Bloom filters + compaction.
// Build: g++ -std=c++20 -O2 -Wall -Wextra project10_lsm_kv.cpp -o lsm
//
// Usage:
//   ./lsm              interactive shell: type 'help' (data kept in ./lsm_data between runs)
//   ./lsm <dir>        interactive shell using a custom data directory
//   ./lsm --demo       run the built-in correctness test (20,000 keys, flushes, compaction, recovery)
//
// Layout on disk: <dir>/wal.log   write-ahead log (checksummed records)
//                 <dir>/sst_<id>.dat  immutable sorted tables, higher id = newer
// Known limits: compaction merges everything in memory and keeps tombstones (a manifest file
// would make dropping them crash-safe); no block cache; single writer.
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cctype>
#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;

struct Val {
    std::string value;
    bool deleted = false; // tombstone
};

// ---------- encoding helpers ----------
static uint32_t fnv1a(const std::string& s) {
    uint32_t h = 2166136261u;
    for (unsigned char c : s) { h ^= c; h *= 16777619u; }
    return h;
}
static uint64_t fnv64(const std::string& s) {
    uint64_t h = 1469598103934665603ull;
    for (unsigned char c : s) { h ^= c; h *= 1099511628211ull; }
    return h;
}
static void put_u32(std::string& s, uint32_t v) { s.append(reinterpret_cast<const char*>(&v), 4); }

// Entry = [klen u32][vlen u32][tombstone u8][key][value]
static std::string encode(const std::string& k, const Val& v) {
    std::string s;
    put_u32(s, static_cast<uint32_t>(k.size()));
    put_u32(s, static_cast<uint32_t>(v.value.size()));
    s.push_back(v.deleted ? 1 : 0);
    s += k;
    s += v.value;
    return s;
}
static bool read_entry(FILE* f, std::string& k, Val& v) {
    uint32_t kl, vl;
    uint8_t t;
    if (std::fread(&kl, 4, 1, f) != 1 || std::fread(&vl, 4, 1, f) != 1 || std::fread(&t, 1, 1, f) != 1)
        return false;
    if (kl > (1u << 28) || vl > (1u << 28)) return false; // corrupt length guard
    k.resize(kl);
    v.value.resize(vl);
    if (kl && std::fread(k.data(), 1, kl, f) != kl) return false;
    if (vl && std::fread(v.value.data(), 1, vl, f) != vl) return false;
    v.deleted = t != 0;
    return true;
}

// ---------- Bloom filter ----------
class Bloom {
public:
    void init(size_t n) {
        nbits_ = std::max<size_t>(64, n * 10); // ~1% false positives with 4 hashes
        bits_.assign((nbits_ + 63) / 64, 0);
    }
    void add(const std::string& k) {
        uint64_t a = std::hash<std::string>{}(k), b = fnv64(k) | 1;
        for (int i = 0; i < 4; ++i) {
            size_t p = (a + i * b) % nbits_;
            bits_[p / 64] |= 1ull << (p % 64);
        }
    }
    bool may_contain(const std::string& k) const {
        uint64_t a = std::hash<std::string>{}(k), b = fnv64(k) | 1;
        for (int i = 0; i < 4; ++i) {
            size_t p = (a + i * b) % nbits_;
            if (!(bits_[p / 64] & (1ull << (p % 64)))) return false;
        }
        return true;
    }
private:
    size_t nbits_ = 0;
    std::vector<uint64_t> bits_;
};

// ---------- SSTable ----------
struct SSTable {
    static constexpr size_t kIndexEvery = 16; // sparse index: one entry per 16 keys
    std::string path;
    uint64_t id = 0;
    Bloom bloom;
    std::vector<std::pair<std::string, long>> index;
    std::string min_key, max_key;

    static void write(const std::string& path, const std::map<std::string, Val>& data) {
        std::string tmp = path + ".tmp";
        FILE* f = std::fopen(tmp.c_str(), "wb");
        if (!f) throw std::runtime_error("cannot create " + tmp);
        for (auto& [k, v] : data) {
            std::string e = encode(k, v);
            std::fwrite(e.data(), 1, e.size(), f);
        }
        std::fflush(f);
        ::fsync(fileno(f));
        std::fclose(f);
        fs::rename(tmp, path); // atomic publish
    }

    static SSTable load(const std::string& path, uint64_t id) {
        SSTable t;
        t.path = path;
        t.id = id;
        FILE* f = std::fopen(path.c_str(), "rb");
        if (!f) throw std::runtime_error("cannot open " + path);
        std::vector<std::string> keys;
        std::string k;
        Val v;
        for (size_t n = 0;; ++n) {
            long off = std::ftell(f);
            if (!read_entry(f, k, v)) break;
            if (n % kIndexEvery == 0) t.index.emplace_back(k, off);
            if (n == 0) t.min_key = k;
            t.max_key = k;
            keys.push_back(k);
        }
        std::fclose(f);
        t.bloom.init(keys.size());
        for (auto& key : keys) t.bloom.add(key);
        return t;
    }

    bool get(const std::string& key, Val& out) const {
        if (index.empty() || key < min_key || key > max_key || !bloom.may_contain(key)) return false;
        auto it = std::upper_bound(index.begin(), index.end(), key,
                                   [](const std::string& k, const auto& e) { return k < e.first; });
        --it; // safe: key >= min_key == index[0].first
        FILE* f = std::fopen(path.c_str(), "rb");
        if (!f) return false;
        std::fseek(f, it->second, SEEK_SET);
        std::string k;
        Val v;
        bool found = false;
        for (size_t i = 0; i < kIndexEvery && read_entry(f, k, v); ++i) {
            if (k == key) { out = v; found = true; break; }
            if (k > key) break;
        }
        std::fclose(f);
        return found;
    }

    void read_all(std::map<std::string, Val>& into) const {
        FILE* f = std::fopen(path.c_str(), "rb");
        if (!f) throw std::runtime_error("cannot open " + path);
        std::string k;
        Val v;
        while (read_entry(f, k, v)) into[k] = v; // later (newer) tables overwrite earlier ones
        std::fclose(f);
    }
};

// ---------- DB ----------
struct Options {
    size_t memtable_bytes = 1 << 20;
    size_t compact_at_tables = 4;
    bool sync_wal = false; // true = fsync every write (durable, slower)
    std::function<void(const std::string&)> log; // optional: report engine events (flush, compaction, recovery)
};

class DB {
public:
    explicit DB(std::string dir, Options opt = {}) : dir_(std::move(dir)), opt_(opt) {
        fs::create_directories(dir_);
        std::vector<std::pair<uint64_t, std::string>> files;
        for (auto& e : fs::directory_iterator(dir_)) {
            std::string name = e.path().filename().string();
            if (name.rfind("sst_", 0) == 0 && name.size() > 4 && e.path().extension() == ".dat")
                files.emplace_back(std::stoull(name.substr(4)), e.path().string());
            else if (e.path().extension() == ".tmp")
                fs::remove(e.path()); // unfinished flush/compaction from a crash
        }
        std::sort(files.begin(), files.end());
        for (auto& [id, p] : files) { tables_.push_back(SSTable::load(p, id)); next_id_ = id + 1; }
        replay_wal();
        wal_ = std::fopen(wal_path().c_str(), "ab");
        if (!wal_) throw std::runtime_error("cannot open WAL");
    }
    ~DB() { if (wal_) std::fclose(wal_); }

    void put(const std::string& k, const std::string& v) { write(k, {v, false}); }
    void del(const std::string& k) { write(k, {"", true}); }

    std::optional<std::string> get(const std::string& k) const {
        if (auto it = mem_.find(k); it != mem_.end())
            return it->second.deleted ? std::nullopt : std::optional(it->second.value);
        Val v;
        for (auto t = tables_.rbegin(); t != tables_.rend(); ++t) // newest first
            if (t->get(k, v)) return v.deleted ? std::nullopt : std::optional(v.value);
        return std::nullopt;
    }

    size_t table_count() const { return tables_.size(); }
    size_t memtable_entries() const { return mem_.size(); }

    // All live (non-deleted) key/value pairs, newest version of each key.
    std::map<std::string, std::string> snapshot() const {
        std::map<std::string, Val> merged;
        for (auto& t : tables_) t.read_all(merged); // oldest -> newest
        for (auto& [k, v] : mem_) merged[k] = v;
        std::map<std::string, std::string> out;
        for (auto& [k, v] : merged) if (!v.deleted) out[k] = v.value;
        return out;
    }

private:
    void note(const std::string& m) const { if (opt_.log) opt_.log(m); }
    std::string wal_path() const { return dir_ + "/wal.log"; }
    std::string sst_path(uint64_t id) const {
        char buf[32];
        std::snprintf(buf, sizeof buf, "/sst_%08llu.dat", static_cast<unsigned long long>(id));
        return dir_ + buf;
    }

    void write(const std::string& k, const Val& v) {
        std::string rec = encode(k, v);
        uint32_t crc = fnv1a(rec);
        std::fwrite(&crc, 4, 1, wal_);
        std::fwrite(rec.data(), 1, rec.size(), wal_);
        std::fflush(wal_);
        if (opt_.sync_wal) ::fsync(fileno(wal_));
        mem_bytes_ += k.size() + v.value.size() + 16;
        mem_[k] = v;
        if (mem_bytes_ >= opt_.memtable_bytes) flush();
    }

    void replay_wal() {
        FILE* f = std::fopen(wal_path().c_str(), "rb");
        if (!f) return;
        std::string k;
        Val v;
        uint32_t crc;
        size_t replayed = 0;
        while (std::fread(&crc, 4, 1, f) == 1 && read_entry(f, k, v)) {
            if (crc != fnv1a(encode(k, v))) break; // torn or corrupt tail: stop replay here
            mem_[k] = v;
            mem_bytes_ += k.size() + v.value.size() + 16;
            ++replayed;
        }
        std::fclose(f);
        if (replayed) note("recovered " + std::to_string(replayed) + " recent writes from the write-ahead log");
    }

    void flush() {
        if (mem_.empty()) return;
        uint64_t id = next_id_++;
        SSTable::write(sst_path(id), mem_);
        note("memory was full: saved " + std::to_string(mem_.size()) + " entries to " +
             fs::path(sst_path(id)).filename().string());
        tables_.push_back(SSTable::load(sst_path(id), id));
        mem_.clear();
        mem_bytes_ = 0;
        std::fclose(wal_); // crash before this truncation only replays duplicates: harmless
        wal_ = std::fopen(wal_path().c_str(), "wb");
        if (tables_.size() >= opt_.compact_at_tables) compact();
    }

    void compact() {
        std::map<std::string, Val> merged;
        for (auto& t : tables_) t.read_all(merged); // oldest -> newest
        uint64_t id = next_id_++;
        SSTable::write(sst_path(id), merged); // new file has the highest id, so it shadows the old ones
        std::vector<std::string> old;
        for (auto& t : tables_) old.push_back(t.path);
        note("too many tables: merged " + std::to_string(old.size()) + " into " +
             fs::path(sst_path(id)).filename().string() + " (" + std::to_string(merged.size()) + " keys)");
        tables_.clear();
        tables_.push_back(SSTable::load(sst_path(id), id));
        for (auto& p : old) fs::remove(p);
    }

    std::string dir_;
    Options opt_;
    std::map<std::string, Val> mem_;
    size_t mem_bytes_ = 0;
    FILE* wal_ = nullptr;
    std::vector<SSTable> tables_;
    uint64_t next_id_ = 1;
};

// ---------- Interactive shell ----------
static std::string lower(std::string s) {
    for (auto& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

static void list_files(const std::string& dir, bool explain = false) {
    std::vector<std::pair<std::string, std::uintmax_t>> files;
    for (auto& e : fs::directory_iterator(dir))
        if (e.is_regular_file()) files.emplace_back(e.path().filename().string(), e.file_size());
    std::sort(files.begin(), files.end());
    if (files.empty()) std::cout << "  (no files yet)\n";
    for (auto& [name, size] : files) {
        std::cout << "  " << name << "  (" << size << " bytes)";
        if (explain) {
            if (name == "wal.log") std::cout << "  <- log of recent writes, used to recover after a crash";
            else if (name.rfind("sst_", 0) == 0) std::cout << "  <- sorted table saved on disk";
        }
        std::cout << "\n";
    }
}

static void print_help() {
    std::cout <<
        "\nCommands\n"
        "  put <key> <value>   save a value               e.g.  put name Regis\n"
        "  get <key>           look a value up            e.g.  get name\n"
        "  del <key>           delete a key               e.g.  del name\n"
        "  list                show every key and value\n"
        "  fill <n>            write n sample keys         e.g.  fill 500\n"
        "                      (watch the [engine] lines to see flushes and compaction)\n"
        "  stats               what is in memory vs. on disk\n"
        "  files               show the files in the data folder\n"
        "  reset               erase all data and start fresh\n"
        "  help                show this list\n"
        "  quit                exit (data is saved; run again and it comes back)\n"
        "\nKeys are one word. Values can contain spaces.\n\n";
}

static int shell(const std::string& dir) {
    Options opt;
    opt.memtable_bytes = 4 * 1024; // small, so a few hundred writes trigger flushes and compaction
    opt.log = [](const std::string& m) { std::cout << "  [engine] " << m << "\n"; };
    auto db = std::make_unique<DB>(dir, opt);

    std::cout << "\nLSM key-value store     data folder: " << dir << "\n"
              << "Quick start:   put name Regis   then   get name   then   quit\n"
              << "Run it again afterward and your data will still be there.\n"
              << "Type 'help' for all commands.\n\n";

    std::string line;
    while (std::cout << "lsm> " && std::getline(std::cin, line)) {
        std::istringstream in(line);
        std::string cmd, key;
        in >> cmd;
        cmd = lower(cmd);
        if (cmd.empty()) continue;

        if (cmd == "quit" || cmd == "exit" || cmd == "q") break;

        if (cmd == "help" || cmd == "?") {
            print_help();
        } else if (cmd == "put" || cmd == "set") {
            std::string value;
            in >> key;
            std::getline(in >> std::ws, value);
            if (key.empty() || value.empty()) { std::cout << "Usage: put <key> <value>      e.g.  put name Regis\n"; continue; }
            db->put(key, value);
            std::cout << "Saved: " << key << " = " << value << "\n";
        } else if (cmd == "get") {
            in >> key;
            if (key.empty()) { std::cout << "Usage: get <key>      e.g.  get name\n"; continue; }
            auto r = db->get(key);
            if (r) std::cout << key << " = " << *r << "\n";
            else std::cout << key << ": not found\n";
        } else if (cmd == "del" || cmd == "delete") {
            in >> key;
            if (key.empty()) { std::cout << "Usage: del <key>      e.g.  del name\n"; continue; }
            if (!db->get(key)) { std::cout << key << ": not found, nothing to delete\n"; continue; }
            db->del(key);
            std::cout << "Deleted: " << key << "\n";
        } else if (cmd == "list" || cmd == "ls") {
            auto all = db->snapshot();
            if (all.empty()) std::cout << "(empty) try:  put name Regis\n";
            for (auto& [k, v] : all) std::cout << "  " << k << " = " << v << "\n";
            if (!all.empty()) std::cout << all.size() << " key(s)\n";
        } else if (cmd == "fill") {
            int n = 0;
            in >> n;
            if (n <= 0 || n > 100000) { std::cout << "Usage: fill <n>   with n from 1 to 100000   e.g.  fill 500\n"; continue; }
            for (int i = 0; i < n; ++i) {
                char k[32];
                std::snprintf(k, sizeof k, "bulk%06d", i);
                db->put(k, "value-" + std::to_string(i));
            }
            std::cout << "Wrote " << n << " sample keys (bulk000000 ...). Tables on disk: " << db->table_count() << "\n";
        } else if (cmd == "stats") {
            std::error_code ec;
            auto wal = fs::file_size(dir + "/wal.log", ec);
            std::cout << "  In memory (not yet in a table): " << db->memtable_entries() << " entries\n"
                      << "  Sorted tables on disk:          " << db->table_count() << "\n"
                      << "  Write-ahead log size:           " << (ec ? 0 : wal) << " bytes\n";
        } else if (cmd == "files") {
            list_files(dir, true);
        } else if (cmd == "reset") {
            std::cout << "This erases ALL data in " << dir << ". Type 'yes' to confirm: ";
            std::string answer;
            std::getline(std::cin, answer);
            if (lower(answer) == "yes") {
                db.reset(); // close files before deleting them
                fs::remove_all(dir);
                db = std::make_unique<DB>(dir, opt);
                std::cout << "All data erased.\n";
            } else {
                std::cout << "Cancelled.\n";
            }
        } else {
            std::cout << "Unknown command '" << cmd << "'. Type 'help' to see what you can do.\n";
        }
    }
    std::cout << "\nGoodbye. Your data is saved in " << dir << " - run ./lsm again to see it recovered.\n";
    return 0;
}

// ---------- Built-in correctness test ----------
static int demo() {
    const std::string dir = "./lsm_demo";
    fs::remove_all(dir);
    Options opt;
    opt.memtable_bytes = 64 * 1024; // tiny, to force many flushes and compactions

    const int N = 20000;
    auto key = [](int i) { char b[32]; std::snprintf(b, sizeof b, "key%06d", i); return std::string(b); };
    auto expect = [](int i) -> std::optional<std::string> {
        if (i % 7 == 0) return std::nullopt; // deleted
        if (i % 5 == 0) return "updated-" + std::to_string(i); // overwritten
        return "value-" + std::to_string(i);
    };
    auto verify = [&](DB& db, const char* label) {
        size_t bad = 0;
        for (int i = 0; i < N; ++i) if (db.get(key(i)) != expect(i)) ++bad;
        if (db.get("missing-key")) ++bad;
        std::cout << label << ": " << (bad ? "FAIL (" + std::to_string(bad) + " wrong)" : "PASS")
                  << ", sstables=" << db.table_count() << "\n";
        return bad == 0;
    };

    bool ok = true;
    {
        DB db(dir, opt);
        for (int i = 0; i < N; ++i) db.put(key(i), "value-" + std::to_string(i));
        for (int i = 0; i < N; i += 5) db.put(key(i), "updated-" + std::to_string(i));
        for (int i = 0; i < N; i += 7) db.del(key(i));
        ok &= verify(db, "live reads ");
    }
    { // reopen: recovers tables from disk and the unflushed tail from the WAL
        DB db(dir, opt);
        ok &= verify(db, "after reopen");
    }
    std::cout << "files left on disk in " << dir << ":\n";
    list_files(dir);
    return ok ? 0 : 1;
}

int main(int argc, char** argv) {
    if (argc > 1 && std::string(argv[1]) == "--demo") return demo();
    return shell(argc > 1 ? argv[1] : "./lsm_data");
}
