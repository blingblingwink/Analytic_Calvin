#pragma once

#include "global.h"
#include "message.h"
#include "row.h"

class Conflict_Stats {
public:
    void init();
    void mark_contention(Message *msg);
    void update_conflict_value(row_t * row);
    void update_conflict_state();
    uint64_t key_to_partition(uint64_t key);
    uint64_t get_total_conflict();
    uint64_t get_highest_conflict();
private:
    static constexpr uint64_t conflict_partition_size{CONFLICT_PARTITION_SIZE};
    static constexpr uint64_t conflict_lower_bound{LOWER_BOUND};
    static constexpr uint64_t conflict_upper_bound{UPPER_BOUND};
    static constexpr uint64_t Npartition{SYNTH_TABLE_SIZE / conflict_partition_size + NODE_CNT};
    std::array<std::atomic<uint64_t>, Npartition> pStats;
    std::array<std::atomic<bool>, Npartition> is_high_conflict;
};