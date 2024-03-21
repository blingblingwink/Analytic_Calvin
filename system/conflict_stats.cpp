#include "conflict_stats.h"
#include "ycsb_query.h"


void Conflict_Stats::init() {
    for (size_t i = 0; i < Npartition; i++) {
        pStats[i] = 0;
        is_high_conflict[i] = false;
    }
}

void Conflict_Stats::mark_contention(Message *msg) {
    auto msg_for_ease = static_cast<YCSBClientQueryMessage*>(msg);
    auto reqs = msg_for_ease->requests;
    size_t sz = reqs.size();
    uint8_t num = 0;
    msg_for_ease->is_high_contended = false;
    for (size_t i = 0; i < sz; i++) {
        if (is_high_conflict[key_to_partition(reqs[i]->key)].load(memory_order_relaxed)) {
            num++;
            if (2 * num >= sz) {
                msg_for_ease->is_high_contended = true;
                break;
            }
        }
    }
}

void Conflict_Stats::update_conflict_value(row_t * row) {
    uint64_t part = key_to_partition(row->get_primary_key());
    pStats[part].fetch_add(1, memory_order_relaxed);
}

void Conflict_Stats::update_conflict_state() {
    for (size_t i = 0; ;i++) {
        // only iterate local partition
        uint64_t part = i * g_node_cnt + g_node_id;
        if (part >= Npartition) {
            break;
        }

        // update state
        if (pStats[part] < conflict_lower_bound && is_high_conflict[part] == true) {
            is_high_conflict[part] = false;
        } else if (pStats[part] > conflict_upper_bound && is_high_conflict[part] == false) {
            is_high_conflict[part] = true;
        }

        // reset to zero
        pStats[part] = 0;
    }    
}

uint64_t Conflict_Stats::key_to_partition(uint64_t key) {
#if NODE_CNT == 1
    return key / conflict_partition_size;
#else
    static constexpr uint64_t denom = NODE_CNT * conflict_partition_size;
    int node_num = key % g_node_cnt;
	int shard_number_in_node = key / denom;
	return shard_number_in_node * g_node_cnt + node_num;
#endif
}

uint64_t Conflict_Stats::get_total_conflict() {
    uint64_t sum = 0;
    for (uint64_t i = 0; i < Npartition; i++) {
        sum += pStats[i].load(memory_order_relaxed);
    }
    return sum;
}

uint64_t Conflict_Stats::get_highest_conflict() {
    return pStats[g_node_id].load(memory_order_relaxed);
}
