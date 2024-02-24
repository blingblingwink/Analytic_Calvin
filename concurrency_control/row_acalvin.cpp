/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "row_acalvin.h"
#include "work_queue.h"

#if CC_ALG == ANALYTIC_CALVIN

void Row_acalvin::init(row_t * row) {
    _row = row;
    owners_head = NULL;
    owners_tail = NULL;
    waiters_head = NULL;
    waiters_tail = NULL;
    owner_cnt = 0;
    waiter_cnt = 0;

    latch = new pthread_mutex_t;
    pthread_mutex_init(latch, NULL);

    lock_type = LOCK_NONE;
    own_starttime = 0;
}

RC Row_acalvin::lock_get(lock_t type, TxnManager * txn) {
    RC rc;
    uint64_t starttime = get_sys_clock();
    uint64_t lock_get_start_time = starttime;
    pthread_mutex_lock( latch );
    INC_STATS(txn->get_thd_id(), mtx[17], get_sys_clock() - starttime);
    INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - lock_get_start_time);
    if (owner_cnt > 0) {
      INC_STATS(txn->get_thd_id(),twopl_already_owned_cnt,1);
    }

    
    LockEntry * entry = get_entry();
    entry->start_ts = get_sys_clock();
    entry->txn = txn;
    entry->type = type;

    if (owners_head == NULL) {
        LIST_PUT_TAIL(owners_head, owners_tail, entry);
        rc = lock_succeeded(txn, type);
    } else {
        bool isConflict = conflict_check(type);
        // txns MUST be ordered either in owners list or waiters list
        // even though lock_type is LOCK_SH, the above must be assured 
        // since owners list may be truncated from the middle by a old txn
        if (owners_tail->txn->get_txn_id() < txn->get_txn_id()) {   // new txn compared to owners_tail
            auto en = truncate_list(waiters_head, txn->get_txn_id());
            
            if (!isConflict && (waiters_head == NULL || en == waiters_head)) {
                // the txn is not conflict with owners_list, AND waiters_list is empty or the txn is older than waiters_head
                // put into owners_list
                LIST_PUT_TAIL(owners_head, owners_tail, entry);
                rc = lock_succeeded(txn, type);
            } else {
                // the txn is conflict with owners_list OR the txn should be placed into waiters_list
                if (waiters_head == NULL || en == NULL) {  // waiters_list is empty or the txn is newer than waiters_tail
                    LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
                    rc = lock_failed(txn);
                } else {    // the txn is older than waiters_head or the txn should be placed in the middle
                    LIST_INSERT_BEFORE(en, entry, waiters_head);
                    rc = lock_failed(txn);
                }
            }
        } else {    // old txn compared to owners_tail
            // note that owners_list is not empty for now
            auto en = truncate_list(owners_head, txn->get_txn_id());
            if (!isConflict) {  // lock_type and type are both LOCK_SH, put into correct position
                LIST_INSERT_BEFORE(en, entry, owners_head);
                rc = lock_succeeded(txn, type);
            } else {
                /* there are 4 cases when conflict happens
                    owner    ----   txn
                    LOCK_SH         LOCK_EX     (txn is older than all owners, then all owners become waiters, txn itself holds the lock)
                    LOCK_SH         LOCK_EX     (txn is older than partial owners, then txn itself and partial owners become waiters)
                    LOCK_EX         LOCK_SH     (there is only one owner and original owner become waiter, txn itself holds the lock)
                    LOCK_EX         LOCK_EX     (there is only one owner and original owner become waiter, txn itself holds the lock)
                */

                /*  truncate owners_list, put these txns into waiters_list, structured like below
                    owners_head --- new_owners_tail --- en --- old_owners_tail
                    waiters_head --- waiters_tail
                */
                auto new_owners_tail = en->prev, old_owners_tail = owners_tail;
                en->prev = NULL;

                // deprive lock from en to owners_tail
                deprive_lock(en);

                if (en != owners_head) {    // case 2, txn iteself should become waiter, add it
                    en->prev = entry;
                    entry->next = en;
                    entry->prev = NULL;
                    en = entry;

                    owners_tail = new_owners_tail;
                    owners_tail->next = NULL;
                    rc = lock_failed(txn);
                } else {
                    // case 1, 3, 4, txn itself become the only owner for now
                    lock_type = type;
                    owners_head = owners_tail = entry;
                    rc = lock_succeeded(txn, type);
                }
                
                // move corresponding txns to waiters_list
                if (waiters_head == NULL) {
                    waiters_head = en;
                    waiters_tail = old_owners_tail;
                } else {
                    old_owners_tail->next = waiters_head;
                    waiters_head->prev = old_owners_tail;
                    waiters_head = en;
                }
            }
        }
    }

    uint64_t curr_time = get_sys_clock();
    uint64_t timespan = curr_time - starttime;
    if (rc == WAIT && txn->twopl_wait_start == 0) {
        txn->twopl_wait_start = curr_time;
    }
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;
    INC_STATS(txn->get_thd_id(),twopl_getlock_time,timespan);
    INC_STATS(txn->get_thd_id(),twopl_getlock_cnt,1);

    pthread_mutex_unlock(latch);
	return rc;
}


RC Row_acalvin::lock_release(TxnManager * txn) {

#if WORKLOAD == PPS
    if (txn->isRecon()) {
        return RCOK;
    }
#endif

    uint64_t starttime = get_sys_clock();
    pthread_mutex_lock( latch );
    INC_STATS(txn->get_thd_id(),mtx[18],get_sys_clock() - starttime);

    DEBUG("unlock (%ld,%ld): owners %d, own type %d, key %ld %lx\n", txn->get_txn_id(),
        txn->get_batch_id(), owner_cnt, lock_type, _row->get_primary_key(), (uint64_t)_row);

    // Try to find the entry in the owners
    LockEntry * en = owners_head;
    while (en && en->txn != txn) {
        en = en->next;
    }

    assert(en);
    LIST_REMOVE_HT(en, owners_head, owners_tail);
    return_entry(en);
    owner_cnt --;
    if (owner_cnt == 0) {
        INC_STATS(txn->get_thd_id(),twopl_owned_cnt,1);
        uint64_t endtime = get_sys_clock();
        INC_STATS(txn->get_thd_id(),twopl_owned_time,endtime - own_starttime);
        if (lock_type == LOCK_SH) {
            INC_STATS(txn->get_thd_id(),twopl_sh_owned_time,endtime - own_starttime);
            INC_STATS(txn->get_thd_id(),twopl_sh_owned_cnt,1);
        } else {
            INC_STATS(txn->get_thd_id(),twopl_ex_owned_time,endtime - own_starttime);
            INC_STATS(txn->get_thd_id(),twopl_ex_owned_cnt,1);
        }
        lock_type = LOCK_NONE;
    }

    if (owner_cnt == 0) ASSERT(lock_type == LOCK_NONE);

    LockEntry * entry;
    // If any waiter can join the owners, just do it!
    while (waiters_head && !conflict_check(waiters_head->type)) {
        LIST_GET_HEAD(waiters_head, waiters_tail, entry);
#if DEBUG_TIMELINE
        printf("LOCK %ld %ld\n",entry->txn->get_txn_id(),get_sys_clock());
#endif
        DEBUG("2lock (%ld,%ld): owners %d, own type %d, req type %d, key %ld %lx\n",
          entry->txn->get_txn_id(), entry->txn->get_batch_id(), owner_cnt, lock_type, entry->type,
          _row->get_primary_key(), (uint64_t)_row);
        uint64_t timespan = get_sys_clock() - entry->txn->twopl_wait_start;
        entry->txn->twopl_wait_start = 0;

        INC_STATS(txn->get_thd_id(), twopl_wait_time, timespan);

        LIST_PUT_TAIL(owners_head, owners_tail, entry);
        owner_cnt++;

        ASSERT(entry->txn->lock_ready == false);
        if (entry->txn->decr_lr() == 0) {
            entry->txn->txn_stats.cc_block_time += timespan;
            entry->txn->txn_stats.cc_block_time_short += timespan;

            // put into validation queue
            auto cnt = ++entry->txn->enter_pending_cnt;
            ATOM_CAS(entry->txn->lock_ready, false, true);
            TMP_DEBUG("restart pending enqueue: %ld\n", entry->txn->get_txn_id());
            work_queue.pending_enqueue(entry->txn, cnt);
        }
        if (lock_type == LOCK_NONE) {
            own_starttime = get_sys_clock();
        }
        lock_type = entry->type;
    }

    uint64_t timespan = get_sys_clock() - starttime;
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;
    INC_STATS(txn->get_thd_id(), twopl_release_time, timespan);
    INC_STATS(txn->get_thd_id(), twopl_release_cnt, 1);

    pthread_mutex_unlock(latch);
        
    return RCOK;
}

inline bool Row_acalvin::conflict_check(lock_t type) {
    if (lock_type == LOCK_NONE) {
        return false;
    }
    return lock_type == LOCK_EX || type == LOCK_EX;
}

LockEntry * Row_acalvin::get_entry() {
  LockEntry *entry = (LockEntry *)mem_allocator.alloc(sizeof(LockEntry));
    entry->type = LOCK_NONE;
    entry->txn = NULL;
    entry->prev = NULL;
    entry->next = NULL;
    return entry;
}
void Row_acalvin::return_entry(LockEntry * entry) {
    mem_allocator.free(entry, sizeof(LockEntry));
}

LockEntry* Row_acalvin::truncate_list(LockEntry* head, uint64_t txn_id) {
    LockEntry *en = head;
    while (en && en->txn->get_txn_id() < txn_id) {
        en = en->next;
    }
    return en;
}

RC Row_acalvin::lock_succeeded(TxnManager *txn, lock_t type) {
    if(owner_cnt > 0) {
        assert(type == LOCK_SH);
        INC_STATS(txn->get_thd_id(), twopl_sh_bypass_cnt, 1);
    }
    owner_cnt++;
    if (lock_type == LOCK_NONE) {
        own_starttime = get_sys_clock();
    }
    lock_type = type;
    return RCOK;
}

RC Row_acalvin::lock_failed(TxnManager *txn) {
    ATOM_CAS(txn->lock_ready, true, false);
    txn->incr_lr();
    return WAIT;
}

void Row_acalvin::deprive_lock(LockEntry *start) {
    LockEntry *en = start;
    while (en != NULL) {
        owner_cnt--;
        en->txn->incr_lr();
        ATOM_CAS(en->txn->lock_ready, true, false);
        en = en->next;
    }
}

#endif