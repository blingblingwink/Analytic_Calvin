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

#include "global.h"
#include "manager.h"
#include "thread.h"
#include "calvin_thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "math.h"
#include "helper.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "sequencer.h"
#include "logger.h"
#include "message.h"
#include "work_queue.h"

void CalvinLockThread::setup() {}

#if CC_ALG == ANALYTIC_CALVIN
void CalvinLockThread::init(uint64_t thd_id, uint64_t node_id, Workload * workload, uint64_t id) {
	this->id = id;
	Thread::init(thd_id, node_id, workload);
}

FetchType CalvinLockThread::fetch_msg_to_lock(Message *&msg) {
#if CONTENTION_CHECK
	if (id == 0) {	// contended msg is handled by locker 0 dedicatedly
		msg = work_queue.contended_dequeue(_thd_id);
	} else {
		msg = work_queue.sched_dequeue(_thd_id);
	}
#else
	msg = work_queue.sched_dequeue(_thd_id);
#endif
	if (!msg) {
		return EMPTY_MSG;
	} else if (msg->rtype == RDONE) {
		msg->release();
		delete msg;
		msg = NULL;
		return RDONE_MSG;
	} else {
		return QRY_MSG;
	}
}

bool CalvinLockThread::double_check(Message *&msg) {
	failed_cnt++;
	if (failed_cnt % failed_mod != 0 || watermarks[id] != min_watermark) {
		return false;
	}
	// failed enough times to FORCE update its watermark and this locker is lagged behind

	// find current max watermark
	uint64_t cur_max_val = 0;
	for (uint32_t i = 0; i < g_scheduler_thread_cnt; i++) {
		cur_max_val = std::max(cur_max_val, watermarks[i].load(memory_order_acquire));
	}

	if (watermarks[id].load(memory_order_relaxed) == cur_max_val) {
		// this may happen when a batch of txns are all processed and no txns available for now
		return false;
	}

	/*
		check again if it can fetch msg now, 
		double check aims to avoid a rare concurrent problem, which is, say, we have 2 locker threads
		and a batch of txns: T1, T2, T3, T4. And T1 belongs to contended_queue, T2, T3, T4 belong to sched_queue
		locker 0 try to fetch msg, but T1 is not yet pushed into contended_queue, so locker 0 failed
		and then T1 is pushed into contended_queue while T2, T3, T4 being pushed into sched_queue
		then locker 1 successfully fetch T2, set its watermark to 2
		Aparrently, it would be a mistake for locker 0 to FORCE update its watermark to 2 since T1 is not processed yet
		A simple solution is to call fetch_msg_to_lock again, 
		if msg successfully fetched, then just process its locking process
		else, it means that there is INDEED no txns available for now, do FORCE update watermark as cur_max_val
	*/
	auto res = fetch_msg_to_lock(msg);
	if (res == EMPTY_MSG) {
		assert(watermarks[id] == min_watermark);
		watermarks[id].store(cur_max_val, memory_order_release);
		return false;
	} else if (res == RDONE_MSG) {
		return false;
	} else {
		return true;	// pass double check, fetch query successfully
	}
}

RC CalvinLockThread::run() {
	tsetup();

	TxnManager * txn_man;
	uint64_t prof_starttime = get_sys_clock();
	uint64_t idle_starttime = 0;
	failed_cnt = 0;

	while(!simulation->is_done()) {
		// Message * msg = work_queue.sched_dequeue(_thd_id);
		Message * msg = NULL;
		auto res = fetch_msg_to_lock(msg);
		if (res == EMPTY_MSG) {
			if (idle_starttime == 0) idle_starttime = get_sys_clock();
			if (!double_check(msg)) {
				// no txns available
				continue;
			}
		} else if (res == RDONE_MSG) {
			continue;
		}

		// watermark
		uint64_t watermark = msg->get_txn_id();
		assert(watermark > watermarks[id].load());
		watermarks[id].store(watermark, memory_order_release);
		TMP_DEBUG("thd: %ld set watermark: %ld\n", id, watermark);

		if(idle_starttime > 0) {
			INC_STATS(_thd_id,sched_idle_time,get_sys_clock() - idle_starttime);
			idle_starttime = 0;
		}
		
		prof_starttime = get_sys_clock();
		assert(msg->rtype == CL_QRY || msg->rtype == SUB_CL_QRY || msg->rtype == CL_QRY_O);
		assert(msg->get_txn_id() != UINT64_MAX);

		txn_man = txn_table.get_transaction_manager(get_thd_id(), msg->get_txn_id(), msg->get_batch_id());
		while (!txn_man->unset_ready()) {
		}

		assert(ISSERVERN(msg->get_return_id()));
		txn_man->txn_stats.starttime = get_sys_clock();
		txn_man->txn_stats.lat_network_time_start = msg->lat_network_time;
		txn_man->txn_stats.lat_other_time_start = msg->lat_other_time;

		msg->copy_to_txn(txn_man);
		txn_man->msg = msg;
		txn_man->register_thread(this);
		assert(ISSERVERN(txn_man->return_id));
		INC_STATS(get_thd_id(),sched_txn_table_time,get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();

		// Acquire locks
#if WORKLOAD == PPS
		RC rc = RCOK;
		if (!txn_man->isRecon()) {
			rc = txn_man->acquire_locks();
		}
#else
		txn_man->acquire_locks();
#endif
		uint16_t cnt;
		if(txn_man->decr_lr(cnt) == 0) {
			// if (ATOM_CAS(lock_ready, false, true)) rc = RCOK;
			work_queue.pending_enqueue(txn_man, cnt);
		}
		
		txn_man->set_ready();

		INC_STATS(_thd_id,mtx[33],get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();
	}
	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;
}
#else
RC CalvinLockThread::run() {
	tsetup();

	RC rc = RCOK;
	TxnManager * txn_man;
	uint64_t prof_starttime = get_sys_clock();
	uint64_t idle_starttime = 0;

	while(!simulation->is_done()) {
		txn_man = NULL;

		Message * msg = work_queue.sched_dequeue(_thd_id);

		if(!msg) {
			if (idle_starttime == 0) idle_starttime = get_sys_clock();
			continue;
		}
		if(idle_starttime > 0) {
			INC_STATS(_thd_id,sched_idle_time,get_sys_clock() - idle_starttime);
			idle_starttime = 0;
		}

		prof_starttime = get_sys_clock();
		assert(msg->get_rtype() == CL_QRY || msg->get_rtype() == CL_QRY_O);
		assert(msg->get_txn_id() != UINT64_MAX);

		txn_man = txn_table.get_transaction_manager(get_thd_id(), msg->get_txn_id(), msg->get_batch_id());
		while (!txn_man->unset_ready()) {
		}
		assert(ISSERVERN(msg->get_return_id()));
		txn_man->txn_stats.starttime = get_sys_clock();

		txn_man->txn_stats.lat_network_time_start = msg->lat_network_time;
		txn_man->txn_stats.lat_other_time_start = msg->lat_other_time;

		msg->copy_to_txn(txn_man);
		txn_man->register_thread(this);
		assert(ISSERVERN(txn_man->return_id));

		INC_STATS(get_thd_id(),sched_txn_table_time,get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();

		rc = RCOK;
		// Acquire locks
#if WORKLOAD == PPS
		if (!txn_man->isRecon()) {
			rc = txn_man->acquire_locks();
		}
#else
		rc = txn_man->acquire_locks();
#endif

		if(rc == RCOK) {
			work_queue.enqueue(_thd_id,msg,false);
		}
		txn_man->set_ready();

		INC_STATS(_thd_id,mtx[33],get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();
	}
	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;
}
#endif

void CalvinSequencerThread::setup() {}

bool CalvinSequencerThread::is_batch_ready() {
	bool ready = get_wall_clock() - simulation->last_seq_epoch_time >= g_seq_batch_time_limit;
	return ready;
}

RC CalvinSequencerThread::run() {
	tsetup();

	Message * msg;
	uint64_t idle_starttime = 0;
	uint64_t prof_starttime = 0;

	while(!simulation->is_done()) {

		prof_starttime = get_sys_clock();

		if(is_batch_ready()) {
			simulation->advance_seq_epoch();
			//last_batchtime = get_wall_clock();
			seq_man.send_next_batch(_thd_id);
		}

		INC_STATS(_thd_id,mtx[30],get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();

		msg = work_queue.sequencer_dequeue(_thd_id);

		INC_STATS(_thd_id,mtx[31],get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();

		if(!msg) {
			if (idle_starttime == 0) {
				idle_starttime = get_sys_clock();
			}
			continue;
		}
		if(idle_starttime > 0) {
			INC_STATS(_thd_id,seq_idle_time,get_sys_clock() - idle_starttime);
			idle_starttime = 0;
		}

		switch (msg->get_rtype()) {
			case CL_QRY:
			case CL_QRY_O:
				// Query from client
				DEBUG("SEQ process_txn\n");
#if CC_ALG == ANALYTIC_CALVIN && QUERY_SPLIT
	#if WORKLOAD == YCSB
				if (((YCSBClientQueryMessage*)msg)->requests.size() == g_short_req_per_query) {
					seq_man.process_txn(msg, get_thd_id(), 0, 0, 0, 0);
				} else {
					seq_man.process_long_txn(msg, get_thd_id());
				}
	#endif
#else
				seq_man.process_txn(msg,get_thd_id(),0,0,0,0);
#endif
				// Don't free message yet
				break;
			case CALVIN_ACK:
				// Ack from server
				DEBUG("SEQ process_ack (%ld,%ld) from %ld\n", msg->get_txn_id(), msg->get_batch_id(),
							msg->get_return_id());
				seq_man.process_ack(msg,get_thd_id());
				// Free message here
				msg->release();
				delete msg;
				break;
			default:
				assert(false);
		}

		INC_STATS(_thd_id,mtx[32],get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();
	}
	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;

}
