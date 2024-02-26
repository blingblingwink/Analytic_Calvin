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

#include "work_queue.h"
#include "mem_alloc.h"
#include "query.h"
#include "message.h"
#include "client_query.h"
#include <boost/lockfree/queue.hpp>
#include "txn.h"

void QWorkQueue::init() {

	last_sched_dq = NULL;
	sched_ptr = 0;

	seq_queue = new boost::lockfree::queue<work_queue_entry* > (0);
	sub_txn_queue = new boost::lockfree::queue<work_queue_entry* > (0);
	new_txn_queue = new boost::lockfree::queue<work_queue_entry* >(0);
	sched_queue = new boost::lockfree::queue<work_queue_entry* > * [g_node_cnt];
	sched_ready = true;
	for ( uint64_t i = 0; i < g_node_cnt; i++) {
		sched_queue[i] = new boost::lockfree::queue<work_queue_entry* > (0);
	}

	txn_queue_size = 0;
	work_queue_size = 0;

	work_enqueue_size = 0;
	work_dequeue_size = 0;
	txn_enqueue_size = 0;
	txn_dequeue_size = 0;

	sem_init(&_semaphore, 0, 1);
	top_element=NULL;
}

void QWorkQueue::sequencer_enqueue(uint64_t thd_id, Message * msg) {
	uint64_t starttime = get_sys_clock();
	assert(msg);
	DEBUG_M("SeqQueue::enqueue work_queue_entry alloc\n");
	work_queue_entry * entry = (work_queue_entry*)mem_allocator.align_alloc(sizeof(work_queue_entry));
	entry->msg = msg;
	entry->rtype = msg->rtype;
	entry->txn_id = msg->txn_id;
	entry->batch_id = msg->batch_id;
	entry->starttime = get_sys_clock();
	assert(ISSERVER);

	DEBUG("Seq Enqueue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
		while (!seq_queue->push(entry) && !simulation->is_done()) {
		}

	INC_STATS(thd_id,seq_queue_enqueue_time,get_sys_clock() - starttime);
	INC_STATS(thd_id,seq_queue_enq_cnt,1);

}

Message * QWorkQueue::sequencer_dequeue(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();
	assert(ISSERVER);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;
	bool valid = seq_queue->pop(entry);

	if(valid) {
		msg = entry->msg;
		assert(msg);
		DEBUG("Seq Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,seq_queue_wait_time,queue_time);
		INC_STATS(thd_id,seq_queue_cnt,1);
		// DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d,
		// 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
	DEBUG_M("SeqQueue::dequeue work_queue_entry free\n");
		mem_allocator.free(entry,sizeof(work_queue_entry));
		INC_STATS(thd_id,seq_queue_dequeue_time,get_sys_clock() - starttime);
	}

	return msg;

}

void QWorkQueue::sched_enqueue(uint64_t thd_id, Message * msg) {
	assert(CC_ALG == CALVIN || CC_ALG == ANALYTIC_CALVIN);
	assert(msg);
	assert(ISSERVERN(msg->return_node_id));
	uint64_t starttime = get_sys_clock();

	DEBUG_M("QWorkQueue::sched_enqueue work_queue_entry alloc\n");
	work_queue_entry * entry = (work_queue_entry*)mem_allocator.alloc(sizeof(work_queue_entry));
	entry->msg = msg;
	entry->rtype = msg->rtype;
	entry->txn_id = msg->txn_id;
	entry->batch_id = msg->batch_id;
	entry->starttime = get_sys_clock();

	DEBUG("Sched Enqueue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
	uint64_t mtx_time_start = get_sys_clock();
	while (!sched_queue[msg->get_return_id()]->push(entry) && !simulation->is_done()) {
	}
	INC_STATS(thd_id,mtx[37],get_sys_clock() - mtx_time_start);

	INC_STATS(thd_id,sched_queue_enqueue_time,get_sys_clock() - starttime);
	INC_STATS(thd_id,sched_queue_enq_cnt,1);
}

Message * QWorkQueue::sched_dequeue(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();

	assert(CC_ALG == CALVIN || CC_ALG == ANALYTIC_CALVIN);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;

#if CC_ALG == CALVIN
	bool valid = sched_queue[sched_ptr]->pop(entry);
	if(valid) {

		msg = entry->msg;
		DEBUG("Sched Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);

		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,sched_queue_wait_time,queue_time);
		INC_STATS(thd_id,sched_queue_cnt,1);

		DEBUG_M("QWorkQueue::sched_enqueue work_queue_entry free\n");
		mem_allocator.free(entry,sizeof(work_queue_entry));

		if(msg->rtype == RDONE) {
			// Advance to next queue or next epoch
			DEBUG("Sched RDONE %ld %ld\n",sched_ptr,simulation->get_worker_epoch());
			assert(msg->get_batch_id() == simulation->get_worker_epoch());
			if(sched_ptr == g_node_cnt - 1) {
				INC_STATS(thd_id,sched_epoch_cnt,1);
				INC_STATS(thd_id,sched_epoch_diff,get_sys_clock()-simulation->last_worker_epoch_time);
				simulation->next_worker_epoch();
			}
			sched_ptr = (sched_ptr + 1) % g_node_cnt;
			msg->release();
			msg = NULL;
		} else {
			simulation->inc_epoch_txn_cnt();
			DEBUG("Sched msg dequeue %ld (%ld,%ld) %ld\n", sched_ptr, msg->txn_id, msg->batch_id,
						simulation->get_worker_epoch());
			assert(msg->batch_id == simulation->get_worker_epoch());
		}

		INC_STATS(thd_id,sched_queue_dequeue_time,get_sys_clock() - starttime);
	}
#else
	/*
		sched_ready serves as spinlock, protecting concurrent accesses on sched_ptr
		which ensures correct order of sched_queue dequeue, say, g_node_cnt = 2
		pop all entries from sched_queue[0] until meeting RDONE, then move to next one
		which is, pop all entries from sched_queue[1] until meering RDONE, 
		then moving to sched_queue[0] again, and so on
		when meeting RDONE from sched_queue[0], it is prohibited to still pop entry from sched_queue[0]
	*/
	while (!ATOM_CAS(sched_ready, true, false)) {
	}
	bool valid = sched_queue[sched_ptr]->pop(entry);

	if(valid) {
		msg = entry->msg;
		if(msg->rtype == RDONE) {
			sched_ptr = (sched_ptr + 1) % g_node_cnt;
			ATOM_CAS(sched_ready, false, true);
			msg->release();
			msg = NULL;
		} else {
			ATOM_CAS(sched_ready, false, true);
		}

		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,sched_queue_wait_time,queue_time);
		INC_STATS(thd_id,sched_queue_cnt,1);
		INC_STATS(thd_id,sched_queue_dequeue_time,get_sys_clock() - starttime);
		mem_allocator.free(entry,sizeof(work_queue_entry));
	} else {
		ATOM_CAS(sched_ready, false, true);
	}
#endif
	return msg;
}

void QWorkQueue::enqueue(uint64_t thd_id, Message * msg,bool busy) {
	uint64_t starttime = get_sys_clock();
	assert(msg);
	DEBUG_M("QWorkQueue::enqueue work_queue_entry alloc\n");
	work_queue_entry * entry = (work_queue_entry*)mem_allocator.align_alloc(sizeof(work_queue_entry));
	entry->msg = msg;
	entry->rtype = msg->rtype;
	entry->txn_id = msg->txn_id;
	entry->batch_id = msg->batch_id;
	entry->starttime = get_sys_clock();
	assert(ISSERVER || ISREPLICA);
	DEBUG("Work Enqueue (%ld,%ld) %d\n",entry->txn_id,entry->batch_id,entry->rtype);

	uint64_t mtx_wait_starttime = get_sys_clock();
	if(msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
		while (!new_txn_queue->push(entry) && !simulation->is_done()) {
		}
		sem_wait(&_semaphore);
		txn_queue_size ++;
		txn_enqueue_size ++;
		sem_post(&_semaphore);
	} else {
		while (!sub_txn_queue->push(entry) && !simulation->is_done()) {
		}
		sem_wait(&_semaphore);
		work_queue_size ++;
		work_enqueue_size ++;
		sem_post(&_semaphore);
	}
	INC_STATS(thd_id,mtx[13],get_sys_clock() - mtx_wait_starttime);

	if(busy) {
		INC_STATS(thd_id,work_queue_conflict_cnt,1);
	}
	INC_STATS(thd_id,work_queue_enqueue_time,get_sys_clock() - starttime);
	INC_STATS(thd_id,work_queue_enq_cnt,1);
	INC_STATS(thd_id,trans_work_queue_item_total,txn_queue_size+work_queue_size);
}

void QWorkQueue::statqueue(uint64_t thd_id, work_queue_entry * entry) {
	Message *msg = entry->msg;
	if (msg->rtype == RTXN_CONT ||
		msg->rtype == RQRY_RSP || msg->rtype == RACK_PREP  ||
		msg->rtype == RACK_FIN || msg->rtype == RTXN  ||
		msg->rtype == CL_RSP) {
		uint64_t queue_time = get_sys_clock() - entry->starttime;
			INC_STATS(thd_id,trans_work_local_wait,queue_time);
	} else if (msg->rtype == RQRY || msg->rtype == RQRY_CONT ||
				msg->rtype == RFIN || msg->rtype == RPREPARE ||
				msg->rtype == RFWD){
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,trans_work_remote_wait,queue_time);
	}else if (msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,trans_get_client_wait,queue_time);
	}
}

Message * QWorkQueue::dequeue(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();
	assert(ISSERVER || ISREPLICA);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;
	uint64_t mtx_wait_starttime = get_sys_clock();
	bool valid = false;

#ifdef THD_ID_QUEUE
	if (thd_id < THREAD_CNT / 2)
		valid = sub_txn_queue->pop(entry);
	else
		valid = new_txn_queue->pop(entry);
#else
	valid = sub_txn_queue->pop(entry);
	if(!valid) {
#if SERVER_GENERATE_QUERIES
		if(ISSERVER) {
			BaseQuery * m_query = client_query_queue.get_next_query(thd_id,thd_id);
			if(m_query) {
				assert(m_query);
				msg = Message::create_message((BaseQuery*)m_query,CL_QRY);
			}
		}
#else
		valid = new_txn_queue->pop(entry);
#endif
	}
#endif
	INC_STATS(thd_id,mtx[14],get_sys_clock() - mtx_wait_starttime);

	if(valid) {
		msg = entry->msg;
		assert(msg);
		//printf("%ld WQdequeue %ld\n",thd_id,entry->txn_id);
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,work_queue_wait_time,queue_time);
		INC_STATS(thd_id,work_queue_cnt,1);
    	statqueue(thd_id, entry);
		if(msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
			sem_wait(&_semaphore);
			txn_queue_size --;
			txn_dequeue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_new_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_new_cnt,1);
		} else {
			sem_wait(&_semaphore);
			txn_queue_size --;
			txn_dequeue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_old_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_old_cnt,1);
		}
		msg->wq_time = queue_time;
		// DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d,
		// 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
		DEBUG("Work Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
		DEBUG_M("QWorkQueue::dequeue work_queue_entry free\n");
		mem_allocator.free(entry,sizeof(work_queue_entry));
		INC_STATS(thd_id,work_queue_dequeue_time,get_sys_clock() - starttime);
	}

#if SERVER_GENERATE_QUERIES
	if(msg && msg->rtype == CL_QRY) {
		INC_STATS(thd_id,work_queue_new_wait_time,get_sys_clock() - starttime);
		INC_STATS(thd_id,work_queue_new_cnt,1);
	}
#endif
	return msg;
}


//elioyan TODO
Message * QWorkQueue::queuetop(uint64_t thd_id)
{
	uint64_t starttime = get_sys_clock();
	assert(ISSERVER || ISREPLICA);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;
	uint64_t mtx_wait_starttime = get_sys_clock();
	bool valid = sub_txn_queue->pop(entry);
	if(!valid) {
#if SERVER_GENERATE_QUERIES
		if(ISSERVER) {
			BaseQuery * m_query = client_query_queue.get_next_query(thd_id,thd_id);
			if(m_query) {
				assert(m_query);
				msg = Message::create_message((BaseQuery*)m_query,CL_QRY);
			}
		}
#else
		valid = new_txn_queue->pop(entry);
#endif
	}
	INC_STATS(thd_id,mtx[14],get_sys_clock() - mtx_wait_starttime);

	if(valid) {
		msg = entry->msg;
		assert(msg);
		//printf("%ld WQdequeue %ld\n",thd_id,entry->txn_id);
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,work_queue_wait_time,queue_time);
		INC_STATS(thd_id,work_queue_cnt,1);
		if(msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
			sem_wait(&_semaphore);
			txn_queue_size --;
			txn_dequeue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_new_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_new_cnt,1);
		} else {
			sem_wait(&_semaphore);
			work_queue_size --;
			work_dequeue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_old_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_old_cnt,1);
		}
		msg->wq_time = queue_time;
		//DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d, 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
		DEBUG("Work Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
		DEBUG_M("QWorkQueue::dequeue work_queue_entry free\n");
		mem_allocator.free(entry,sizeof(work_queue_entry));
		INC_STATS(thd_id,work_queue_dequeue_time,get_sys_clock() - starttime);
	}

#if SERVER_GENERATE_QUERIES
	if(msg && msg->rtype == CL_QRY) {
		INC_STATS(thd_id,work_queue_new_wait_time,get_sys_clock() - starttime);
		INC_STATS(thd_id,work_queue_new_cnt,1);
	}
#endif
	return msg;
}

#if CC_ALG == ANALYTIC_CALVIN
void QWorkQueue::pending_enqueue(TxnManager * txn_man, uint16_t cnt) {
	while (!validation_queue.pending_txn_queue.push({txn_man, cnt}) && !simulation->is_done()) {
	}
}

TxnManager * QWorkQueue::pending_dequeue(uint64_t thd_id) {
	TxnManager *txn = NULL;
	// if lockfree queue pop failed, element is undefined, so use return value to check
	if (validation_queue.executable_txn_queue.pop(txn)) {
		return txn;
	} else {
		return NULL;
	}
}

void QWorkQueue::pending_validate(uint64_t thd_id) {
	// this implementation may lead to last few txns remain to be unexecuted, but experiments are not affected

	uint64_t handled = 0;
	uint64_t watermark = min_watermark.load();
	auto send_txn = [&](TxnManager* txn) {
		handled++;
		while (!validation_queue.executable_txn_queue.push(txn) && !simulation->is_done()) {
		}
	};
	// check txns remained from last time
	for (auto it = validation_queue.to_be_validate.begin(); it != validation_queue.to_be_validate.end(); ) {
		if (it->txn->get_txn_id() > watermark) {
			it++;
		} else {
			if (it->txn->lock_ready == true && it->txn->enter_pending_cnt == it->cnt) {
				send_txn(it->txn);
			}
			it = validation_queue.to_be_validate.erase(it);
		} 
	}

	// pending_txn_queue check
	VTxn element;
	while (validation_queue.pending_txn_queue.pop(element)) {
		if (element.txn->get_txn_id() > watermark) {
			validation_queue.to_be_validate.push_back(element);
		} else {
			if (element.txn->lock_ready == true && element.txn->enter_pending_cnt == element.cnt) {
				send_txn(element.txn);
			}
		}
	}

	TMP_DEBUG("watermark: %ld executable: %ld\n", watermark, handled);
}

void QWorkQueue::back_to_executable_queue(TxnManager *txn) {
	while (!validation_queue.executable_txn_queue.push(txn) && !simulation->is_done()) {
	}
}

void QWorkQueue::immediate_execute(TxnManager *txn) {
	while (!validation_queue.executable_txn_queue.push(txn) && !simulation->is_done()) {
	}
}
#endif