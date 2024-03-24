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
#include "helper.h"
#include "manager.h"
#include "thread.h"
#include "io_thread.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "math.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "message.h"
#include "client_txn.h"
#include "work_queue.h"
#include "txn.h"
#include "ycsb.h"
#include "conflict_stats.h"

void InputThread::setup() {

	std::vector<Message*> * msgs;
	while(!simulation->is_setup_done()) {
		msgs = tport_man.recv_msg(get_thd_id());
		if (msgs == NULL) continue;
		while(!msgs->empty()) {
			Message * msg = msgs->front();
			if(msg->rtype == INIT_DONE) {
				printf("Received INIT_DONE from node %ld\n",msg->return_node_id);
				fflush(stdout);
				simulation->process_setup_msg();
			} else {
				assert(ISSERVER || ISREPLICA);
#if CC_ALG == CALVIN || CC_ALG == ANALYTIC_CALVIN
				if(msg->rtype == CALVIN_ACK ||(msg->rtype == CL_QRY && ISCLIENTN(msg->get_return_id())) ||
					(msg->rtype == CL_QRY_O && ISCLIENTN(msg->get_return_id()))) {
	#if CC_ALG == ANALYTIC_CALVIN && (QUERY_SPLIT || CONTENTION_CHECK)
					txn_handle(msg);
	#endif
					work_queue.sequencer_enqueue(get_thd_id(),msg);
					msgs->erase(msgs->begin());
					continue;
				}
				if( msg->rtype == RDONE || msg->rtype == CL_QRY || msg->rtype == SUB_CL_QRY || msg->rtype == CL_QRY_O) {
					assert(ISSERVERN(msg->get_return_id()));
	#if CC_ALG == ANALYTIC_CALVIN && CONTENTION_CHECK
				if (msg->rtype == RDONE) {
					work_queue.sched_enqueue(get_thd_id(), msg);
					// duplicate RDONE is needed to ensure contended_queue progresses smoothly
					msg = Message::create_message(RDONE);
					work_queue.contended_enqueue(get_thd_id(), msg);
				} else {
					if (static_cast<ClientQueryMessage*>(msg)->is_high_contended) {
						work_queue.contended_enqueue(get_thd_id(), msg);
					} else {
						work_queue.sched_enqueue(get_thd_id(), msg);
					}
				}
	#else
				work_queue.sched_enqueue(get_thd_id(), msg);
	#endif
					msgs->erase(msgs->begin());
					continue;
				}
#endif
				work_queue.enqueue(get_thd_id(),msg,false);
			}
			msgs->erase(msgs->begin());
		}
		delete msgs;
	}
	if (!ISCLIENT) {
		txn_man = (YCSBTxnManager *)
			mem_allocator.align_alloc( sizeof(YCSBTxnManager));
		new(txn_man) YCSBTxnManager();
		// txn_man = (TxnManager*) malloc(sizeof(TxnManager));
		uint64_t thd_id = get_thd_id();
		txn_man->init(thd_id, NULL);
	}
}

RC InputThread::run() {
	tsetup();
	printf("Running InputThread %ld\n",_thd_id);

	if(ISCLIENT) {
		client_recv_loop();
	} else {
		server_recv_loop();
	}

	return FINISH;

}

RC InputThread::client_recv_loop() {
	int rsp_cnts[g_servers_per_client];
	memset(rsp_cnts, 0, g_servers_per_client * sizeof(int));

	run_starttime = get_sys_clock();
	uint64_t return_node_offset;
	uint64_t inf;

	std::vector<Message*> * msgs;

	while (!simulation->is_done()) {
		heartbeat();
		uint64_t starttime = get_sys_clock();
		msgs = tport_man.recv_msg(get_thd_id());
		INC_STATS(_thd_id,mtx[28], get_sys_clock() - starttime);
		starttime = get_sys_clock();
		//while((m_query = work_queue.get_next_query(get_thd_id())) != NULL) {
		//Message * msg = work_queue.dequeue();
		if (msgs == NULL) continue;
		while(!msgs->empty()) {
			Message * msg = msgs->front();
			assert(msg->rtype == CL_RSP);
		#if CC_ALG == BOCC || CC_ALG == FOCC || ONE_NODE_RECIEVE == 1
			return_node_offset = msg->return_node_id;
		#else
			return_node_offset = msg->return_node_id - g_server_start_node;
		#endif
			assert(return_node_offset < g_servers_per_client);
			rsp_cnts[return_node_offset]++;
			INC_STATS(get_thd_id(),txn_cnt,1);
			uint64_t timespan = get_sys_clock() - ((ClientResponseMessage*)msg)->client_startts;
			INC_STATS(get_thd_id(),txn_run_time, timespan);
			if (warmup_done) {
				INC_STATS_ARR(get_thd_id(),client_client_latency, timespan);
			}
			//INC_STATS_ARR(get_thd_id(),all_lat,timespan);
			inf = client_man.dec_inflight(return_node_offset);
			DEBUG("Recv %ld from %ld, %ld -- %f\n", ((ClientResponseMessage *)msg)->txn_id,
						msg->return_node_id, inf, float(timespan) / BILLION);
			assert(inf >=0);
			// delete message here
			msgs->erase(msgs->begin());
		}
		delete msgs;
		INC_STATS(_thd_id,mtx[29], get_sys_clock() - starttime);

	}

	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;
}

RC InputThread::server_recv_loop() {

	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
	assert (rc == RCOK);
	uint64_t starttime;
	uint64_t idle_starttime = 0;

	std::vector<Message*> * msgs;
	while (!simulation->is_done()) {
		heartbeat();
		starttime = get_sys_clock();

		msgs = tport_man.recv_msg(get_thd_id());

		INC_STATS(_thd_id,mtx[28], get_sys_clock() - starttime);
		starttime = get_sys_clock();

		if (msgs == NULL) {
			if (idle_starttime == 0) idle_starttime = get_sys_clock();
			continue;
		}
		if(idle_starttime > 0) {
			INC_STATS(_thd_id, input_idle_time, get_sys_clock() - idle_starttime);
			idle_starttime = 0;
		}
		while(!msgs->empty()) {
			Message * msg = msgs->front();
			if(msg->rtype == INIT_DONE) {
				msgs->erase(msgs->begin());
				continue;
			}
#if CC_ALG == CALVIN || CC_ALG == ANALYTIC_CALVIN
			if(msg->rtype == CALVIN_ACK ||(msg->rtype == CL_QRY && ISCLIENTN(msg->get_return_id())) ||
			(msg->rtype == CL_QRY_O && ISCLIENTN(msg->get_return_id()))) {
	#if CC_ALG == ANALYTIC_CALVIN && (QUERY_SPLIT || CONTENTION_CHECK)
				txn_handle(msg);
	#endif
				work_queue.sequencer_enqueue(get_thd_id(),msg);
				msgs->erase(msgs->begin());
				continue;
			}
			if( msg->rtype == RDONE || msg->rtype == CL_QRY || msg->rtype == SUB_CL_QRY || msg->rtype == CL_QRY_O) {
				assert(ISSERVERN(msg->get_return_id()));
	#if CC_ALG == ANALYTIC_CALVIN && CONTENTION_CHECK
				if (msg->rtype == RDONE) {
					work_queue.sched_enqueue(get_thd_id(), msg);
					// duplicate RDONE is needed to ensure contended_queue progresses smoothly
					msg = Message::create_message(RDONE);
					work_queue.contended_enqueue(get_thd_id(), msg);
				} else {
					if (static_cast<ClientQueryMessage*>(msg)->is_high_contended) {
						work_queue.contended_enqueue(get_thd_id(), msg);
					} else {
						work_queue.sched_enqueue(get_thd_id(), msg);
					}
				}
	#else
				work_queue.sched_enqueue(get_thd_id(), msg);
	#endif
				msgs->erase(msgs->begin());
				continue;
			}
#endif
			work_queue.enqueue(get_thd_id(),msg,false);
			msgs->erase(msgs->begin());
		}
		delete msgs;
		INC_STATS(_thd_id,mtx[29], get_sys_clock() - starttime);

	}
	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;
}

void InputThread::txn_handle(Message *msg) {
	if (msg->rtype != CL_QRY) {
		return;
	}  
	auto msg_for_ease = static_cast<YCSBClientQueryMessage*>(msg);
	if (msg_for_ease->requests.size() == g_short_req_per_query) {
#if CONTENTION_CHECK
		conflict_stats_man.mark_contention(msg);
#endif
	} else {
#if QUERY_SPLIT
		long_txn_split(msg);
#else
		conflict_stats_man.mark_contention(msg);
#endif
	}
}

void InputThread::long_txn_split(Message *msg) {
	auto start_time = get_sys_clock();

	auto msg_for_ease = static_cast<YCSBClientQueryMessage*>(msg);
	
	if (msg_for_ease->is_logical_abortable) {
		msg_for_ease->pSubmsgs = NULL;
		conflict_stats_man.mark_contention(msg);
		return;
	}

	uint64_t Ncontended_req = conflict_stats_man.adjust_long_query(msg);
	if (Ncontended_req < g_short_req_per_query) {
		msg_for_ease->pSubmsgs = NULL;
		msg_for_ease->is_high_contended = false;
		INC_STATS(0, Nuncontended, 1);
	} else {
		uint8_t Nsubmsg = 0;	// number of sub msgs
		msg_for_ease->pSubmsgs = new std::vector<Message*>();	// vector used for recording sub msgs
		auto pNum = new uint8_t;

		size_t start = 0;
		while (start < g_long_req_per_query) {
			Nsubmsg++;
			Message *submsg;
			if (Ncontended_req >= g_short_req_per_query) {
				submsg = Message::create_submessage(msg, start, start + g_short_req_per_query, pNum);
				static_cast<ClientQueryMessage*>(submsg)->is_high_contended = true;
				INC_STATS(0, Ncontended, 1);
				start += g_short_req_per_query;
				Ncontended_req -= g_short_req_per_query;
			} else {
				submsg = Message::create_submessage(msg, start, g_long_req_per_query, pNum);
				static_cast<ClientQueryMessage*>(submsg)->is_high_contended = false;
				INC_STATS(0, Nuncontended, 1);
				start = g_long_req_per_query;
			}

			msg_for_ease->pSubmsgs->push_back(submsg);
		}
		*pNum = Nsubmsg;
	}

#if EVEN_SPLIT
	uint8_t Nsubmsg = g_long_req_per_query / g_short_req_per_query;	// number of sub msgs
	msg_for_ease->pSubmsgs = new std::vector<Message*>();	// vector used for recording sub msgs
	auto pNum = new uint8_t{Nsubmsg};

	size_t start = 0;
	while (start < g_long_req_per_query) {
		Message *submsg = Message::create_submessage(msg, start, start + g_short_req_per_query, pNum);
#if CONTENTION_CHECK
		conflict_stats_man.mark_contention(submsg);
#endif
		msg_for_ease->pSubmsgs->push_back(submsg);
		start += g_short_req_per_query;
	}
#endif
	INC_STATS(_thd_id, split_time, get_sys_clock() - start_time);
}

void OutputThread::setup() {
	DEBUG_M("OutputThread::setup MessageThread alloc\n");
	messager = (MessageThread *) mem_allocator.alloc(sizeof(MessageThread));
	messager->init(_thd_id);
	while (!simulation->is_setup_done()) {
		messager->run();
	}
}

RC OutputThread::run() {

	tsetup();
	printf("Running OutputThread %ld\n",_thd_id);

	while (!simulation->is_done()) {
		heartbeat();
		messager->run();
	}

	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;
}


