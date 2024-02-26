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

#ifndef _WORK_QUEUE_H_
#define _WORK_QUEUE_H_

#include "global.h"
#include "helper.h"
#include <queue>
#include <boost/lockfree/queue.hpp>
#include <boost/circular_buffer.hpp>
#include "semaphore.h"

class BaseQuery;
class Workload;
class Message;

struct work_queue_entry {
  Message * msg;
  uint64_t batch_id;
  uint64_t txn_id;
  RemReqType rtype;
  uint64_t starttime;
};

struct CompareSchedEntry {
  bool operator()(const work_queue_entry* lhs, const work_queue_entry* rhs) {
    if (lhs->batch_id == rhs->batch_id) return lhs->starttime > rhs->starttime;
    return lhs->batch_id < rhs->batch_id;
  }
};
struct CompareWQEntry {
#if PRIORITY == PRIORITY_FCFS
  bool operator()(const work_queue_entry* lhs, const work_queue_entry* rhs) {
    return lhs->starttime < rhs->starttime;
  }
#elif PRIORITY == PRIORITY_ACTIVE
  bool operator()(const work_queue_entry* lhs, const work_queue_entry* rhs) {
    if ((lhs->rtype == CL_QRY || lhs->rtype == CL_QRY_O) && (rhs->rtype != CL_QRY && rhs->rtype != CL_QRY_O)) return true;
    if ((rhs->rtype == CL_QRY || rhs->rtype == CL_QRY_O) && (lhs->rtype != CL_QRY && lhs->rtype != CL_QRY_O)) return false;
    return lhs->starttime < rhs->starttime;
  }
#elif PRIORITY == PRIORITY_HOME
  bool operator()(const work_queue_entry* lhs, const work_queue_entry* rhs) {
    if (ISLOCAL(lhs->txn_id) && !ISLOCAL(rhs->txn_id)) return true;
    if (ISLOCAL(rhs->txn_id) && !ISLOCAL(lhs->txn_id)) return false;
    return lhs->starttime < rhs->starttime;
  }
#endif
};
typedef boost::circular_buffer<work_queue_entry*> WCircularBuffer;
class QWorkQueue {
public:
  void init();
  void statqueue(uint64_t thd_id, work_queue_entry * entry);
  void enqueue(uint64_t thd_id,Message * msg,bool busy);
  Message * dequeue(uint64_t thd_id);
  Message * queuetop(uint64_t thd_id);
  void sched_enqueue(uint64_t thd_id, Message * msg);
  Message * sched_dequeue(uint64_t thd_id);
  void sequencer_enqueue(uint64_t thd_id, Message * msg);
  Message * sequencer_dequeue(uint64_t thd_id);
#if CC_ALG == ANALYTIC_CALVIN
  void pending_enqueue(TxnManager * txn_man, uint16_t cnt);
  TxnManager * pending_dequeue(uint64_t thd_id);
  void pending_validate(uint64_t thd_id);
  void back_to_executable_queue(TxnManager * txn);
  void immediate_execute(TxnManager * txn);
#endif

  uint64_t get_cnt() {return get_wq_cnt() + get_rem_wq_cnt() + get_new_wq_cnt();}
  uint64_t get_wq_cnt() {return 0;}
  //uint64_t get_wq_cnt() {return work_queue.size();}
  uint64_t get_txn_cnt() {return txn_queue_size;}

  uint64_t get_enwq_cnt() {return work_enqueue_size;}
  uint64_t get_dewq_cnt() {return work_dequeue_size;}
  uint64_t get_entxn_cnt() {return txn_enqueue_size;}
  uint64_t get_detxn_cnt() {return txn_dequeue_size;}

  void set_enwq_cnt() { work_enqueue_size = 0;}
  void set_dewq_cnt() { work_dequeue_size = 0;}
  void set_entxn_cnt() { txn_enqueue_size = 0;}
  void set_detxn_cnt() { txn_dequeue_size = 0;}
  uint64_t get_sched_wq_cnt() {return 0;}
  uint64_t get_rem_wq_cnt() {return 0;}
  uint64_t get_new_wq_cnt() {return 0;}
  Message* top_element;

private:
  boost::lockfree::queue<work_queue_entry* > * sub_txn_queue; // txns apart from CL_QRY, e.g. RTXN, RFWD
  boost::lockfree::queue<work_queue_entry* > * new_txn_queue; // CL_QRY
  boost::lockfree::queue<work_queue_entry* > * seq_queue;
  boost::lockfree::queue<work_queue_entry* > ** sched_queue;
  
#if CC_ALG == ANALYTIC_CALVIN
  struct VTxn {
    TxnManager* txn;
    uint16_t cnt;
  };
  struct VQueue {
    boost::lockfree::queue<VTxn> pending_txn_queue{0};
    boost::lockfree::queue<TxnManager*> executable_txn_queue{0};
    std::vector<VTxn> to_be_validate;
  };
  VQueue validation_queue;
#endif

  uint64_t sched_ptr;
  bool sched_ready;
  BaseQuery * last_sched_dq;
  uint64_t curr_epoch;

  sem_t 	_semaphore;
  uint64_t work_queue_size;
  uint64_t txn_queue_size;

  uint64_t work_enqueue_size;
  uint64_t work_dequeue_size;
  uint64_t txn_enqueue_size;
  uint64_t txn_dequeue_size;


};


#endif
