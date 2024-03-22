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

#ifndef _CALVINTHREAD_H_
#define _CALVINTHREAD_H_

#include "global.h"
#include "message.h"

class Workload;
enum FetchType{EMPTY_MSG, RDONE_MSG, QRY_MSG};

/*
class CalvinThread : public Thread {
public:
	RC 			run();
  void setup();
  uint64_t txn_starttime;
private:
	TxnManager * m_txn;
};
*/

class CalvinLockThread : public Thread {
public:
    RC run();
    void setup();
#if CC_ALG == ANALYTIC_CALVIN
    FetchType fetch_msg_to_lock(Message *&msg);
    bool double_check(Message *&msg);
    void init(uint64_t thd_id, uint64_t node_id, Workload * workload, uint64_t id);
#endif
private:
    TxnManager * m_txn;
#if CC_ALG == ANALYTIC_CALVIN
    uint64_t id;
    uint64_t failed_cnt;
    static constexpr uint64_t failed_mod{FETCH_FAIL_MOD};
#endif
};

class CalvinSequencerThread : public Thread {
public:
    RC run();
    void setup();
private:
    bool is_batch_ready();
	uint64_t last_batchtime;
};

#endif
