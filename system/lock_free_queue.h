#ifndef _LF_QUEUE_H_
#define _LF_QUEUE_H_

#define COMPILER_BARRIER asm volatile("" ::: "memory");
#define PAUSE usleep(1);

struct QueueEntry {
  uintptr_t value; // the value stored in the entry. i.e., the pointer.
  QueueEntry * next;
};

class LockfreeQueue {
public:
  LockfreeQueue();
  bool enqueue(uintptr_t value);
  bool dequeue(uintptr_t &value);
private:
  QueueEntry * _head;
  QueueEntry * _tail;
};

#endif
