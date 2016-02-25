// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include <deque>

#include "txn/lock_manager.h"

using std::deque;

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  bool empty = true;
  LockRequest rq(EXCLUSIVE, txn);
  deque<LockRequest> *dq = lock_table_[key];

  if (dq == NULL) {
    dq = new deque<LockRequest>();
    lock_table_[key] = dq;
  } else {
    empty = dq->empty();
  }

  dq->push_back(rq);

  if (!empty) { // Add to wait list, doesn't own lock.
    txn_waits_[txn]++;
  }
  return empty;
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  deque<LockRequest> *queue = lock_table_[key];
  bool removedOwner = true; // Is the lock removed the lock owner?

  // Delete the txn's exclusive lock.
  for (auto it = queue->begin(); it < queue->end(); it++) {
    if (it->txn_ == txn) { // TODO is it ok to just compare by address?
        queue->erase(it);
        break;
    }
    removedOwner = false;
  }

  if (!queue->empty() && removedOwner) {
    // Give the next transaction the lock
    LockRequest next = queue->front();

    int wait_count = --txn_waits_[next.txn_];
    if (wait_count == 0) {
        ready_txns_->push_back(next.txn_);
        txn_waits_.erase(next.txn_);
    }
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  deque<LockRequest> *dq = lock_table_[key];
  if (dq == NULL || dq->empty()) {
    return UNLOCKED;
  } else {
    vector<Txn*> _owners;
    _owners.push_back(dq->front().txn_);
    *owners = _owners;
    return EXCLUSIVE;
  }
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!
  return UNLOCKED;
}

