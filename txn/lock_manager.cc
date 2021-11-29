// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"

LockManagerA::LockManagerA(deque<Txn *> *ready_txns)
{
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn *txn, const Key &key)
{
  // CPSC 438/538:
  //
  // Implement this method!
  LockRequest rq(EXCLUSIVE, txn);

  // Cari key pada locktable, if it doesnt exist add newly lock request for that key(using find in unordered map)
  if (this->lock_table_.find(key) == this->lock_table_.end())
  {
    deque<LockRequest> *newLr = new deque<LockRequest>();
    newLr->push_back(rq);
    this->lock_table_[key] = newLr;
    std::cout << "Grant exlusive lock " << key << "\n";
    return true;
  }
  else
  {
    // push ke lock request
    this->lock_table_[key]->push_back(rq);

    // Deque LockRequest masih kosong maka grant lock
    if (this->lock_table_[key]->size() == 1){
      std::cout << "Grant exlusive lock " << key << "\n";
      return true;
    }


    // Cek apakah txn ini sudah ada pada txn_waits, jika belum ada inisiasi = 1, jika belum tambahkan
    if (this->txn_waits_.find(txn) == this->txn_waits_.end())
    {
      this->txn_waits_[txn] = 1;
    }
    else
    {
      this->txn_waits_[txn]++;
    }
    // Reject lock, txn waits
    std::cout << "Reject exlusive lock " << key << "\n";
    return false;
  }
}

bool LockManagerA::ReadLock(Txn *txn, const Key &key)
{
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn *txn, const Key &key)
{
  // CPSC 438/538:
  //
  // Implement this method!

  // Cek apakah owner akan di release
  std::cout << "Release lock for key " << key << "\n";
  if (this->lock_table_[key]->front().txn_ == txn)
  {
    this->lock_table_[key]->pop_front();
    // Jika ada transaksi lain dibelakangnya maka grant lock pada txn tersebut
    if (this->lock_table_[key]->size() != 0)
    {
      std::cout << "Granting exclusive lock to next waiting transaction for key " << key << "\n";
      this->ready_txns_->push_back(this->lock_table_[key]->front().txn_);
    }
    return;
  }

  // Delete the txn's exclusive lock.
  for (auto it = this->lock_table_[key]->begin(); it < this->lock_table_[key]->end(); it++)
  {
    if (it->txn_ == txn)
    {
      this->lock_table_[key]->erase(it);
      break;
    }
  }
}

LockMode LockManagerA::Status(const Key &key, vector<Txn *> *owners)
{
  // CPSC 438/538:
  //
  // Implement this method!
  owners->clear();

  // Cek apakah ada item pada deque LockRequest
  if (this->lock_table_[key]->size() > 0)
  {
    owners->push_back(this->lock_table_[key]->front().txn_);
  }

  if (owners->empty())
  {
    std::cout << "Status for key " << key << " is UNLOCKED\n";
    return UNLOCKED;
  }
  else
  {
    std::cout << "Status for key " << key << " is EXCLUSIVE LOCK\n";
    return EXCLUSIVE;
  }
}

LockManagerB::LockManagerB(deque<Txn *> *ready_txns)
{
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn *txn, const Key &key)
{
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

bool LockManagerB::ReadLock(Txn *txn, const Key &key)
{
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

void LockManagerB::Release(Txn *txn, const Key &key)
{
  // CPSC 438/538:
  //
  // Implement this method!
}

LockMode LockManagerB::Status(const Key &key, vector<Txn *> *owners)
{
  // CPSC 438/538:
  //
  // Implement this method!
  return UNLOCKED;
}
