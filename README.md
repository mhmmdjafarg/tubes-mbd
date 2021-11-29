# Tubes MBD 2021-2022 Kelompok 08 K01
## Cara menjalankan test program
- install make jika pada linux dapat melakukan command `sudo apt-get install -y make`
- `make test`


-----------------------------------------------------------
Part 1A: Simple Locking (exclusive locks only)   10 points
-----------------------------------------------------------
Once you've looked through the code and are somewhat familiar with the overall structure and flow, you'll implement a simplified version of two-phase locking. The protocol goes like this:
1) Upon entering the system, each transaction requests an EXCLUSIVE lock on EVERY item that it will either read or write.
2) If any lock request is denied:
   2a) If the entire transaction involves just a single read or write request, then have the transaction simply wait until the request is granted and then proceed to step (3).
   2b) Otherwise, immediately release all locks that were granted before this denial, and immediately abort and queue the transaction for restart at a later point.
3) (We only get to this point if we didn't get to step (2b) which aborts the transaction.) Execute the program logic.
4) Release ALL locks at commit/abort time.

---------------------------------------------------------------------------
Part 2: Serial Optimistic Concurrency Control (OCC)   10 points
---------------------------------------------------------------------------
For OCC, you will have to implement the 'TxnProcessor::RunOCCScheduler' method.
This is a simplified version of OCC compared to the one presented in the paper.

Pseudocode for the OCC algorithm to implement (in the RunOCCScheduler method):

  while (tp_.Active()) {
    Get the next new transaction request (if one is pending) and pass it to an execution thread.
    Deal with all transactions that have finished running (see below).
  }

  In the execution thread (we are providing you this code):
    Record start time
    Perform "read phase" of transaction:
       Read all relevant data from storage
       Execute the transaction logic (i.e. call Run() on the transaction)

  Dealing with a finished transaction (you must write this code):
    // Validation phase:
    for (each record whose key appears in the txn's read and write sets) {
      if (the record was last updated AFTER this transaction's start time) {
        Validation fails!
      }
    }

    // Commit/restart
    if (validation failed) {
      Cleanup txn
      Completely restart the transaction.
    } else {
      Apply all writes
      Mark transaction as committed
    }

  cleanup txn:
    txn->reads_.empty();
    txn->writes_.empty();
    txn->status_ = INCOMPLETE;
       
  Restart txn:
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock(); 

Part 4: Multiversion Timestamp Ordering Concurrency Control.  20 points
--------------------------------------------------------------------------------
For MVCC, you will have to implement the 'TxnProcessor::RunMVCCScheduler' method
 based on the pseudocode below. The pseudocode implements the MVCC timestamp ordering protocol from the paper that we read for class. 

Although we give you a version of pseudocode, if you want, you can write your own pseudocode and argue why it's better than the code presented here (see analysis question 5). 

In addition you will have to implement the MVCCStorage::Read, MVCCStorage::Write, MVCCStorage::CheckWrite.


Pseudocode for the algorithm to implement (in the RunMVCCScheduler method):

  while (tp_.Active()) {
    Get the next new transaction request (if one is pending) and pass it to an execution thread.
  }

  In the execution thread:
    
    Read all necessary data for this transaction from storage (Note that you should lock the key before each read)
    Execute the transaction logic (i.e. call Run() on the transaction)
    Acquire all locks for keys in the write_set_
    Call MVCCStorage::CheckWrite method to check all keys in the write_set_
    If (each key passed the check)
      Apply the writes
      Release all locks for keys in the write_set_
    else if (at least one key failed the check)
      Release all locks for keys in the write_set_
      Cleanup txn
      Completely restart the transaction.
  
  cleanup txn:
    txn->reads_.empty();
    txn->writes_.empty();
    txn->status_ = INCOMPLETE;

  Restart txn:
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock(); 
