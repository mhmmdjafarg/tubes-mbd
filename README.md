# Tubes MBD 2021-2022 Kelompok 08 K01

CPSC 438, Assignment 2: Concurrency Control (Locking, OCC and MVCC)
Lead TF: Kun Ren (Questions or clarification requests should be emailed to kun.ren@yale.edu)

Please take advantage of teaching fellow office hours if you have trouble understanding the code that we are providing you or have other questions about the assignment.
Time:     Monday   7:30-9:30 PM
          Thursday 7:30-9:30 PM
Location: AKW 203
 
 
------------------------------------
Understanding Locking, OCC and MVCC
------------------------------------
Before beginning this assignment, please be sure you have a clear understanding of the goals and challenges of concurrency control mechanisms in database systems. The paper you read for class (http://zoo.cs.yale.edu/classes/cs637/franklin97concurrency.pdf) provides a good introduction to this material.

In this assignment you will be implementing five concurrency control schemes:
  * two versions of locking schemes, both of which are considerably simpler than standard two-phase locking
  * a version of OCC very similar to the serial-validation version described in the OCC paper you read for class (http://www.seas.upenn.edu/~zives/cis650/papers/opt-cc.pdf)
  * a version of OCC somewhat similar to the parallel-validation version described in the OCC paper
  * a version of MVCC timestamp ordering scheme based on the paper we read for class on this topic.
 

---------
Framework
---------
You'll be implementing these concurrency control schemes within a transaction processing framework that implements a simple, main-memory resident key-value store. This is a prototype system designed specially for this assignment, and may not be 100% perfect, so please watch for class emails in the coming weeks, as parts of this assignment may change slightly. Please report any bugs or problems to Kun Ren.

To setup our framework, simply download the code(run: git clone https://github.com/kunrenyale/assignment_cc.git). You'll see that it contains two subdirectories---'txn' and 'utils'. Nearly all of the source files you need to worry about are in the 'txn' subdirectory, though you might need to occasionally take a peek at a file or two in the 'util' subdirectory.

To build and test the system, you can run
  make test
at any time. This will first compile the system; if this succeeds with no errors, it will also run two test scripts: one which performs some basic correctness tests of your lock manager implementation, and a second which profiles performance of the system. This second one takes a number of minutes to run, but you can cancel it at any time by pressing ctrl-C.(Note that you can not pass the lock manager test right now until you complete the lock manager implementation.)

Your submissions will be graded on code clarity as well correctness and efficiency. When implementing your solutions, please:
  * Comment your header files & code thoroughly in the manner demonstrated in the rest of the framework.
  * Organize your code logically.
  * Use descriptive variable names.

In this assignment, you will need to make changes to the following files/classes/methods:

  txn/lock_manager.cc:
    all methods (aside for the constructor and deconstructor) for classes 'LockManagerA' (Part 1A) and 'LockManagerB' (Part 1B)

  txn/txn_processor.cc:
    'TxnProcessor::RunOCCScheduler' method (Part 2)
    'TxnProcessor::RunOCCParallelScheduler' method (Part 3)
    'TxnProcessor::RunMVCCScheduler' method (Part 4)
    
  txn/mvcc_storage.cc:
    'MVCCStorage::Read' method (Part 4)
    'MVCCStorage::Write' method (Part 4)
    'MVCCStorage::CheckWrite' method (Part 4)
    
However, to understand what's going on in the framework, you will need to look through most of the files in the txn/ directory. We suggest looking first at the TxnProcessor object (txn/txn_processor.h) and in particular the 'TxnProcessor::RunSerialScheduler()' and 'TxnProcessor::RunLockingScheduler()' methods (txn/txn_processor.cc) and examining how it interacts with various objects in the system.

Note: The framework relies heavily on the C++ standard template library (STL). If you have any questions about how to use the STL (it's really quite easy and friendly, I promise), please consult your search engine of choice.


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

In order to avoid the complexities of creating a thread-safe lock manager in this assignment, our implementation only has a single thread that manages the state of the lock manager. This thread performs all the lock requests on behalf of the transactions and then hands over control to a separate execution thread in step (3) above. Note that for workloads where transactions make heavy use of the lock manager, this single lock manager thread may become a performance bottleneck as it has to request and release locks on behalf of ALL transactions.

To help you get comfortable using the transaction processing framework, most of this algorithm is already implemented in 'TxnProcessor::RunLockingScheduler()'. Locks are requested and released at all the right times, and all necessary data structures for an efficient lock manager are already in place. All you need to do is implement the 'WriteLock', 'Release', and 'Status' methods in the class 'LockManagerA'. Make sure you look at the file lock_manager.h which explains the data structures that you will be using to queue up requests for locks in the lock manager.

The test file 'txn/lock_manager_test.cc' provides some rudimentary correctness tests for your lock manager implementations, but additional tests may be added when we grade the assignment. We therefore suggest that you augment the tests with any additional cases you can think of that the existing tests do not cover.


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
