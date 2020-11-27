/*****************************************************************************

Copyright (c) 2020, MariaDB Corporation.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1335 USA

*****************************************************************************/

#pragma once
#include "univ.i"

#if 0 // defined SAFE_MUTEX
# define SRW_LOCK_DUMMY /* Use mysql_rwlock_t for debugging purposes */
#endif

#if defined SRW_LOCK_DUMMY || (!defined _WIN32 && !defined __linux__)
/** An exclusive-only variant of srw_lock */
class srw_mutex
{
  pthread_mutex_t lock;
public:
  void init() { pthread_mutex_init(&lock, nullptr); }
  void destroy() { pthread_mutex_destroy(&lock); }
  void wr_lock() { pthread_mutex_lock(&lock); }
  void wr_unlock() { pthread_mutex_unlock(&lock); }
  bool wr_lock_try() { return !pthread_mutex_trylock(&lock); }
};
#else
# define srw_mutex srw_lock_low
# ifdef _WIN32
#  include <windows.h>
# else
#  include "rw_lock.h"
# endif
#endif

#ifdef UNIV_PFS_RWLOCK
# define SRW_LOCK_INIT(key) init(key)
#else
# define SRW_LOCK_INIT(key) init()
#endif

/** Slim reader-writer lock with no recursion */
class srw_lock_low final
#if defined __linux__ && !defined SRW_LOCK_DUMMY
  : protected rw_lock
#endif
{
#if defined SRW_LOCK_DUMMY || (!defined _WIN32 && !defined __linux__)
  rw_lock_t lock;
public:
  void init() { my_rwlock_init(&lock, nullptr); }
  void destroy() { rwlock_destroy(&lock); }
  void rd_lock() { rw_rdlock(&lock); }
  void rd_unlock() { rw_unlock(&lock); }
  void wr_lock() { rw_wrlock(&lock); }
  void wr_unlock() { rw_unlock(&lock); }
  bool rd_lock_try() { return !rw_tryrdlock(&lock); }
  bool wr_lock_try() { return !rw_trywrlock(&lock); }
#else
# ifdef _WIN32
  SRWLOCK lock;
  bool read_trylock() { return TryAcquireSRWLockShared(&lock); }
  bool write_trylock() { return TryAcquireSRWLockExclusive(&lock); }
  void read_lock() { AcquireSRWLockShared(&lock); }
  void write_lock() { AcquireSRWLockExclusive(&lock); }
  bool available() const
  {
    SRWLOCK inited= SRWLOCK_INIT;
    return !memcmp(&lock, &inited, sizeof lock);
  }
# else
  /** @return pointer to the lock word */
  rw_lock *word() { return static_cast<rw_lock*>(this); }
  /** Wait for a read lock.
  @param l lock word from a failed read_trylock() */
  void read_lock(uint32_t l);
  /** Wait for a write lock after a failed write_trylock() */
  void write_lock();
  bool available() const
  {
    static_assert(4 == sizeof(rw_lock), "ABI");
    return !is_locked_or_waiting();
  }
# endif

public:
  void init()
  {
    DBUG_ASSERT(available());
  }
  void destroy() { DBUG_ASSERT(available()); }
  bool rd_lock_try()
  { IF_WIN(,uint32_t l); return read_trylock(IF_WIN(, l)); }
  bool wr_lock_try() { return write_trylock(); }
  void rd_lock()
  {
    IF_WIN(read_lock(), uint32_t l; if (!read_trylock(l)) read_lock(l));
  }
  void wr_lock()
  {
    IF_WIN(, if (!write_trylock())) write_lock();
  }
#ifdef _WIN32
  void rd_unlock() { ReleaseSRWLockShared(&lock); }
  void wr_unlock() { ReleaseSRWLockExclusive(&lock); }
#else
  void rd_unlock();
  void wr_unlock();
#endif
#endif
};

#ifndef UNIV_PFS_RWLOCK
typedef srw_lock_low srw_lock;
#else
/** Slim reader-writer lock with optional PERFORMANCE_SCHEMA instrumentation */
class srw_lock
{
  srw_lock_low lock;
  PSI_rwlock *pfs_psi;

public:
  /** needed for dict_index_t::clone() */
  void operator=(const srw_lock &) {}

  void init(mysql_pfs_key_t key)
  {
    lock.init();
    pfs_psi= PSI_RWLOCK_CALL(init_rwlock)(key, this);
  }
  void destroy()
  {
    if (pfs_psi)
    {
      PSI_RWLOCK_CALL(destroy_rwlock)(pfs_psi);
      pfs_psi= nullptr;
    }
    lock.destroy();
  }
  void rd_lock()
  {
    if (pfs_psi)
    {
      if (lock.rd_lock_try())
        return;
      PSI_rwlock_locker_state state;
      PSI_rwlock_locker *locker= PSI_RWLOCK_CALL(start_rwlock_rdwait)
        (&state, pfs_psi, PSI_RWLOCK_READLOCK, __FILE__, __LINE__);
      lock.rd_lock();
      if (locker)
        PSI_RWLOCK_CALL(end_rwlock_rdwait)(locker, 0);
      return;
    }
    lock.rd_lock();
  }
  void rd_unlock()
  {
    if (pfs_psi)
      PSI_RWLOCK_CALL(unlock_rwlock)(pfs_psi);
    lock.rd_unlock();
  }
  void wr_lock()
  {
    if (pfs_psi)
    {
      if (lock.wr_lock_try())
        return;
      PSI_rwlock_locker_state state;
      PSI_rwlock_locker *locker= PSI_RWLOCK_CALL(start_rwlock_wrwait)
        (&state, pfs_psi, PSI_RWLOCK_WRITELOCK, __FILE__, __LINE__);
      lock.wr_lock();
      if (locker)
        PSI_RWLOCK_CALL(end_rwlock_rdwait)(locker, 0);
      return;
    }
    lock.wr_lock();
  }
  void wr_unlock()
  {
    if (pfs_psi)
      PSI_RWLOCK_CALL(unlock_rwlock)(pfs_psi);
    lock.wr_unlock();
  }
  bool rd_lock_try() { return lock.rd_lock_try(); }
  bool wr_lock_try() { return lock.wr_lock_try(); }
};
#endif
