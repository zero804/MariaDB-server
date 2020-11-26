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
#include "srw_lock.h"
#include "my_atomic_wrapper.h"
#include "os0thread.h"

#if 0 // FIXME: defined UNIV_PFS_RWLOCK
# define SUX_LOCK_INIT(key, level) init(key, level)
#else
# define SUX_LOCK_INIT(key, level) init(level)
#endif

/** A "fat" rw-lock that supports
S (shared), U (update, or shared-exclusive), and X (exclusive) modes
as well as recursive U and X latch acquisition */
class sux_lock ut_d(: public latch_t)
{
  /** The first lock component for U and X modes. Only acquired in X mode. */
  srw_lock_low write_lock;
  /** The owner of the U or X lock (0 if none); protected by write_lock */
  std::atomic<os_thread_id_t> writer;
  /** Special writer!=0 value to indicate that the lock is non-recursive
  and will be released by an I/O thread */
  static constexpr os_thread_id_t FOR_IO= IF_WIN(INVALID_HANDLE_VALUE, -1);
  /** Number of recursive U or X locks. Protected by write_lock.
  In debug builds, this is incremented also for the first lock request. */
  uint32_t recursive;
  /** The second component for U and X modes; the only component for S mode */
  srw_lock_low read_lock;
#ifdef UNIV_DEBUG
  /** debug_list lock. Only acquired in X mode. */
  srw_lock_low debug_lock;
#endif

  /** The multiplier in recursive for X locks */
  static constexpr uint32_t RECURSIVE_X= 1U;
  /** The multiplier in recursive for U locks */
  static constexpr uint32_t RECURSIVE_U= 1U << 16;
  /** The maximum allowed level of recursion */
  static constexpr uint32_t RECURSIVE_MAX= RECURSIVE_U - 1;

public:
  void SUX_LOCK_INIT(mysql_pfs_key_t key= PFS_NOT_INSTRUMENTED,
                     latch_level_t level= SYNC_LEVEL_VARYING)
  {
    write_lock.init();
    writer.store(0, std::memory_order_relaxed);
    recursive= 0;
    read_lock.init();
    ut_d(debug_lock.init());
    ut_d(m_rw_lock= true);
    //ut_d(UT_LIST_INIT(debug_list, &rw_lock_debug_t::list));
    ut_d(m_id= sync_latch_get_id(sync_latch_get_name(level)));
    ut_d(this->level= level);
    ut_ad(level != SYNC_UNKNOWN);
  }

  /** Free the rw-lock after create() */
  void free()
  {
    ut_ad(created());
    ut_ad(!writer);
    ut_ad(!recursive);
    write_lock.destroy();
    read_lock.destroy();
    ut_d(debug_lock.destroy());
    ut_d(level= SYNC_UNKNOWN);
  }

  /** needed for dict_index_t::clone() */
  void operator=(const sux_lock&) {}

#ifdef UNIV_DEBUG
  /** @return whether no recursive locks are being held */
  bool not_recursive() const
  {
    ut_ad(recursive);
    return recursive == RECURSIVE_X || recursive == RECURSIVE_U;
  }
#endif

  /** Acquire a recursive lock */
  template<bool allow_readers> void writer_recurse()
  {
    ut_ad(writer == os_thread_get_curr_id());
    ut_d(auto rec= (recursive / (allow_readers ? RECURSIVE_U : RECURSIVE_X)) &
         RECURSIVE_MAX);
    ut_ad(allow_readers ? recursive : rec);
    ut_ad(rec < RECURSIVE_MAX);
    recursive+= allow_readers ? RECURSIVE_U : RECURSIVE_X;
  }

private:
  /** Acquire the writer lock component (for U or X lock)
  @param for_io  whether the lock will be released by another thread
  @return whether this was a recursive acquisition */
  template<bool allow_readers> bool writer_lock(bool for_io= false)
  {
    os_thread_id_t id= os_thread_get_curr_id();
    if (writer.load(std::memory_order_relaxed) == id)
    {
      ut_ad(!for_io);
      writer_recurse<allow_readers>();
      return true;
    }
    else
    {
      write_lock.wr_lock();
      ut_ad(!recursive);
      ut_d(recursive= allow_readers ? RECURSIVE_U : RECURSIVE_X);
      set_first_owner(for_io ? FOR_IO : id);
      return false;
    }
  }
  /** Release the writer lock component (for U or X lock)
  @param allow_readers    whether we are releasing a U lock
  @param claim_ownership  whether the lock was acquired by another thread
  @return whether this was a recursive release */
  bool writer_unlock(bool allow_readers, bool claim_ownership= false)
  {
    ut_d(auto owner= writer.load(std::memory_order_relaxed));
    ut_ad(owner == os_thread_get_curr_id() ||
          (owner == FOR_IO && claim_ownership &&
           recursive == (allow_readers ? RECURSIVE_U : RECURSIVE_X)));
    ut_d(auto rec= (recursive / (allow_readers ? RECURSIVE_U : RECURSIVE_X)) &
         RECURSIVE_MAX);
    ut_ad(rec >= 1);
    ut_d(recursive-= allow_readers ? RECURSIVE_U : RECURSIVE_X);

    if (UNIV_UNLIKELY(recursive))
    {
      ut_d(return true);
      recursive-= allow_readers ? RECURSIVE_U : RECURSIVE_X;
      return true;
    }
    set_new_owner(0);
    write_lock.wr_unlock();
    return false;
  }
  /** Transfer the ownership of a write lock to another thread
  @param id the new owner of the U or X lock */
  void set_new_owner(os_thread_id_t id)
  {
    IF_DBUG(DBUG_ASSERT(writer.exchange(id, std::memory_order_relaxed)),
            writer.store(id, std::memory_order_relaxed));
  }
  /** Assign the ownership of a write lock to a thread
  @param id the owner of the U or X lock */
  void set_first_owner(os_thread_id_t id)
  {
    IF_DBUG(DBUG_ASSERT(writer.exchange(id, std::memory_order_acquire)),
            writer.store(id, std::memory_order_acquire));
  }
public:
  /** In crash recovery or the change buffer, claim the ownership
  of the exclusive block lock to the current thread */
  void claim_ownership() { set_new_owner(os_thread_get_curr_id()); }

  /** @return whether the current thread is holding X or U latch */
  bool have_u_or_x() const
  {
    if (os_thread_get_curr_id() != writer.load(std::memory_order_relaxed))
      return false;
    ut_ad(recursive);
    return true;
  }
  /** @return whether the current thread is holding U but not X latch */
  bool have_u_not_x() const
  { return have_u_or_x() && !((recursive / RECURSIVE_X) & RECURSIVE_MAX); }
  /** @return whether the current thread is holding X latch */
  bool have_x() const
  { return have_u_or_x() && ((recursive / RECURSIVE_X) & RECURSIVE_MAX); }
#ifdef UNIV_DEBUG
  /** @return whether the current thread is holding the latch */
  bool have_any() const { return have_u_or_x() /* FIXME */; }
#endif

  /** Acquire a shared lock */
  void s_lock() { ut_ad(!have_x()); read_lock.rd_lock(); }
  /** Acquire an update lock */
  void u_lock() { if (!writer_lock<true>()) read_lock.rd_lock(); }
  /** Acquire an exclusive lock
  @param for_io  whether the lock will be released by another thread */
  void x_lock(bool for_io= false)
  { if (!writer_lock<false>(for_io)) read_lock.wr_lock(); }
  /** Acquire a recursive exclusive lock */
  void x_lock_recursive() { writer_recurse<false>(); }
  /** Acquire a shared lock */
  void s_lock(const char *, unsigned) { s_lock(); }
  /** Acquire an update lock */
  void u_lock(const char *, unsigned) { u_lock(); }
  /** Acquire an exclusive lock */
  void x_lock(const char *, unsigned, bool for_io= false) { x_lock(for_io); }
  /** Acquire an exclusive lock or upgrade an update lock
  @return whether U locks were upgraded to X */
  bool x_lock_upgraded()
  {
    os_thread_id_t id= os_thread_get_curr_id();
    if (writer.load(std::memory_order_relaxed) == id)
    {
      ut_ad(recursive);
      static_assert(RECURSIVE_X == 1, "compatibility");
      if (recursive & RECURSIVE_MAX)
      {
        writer_recurse<false>();
        return false;
      }
      /* Upgrade the lock. */
      read_lock.rd_unlock();
      read_lock.wr_lock();
      recursive/= RECURSIVE_U;
      return true;
    }
    else
    {
      write_lock.wr_lock();
      ut_ad(!recursive);
      ut_d(recursive= RECURSIVE_X);
      set_first_owner(id);
      read_lock.wr_lock();
      return false;
    }
  }
  /** Acquire an exclusive lock or upgrade an update lock
  @return whether U locks were upgraded to X */
  bool x_lock_upgraded(const char *, unsigned) { return x_lock_upgraded(); }

  /** @return whether a shared lock was acquired */
  bool s_lock_try() { return read_lock.rd_lock_try(); }
  /** Try to acquire an update or exclusive lock
  @tparam allow_readers  true=update, false=exclusive
  @param for_io  whether the lock will be released by another thread
  @return whether the lock was acquired */
  template<bool allow_readers> bool u_or_x_lock_try(const char *file_name,
                                                    unsigned line,
                                                    bool for_io= false)
  {
    os_thread_id_t id= os_thread_get_curr_id();
    if (writer.load(std::memory_order_relaxed) == id)
    {
      if (for_io)
        return false;
      writer_recurse<allow_readers>();
      return true;
    }

    if (write_lock.wr_lock_try())
    {
      ut_ad(!recursive);
      if (allow_readers ? read_lock.rd_lock_try() : read_lock.wr_lock_try())
      {
        ut_ad(!recursive);
        ut_d(recursive= allow_readers ? RECURSIVE_U : RECURSIVE_X);
        set_first_owner(for_io ? FOR_IO : id);
        return true;
      }
      write_lock.wr_unlock();
    }
    return false;
  }
  /** Try to acquire an update lock
  @param for_io  whether the lock will be released by another thread
  @return whether the update lock was acquired */
  bool u_lock_try(bool for_io= false)
  { return u_or_x_lock_try<true>(nullptr, 0, for_io); }
  /** Try to acquire an exclusive lock
  @param for_io  whether the lock will be released by another thread
  @return whether an exclusive lock was acquired */
  bool x_lock_try(const char *file_name, unsigned line, bool for_io= false)
  { return u_or_x_lock_try<false>(file_name, line); }

  /** Release a shared lock */
  void s_unlock() { read_lock.rd_unlock(); }
  /** Release an update lock */
  void u_unlock(bool claim_ownership= false)
  { if (!writer_unlock(true, claim_ownership)) read_lock.rd_unlock(); }
  /** Release an exclusive lock */
  void x_unlock(bool claim_ownership= false)
  { if (!writer_unlock(false, claim_ownership)) read_lock.wr_unlock(); }
  /** Release an update or exclusive lock */
  void u_or_x_unlock(bool allow_readers= false)
  {
    if (writer_unlock(allow_readers));
    else if (allow_readers)
      read_lock.rd_unlock();
    else
      read_lock.wr_unlock();
  }

  /** Count of os_waits. May not be accurate */
  static constexpr uint32_t count_os_wait= 0; /* FIXME: move to dict_index_t */

#ifdef UNIV_DEBUG
  std::string to_string() const override;
  /** In the debug version: pointer to the debug info list of the lock */
  // FIXME: UT_LIST_BASE_NODE_T(rw_lock_debug_t) debug_list;

  /** Level in the global latching order. */
  latch_level_t level;
public:
  bool created() const { return level != SYNC_UNKNOWN; }
#endif /* UNIV_DEBUG */
};
