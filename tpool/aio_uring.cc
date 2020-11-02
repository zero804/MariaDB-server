#include <liburing.h>

#include <atomic>
#include <thread>

#include "tpool.h"

namespace tpool
{
class aio_uring final : public aio
{
public:
  aio_uring(int max_aio, thread_pool *tpool) : tpool_(tpool)
  {
    if (io_uring_queue_init(max_aio, &uring_, 0) != 0)
      throw "catch me if you can";
    thread_= std::thread{thread_routine, this};
  }

  ~aio_uring()
  {
    in_shutdown_= true;
    thread_.join();
    io_uring_queue_exit(&uring_);
  }

  int submit_io(aiocb *cb) override
  {
    io_uring_sqe *sqe= io_uring_get_sqe(&uring_);
    if (cb->m_opcode == aio_opcode::AIO_PREAD)
      io_uring_prep_read(sqe, cb->m_fh, cb->m_buffer, cb->m_len, cb->m_offset);
    else
      io_uring_prep_write(sqe, cb->m_fh, cb->m_buffer, cb->m_len,
                          cb->m_offset);
    io_uring_sqe_set_data(sqe, cb);
    (void) io_uring_submit(&uring_);
    return 0;
  }

  int bind(native_file_handle &fd) override { return 0; }
  int unbind(const native_file_handle &fd) override { return 0; }

private:
  static void thread_routine(aio_uring *aio)
  {
    for (;;)
    {
      io_uring_cqe *cqe;
      int ret= io_uring_peek_cqe(&aio->uring_, &cqe);

      if (aio->in_shutdown_.load(std::memory_order_relaxed))
        break;

      if (ret)
      {
        if (ret == -EAGAIN)
        {
          std::this_thread::sleep_for(std::chrono::microseconds(50));
          continue;
        }

        fprintf(stderr, "io_uring_wait_ceq_timeout returned %d\n", ret);
        abort();
      }

      aiocb *iocb= (aiocb *) io_uring_cqe_get_data(cqe);
      long long res= cqe->res;
      if (res < 0)
      {
        iocb->m_err= static_cast<int>(-res);
        iocb->m_ret_len= 0;
      }
      else
      {
        iocb->m_ret_len= ret;
        iocb->m_err= 0;
      }

      io_uring_cqe_seen(&aio->uring_, cqe);

      iocb->m_internal_task.m_func= iocb->m_callback;
      iocb->m_internal_task.m_arg= iocb;
      iocb->m_internal_task.m_group= iocb->m_group;
      aio->tpool_->submit_task(&iocb->m_internal_task);
    }
  }

  io_uring uring_;
  thread_pool *tpool_;
  std::atomic<bool> in_shutdown_{false};
  std::thread thread_;
};

aio *create_uring_aio(thread_pool *pool, int max_aio)
{
  return new aio_uring(max_aio, pool);
}

} // namespace tpool
