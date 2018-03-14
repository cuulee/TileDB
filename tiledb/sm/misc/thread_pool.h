/**
 * @file   thread_pool.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file declares the ThreadPool class.
 */

#ifndef TILEDB_THREAD_POOL_H
#define TILEDB_THREAD_POOL_H

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace tiledb {
namespace sm {

class ThreadPool {
 public:
  explicit ThreadPool(uint64_t num_threads = 1);

  ~ThreadPool();

  void enqueue(const std::function<void()> &function) {
    {
      std::unique_lock<std::mutex> lck(queue_mutex_);
      task_queue_.push(function);
      queue_cv_.notify_one();
    }
  }

  void wait_all();

 private:
  std::mutex queue_mutex_;

  std::condition_variable queue_cv_;

  bool should_terminate_;

  std::queue<std::function<void()>> task_queue_;

  std::vector<std::thread> threads_;

  static void worker(ThreadPool& pool);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_THREAD_POOL_H