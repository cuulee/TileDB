/**
 * @file   unit-threadpool.cc
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
 * Tests the `ThreadPool` class.
 */

#include <atomic>
#include <catch.hpp>
#include "tiledb/sm/misc/thread_pool.h"

using namespace tiledb::sm;

TEST_CASE("ThreadPool: Test empty wait", "[threadpool]") {
  ThreadPool pool;
  pool.wait_all();
}

TEST_CASE("ThreadPool: Test single", "[threadpool]") {
  ThreadPool pool;
  int result = 0;
  for (int i = 0; i < 100; i ++) {
    pool.enqueue([&result]() { result++; });
  }
  pool.wait_all();
  CHECK(result == 100);
}

TEST_CASE("ThreadPool: Test multiple", "[threadpool]") {
  ThreadPool pool(4);
  std::atomic<int> result(0);
  for (int i = 0; i < 100; i ++) {
    pool.enqueue([&result]() { result++; });
  }
  pool.wait_all();
  CHECK(result == 100);
}

TEST_CASE("ThreadPool: Test no wait", "[threadpool]") {
  ThreadPool pool(4);
  std::atomic<int> result(0);
  for (int i = 0; i < 100; i ++) {
    pool.enqueue([&result]() { result++; });
  }
}