/**
 * @file   tiledb_dense_read_subset_incomplete.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This example shows how to read from a dense array, constraining the read
 * to a specific subarray and a subset of attributes. Moreover, the
 * program shows how to handle queries that did not complete
 * because the input buffers were not big enough to hold the entire
 * result.
 *
 * You need to run the following to make it work:
 *
 * ```
 * $ ./tiledb_dense_create_c
 * $ ./tiledb_dense_write_global_1_c
 * $ ./tiledb_dense_read_subset_incomplete_c
 * a1
 * ---
 * Reading cells...
 * 9
 * 11
 * Reading cells...
 * 12
 * 14
 * Reading cells...
 * 13
 * 15
 * ```
 *
 * The subarray query is shown in figure
 * `<TileDB-repo>/examples/figures/dense_subarray.png`.
 *
 *  The program prints the cell values of `a1` in the subarray in column-major
 *  order. Observe that the loop is executed 3 times, retrieving two cells at
 *  a time (since our buffer had space only for 2 cells).
 */

#include <stdio.h>
#include <tiledb/tiledb.h>

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx, NULL);

  // Prepare cell buffers. Notice that this time we prepare a buffer only for
  // `a1` (as we will not be querying the rest of the attributes) and we assign
  // space that **will not** be able to hold the entire result.
  int buffer_a1[2];
  void* buffers[] = {buffer_a1};
  uint64_t buffer_sizes[] = {sizeof(buffer_a1)};

  // Create the query, which focuses on subarray `[3,4], [2,4]` and attribute
  // `a1`. Also notice that we set the layout to `TILEDB_COL_MAJOR`, which will
  // retrieve the cells in column-major order within the selected subarray.
  tiledb_query_t* query;
  const char* attributes[] = {"a1"};
  uint64_t subarray[] = {3, 4, 2, 4};
  tiledb_query_create(ctx, &query, "my_dense_array", TILEDB_READ);
  tiledb_query_set_layout(ctx, query, TILEDB_COL_MAJOR);
  tiledb_query_set_subarray(ctx, query, subarray);
  tiledb_query_set_buffers(ctx, query, attributes, 1, buffers, buffer_sizes);

  // Loop until the query is completed. The buffer we created the query with
  // cannot hold the entire result. Instead of crashing, query submission will
  // try to fill as many result cells in the buffer as it can and then
  // gracefully terminate. TileDB allows the user to check the query status for
  // `a1` via API functions. While the status is "incomplete", the code
  // continues the loop to retrieve the next results. Notice that we are
  // submitting the **same** query; the query is **stateful** and will resume
  // from where it stopped. Eventually the status becomes "completed" and the
  // loop exits.
  printf("a1\n---\n");
  tiledb_query_status_t status;
  do {
    printf("Reading cells...\n");
    tiledb_query_submit(ctx, query);

    // Print cell values
    uint64_t result_num = buffer_sizes[0] / sizeof(int);
    for (uint64_t i = 0; i < result_num; ++i)
      printf("%d\n", buffer_a1[i]);

    // Get status
    tiledb_query_get_attribute_status(ctx, query, "a1", &status);
  } while (status == TILEDB_INCOMPLETE);

  // Clean up
  tiledb_query_free(ctx, &query);
  tiledb_ctx_free(&ctx);

  return 0;
}
