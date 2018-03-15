/**
 * @file   vfs.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file declares the VFS class.
 */

#ifndef TILEDB_VFS_H
#define TILEDB_VFS_H

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/filesystem.h"
#include "tiledb/sm/enums/vfs_mode.h"
#include "tiledb/sm/filesystem/filelock.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/misc/thread_pool.h"
#include "tiledb/sm/misc/uri.h"
#include "tiledb/sm/storage_manager/config.h"

#ifdef HAVE_HDFS
#include "hdfs.h"
#endif

#ifdef HAVE_S3
#include "tiledb/sm/filesystem/s3.h"
#endif

#include <set>
#include <string>
#include <vector>

namespace tiledb {
namespace sm {

/**
 * This class implements a virtual filesystem that directs filesystem-related
 * function execution to the appropriate backend based on the input URI.
 */
class VFS {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  VFS();

  /** Destructor. */
  ~VFS();

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Returns the absolute path of the input string (mainly useful for
   * posix URI's).
   *
   * @param path The input path.
   * @return The string with the absolute path.
   */
  static std::string abs_path(const std::string& path);

  /**
   * Creates a directory.
   *
   * @param uri The URI of the directory.
   * @return Status
   */
  Status create_dir(const URI& uri) const;

  /**
   * Creates an empty file.
   *
   * @param uri The URI of the file.
   * @return Status
   */
  Status create_file(const URI& uri) const;

  /**
   * Creates an object-store bucket.
   *
   * @param uri The name of the bucket to be created.
   * @return Status
   */
  Status create_bucket(const URI& uri) const;

  /**
   * Deletes an object-store bucket.
   *
   * @param uri The name of the bucket to be deleted.
   * @return Status
   */
  Status remove_bucket(const URI& uri) const;

  /**
   * Deletes the contents of an object-store bucket.
   *
   * @param uri The name of the bucket to be emptied.
   * @return Status
   */
  Status empty_bucket(const URI& uri) const;

  /**
   * Removes a given path (recursive)
   *
   * @param uri The uri of the path to be removed
   * @return Status
   */
  Status remove_path(const URI& uri) const;

  /**
   * Deletes a file.
   *
   * @param uri The URI of the file.
   * @return Status
   */
  Status remove_file(const URI& uri) const;

  /**
   * Locks a filelock.
   *
   * @param uri The URI of the filelock.
   * @param lock A handle for the filelock (used in unlocking the
   *     filelock).
   * @param shared *True* if it is a shared lock, *false* if it is an
   *     exclusive lock.
   * @return Status
   */
  Status filelock_lock(const URI& uri, filelock_t* lock, bool shared) const;

  /**
   * Unlocks a filelock.
   *
   * @param uri The URI of the filelock.
   * @param lock The handle of the filelock.
   * @return Status
   */
  Status filelock_unlock(const URI& uri, filelock_t fd) const;

  /**
   * Retrieves the size of a file.
   *
   * @param uri The URI of the file.
   * @param size The file size to be retrieved.
   * @return Status
   */
  Status file_size(const URI& uri, uint64_t* size) const;

  /**
   * Checks if a directory exists.
   *
   * @param uri The URI of the directory.
   * @return `True` if the directory exists and `false` otherwise.
   */
  bool is_dir(const URI& uri) const;

  /**
   * Checks if a file exists.
   *
   * @param uri The URI of the file.
   * @return `True` if the file exists and `false` otherwise.
   */
  bool is_file(const URI& uri) const;

  /**
   * Checks if an object-store bucket exists.
   *
   * @param uri The name of the S3 bucket.
   * @return `True` if the bucket exists and `false` otherwise.
   */
  bool is_bucket(const URI& uri) const;

  /**
   * Checks if an object-store bucket is empty.
   *
   * @param uri The name of the S3 bucket.
   * @param is_empty Set to `true` if the bucket is empty and `false` otherwise.
   */
  Status is_empty_bucket(const URI& uri, bool* is_empty) const;

  /** Initializes the virtual filesystem. */
  Status init(const Config::VFSParams& vfs_params);

  /**
   * Retrieves all the URIs that have the first input as parent.
   *
   * @param parent The target directory to list.
   * @param uris The URIs that are contained in the parent.
   * @return Status
   */
  Status ls(const URI& parent, std::vector<URI>* uris) const;

  /**
   * Renames a path.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   * @param force Force the rename even if `new_uri` exists. In the latter
   *     case `new_uri` will be overwritten.
   * @return Status
   */
  Status move_path(const URI& old_uri, const URI& new_uri, bool force);

  /**
   * Reads from a file.
   *
   * @param uri The URI of the file.
   * @param offset The offset where the read begins.
   * @param buffer The buffer to read into.
   * @param nbytes Number of bytes to read.
   * @return Status
   */
  Status read(
      const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) const;

  /** Checks if a given filesystem is supported. */
  bool supports_fs(Filesystem fs) const;

  /**
   * Syncs (flushes) a file. Note that for S3 this is a noop.
   *
   * @param uri The URI of the file.
   * @return Status
   */
  Status sync(const URI& uri);

  /**
   * Opens a file in a given mode.
   *
   *
   * @param uri The URI of the file.
   * @param mode The mode in which the file is opened:
   *     - READ <br>
   *       The file is opened for reading. An error is returned if the file
   *       does not exist.
   *     - WRITE <br>
   *       The file is opened for writing. If the file exists, it will be
   *       overwritten.
   *     - APPEND <b>
   *       The file is opened for writing. If the file exists, the write
   *       will start from the end of the file. Note that S3 does not
   *       support this operation and, thus, an error will be thrown in
   *       that case.
   * @return Status
   */
  Status open_file(const URI& uri, VFSMode mode);

  /**
   * Closes a file, flushing its contents to persistent storage.
   *
   * @param uri The URI of the file.
   * @return Status
   */
  Status close_file(const URI& uri);

  /**
   * Writes the contents of a buffer into a file.
   *
   * @param uri The URI of the file.
   * @param buffer The buffer to write from.
   * @param buffer_size The buffer size.
   * @return Status
   */
  Status write(const URI& uri, const void* buffer, uint64_t buffer_size);

 private:
/* ********************************* */
/*         PRIVATE ATTRIBUTES        */
/* ********************************* */
#ifdef HAVE_HDFS
  hdfsFS hdfs_;
#endif

#ifdef HAVE_S3
  S3 s3_;
#endif

  /** The set with the supported filesystems. */
  std::set<Filesystem> supported_fs_;

  /** Thread pool for parallel I/O operations. */
  std::unique_ptr<ThreadPool> thread_pool_;

  /** Threshold (in bytes) for read operations to be parallelized. */
  uint64_t parallel_read_threshold_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_VFS_H
