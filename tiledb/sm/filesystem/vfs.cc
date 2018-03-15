/**
 * @file   vfs.cc
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
 * This file implements the VFS class.
 */

#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/filesystem/hdfs_filesystem.h"
#include "tiledb/sm/filesystem/posix_filesystem.h"
#include "tiledb/sm/filesystem/win_filesystem.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/storage_manager/config.h"

#include <iostream>

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

VFS::VFS() {
  STATS_FUNC_VOID_IN(vfs_constructor);

#ifdef HAVE_HDFS
  supported_fs_.insert(Filesystem::HDFS);
#endif
#ifdef HAVE_S3
  supported_fs_.insert(Filesystem::S3);
#endif

  STATS_FUNC_VOID_OUT(vfs_constructor);
}

VFS::~VFS() {
  STATS_FUNC_VOID_IN(vfs_destructor);

#ifdef HAVE_HDFS
  if (hdfs_ != nullptr) {
    // Do not disconnect - may lead to problems
    // Status st = hdfs::disconnect(hdfs_);
  }
#endif
  // Do not disconnect - may lead to problems
  // Status st = s3_.disconnect();

  STATS_FUNC_VOID_OUT(vfs_destructor);
}

/* ********************************* */
/*                API                */
/* ********************************* */

std::string VFS::abs_path(const std::string& path) {
  STATS_FUNC_IN(vfs_abs_path);

#ifdef _WIN32
  if (win::is_win_path(path))
    return win::uri_from_path(win::abs_path(path));
  else if (URI::is_file(path))
    return win::uri_from_path(win::abs_path(win::path_from_uri(path)));
#else
  if (URI::is_file(path))
    return posix::abs_path(path);
#endif
  if (URI::is_hdfs(path))
    return path;
  if (URI::is_s3(path))
    return path;
  // Certainly starts with "<resource>://" other than "file://"
  return path;

  STATS_FUNC_OUT(vfs_abs_path);
}

Status VFS::create_dir(const URI& uri) const {
  STATS_FUNC_IN(vfs_create_dir);

  if (is_dir(uri))
    return LOG_STATUS(Status::VFSError(
        std::string("Cannot create directory '") + uri.c_str() +
        "'; Directory already exists"));

  if (uri.is_file()) {
#ifdef _WIN32
    return win::create_dir(uri.to_path());
#else
    return posix::create_dir(uri.to_path());
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::create_dir(hdfs_, uri);
#else
    return Status::VFSError("TileDB was built without HDFS support");
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.create_dir(uri);
#else
    return Status::VFSError("TileDB was built without S3 support");
#endif
  }
  return Status::Error(
      std::string("Unsupported URI scheme: ") + uri.to_string());

  STATS_FUNC_OUT(vfs_create_dir);
}

Status VFS::create_file(const URI& uri) const {
  STATS_FUNC_IN(vfs_create_file);

  // Do nothing if the file already exists
  if (is_file(uri))
    return Status::Ok();

  if (uri.is_file()) {
#ifdef _WIN32
    return win::create_file(uri.to_path());
#else
    return posix::create_file(uri.to_path());
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::create_file(hdfs_, uri);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.create_file(uri);
#else
    return Status::VFSError("TileDB was built without S3 support");
#endif
  }
  return LOG_STATUS(Status::VFSError(
      std::string("Unsupported URI scheme: ") + uri.to_string()));

  STATS_FUNC_OUT(vfs_create_file);
}

Status VFS::create_bucket(const URI& uri) const {
  STATS_FUNC_IN(vfs_create_bucket);

  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.create_bucket(uri);
#else
    (void)uri;
    return LOG_STATUS(Status::VFSError(std::string("S3 is not supported")));
#endif
  }
  return LOG_STATUS(Status::VFSError(
      std::string("Cannot create bucket; Unsupported URI scheme: ") +
      uri.to_string()));

  STATS_FUNC_OUT(vfs_create_bucket);
}

Status VFS::remove_bucket(const URI& uri) const {
  STATS_FUNC_IN(vfs_remove_bucket);

  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.delete_bucket(uri);
#else
    (void)uri;
    return LOG_STATUS(Status::VFSError(std::string("S3 is not supported")));
#endif
  }
  return LOG_STATUS(Status::VFSError(
      std::string("Cannot remove bucket; Unsupported URI scheme: ") +
      uri.to_string()));

  STATS_FUNC_OUT(vfs_remove_bucket);
}

Status VFS::empty_bucket(const URI& uri) const {
  STATS_FUNC_IN(vfs_empty_bucket);

  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.empty_bucket(uri);
#else
    (void)uri;
    return LOG_STATUS(Status::VFSError(std::string("S3 is not supported")));
#endif
  }
  return LOG_STATUS(Status::VFSError(
      std::string("Cannot remove bucket; Unsupported URI scheme: ") +
      uri.to_string()));

  STATS_FUNC_OUT(vfs_empty_bucket);
}

Status VFS::is_empty_bucket(const URI& uri, bool* is_empty) const {
  STATS_FUNC_IN(vfs_is_empty_bucket);

  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.is_empty_bucket(uri, is_empty);
#else
    (void)uri;
    (void)is_empty;
    return LOG_STATUS(Status::VFSError(std::string("S3 is not supported")));
#endif
  }
  return LOG_STATUS(Status::VFSError(
      std::string("Cannot remove bucket; Unsupported URI scheme: ") +
      uri.to_string()));

  STATS_FUNC_OUT(vfs_is_empty_bucket);
}

Status VFS::remove_path(const URI& uri) const {
  STATS_FUNC_IN(vfs_remove_path);

  if (uri.is_file()) {
#ifdef _WIN32
    return win::remove_path(uri.to_path());
#else
    return posix::remove_path(uri.to_path());
#endif
  } else if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::remove_path(hdfs_, uri);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  } else if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.remove_path(uri);
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  } else {
    return LOG_STATUS(
        Status::VFSError("Unsupported URI scheme: " + uri.to_string()));
  }

  STATS_FUNC_OUT(vfs_remove_path);
}

Status VFS::remove_file(const URI& uri) const {
  STATS_FUNC_IN(vfs_remove_file);

  if (uri.is_file()) {
#ifdef _WIN32
    return win::remove_file(uri.to_path());
#else
    return posix::remove_file(uri.to_path());
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::remove_file(hdfs_, uri);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.remove_file(uri);
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI scheme: " + uri.to_string()));

  STATS_FUNC_OUT(vfs_remove_file);
}

Status VFS::filelock_lock(const URI& uri, filelock_t* fd, bool shared) const {
  STATS_FUNC_IN(vfs_filelock_lock);

  if (uri.is_file())
#ifdef _WIN32
    return win::filelock_lock(uri.to_path(), fd, shared);
#else
    return posix::filelock_lock(uri.to_path(), fd, shared);
#endif

  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return Status::Ok();
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return Status::Ok();
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI scheme: " + uri.to_string()));

  STATS_FUNC_OUT(vfs_filelock_lock);
}

Status VFS::filelock_unlock(const URI& uri, filelock_t fd) const {
  STATS_FUNC_IN(vfs_filelock_unlock);

  if (uri.is_file()) {
#ifdef _WIN32
    return win::filelock_unlock(fd);
#else
    return posix::filelock_unlock(fd);
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return Status::Ok();
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return Status::Ok();
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI scheme: " + uri.to_string()));

  STATS_FUNC_OUT(vfs_filelock_unlock);
}

Status VFS::file_size(const URI& uri, uint64_t* size) const {
  STATS_FUNC_IN(vfs_file_size);

  if (uri.is_file()) {
#ifdef _WIN32
    return win::file_size(uri.to_path(), size);
#else
    return posix::file_size(uri.to_path(), size);
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::file_size(hdfs_, uri, size);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.file_size(uri, size);
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI scheme: " + uri.to_string()));

  STATS_FUNC_OUT(vfs_file_size);
}

bool VFS::is_dir(const URI& uri) const {
  STATS_FUNC_IN(vfs_is_dir);

  if (uri.is_file()) {
#ifdef _WIN32
    return win::is_dir(uri.to_path());
#else
    return posix::is_dir(uri.to_path());
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::is_dir(hdfs_, uri);
#else
    return false;
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.is_dir(uri);
#else
    return false;
#endif
  }
  return false;

  STATS_FUNC_OUT(vfs_is_dir);
}

bool VFS::is_file(const URI& uri) const {
  STATS_FUNC_IN(vfs_is_file);

  if (uri.is_file()) {
#ifdef _WIN32
    return win::is_file(uri.to_path());
#else
    return posix::is_file(uri.to_path());
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::is_file(hdfs_, uri);
#else
    return false;
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.is_file(uri);
#else
    return false;
#endif
  }
  return false;

  STATS_FUNC_OUT(vfs_is_file);
}

bool VFS::is_bucket(const URI& uri) const {
  STATS_FUNC_IN(vfs_is_bucket);

  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.is_bucket(uri);
#else
    (void)uri;
    return false;
#endif
  }
  return false;

  STATS_FUNC_OUT(vfs_is_bucket);
}

Status VFS::init(const Config::VFSParams& vfs_params) {
  STATS_FUNC_IN(vfs_init);

#ifdef HAVE_HDFS
  RETURN_NOT_OK(hdfs::connect(hdfs_, vfs_params.hdfs_params_));
#endif
#ifdef HAVE_S3
  S3::S3Config s3_config;
  s3_config.region_ = vfs_params.s3_params_.region_;
  s3_config.scheme_ = vfs_params.s3_params_.scheme_;
  s3_config.endpoint_override_ = vfs_params.s3_params_.endpoint_override_;
  s3_config.use_virtual_addressing_ =
      vfs_params.s3_params_.use_virtual_addressing_;
  s3_config.file_buffer_size_ = vfs_params.s3_params_.file_buffer_size_;
  s3_config.connect_timeout_ms_ = vfs_params.s3_params_.connect_timeout_ms_;
  s3_config.request_timeout_ms_ = vfs_params.s3_params_.request_timeout_ms_;
  RETURN_NOT_OK(s3_.connect(s3_config));
#endif

  thread_pool_ = std::unique_ptr<ThreadPool>(
      new (std::nothrow) ThreadPool(vfs_params.num_parallel_operations_));
  if (thread_pool_.get() == nullptr) {
    return LOG_STATUS(Status::VFSError("Could not create VFS thread pool"));
  }

  parallel_read_threshold_ = vfs_params.parallel_read_threshold_;

  return Status::Ok();

  STATS_FUNC_OUT(vfs_init);
}

Status VFS::ls(const URI& parent, std::vector<URI>* uris) const {
  STATS_FUNC_IN(vfs_ls);

  std::vector<std::string> paths;
  if (parent.is_file()) {
#ifdef _WIN32
    RETURN_NOT_OK(win::ls(parent.to_path(), &paths));
#else
    RETURN_NOT_OK(posix::ls(parent.to_path(), &paths));
#endif
  } else if (parent.is_hdfs()) {
#ifdef HAVE_HDFS
    RETURN_NOT_OK(hdfs::ls(hdfs_, parent, &paths));
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  } else if (parent.is_s3()) {
#ifdef HAVE_S3
    RETURN_NOT_OK(s3_.ls(parent, &paths));
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  } else {
    return LOG_STATUS(
        Status::VFSError("Unsupported URI scheme: " + parent.to_string()));
  }
  std::sort(paths.begin(), paths.end());
  for (auto& path : paths) {
    uris->emplace_back(path);
  }
  return Status::Ok();

  STATS_FUNC_OUT(vfs_ls);
}

Status VFS::move_path(const URI& old_uri, const URI& new_uri, bool force) {
  STATS_FUNC_IN(vfs_move_path);

  // If new_uri exists, delete it
  if (force && (is_dir(new_uri) || is_file(new_uri)))
    RETURN_NOT_OK(remove_path(new_uri));

  // File
  if (old_uri.is_file()) {
    if (new_uri.is_file()) {
#ifdef _WIN32
      return win::move_path(old_uri.to_path(), new_uri.to_path());
#else
      return posix::move_path(old_uri.to_path(), new_uri.to_path());
#endif
    }
    return LOG_STATUS(Status::VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // HDFS
  if (old_uri.is_hdfs()) {
    if (new_uri.is_hdfs())
#ifdef HAVE_HDFS
      return hdfs::move_path(hdfs_, old_uri, new_uri);
#else
      return LOG_STATUS(
          Status::VFSError("TileDB was built without HDFS support"));
#endif
    return LOG_STATUS(Status::VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // S3
  if (old_uri.is_s3()) {
    if (new_uri.is_s3())
#ifdef HAVE_S3
      return s3_.move_path(old_uri, new_uri);
#else
      return LOG_STATUS(
          Status::VFSError("TileDB was built without S3 support"));
#endif
    return LOG_STATUS(Status::VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // Unsupported filesystem
  return LOG_STATUS(Status::VFSError(
      "Unsupported URI schemes: " + old_uri.to_string() + ", " +
      new_uri.to_string()));

  STATS_FUNC_OUT(vfs_move_path);
}

Status VFS::read(
    const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) const {
  STATS_FUNC_IN(vfs_read);
  STATS_COUNTER_ADD(vfs_read_total_bytes, nbytes);

  if (!is_file(uri))
    return LOG_STATUS(
        Status::VFSError("Cannot read from file; File does not exist"));

  uint64_t num_threads =
      nbytes >= parallel_read_threshold_ ? thread_pool_->num_threads() : 1;

  if (num_threads == 1) {
    return read_impl(uri, offset, buffer, nbytes);
  } else {
    std::vector<std::future<Status>> results;
    uint64_t thread_read_nbytes = utils::ceil(nbytes, num_threads);

    for (uint64_t i = 0; i < num_threads; i++) {
      uint64_t begin = i * thread_read_nbytes,
               end = std::min((i + 1) * thread_read_nbytes - 1, nbytes - 1);
      uint64_t thread_nbytes = end - begin + 1;
      uint64_t thread_offset = offset + begin;
      auto thread_buffer = reinterpret_cast<char*>(buffer) + begin;
      results.push_back(thread_pool_->enqueue(
          [this, &uri, thread_offset, thread_buffer, thread_nbytes]() {
            return read_impl(uri, thread_offset, thread_buffer, thread_nbytes);
          }));
    }

    bool all_ok = thread_pool_->wait_all(results);
    return all_ok ? Status::Ok() :
                    LOG_STATUS(Status::VFSError("VFS parallel read error"));
  }

  STATS_FUNC_OUT(vfs_read);
}

Status VFS::read_impl(
    const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) const {
  if (uri.is_file()) {
#ifdef _WIN32
    return win::read(uri.to_path(), offset, buffer, nbytes);
#else
    return posix::read(uri.to_path(), offset, buffer, nbytes);
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::read(hdfs_, uri, offset, buffer, nbytes);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.read(uri, offset, buffer, nbytes);
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI schemes: " + uri.to_string()));
}

bool VFS::supports_fs(Filesystem fs) const {
  STATS_FUNC_IN(vfs_supports_fs);

  return (supported_fs_.find(fs) != supported_fs_.end());

  STATS_FUNC_OUT(vfs_supports_fs);
}

Status VFS::sync(const URI& uri) {
  STATS_FUNC_IN(vfs_sync);

  if (uri.is_file()) {
#ifdef _WIN32
    return win::sync(uri.to_path());
#else
    return posix::sync(uri.to_path());
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::sync(hdfs_, uri);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return Status::Ok();
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI schemes: " + uri.to_string()));

  STATS_FUNC_OUT(vfs_sync);
}

Status VFS::open_file(const URI& uri, VFSMode mode) {
  STATS_FUNC_IN(vfs_open_file);

  switch (mode) {
    case VFSMode::VFS_READ:
      if (!is_file(uri))
        return LOG_STATUS(Status::VFSError(
            std::string("Cannot open file '") + uri.c_str() +
            "'; File does not exist"));
      break;
    case VFSMode::VFS_WRITE:
      if (is_file(uri))
        RETURN_NOT_OK(remove_file(uri));
      break;
    case VFSMode::VFS_APPEND:
      if (uri.is_s3()) {
#ifdef HAVE_S3
        return LOG_STATUS(Status::VFSError(
            std::string("Cannot open file '") + uri.c_str() +
            "'; S3 does not support append mode"));
#else
        return LOG_STATUS(Status::VFSError(
            "Cannot open file; TileDB was built without S3 support"));
#endif
      }
      break;
  }

  return Status::Ok();

  STATS_FUNC_OUT(vfs_open_file);
}

Status VFS::close_file(const URI& uri) {
  STATS_FUNC_IN(vfs_close_file);

  if (uri.is_file()) {
#ifdef _WIN32
    return win::sync(uri.to_path());
#else
    return posix::sync(uri.to_path());
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::sync(hdfs_, uri);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.flush_file(uri);
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI schemes: " + uri.to_string()));

  STATS_FUNC_OUT(vfs_close_file);
}

Status VFS::write(const URI& uri, const void* buffer, uint64_t buffer_size) {
  STATS_FUNC_IN(vfs_write);
  STATS_COUNTER_ADD(vfs_write_total_bytes, buffer_size);

  if (uri.is_file()) {
#ifdef _WIN32
    return win::write(uri.to_path(), buffer, buffer_size);
#else
    return posix::write(uri.to_path(), buffer, buffer_size);
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::write(hdfs_, uri, buffer, buffer_size);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.write(uri, buffer, buffer_size);
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI schemes: " + uri.to_string()));

  STATS_FUNC_OUT(vfs_write);
}

}  // namespace sm
}  // namespace tiledb
