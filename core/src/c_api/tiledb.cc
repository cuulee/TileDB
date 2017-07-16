/**
 * @file   tiledb.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file defines the C API of TileDB.
 */

#include "aio_request.h"
#include "array_schema.h"
#include "array_type.h"
#include "attribute.h"
#include "basic_array.h"
#include "configurator.h"
#include "datatype.h"
#include "dimension.h"
#include "metadata_schema.h"
#include "storage_manager.h"
#include "utils.h"

/* ****************************** */
/*              MACROS            */
/* ****************************** */

#define SANITY_CHECK_CTX(ctx)                                           \
  do {                                                                  \
    if (ctx == nullptr)                                                 \
      return TILEDB_ERR;                                                \
    if (ctx->storage_manager_ == nullptr) {                             \
      save_error(ctx, tiledb::Status::Error("Invalid TileDB context")); \
      return TILEDB_ERR;                                                \
    }                                                                   \
  } while (0)

#define SANITY_CHECK_CONFIG(ctx, config)                                     \
  do {                                                                       \
    if (config == nullptr || config->config_ == nullptr) {                   \
      save_error(                                                            \
          ctx, tiledb::Status::Error("Invalid TileDB configurator struct")); \
      return TILEDB_ERR;                                                     \
    }                                                                        \
  } while (0)

#define SANITY_CHECK_ERROR(ctx, err)                                         \
  do {                                                                       \
    if (err == nullptr || err->status_ == nullptr) {                         \
      save_error(ctx, tiledb::Status::Error("Invalid TileDB error struct")); \
      return TILEDB_ERR;                                                     \
    }                                                                        \
  } while (0)

#define SANITY_CHECK_ATTRIBUTE(ctx, attr)                                 \
  do {                                                                    \
    if (attr == nullptr || attr->attr_ == nullptr) {                      \
      save_error(                                                         \
          ctx, tiledb::Status::Error("Invalid TileDB attribute struct")); \
      return TILEDB_ERR;                                                  \
    }                                                                     \
  } while (0)

#define SANITY_CHECK_ATTRIBUTE_ITER(ctx, attr_it)                             \
  do {                                                                        \
    if (attr_it == nullptr || (attr_it->array_schema_ == nullptr &&           \
                               attr_it->metadata_schema_ == nullptr)) {       \
      save_error(                                                             \
          ctx,                                                                \
          tiledb::Status::Error("Invalid TileDB attribute iterator struct")); \
      return TILEDB_ERR;                                                      \
    }                                                                         \
  } while (0)

#define SANITY_CHECK_DIMENSION_ITER(ctx, dim_it)                              \
  do {                                                                        \
    if (dim_it == nullptr || dim_it->array_schema_ == nullptr) {              \
      save_error(                                                             \
          ctx,                                                                \
          tiledb::Status::Error("Invalid TileDB dimension iterator struct")); \
      return TILEDB_ERR;                                                      \
    }                                                                         \
  } while (0)

#define SANITY_CHECK_DIMENSION(ctx, dim)                                  \
  do {                                                                    \
    if (dim == nullptr || dim->dim_ == nullptr) {                         \
      save_error(                                                         \
          ctx, tiledb::Status::Error("Invalid TileDB dimension struct")); \
      return TILEDB_ERR;                                                  \
    }                                                                     \
  } while (0)

#define SANITY_CHECK_ARRAY_SCHEMA(ctx, array_schema)                         \
  do {                                                                       \
    if (array_schema == nullptr || array_schema->array_schema_ == nullptr) { \
      save_error(                                                            \
          ctx, tiledb::Status::Error("Invalid TileDB array_schema struct")); \
      return TILEDB_ERR;                                                     \
    }                                                                        \
  } while (0)

#define SANITY_CHECK_METADATA_SCHEMA(ctx, metadata_schema)                 \
  do {                                                                     \
    if (metadata_schema == nullptr ||                                      \
        metadata_schema->metadata_schema_ == nullptr) {                    \
      save_error(                                                          \
          ctx,                                                             \
          tiledb::Status::Error("Invalid TileDB metadata_schema struct")); \
      return TILEDB_ERR;                                                   \
    }                                                                      \
  } while (0)

/* ****************************** */
/*            CONSTANTS           */
/* ****************************** */

const char* tiledb_coords() {
  return tiledb::Configurator::coords();
}

const char* tiledb_key() {
  return tiledb::Configurator::key();
}

int tiledb_var_num() {
  return tiledb::Configurator::var_num();
}

uint64_t tiledb_var_size() {
  return tiledb::Configurator::var_size();
}

/* ****************************** */
/*            VERSION             */
/* ****************************** */

void tiledb_version(int* major, int* minor, int* rev) {
  *major = TILEDB_VERSION_MAJOR;
  *minor = TILEDB_VERSION_MINOR;
  *rev = TILEDB_VERSION_REVISION;
}

/* ********************************* */
/*           TILEDB TYPES            */
/* ********************************* */

struct tiledb_ctx_t {
  // storage manager instance
  tiledb::StorageManager* storage_manager_;
  // last error associated with this context
  tiledb::Status* last_error_;
};

struct tiledb_config_t {
  // The configurator instance
  tiledb::Configurator* config_;
};

struct tiledb_error_t {
  // Pointer to a copy of the last TileDB error associated with a given ctx
  const tiledb::Status* status_;
  std::string* errmsg_;
};

struct tiledb_basic_array_t {
  tiledb::BasicArray* basic_array_;
};

struct tiledb_attribute_t {
  tiledb::Attribute* attr_;
};

struct tiledb_dimension_t {
  tiledb::Dimension* dim_;
};

struct tiledb_array_schema_t {
  tiledb::ArraySchema* array_schema_;
};

struct tiledb_attribute_iter_t {
  const tiledb_array_schema_t* array_schema_;
  const tiledb_metadata_schema_t* metadata_schema_;
  tiledb_attribute_t* attr_;
  int attr_num_;
  int current_attr_;
};

struct tiledb_dimension_iter_t {
  const tiledb_array_schema_t* array_schema_;
  tiledb_dimension_t* dim_;
  int dim_num_;
  int current_dim_;
};

struct tiledb_array_t {
  tiledb::Array* array_;
  tiledb_ctx_t* ctx_;
};

struct tiledb_metadata_schema_t {
  tiledb::MetadataSchema* metadata_schema_;
};

struct tiledb_metadata_t {
  tiledb::Metadata* metadata_;
  tiledb_ctx_t* ctx_;
};

/* ****************************** */
/*            CONTEXT             */
/* ****************************** */

/* Auxiliary function that saves a status inside the context object. */
static bool save_error(tiledb_ctx_t* ctx, const tiledb::Status& st) {
  // No error
  if (st.ok())
    return false;

  // Delete previous error
  if (ctx->last_error_ != nullptr)
    delete ctx->last_error_;

  // Store new error and return
  ctx->last_error_ = new tiledb::Status(st);

  // There is an error
  return true;
}

int tiledb_ctx_create(tiledb_ctx_t** ctx) {
  // Initialize context
  *ctx = (tiledb_ctx_t*)malloc(sizeof(struct tiledb_ctx_t));
  if (*ctx == nullptr)
    return TILEDB_OOM;

  // Create storage manager
  (*ctx)->storage_manager_ = new tiledb::StorageManager();
  if ((*ctx)->storage_manager_ == nullptr) {
    save_error(
        *ctx,
        tiledb::Status::Error(
            "Failed to allocate storage manager in TileDB context"));
    return TILEDB_ERR;
  }

  // Initialize last error
  (*ctx)->last_error_ = nullptr;

  // Initialize storage manager
  if (save_error(*ctx, ((*ctx)->storage_manager_->init(nullptr)))) {
    delete (*ctx)->storage_manager_;
    (*ctx)->storage_manager_ = nullptr;
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_ctx_free(tiledb_ctx_t* ctx) {
  if (ctx == nullptr)
    return;

  if (ctx->storage_manager_ != nullptr)
    delete ctx->storage_manager_;

  if (ctx->last_error_ != nullptr)
    delete ctx->last_error_;

  free(ctx);
}

/* ********************************* */
/*              CONFIG               */
/* ********************************* */

int tiledb_config_create(tiledb_ctx_t* ctx, tiledb_config_t** config) {
  SANITY_CHECK_CTX(ctx);

  // Create struct
  *config = (tiledb_config_t*)malloc(sizeof(tiledb_config_t));
  if (*config == nullptr) {
    save_error(
        ctx, tiledb::Status::Error("Failed to allocate configurator struct"));
    return TILEDB_OOM;
  }

  // Create configurator
  (*config)->config_ = new tiledb::Configurator();
  if ((*config)->config_ == nullptr) {  // Allocation error
    free(*config);
    *config = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate configurator object in struct"));
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

int tiledb_ctx_set_config(tiledb_ctx_t* ctx, tiledb_config_t* config) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_CONFIG(ctx, config);

  ctx->storage_manager_->set_config(config->config_);

  return TILEDB_OK;
}

void tiledb_config_free(tiledb_config_t* config) {
  // Trivial case
  if (config == nullptr)
    return;

  // Free configurator
  if (config->config_ != nullptr)
    delete config->config_;

  // Free struct
  free(config);
}

#ifdef HAVE_MPI
int tiledb_config_set_mpi_comm(
    tiledb_ctx_t* ctx, tiledb_config_t* config, MPI_Comm* mpi_comm) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_CONFIG(ctx, config);

  // Set MPI communicator
  config->config_->set_mpi_comm(mpi_comm);

  // Success
  return TILEDB_OK;
}
#endif

int tiledb_config_set_read_method(
    tiledb_ctx_t* ctx, tiledb_config_t* config, tiledb_io_t read_method) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_CONFIG(ctx, config);

  // Set read method
  config->config_->set_read_method(static_cast<tiledb::IOMethod>(read_method));

  // Success
  return TILEDB_OK;
}

int tiledb_config_set_write_method(
    tiledb_ctx_t* ctx, tiledb_config_t* config, tiledb_io_t write_method) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_CONFIG(ctx, config);

  // Set write method
  config->config_->set_write_method(
      static_cast<tiledb::IOMethod>(write_method));

  // Success
  return TILEDB_OK;
}

/* ********************************* */
/*              ERROR                */
/* ********************************* */

int tiledb_error_last(tiledb_ctx_t* ctx, tiledb_error_t** err) {
  SANITY_CHECK_CTX(ctx);

  // No last error
  if (ctx->last_error_ == nullptr) {
    *err = nullptr;
    return TILEDB_OK;
  }

  // Create error struct
  *err = (tiledb_error_t*)malloc(sizeof(tiledb_error_t));
  if (*err == nullptr) {
    save_error(ctx, tiledb::Status::Error("Failed to allocate error struct"));
    return TILEDB_OOM;
  }

  // Create status
  (*err)->status_ = new tiledb::Status(*(ctx->last_error_));
  if ((*err)->status_ == nullptr) {
    free(*err);
    *err = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate status object in TileDB error struct"));
    return TILEDB_OOM;
  }

  // Set error message
  (*err)->errmsg_ = new std::string((*err)->status_->to_string());

  // Success
  return TILEDB_OK;
}

int tiledb_error_message(
    tiledb_ctx_t* ctx, tiledb_error_t* err, const char** errmsg) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ERROR(ctx, err);

  // Set error message
  if (err->status_->ok())
    *errmsg = nullptr;
  else
    *errmsg = err->errmsg_->c_str();

  return TILEDB_OK;
}

void tiledb_error_free(tiledb_error_t* err) {
  if (err != nullptr) {
    if (err->status_ != nullptr) {
      delete err->status_;
    }
    delete err->errmsg_;
    free(err);
  }
}

/* ****************************** */
/*              GROUP             */
/* ****************************** */

int tiledb_group_create(tiledb_ctx_t* ctx, const char* group) {
  SANITY_CHECK_CTX(ctx);

  // Create the group
  if (save_error(ctx, ctx->storage_manager_->group_create(group)))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

/* ********************************* */
/*            BASIC ARRAY            */
/* ********************************* */

int tiledb_basic_array_create(tiledb_ctx_t* ctx, const char* name) {
  SANITY_CHECK_CTX(ctx);

  // Create the basic array
  if (save_error(ctx, ctx->storage_manager_->basic_array_create(name)))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

/* ********************************* */
/*            ATTRIBUTE              */
/* ********************************* */

int tiledb_attribute_create(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t** attr,
    const char* name,
    tiledb_datatype_t type) {
  SANITY_CHECK_CTX(ctx);

  // Create an attribute struct
  *attr = (tiledb_attribute_t*)malloc(sizeof(tiledb_attribute_t));
  if (*attr == nullptr) {
    save_error(
        ctx,
        tiledb::Status::Error("Failed to allocate TileDB attribute struct"));
    return TILEDB_OOM;
  }

  // Create a new Attribute object
  (*attr)->attr_ =
      new tiledb::Attribute(name, static_cast<tiledb::Datatype>(type));
  if ((*attr)->attr_ == nullptr) {
    free(*attr);
    *attr = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB attribute object in struct"));
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_attribute_free(tiledb_attribute_t* attr) {
  if (attr == nullptr)
    return;

  if (attr->attr_ != nullptr)
    delete attr->attr_;

  free(attr);
}

int tiledb_attribute_set_compressor(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    tiledb_compressor_t compressor,
    int compression_level) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ATTRIBUTE(ctx, attr);

  attr->attr_->set_compressor(static_cast<tiledb::Compressor>(compressor));
  attr->attr_->set_compression_level(compression_level);

  return TILEDB_OK;
}

int tiledb_attribute_set_cell_val_num(
    tiledb_ctx_t* ctx, tiledb_attribute_t* attr, unsigned int cell_val_num) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ATTRIBUTE(ctx, attr);

  attr->attr_->set_cell_val_num(cell_val_num);

  return TILEDB_OK;
}

int tiledb_attribute_get_name(
    tiledb_ctx_t* ctx, const tiledb_attribute_t* attr, const char** name) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ATTRIBUTE(ctx, attr);

  *name = attr->attr_->name().c_str();

  return TILEDB_OK;
}

int tiledb_attribute_get_type(
    tiledb_ctx_t* ctx,
    const tiledb_attribute_t* attr,
    tiledb_datatype_t* type) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ATTRIBUTE(ctx, attr);

  *type = static_cast<tiledb_datatype_t>(attr->attr_->type());

  return TILEDB_OK;
}

int tiledb_attribute_get_compressor(
    tiledb_ctx_t* ctx,
    const tiledb_attribute_t* attr,
    tiledb_compressor_t* compressor,
    int* compression_level) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ATTRIBUTE(ctx, attr);

  *compressor = static_cast<tiledb_compressor_t>(attr->attr_->compressor());
  *compression_level = attr->attr_->compression_level();

  return TILEDB_OK;
}

int tiledb_attribute_get_cell_val_num(
    tiledb_ctx_t* ctx,
    const tiledb_attribute_t* attr,
    unsigned int* cell_val_num) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ATTRIBUTE(ctx, attr);

  *cell_val_num = attr->attr_->cell_val_num();

  return TILEDB_OK;
}

int tiledb_attribute_dump(
    tiledb_ctx_t* ctx, const tiledb_attribute_t* attr, FILE* out) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ATTRIBUTE(ctx, attr);

  attr->attr_->dump(out);

  // Success
  return TILEDB_OK;
}

/* ********************************* */
/*            DIMENSION              */
/* ********************************* */

int tiledb_dimension_create(
    tiledb_ctx_t* ctx,
    tiledb_dimension_t** dim,
    const char* name,
    tiledb_datatype_t type,
    const void* domain,
    const void* tile_extent) {
  SANITY_CHECK_CTX(ctx);

  // Create an dimension struct
  *dim = (tiledb_dimension_t*)malloc(sizeof(tiledb_dimension_t));
  if (*dim == nullptr) {
    save_error(
        ctx,
        tiledb::Status::Error("Failed to allocate TileDB dimension struct"));
    return TILEDB_OOM;
  }

  // Create a new Dimension object
  (*dim)->dim_ = new tiledb::Dimension(
      name, static_cast<tiledb::Datatype>(type), domain, tile_extent);
  if ((*dim)->dim_ == nullptr) {
    free(*dim);
    *dim = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB dimension object in struct"));
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_dimension_free(tiledb_dimension_t* dim) {
  if (dim == nullptr)
    return;

  if (dim->dim_ != nullptr)
    delete dim->dim_;

  free(dim);
}

int tiledb_dimension_set_compressor(
    tiledb_ctx_t* ctx,
    tiledb_dimension_t* dim,
    tiledb_compressor_t compressor,
    int compression_level) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_DIMENSION(ctx, dim);

  dim->dim_->set_compressor(static_cast<tiledb::Compressor>(compressor));
  dim->dim_->set_compression_level(compression_level);

  return TILEDB_OK;
}

int tiledb_dimension_get_name(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, const char** name) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_DIMENSION(ctx, dim);

  *name = dim->dim_->name().c_str();

  return TILEDB_OK;
}

int tiledb_dimension_get_type(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, tiledb_datatype_t* type) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_DIMENSION(ctx, dim);

  *type = static_cast<tiledb_datatype_t>(dim->dim_->type());

  return TILEDB_OK;
}

int tiledb_dimension_get_compressor(
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dim,
    tiledb_compressor_t* compressor,
    int* compression_level) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_DIMENSION(ctx, dim);

  *compressor = static_cast<tiledb_compressor_t>(dim->dim_->compressor());
  *compression_level = dim->dim_->compression_level();

  return TILEDB_OK;
}

int tiledb_dimension_get_domain(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, const void** domain) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_DIMENSION(ctx, dim);

  *domain = dim->dim_->domain();

  return TILEDB_OK;
}

int tiledb_dimension_get_tile_extent(
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dim,
    const void** tile_extent) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_DIMENSION(ctx, dim);

  *tile_extent = dim->dim_->tile_extent();

  return TILEDB_OK;
}

int tiledb_dimension_dump(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, FILE* out) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_DIMENSION(ctx, dim);

  dim->dim_->dump(out);

  // Success
  return TILEDB_OK;
}

/* ****************************** */
/*           ARRAY SCHEMA         */
/* ****************************** */

int tiledb_array_schema_create(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t** array_schema,
    const char* array_name) {
  SANITY_CHECK_CTX(ctx);

  // Create array schema struct
  *array_schema = (tiledb_array_schema_t*)malloc(sizeof(tiledb_array_schema_t));
  if (*array_schema == nullptr) {
    save_error(
        ctx,
        tiledb::Status::Error("Failed to allocate TileDB array schema struct"));
    return TILEDB_OOM;
  }

  // Create a new ArraySchema object
  (*array_schema)->array_schema_ = new tiledb::ArraySchema(array_name);
  if ((*array_schema)->array_schema_ == nullptr) {
    free(*array_schema);
    *array_schema = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB array schema object in struct"));
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_array_schema_free(tiledb_array_schema_t* array_schema) {
  if (array_schema == nullptr)
    return;

  if (array_schema->array_schema_ != nullptr)
    delete array_schema->array_schema_;

  free(array_schema);
}

int tiledb_array_schema_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_attribute_t* attr) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ARRAY_SCHEMA(ctx, array_schema);
  SANITY_CHECK_ATTRIBUTE(ctx, attr);

  array_schema->array_schema_->add_attribute(attr->attr_);

  return TILEDB_OK;
}

int tiledb_array_schema_add_dimension(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_dimension_t* dim) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ARRAY_SCHEMA(ctx, array_schema);
  SANITY_CHECK_DIMENSION(ctx, dim);

  array_schema->array_schema_->add_dimension(dim->dim_);

  return TILEDB_OK;
}

int tiledb_array_schema_set_capacity(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema, uint64_t capacity) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ARRAY_SCHEMA(ctx, array_schema);

  array_schema->array_schema_->set_capacity(capacity);

  return TILEDB_OK;
}

int tiledb_array_schema_set_cell_order(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_layout_t cell_order) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ARRAY_SCHEMA(ctx, array_schema);

  array_schema->array_schema_->set_cell_order(
      static_cast<tiledb::Layout>(cell_order));

  return TILEDB_OK;
}

int tiledb_array_schema_set_tile_order(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_layout_t tile_order) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ARRAY_SCHEMA(ctx, array_schema);

  array_schema->array_schema_->set_tile_order(
      static_cast<tiledb::Layout>(tile_order));

  return TILEDB_OK;
}

int tiledb_array_schema_set_array_type(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_array_type_t array_type) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ARRAY_SCHEMA(ctx, array_schema);

  array_schema->array_schema_->set_array_type(
      static_cast<tiledb::ArrayType>(array_type));

  return TILEDB_OK;
}

int tiledb_array_schema_check(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ARRAY_SCHEMA(ctx, array_schema);

  if (save_error(ctx, array_schema->array_schema_->check()))
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}

int tiledb_array_schema_load(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t** array_schema,
    const char* array_name) {
  SANITY_CHECK_CTX(ctx);

  // Create array schema
  *array_schema = (tiledb_array_schema_t*)malloc(sizeof(tiledb_array_schema_t));
  if (array_schema == nullptr) {
    save_error(
        ctx,
        tiledb::Status::Error("Failed to allocate TileDB array schema struct"));
    return TILEDB_OOM;
  }

  // Create ArraySchema object
  (*array_schema)->array_schema_ = new tiledb::ArraySchema();
  if ((*array_schema)->array_schema_ == nullptr) {
    free(*array_schema);
    *array_schema = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB array schema object in struct"));
    return TILEDB_OOM;
  }

  // Load array schema
  if (save_error(ctx, (*array_schema)->array_schema_->load(array_name))) {
    delete (*array_schema)->array_schema_;
    free(*array_schema);
    *array_schema = nullptr;
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

int tiledb_array_schema_get_array_name(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char** array_name) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ARRAY_SCHEMA(ctx, array_schema);

  *array_name = array_schema->array_schema_->array_name().c_str();

  // Success
  return TILEDB_OK;
}

int tiledb_array_schema_get_array_type(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_array_type_t* array_type) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ARRAY_SCHEMA(ctx, array_schema);

  *array_type = static_cast<tiledb_array_type_t>(
      array_schema->array_schema_->array_type());

  // Success
  return TILEDB_OK;
}

int tiledb_array_schema_get_capacity(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    uint64_t* capacity) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ARRAY_SCHEMA(ctx, array_schema);

  *capacity = array_schema->array_schema_->capacity();

  // Success
  return TILEDB_OK;
}

int tiledb_array_schema_get_cell_order(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_layout_t* cell_order) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ARRAY_SCHEMA(ctx, array_schema);

  *cell_order =
      static_cast<tiledb_layout_t>(array_schema->array_schema_->cell_order());

  // Success
  return TILEDB_OK;
}

int tiledb_array_schema_get_tile_order(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_layout_t* tile_order) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ARRAY_SCHEMA(ctx, array_schema);

  *tile_order =
      static_cast<tiledb_layout_t>(array_schema->array_schema_->tile_order());

  // Success
  return TILEDB_OK;
}

int tiledb_array_schema_dump(
    tiledb_ctx_t* ctx, const tiledb_array_schema_t* array_schema, FILE* out) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ARRAY_SCHEMA(ctx, array_schema);

  array_schema->array_schema_->dump(out);

  // Success
  return TILEDB_OK;
}

/* ****************************** */
/*       ATTRIBUTE ITERATOR       */
/* ****************************** */

int tiledb_attribute_iter_create(
    tiledb_ctx_t* ctx,
    const void* schema,
    tiledb_attribute_iter_t** attr_it,
    tiledb_object_t object_type) {
  SANITY_CHECK_CTX(ctx);

  // Check object type
  if (object_type != TILEDB_METADATA && object_type != TILEDB_ARRAY) {
    save_error(ctx, tiledb::Status::Error("Invalid object type"));
    return TILEDB_ERR;
  }

  // Get array or metadata schema
  const tiledb_array_schema_t* array_schema = nullptr;
  const tiledb_metadata_schema_t* metadata_schema = nullptr;
  if (object_type == TILEDB_ARRAY) {
    array_schema = static_cast<const tiledb_array_schema_t*>(schema);
    SANITY_CHECK_ARRAY_SCHEMA(ctx, array_schema);
  } else {  // TILEDB_METADATA
    metadata_schema = static_cast<const tiledb_metadata_schema_t*>(schema);
    SANITY_CHECK_METADATA_SCHEMA(ctx, metadata_schema);
  }

  // Create attribute iterator struct
  *attr_it = (tiledb_attribute_iter_t*)malloc(sizeof(tiledb_attribute_iter_t));
  if (*attr_it == nullptr) {
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB attribute iterator struct"));
    return TILEDB_OOM;
  }

  // Initialize the iterator for the array schema
  (*attr_it)->array_schema_ = array_schema;
  if (array_schema != nullptr)
    (*attr_it)->attr_num_ = array_schema->array_schema_->attr_num();

  // Initialize the iterator for the metadata schema
  (*attr_it)->metadata_schema_ = metadata_schema;
  if (metadata_schema != nullptr)
    (*attr_it)->attr_num_ = metadata_schema->metadata_schema_->attr_num();

  // Initialize the rest members of the iterator
  (*attr_it)->current_attr_ = 0;
  if ((*attr_it)->attr_num_ <= 0) {
    (*attr_it)->attr_ = nullptr;
  } else {
    // Create an attribute struct inside the iterator struct
    (*attr_it)->attr_ = (tiledb_attribute_t*)malloc(sizeof(tiledb_attribute_t));
    if ((*attr_it)->attr_ == nullptr) {
      save_error(
          ctx,
          tiledb::Status::Error(
              "Failed to allocate TileDB attribute struct in iterator struct"));
      free(*attr_it);
      *attr_it = nullptr;
      return TILEDB_OOM;
    }

    // Create an attribute object
    if (array_schema != nullptr)
      (*attr_it)->attr_->attr_ =
          new tiledb::Attribute(array_schema->array_schema_->attr(0));
    if (metadata_schema != nullptr)
      (*attr_it)->attr_->attr_ =
          new tiledb::Attribute(metadata_schema->metadata_schema_->attr(0));

    // Check for allocation error
    if ((*attr_it)->attr_->attr_ == nullptr) {
      free((*attr_it)->attr_);
      free(*attr_it);
      *attr_it = nullptr;
      save_error(
          ctx,
          tiledb::Status::Error(
              "Failed to allocate TileDB attribute object in iterator struct"));
      return TILEDB_OOM;
    }
  }

  // Success
  return TILEDB_OK;
}

void tiledb_attribute_iter_free(tiledb_attribute_iter_t* attr_it) {
  if (attr_it == nullptr)
    return;

  if (attr_it->attr_ != nullptr) {
    if (attr_it->attr_->attr_ != nullptr)
      delete (attr_it->attr_->attr_);

    free(attr_it->attr_);
  }

  free(attr_it);
}

int tiledb_attribute_iter_done(
    tiledb_ctx_t* ctx, tiledb_attribute_iter_t* attr_it, int* done) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ATTRIBUTE_ITER(ctx, attr_it);

  *done = (attr_it->current_attr_ == attr_it->attr_num_) ? 1 : 0;

  return TILEDB_OK;
}

int tiledb_attribute_iter_next(
    tiledb_ctx_t* ctx, tiledb_attribute_iter_t* attr_it) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ATTRIBUTE_ITER(ctx, attr_it);

  ++(attr_it->current_attr_);
  if (attr_it->attr_ != nullptr) {
    if (attr_it->attr_->attr_ != nullptr)
      delete attr_it->attr_->attr_;

    if (attr_it->current_attr_ >= 0 &&
        attr_it->current_attr_ < attr_it->attr_num_) {
      if (attr_it->array_schema_ != nullptr)
        attr_it->attr_->attr_ =
            new tiledb::Attribute(attr_it->array_schema_->array_schema_->attr(
                attr_it->current_attr_));
      if (attr_it->metadata_schema_ != nullptr)
        attr_it->attr_->attr_ = new tiledb::Attribute(
            attr_it->metadata_schema_->metadata_schema_->attr(
                attr_it->current_attr_));
    } else {
      attr_it->attr_->attr_ = nullptr;
    }
  }

  return TILEDB_OK;
}

int tiledb_attribute_iter_here(
    tiledb_ctx_t* ctx,
    tiledb_attribute_iter_t* attr_it,
    const tiledb_attribute_t** attr) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ATTRIBUTE_ITER(ctx, attr_it);

  *attr = attr_it->attr_;

  return TILEDB_OK;
}

int tiledb_attribute_iter_first(
    tiledb_ctx_t* ctx, tiledb_attribute_iter_t* attr_it) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ATTRIBUTE_ITER(ctx, attr_it);

  attr_it->current_attr_ = 0;
  if (attr_it->attr_ != nullptr) {
    if (attr_it->attr_->attr_ != nullptr)
      delete attr_it->attr_;
    if (attr_it->attr_num_ > 0) {
      if (attr_it->array_schema_ != nullptr)
        attr_it->attr_->attr_ = new tiledb::Attribute(
            attr_it->array_schema_->array_schema_->attr(0));
      if (attr_it->metadata_schema_ != nullptr)
        attr_it->attr_->attr_ = new tiledb::Attribute(
            attr_it->metadata_schema_->metadata_schema_->attr(0));
    } else {
      attr_it->attr_->attr_ = nullptr;
    }
  }

  return TILEDB_OK;
}

/* ****************************** */
/*       DIMENSION ITERATOR       */
/* ****************************** */

int tiledb_dimension_iter_create(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_dimension_iter_t** dim_it) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_ARRAY_SCHEMA(ctx, array_schema);

  // Create attribute iterator struct
  *dim_it = (tiledb_dimension_iter_t*)malloc(sizeof(tiledb_dimension_iter_t));
  if (*dim_it == nullptr) {
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB dimension iterator struct"));
    return TILEDB_OOM;
  }

  // Initialize the iterator
  (*dim_it)->array_schema_ = array_schema;
  if (array_schema != nullptr)
    (*dim_it)->dim_num_ = array_schema->array_schema_->dim_num();
  (*dim_it)->current_dim_ = 0;
  if ((*dim_it)->dim_num_ <= 0) {
    (*dim_it)->dim_ = nullptr;
  } else {
    // Create a dimension struct inside the iterator struct
    (*dim_it)->dim_ = (tiledb_dimension_t*)malloc(sizeof(tiledb_dimension_t));
    if ((*dim_it)->dim_ == nullptr) {
      save_error(
          ctx,
          tiledb::Status::Error(
              "Failed to allocate TileDB dimension struct in iterator struct"));
      free(*dim_it);
      *dim_it = nullptr;
      return TILEDB_OOM;
    }

    // Create a dimension object
    (*dim_it)->dim_->dim_ =
        new tiledb::Dimension(array_schema->array_schema_->dim(0));

    // Check for allocation error
    if ((*dim_it)->dim_->dim_ == nullptr) {
      free((*dim_it)->dim_);
      free(*dim_it);
      *dim_it = nullptr;
      save_error(
          ctx,
          tiledb::Status::Error(
              "Failed to allocate TileDB dimension object in iterator struct"));
      return TILEDB_OOM;
    }
  }

  // Success
  return TILEDB_OK;
}

void tiledb_dimension_iter_free(tiledb_dimension_iter_t* dim_it) {
  if (dim_it == nullptr)
    return;

  if (dim_it->dim_ != nullptr) {
    if (dim_it->dim_->dim_ != nullptr)
      delete (dim_it->dim_->dim_);

    free(dim_it->dim_);
  }

  free(dim_it);
}

int tiledb_dimension_iter_done(
    tiledb_ctx_t* ctx, tiledb_dimension_iter_t* dim_it, int* done) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_DIMENSION_ITER(ctx, dim_it);

  *done = (dim_it->current_dim_ == dim_it->dim_num_) ? 1 : 0;

  return TILEDB_OK;
}

int tiledb_dimension_iter_next(
    tiledb_ctx_t* ctx, tiledb_dimension_iter_t* dim_it) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_DIMENSION_ITER(ctx, dim_it);

  ++(dim_it->current_dim_);
  if (dim_it->dim_ != nullptr) {
    if (dim_it->dim_->dim_ != nullptr)
      delete dim_it->dim_->dim_;

    if (dim_it->current_dim_ >= 0 && dim_it->current_dim_ < dim_it->dim_num_)
      dim_it->dim_->dim_ = new tiledb::Dimension(
          dim_it->array_schema_->array_schema_->dim(dim_it->current_dim_));
    else
      dim_it->dim_->dim_ = nullptr;
  }

  return TILEDB_OK;
}

int tiledb_dimension_iter_here(
    tiledb_ctx_t* ctx,
    tiledb_dimension_iter_t* dim_it,
    const tiledb_dimension_t** dim) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_DIMENSION_ITER(ctx, dim_it);

  *dim = dim_it->dim_;

  return TILEDB_OK;
}

int tiledb_dimension_iter_first(
    tiledb_ctx_t* ctx, tiledb_dimension_iter_t* dim_it) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_DIMENSION_ITER(ctx, dim_it);

  dim_it->current_dim_ = 0;
  if (dim_it->dim_ != nullptr) {
    if (dim_it->dim_->dim_ != nullptr)
      delete dim_it->dim_;
    if (dim_it->dim_num_ > 0)
      dim_it->dim_->dim_ =
          new tiledb::Dimension(dim_it->array_schema_->array_schema_->dim(0));
    else
      dim_it->dim_->dim_ = nullptr;
  }

  return TILEDB_OK;
}

/* ****************************** */
/*              ARRAY             */
/* ****************************** */

int tiledb_array_create(
    tiledb_ctx_t* ctx, const tiledb_array_schema_t* array_schema) {
  // Sanity checks
  // TODO: sanity checks here

  // Create the array
  if (save_error(
          ctx,
          ctx->storage_manager_->array_create(array_schema->array_schema_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_init(
    tiledb_ctx_t* ctx,
    tiledb_array_t** tiledb_array,
    const char* array,
    tiledb_array_mode_t mode,
    const void* subarray,
    const char** attributes,
    int attribute_num) {
  // TODO: sanity checks here

  // Allocate memory for the array struct
  *tiledb_array = (tiledb_array_t*)malloc(sizeof(struct tiledb_array_t));
  if (tiledb_array == nullptr)
    return TILEDB_OOM;

  // Set TileDB context
  (*tiledb_array)->ctx_ = ctx;

  // Init the array
  if (save_error(
          ctx,
          ctx->storage_manager_->array_init(
              (*tiledb_array)->array_,
              array,
              static_cast<tiledb::ArrayMode>(mode),
              subarray,
              attributes,
              attribute_num))) {
    free(*tiledb_array);
    return TILEDB_ERR;
  };

  return TILEDB_OK;
}

int tiledb_array_reset_subarray(
    const tiledb_array_t* tiledb_array, const void* subarray) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_array->ctx_;

  if (save_error(ctx, tiledb_array->array_->reset_subarray(subarray)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_reset_attributes(
    const tiledb_array_t* tiledb_array,
    const char** attributes,
    int attribute_num) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_array->ctx_;

  // Re-Init the array
  if (save_error(
          ctx,
          tiledb_array->array_->reset_attributes(attributes, attribute_num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

tiledb_array_schema_t* tiledb_array_get_schema(
    const tiledb_array_t* tiledb_array) {
  // Sanity check
  // TODO: sanity checks here

  // Create an array schema struct object
  tiledb_array_schema_t* array_schema =
      (tiledb_array_schema_t*)malloc(sizeof(tiledb_array_schema_t));
  if (array_schema == nullptr)
    return nullptr;
  array_schema->array_schema_ =
      new tiledb::ArraySchema(tiledb_array->array_->array_schema());

  return array_schema;
}

int tiledb_array_write(
    const tiledb_array_t* tiledb_array,
    const void** buffers,
    const size_t* buffer_sizes) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_array->ctx_;

  if (save_error(ctx, tiledb_array->array_->write(buffers, buffer_sizes)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_read(
    const tiledb_array_t* tiledb_array, void** buffers, size_t* buffer_sizes) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_array->ctx_;

  if (save_error(ctx, tiledb_array->array_->read(buffers, buffer_sizes)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_overflow(
    const tiledb_array_t* tiledb_array, int attribute_id) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_array->ctx_;

  return (int)tiledb_array->array_->overflow(attribute_id);
}

int tiledb_array_consolidate(tiledb_ctx_t* ctx, const char* array) {
  // TODO: sanity checks here

  if (save_error(ctx, ctx->storage_manager_->array_consolidate(array)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_finalize(tiledb_array_t* tiledb_array) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_array->ctx_;

  if (save_error(
          ctx,
          tiledb_array->ctx_->storage_manager_->array_finalize(
              tiledb_array->array_))) {
    free(tiledb_array);
    return TILEDB_ERR;
  }

  free(tiledb_array);

  return TILEDB_OK;
}

int tiledb_array_sync(tiledb_array_t* tiledb_array) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_array->ctx_;

  if (save_error(
          ctx,
          tiledb_array->ctx_->storage_manager_->array_sync(
              tiledb_array->array_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_sync_attribute(
    tiledb_array_t* tiledb_array, const char* attribute) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_array->ctx_;

  if (save_error(
          ctx,
          tiledb_array->ctx_->storage_manager_->array_sync_attribute(
              tiledb_array->array_, attribute)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

typedef struct tiledb_array_iterator_t {
  tiledb::ArrayIterator* array_it_;
  tiledb_ctx_t* ctx_;
} tiledb_array_iterator_t;

int tiledb_array_iterator_init(
    tiledb_ctx_t* ctx,
    tiledb_array_iterator_t** tiledb_array_it,
    const char* array,
    tiledb_array_mode_t mode,
    const void* subarray,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes) {
  // TODO: sanity checks here

  *tiledb_array_it =
      (tiledb_array_iterator_t*)malloc(sizeof(struct tiledb_array_iterator_t));
  if (*tiledb_array_it == nullptr)
    return TILEDB_OOM;

  (*tiledb_array_it)->ctx_ = ctx;

  if (save_error(
          ctx,
          ctx->storage_manager_->array_iterator_init(
              (*tiledb_array_it)->array_it_,
              array,
              static_cast<tiledb::ArrayMode>(mode),
              subarray,
              attributes,
              attribute_num,
              buffers,
              buffer_sizes))) {
    free(*tiledb_array_it);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int tiledb_array_iterator_get_value(
    tiledb_array_iterator_t* tiledb_array_it,
    int attribute_id,
    const void** value,
    size_t* value_size) {
  tiledb_ctx_t* ctx = tiledb_array_it->ctx_;

  // TODO: sanity checks

  // Get value
  if (save_error(
          ctx,
          tiledb_array_it->array_it_->get_value(
              attribute_id, value, value_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_iterator_next(tiledb_array_iterator_t* tiledb_array_it) {
  // TODO: sanity checks

  tiledb_ctx_t* ctx = tiledb_array_it->ctx_;

  if (save_error(ctx, tiledb_array_it->array_it_->next()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_iterator_end(tiledb_array_iterator_t* tiledb_array_it) {
  // TODO: sanity checks

  tiledb_ctx_t* ctx = tiledb_array_it->ctx_;

  return (int)tiledb_array_it->array_it_->end();
}

int tiledb_array_iterator_finalize(tiledb_array_iterator_t* tiledb_array_it) {
  // TODO: sanity checks

  tiledb_ctx_t* ctx = tiledb_array_it->ctx_;

  if (save_error(
          ctx,
          tiledb_array_it->ctx_->storage_manager_->array_iterator_finalize(
              tiledb_array_it->array_it_))) {
    free(tiledb_array_it);
    return TILEDB_ERR;
  }

  free(tiledb_array_it);

  return TILEDB_OK;
}

/* ****************************** */
/*        METADATA SCHEMA         */
/* ****************************** */

int tiledb_metadata_schema_create(
    tiledb_ctx_t* ctx,
    tiledb_metadata_schema_t** metadata_schema,
    const char* metadata_name) {
  SANITY_CHECK_CTX(ctx);

  // Create metadata schema struct
  *metadata_schema =
      (tiledb_metadata_schema_t*)malloc(sizeof(tiledb_metadata_schema_t));
  if (*metadata_schema == nullptr) {
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB metadata schema struct"));
    return TILEDB_OOM;
  }

  // Create a new MetadataSchema object
  (*metadata_schema)->metadata_schema_ =
      new tiledb::MetadataSchema(metadata_name);

  if ((*metadata_schema)->metadata_schema_ == nullptr) {
    free(*metadata_schema);
    *metadata_schema = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB metadata schema object in struct"));
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_metadata_schema_free(tiledb_metadata_schema_t* metadata_schema) {
  if (metadata_schema == nullptr)
    return;

  if (metadata_schema->metadata_schema_ != nullptr)
    delete metadata_schema->metadata_schema_;

  free(metadata_schema);
}

int tiledb_metadata_schema_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_metadata_schema_t* metadata_schema,
    tiledb_attribute_t* attr) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_METADATA_SCHEMA(ctx, metadata_schema);
  SANITY_CHECK_ATTRIBUTE(ctx, attr);

  metadata_schema->metadata_schema_->add_attribute(attr->attr_);

  return TILEDB_OK;
}

int tiledb_metadata_schema_set_capacity(
    tiledb_ctx_t* ctx,
    tiledb_metadata_schema_t* metadata_schema,
    uint64_t capacity) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_METADATA_SCHEMA(ctx, metadata_schema);

  metadata_schema->metadata_schema_->set_capacity(capacity);

  return TILEDB_OK;
}

int tiledb_metadata_schema_set_cell_order(
    tiledb_ctx_t* ctx,
    tiledb_metadata_schema_t* metadata_schema,
    tiledb_layout_t cell_order) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_METADATA_SCHEMA(ctx, metadata_schema);

  metadata_schema->metadata_schema_->set_cell_order(
      static_cast<tiledb::Layout>(cell_order));

  return TILEDB_OK;
}

int tiledb_metadata_schema_set_tile_order(
    tiledb_ctx_t* ctx,
    tiledb_metadata_schema_t* metadata_schema,
    tiledb_layout_t tile_order) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_METADATA_SCHEMA(ctx, metadata_schema);

  metadata_schema->metadata_schema_->set_tile_order(
      static_cast<tiledb::Layout>(tile_order));

  return TILEDB_OK;
}

int tiledb_metadata_schema_check(
    tiledb_ctx_t* ctx, tiledb_metadata_schema_t* metadata_schema) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_METADATA_SCHEMA(ctx, metadata_schema);

  if (save_error(ctx, metadata_schema->metadata_schema_->check()))
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}

int tiledb_metadata_schema_load(
    tiledb_ctx_t* ctx,
    tiledb_metadata_schema_t** metadata_schema,
    const char* metadata_name) {
  SANITY_CHECK_CTX(ctx);

  // Create metadata schema
  *metadata_schema =
      (tiledb_metadata_schema_t*)malloc(sizeof(tiledb_metadata_schema_t));
  if (metadata_schema == nullptr) {
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB metadata schema struct"));
    return TILEDB_OOM;
  }

  // Create MetadataSchema object
  (*metadata_schema)->metadata_schema_ = new tiledb::MetadataSchema();
  if ((*metadata_schema)->metadata_schema_ == nullptr) {
    free(*metadata_schema);
    *metadata_schema = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB metadata schema object in struct"));
    return TILEDB_OOM;
  }

  // Load metadata schema
  if (save_error(
          ctx, (*metadata_schema)->metadata_schema_->load(metadata_name))) {
    delete (*metadata_schema)->metadata_schema_;
    free(*metadata_schema);
    *metadata_schema = nullptr;
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

int tiledb_metadata_schema_get_metadata_name(
    tiledb_ctx_t* ctx,
    const tiledb_metadata_schema_t* metadata_schema,
    const char** metadata_name) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_METADATA_SCHEMA(ctx, metadata_schema);

  *metadata_name = metadata_schema->metadata_schema_->metadata_name().c_str();

  // Success
  return TILEDB_OK;
}

int tiledb_metadata_schema_get_capacity(
    tiledb_ctx_t* ctx,
    const tiledb_metadata_schema_t* metadata_schema,
    uint64_t* capacity) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_METADATA_SCHEMA(ctx, metadata_schema);

  *capacity = metadata_schema->metadata_schema_->capacity();

  // Success
  return TILEDB_OK;
}

int tiledb_metadata_schema_get_cell_order(
    tiledb_ctx_t* ctx,
    const tiledb_metadata_schema_t* metadata_schema,
    tiledb_layout_t* cell_order) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_METADATA_SCHEMA(ctx, metadata_schema);

  *cell_order = static_cast<tiledb_layout_t>(
      metadata_schema->metadata_schema_->cell_order());

  // Success
  return TILEDB_OK;
}

int tiledb_metadata_schema_get_tile_order(
    tiledb_ctx_t* ctx,
    const tiledb_metadata_schema_t* metadata_schema,
    tiledb_layout_t* tile_order) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_METADATA_SCHEMA(ctx, metadata_schema);

  *tile_order = static_cast<tiledb_layout_t>(
      metadata_schema->metadata_schema_->tile_order());

  // Success
  return TILEDB_OK;
}

int tiledb_metadata_schema_dump(
    tiledb_ctx_t* ctx,
    const tiledb_metadata_schema_t* metadata_schema,
    FILE* out) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_METADATA_SCHEMA(ctx, metadata_schema);

  metadata_schema->metadata_schema_->dump(out);

  // Success
  return TILEDB_OK;
}

/* ****************************** */
/*            METADATA            */
/* ****************************** */

int tiledb_metadata_create(
    tiledb_ctx_t* ctx, const tiledb_metadata_schema_t* metadata_schema) {
  SANITY_CHECK_CTX(ctx);
  SANITY_CHECK_METADATA_SCHEMA(ctx, metadata_schema);

  if (save_error(
          ctx,
          ctx->storage_manager_->metadata_create(
              metadata_schema->metadata_schema_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_metadata_init(
    tiledb_ctx_t* ctx,
    tiledb_metadata_t** tiledb_metadata,
    const char* metadata,
    tiledb_metadata_mode_t mode,
    const char** attributes,
    int attribute_num) {
  SANITY_CHECK_CTX(ctx);

  // Allocate memory for the array struct
  *tiledb_metadata =
      (tiledb_metadata_t*)malloc(sizeof(struct tiledb_metadata_t));
  if (*tiledb_metadata == nullptr)
    return TILEDB_OOM;

  // Set TileDB context
  (*tiledb_metadata)->ctx_ = ctx;

  // Init the metadata
  if (save_error(
          ctx,
          ctx->storage_manager_->metadata_init(
              (*tiledb_metadata)->metadata_,
              metadata,
              mode,
              attributes,
              attribute_num))) {
    free(*tiledb_metadata);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int tiledb_metadata_reset_attributes(
    const tiledb_metadata_t* tiledb_metadata,
    const char** attributes,
    int attribute_num) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_metadata->ctx_;

  // Reset attributes
  if (save_error(
          ctx,
          tiledb_metadata->metadata_->reset_attributes(
              attributes, attribute_num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

tiledb_metadata_schema_t* tiledb_metadata_get_schema(
    const tiledb_metadata_t* tiledb_metadata) {
  // Sanity check
  // TODO

  // Create an array schema struct object
  tiledb_metadata_schema_t* metadata_schema =
      (tiledb_metadata_schema_t*)malloc(sizeof(tiledb_metadata_schema_t));
  if (metadata_schema == nullptr)
    return nullptr;
  metadata_schema->metadata_schema_ =
      new tiledb::MetadataSchema(tiledb_metadata->metadata_->metadata_schema());

  return metadata_schema;
}

int tiledb_metadata_write(
    const tiledb_metadata_t* tiledb_metadata,
    const char* keys,
    size_t keys_size,
    const void** buffers,
    const size_t* buffer_sizes) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_metadata->ctx_;

  if (save_error(
          ctx,
          tiledb_metadata->metadata_->write(
              keys, keys_size, buffers, buffer_sizes)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_metadata_read(
    const tiledb_metadata_t* tiledb_metadata,
    const char* key,
    void** buffers,
    size_t* buffer_sizes) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_metadata->ctx_;

  if (save_error(
          ctx, tiledb_metadata->metadata_->read(key, buffers, buffer_sizes)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_metadata_overflow(
    const tiledb_metadata_t* tiledb_metadata, int attribute_id) {
  // TODO: sanity checks here

  return (int)tiledb_metadata->metadata_->overflow(attribute_id);
}

int tiledb_metadata_consolidate(tiledb_ctx_t* ctx, const char* metadata) {
  // TODO: sanity checks here

  if (save_error(ctx, ctx->storage_manager_->metadata_consolidate(metadata)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_metadata_finalize(tiledb_metadata_t* tiledb_metadata) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_metadata->ctx_;

  if (save_error(
          ctx,
          tiledb_metadata->ctx_->storage_manager_->metadata_finalize(
              tiledb_metadata->metadata_))) {
    free(tiledb_metadata);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

typedef struct tiledb_metadata_iterator_t {
  tiledb::MetadataIterator* metadata_it_;
  tiledb_ctx_t* ctx_;
} tiledb_metadata_iterator_t;

int tiledb_metadata_iterator_init(
    tiledb_ctx_t* ctx,
    tiledb_metadata_iterator_t** tiledb_metadata_it,
    const char* metadata,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes) {
  // TODO: sanity checks here

  // Allocate memory for the metadata struct
  *tiledb_metadata_it = (tiledb_metadata_iterator_t*)malloc(
      sizeof(struct tiledb_metadata_iterator_t));
  if (*tiledb_metadata_it == nullptr)
    return TILEDB_ERR;

  // Set TileDB context
  (*tiledb_metadata_it)->ctx_ = ctx;

  // Initialize the metadata iterator
  if (save_error(
          ctx,
          ctx->storage_manager_->metadata_iterator_init(
              (*tiledb_metadata_it)->metadata_it_,
              metadata,
              attributes,
              attribute_num,
              buffers,
              buffer_sizes))) {
    free(*tiledb_metadata_it);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

int tiledb_metadata_iterator_get_value(
    tiledb_metadata_iterator_t* tiledb_metadata_it,
    int attribute_id,
    const void** value,
    size_t* value_size) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_metadata_it->ctx_;

  if (save_error(
          ctx,
          tiledb_metadata_it->metadata_it_->get_value(
              attribute_id, value, value_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_metadata_iterator_next(
    tiledb_metadata_iterator_t* tiledb_metadata_it) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_metadata_it->ctx_;

  // Advance metadata iterator
  if (save_error(ctx, tiledb_metadata_it->metadata_it_->next()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_metadata_iterator_end(
    tiledb_metadata_iterator_t* tiledb_metadata_it) {
  // TODO: sanity checks here

  // Check if the metadata iterator reached its end
  return (int)tiledb_metadata_it->metadata_it_->end();
}

int tiledb_metadata_iterator_finalize(
    tiledb_metadata_iterator_t* tiledb_metadata_it) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_metadata_it->ctx_;

  // Finalize metadata iterator
  if (save_error(
          ctx,
          tiledb_metadata_it->ctx_->storage_manager_
              ->metadata_iterator_finalize(tiledb_metadata_it->metadata_it_))) {
    free(tiledb_metadata_it);
    return TILEDB_ERR;
  }

  free(tiledb_metadata_it);

  return TILEDB_OK;
}

/* ****************************** */
/*       DIRECTORY MANAGEMENT     */
/* ****************************** */

int tiledb_dir_type(tiledb_ctx_t* ctx, const char* dir) {
  if (ctx == nullptr)
    return TILEDB_ERR;
  return ctx->storage_manager_->dir_type(dir);
}

int tiledb_clear(tiledb_ctx_t* ctx, const char* dir) {
  // TODO: sanity checks here

  // TODO: do this everywhere
  if (dir == nullptr) {
    save_error(
        ctx, tiledb::Status::Error("Invalid directory argument is NULL"));
    return TILEDB_ERR;
  }

  if (save_error(ctx, ctx->storage_manager_->clear(dir)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_delete(tiledb_ctx_t* ctx, const char* dir) {
  // TODO: sanity checks here

  if (save_error(ctx, ctx->storage_manager_->delete_entire(dir)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_move(tiledb_ctx_t* ctx, const char* old_dir, const char* new_dir) {
  // TODO: sanity checks here

  if (save_error(ctx, ctx->storage_manager_->move(old_dir, new_dir)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_ls(
    tiledb_ctx_t* ctx,
    const char* parent_dir,
    char** dirs,
    tiledb_object_t* dir_types,
    int* dir_num) {
  // TODO: sanity checks here

  if (save_error(
          ctx,
          ctx->storage_manager_->ls(parent_dir, dirs, dir_types, *dir_num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_ls_c(tiledb_ctx_t* ctx, const char* parent_dir, int* dir_num) {
  // TODO: sanity checks here

  if (save_error(ctx, ctx->storage_manager_->ls_c(parent_dir, *dir_num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

/* ****************************** */
/*     ASYNCHRONOUS I/O (AIO)     */
/* ****************************** */

int tiledb_array_aio_read(
    const tiledb_array_t* tiledb_array,
    TileDB_AIO_Request* tiledb_aio_request) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_array->ctx_;

  // Copy the AIO request
  tiledb::AIO_Request* aio_request =
      (tiledb::AIO_Request*)malloc(sizeof(struct tiledb::AIO_Request));
  aio_request->id_ = (size_t)tiledb_aio_request;
  aio_request->buffers_ = tiledb_aio_request->buffers_;
  aio_request->buffer_sizes_ = tiledb_aio_request->buffer_sizes_;
  aio_request->mode_ =
      static_cast<tiledb_array_mode_t>(tiledb_array->array_->mode());
  aio_request->status_ = &(tiledb_aio_request->status_);
  aio_request->subarray_ = tiledb_aio_request->subarray_;
  aio_request->completion_handle_ = tiledb_aio_request->completion_handle_;
  aio_request->completion_data_ = tiledb_aio_request->completion_data_;

  // Submit the AIO read request
  if (save_error(ctx, tiledb_array->array_->aio_read(aio_request)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_aio_write(
    const tiledb_array_t* tiledb_array,
    TileDB_AIO_Request* tiledb_aio_request) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_array->ctx_;

  // Copy the AIO request
  tiledb::AIO_Request* aio_request =
      (tiledb::AIO_Request*)malloc(sizeof(struct tiledb::AIO_Request));
  aio_request->id_ = (size_t)tiledb_aio_request;
  aio_request->buffers_ = tiledb_aio_request->buffers_;
  aio_request->buffer_sizes_ = tiledb_aio_request->buffer_sizes_;
  aio_request->mode_ =
      static_cast<tiledb_array_mode_t>(tiledb_array->array_->mode());
  aio_request->status_ = &(tiledb_aio_request->status_);
  aio_request->subarray_ = tiledb_aio_request->subarray_;
  aio_request->completion_handle_ = tiledb_aio_request->completion_handle_;
  aio_request->completion_data_ = tiledb_aio_request->completion_data_;

  // Submit the AIO write request
  if (save_error(ctx, tiledb_array->array_->aio_write(aio_request)))
    return TILEDB_ERR;

  return TILEDB_OK;
}
