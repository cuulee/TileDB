/**
 * @file   attribute.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file implements class Attribute.
 */

#include "attribute.h"

namespace tiledb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Attribute::Attribute(const char* name, Datatype type) {
  // Set name
  if (name != nullptr)
    name_ = name;

  // Set type
  type_ = type;

  // Set default compressor and compression level
  compressor_ = Compressor::NO_COMPRESSION;
  compression_level_ = -1;
}

Attribute::~Attribute() = default;

/* ********************************* */
/*              SETTERS              */
/* ********************************* */

void Attribute::set_cell_val_num(int cell_val_num) {
  cell_val_num_ = cell_val_num;
}

void Attribute::set_compressor(Compressor compressor) {
  compressor_ = compressor;
}

void Attribute::set_compression_level(int compression_level) {
  compression_level_ = compression_level;
}

}  // namespace tiledb