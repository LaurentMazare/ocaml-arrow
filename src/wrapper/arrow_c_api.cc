// The Arrow C++ library uses the arrow::Result and arrow::Status
// struct to propagate errors.
// However we convert these to exceptions so that this triggers
// leaving the scope where the C++ objects are allocated. The exceptions
// are then converted to a caml_failwith.
// Calling caml_failwith directly could lead to some memory leaks.
//
// This is done using some C macros:
//
// OCAML_BEGIN_PROTECT_EXN
// ... use the arrow C++ library ...
// return result;
// OCAML_END_PROTECT_EXN
// return nullptr;

#include "arrow_c_api.h"

#include<iostream>

#include<caml/bigarray.h>
#include<caml/mlvalues.h>
#include<caml/threads.h>
#include<caml/fail.h>
// invalid_argument is defined in the ocaml runtime and would
// shadow the C++ std::invalid_argument
#undef invalid_argument

class caml_lock_guard {
  caml_lock_guard() {
    caml_enter_blocking_section();
  }
  ~caml_lock_guard() {
    caml_leave_blocking_section();
  }
};

#define OCAML_BEGIN_PROTECT_EXN                     \
  char *__ocaml_protect_err = nullptr;              \
  try {
#define OCAML_BEGIN_PROTECT_EXN_RELEASE_LOCK        \
  char *__ocaml_protect_err = nullptr;              \
  try {                                             \
    caml_lock_guard lock();
#define OCAML_END_PROTECT_EXN                       \
  } catch (const std::exception& e) {               \
    __ocaml_protect_err = strdup(e.what());         \
  }                                                 \
  if (__ocaml_protect_err) {                        \
    char err[256];                                  \
    snprintf(err, 255, "%s", __ocaml_protect_err);  \
    caml_failwith(err);                             \
  }

template<class T>
T ok_exn(arrow::Result<T> &a) {
  if (!a.ok()) {
    throw std::invalid_argument(a.status().ToString());
  }
  return std::move(a.ValueOrDie());
}

void status_exn(arrow::Status &st) {
  if (!st.ok()) {
    throw std::invalid_argument(st.ToString());
  }
}

struct ArrowSchema *arrow_schema(char *filename) {
  OCAML_BEGIN_PROTECT_EXN

  auto file = arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool());
  std::shared_ptr<arrow::io::RandomAccessFile> infile = ok_exn(file);
  auto reader = arrow::ipc::RecordBatchFileReader::Open(infile);
  std::shared_ptr<arrow::Schema> schema = ok_exn(reader)->schema();
  struct ArrowSchema *out = (struct ArrowSchema*)malloc(sizeof *out);
  arrow::ExportSchema(*schema, out);
  return out;

  OCAML_END_PROTECT_EXN
  return nullptr;
}

struct ArrowSchema *feather_schema(char *filename) {
  OCAML_BEGIN_PROTECT_EXN

  auto file = arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool());
  std::shared_ptr<arrow::io::RandomAccessFile> infile = ok_exn(file);
  auto reader = arrow::ipc::feather::Reader::Open(infile);
  std::shared_ptr<arrow::Schema> schema = ok_exn(reader)->schema();
  struct ArrowSchema *out = (struct ArrowSchema*)malloc(sizeof *out);
  arrow::ExportSchema(*schema, out);
  return out;

  OCAML_END_PROTECT_EXN
  return nullptr;
}

struct ArrowSchema *parquet_schema(char *filename, int64_t *num_rows) {
  OCAML_BEGIN_PROTECT_EXN

  arrow::Status st;
  auto file = arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool());
  std::shared_ptr<arrow::io::RandomAccessFile> infile = ok_exn(file);
  std::unique_ptr<parquet::arrow::FileReader> reader;
  st = parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader);
  status_exn(st);
  std::shared_ptr<arrow::Schema> schema;
  st = reader->GetSchema(&schema);
  status_exn(st);
  *num_rows = reader->parquet_reader()->metadata()->num_rows();
  struct ArrowSchema *out = (struct ArrowSchema*)malloc(sizeof *out);
  arrow::ExportSchema(*schema, out);
  return out;

  OCAML_END_PROTECT_EXN
  return nullptr;
}

void free_schema(struct ArrowSchema *schema) {
  if (schema->release != NULL)
    schema->release(schema);
  schema->release = NULL;
  free(schema);
}

void check_column_idx(int column_idx, int n_cols) {
  if (column_idx < 0 || column_idx >= n_cols) {
    char err[128];
    snprintf(err, 127, "invalid column index %d (ncols: %d)", column_idx, n_cols);
    caml_failwith(err);
  }
}

int timestamp_unit_in_ns(TablePtr *table, char *column_name, int column_idx) {
  int n_cols = (*table)->num_columns();
  if (column_idx >= n_cols) check_column_idx(column_idx, n_cols);

  OCAML_BEGIN_PROTECT_EXN

  std::shared_ptr<arrow::ChunkedArray> array;

  if (column_idx >= 0) {
    array = (*table)->column(column_idx);
  }
  else if (column_name) {
    array = (*table)->GetColumnByName(std::string(column_name));
    if (!array) {
      throw std::invalid_argument(std::string("cannot find column ") + column_name);
    }
  }
  if (!array) {
    throw std::invalid_argument("error finding column");
  }
  auto ts_type = std::dynamic_pointer_cast<arrow::TimestampType>(array->type());
  if (ts_type == nullptr) {
    throw std::invalid_argument("not a timestamp column");
  }
  if (ts_type->unit() == arrow::TimeUnit::SECOND) return 1000000000;
  if (ts_type->unit() == arrow::TimeUnit::MILLI) return 1000000;
  if (ts_type->unit() == arrow::TimeUnit::MICRO) return 1000;
  if (ts_type->unit() == arrow::TimeUnit::NANO) return 1;
  return 1;

  OCAML_END_PROTECT_EXN
  return -1;
}

struct ArrowArray *table_chunked_column_(TablePtr *table, char *column_name, int column_idx, int *nchunks, int dt) {
  OCAML_BEGIN_PROTECT_EXN

  arrow::Type::type expected_type;
  const char *expected_type_str = "";
  if (dt == 0) {
    expected_type = arrow::Type::INT64;
    expected_type_str = "int64";
  }
  else if (dt == 1) {
    expected_type = arrow::Type::DOUBLE;
    expected_type_str = "float64";
  }
  else if (dt == 2) {
    // TODO: also handle large_utf8 here.
    expected_type = arrow::Type::STRING;
    expected_type_str = "utf8";
  }
  else if (dt == 3) {
    expected_type = arrow::Type::DATE32;
    expected_type_str = "date32";
  }
  else if (dt == 4) {
    expected_type = arrow::Type::TIMESTAMP;
    expected_type_str = "timestamp";
  }
  else if (dt == 5) {
    expected_type = arrow::Type::BOOL;
    expected_type_str = "bool";
  }
  else if (dt == 6) {
    expected_type = arrow::Type::FLOAT;
    expected_type_str = "float";
  }
  else {
    throw std::invalid_argument(std::string("unknown datatype ") + std::to_string(dt));
  }
  std::shared_ptr<arrow::ChunkedArray> array;
  if (column_name) {
    array = (*table)->GetColumnByName(std::string(column_name));
    if (!array) {
      throw std::invalid_argument(std::string("cannot find column ") + column_name);
    }
  }
  else {
    array = (*table)->column(column_idx);
  }
  if (!array) {
    throw std::invalid_argument("error finding column");
  }
  *nchunks = array->num_chunks();
  struct ArrowArray *out = (struct ArrowArray*)malloc(array->num_chunks() * sizeof *out);
  for (int i = 0; i < array->num_chunks(); ++i) {
    auto chunk = array->chunk(i);
    if (chunk->type()->id() != expected_type) {
      throw std::invalid_argument(
        std::string("expected type with ") + expected_type_str + " (id "
        + std::to_string(expected_type) + ") got " + chunk->type()->ToString());
    }
    arrow::ExportArray(*chunk, out + i);
  }
  return out;

  OCAML_END_PROTECT_EXN
  return nullptr;
}

struct ArrowArray *table_chunked_column(TablePtr *table, int column_idx, int *nchunks, int dt) {
  int n_cols = (*table)->num_columns();
  check_column_idx(column_idx, n_cols);
  return table_chunked_column_(table, NULL, column_idx, nchunks, dt);
}

struct ArrowArray *table_chunked_column_by_name(TablePtr *table, char *col_name, int *nchunks, int dt) {
  return table_chunked_column_(table, col_name, 0, nchunks, dt);
}

void free_chunked_column(struct ArrowArray *arrays, int nchunks) {
  for (int i = 0; i < nchunks; ++i) {
    if (arrays[i].release != NULL) arrays[i].release(arrays + i);
  }
  free(arrays);
}

TablePtr *table_add_all_columns(TablePtr* t1, TablePtr* t2) {
  OCAML_BEGIN_PROTECT_EXN

  TablePtr result = *t1;
  for (int i = 0; i < (*t2)->num_columns(); ++i) {
    auto field = (*t2)->field(i);
    auto array = (*t2)->column(i);
    auto result_ = result->AddColumn(result->num_columns(), field, array);
    result = ok_exn(result_);
  }

  return new std::shared_ptr<arrow::Table>(result);

  OCAML_END_PROTECT_EXN
  return nullptr;
}

TablePtr *table_add_column(TablePtr* t, char* col_name, ChunkedArrayPtr* array) {
  OCAML_BEGIN_PROTECT_EXN

  auto field = arrow::field(col_name, (*array)->type());
  auto result = (*t)->AddColumn((*t)->num_columns(), field, *array);
  return new std::shared_ptr<arrow::Table>(ok_exn(result));

  OCAML_END_PROTECT_EXN
  return nullptr;
}

ChunkedArrayPtr *table_get_column(TablePtr* t, char* col_name) {
  OCAML_BEGIN_PROTECT_EXN

  auto array = (*t)->GetColumnByName(std::string(col_name));
  if (!array) {
    throw std::invalid_argument(std::string("cannot find column ") + col_name);
  }
  return new std::shared_ptr<arrow::ChunkedArray>(array);

  OCAML_END_PROTECT_EXN
  return nullptr;
}

void free_chunked_array(ChunkedArrayPtr* ptr) {
  delete ptr;
}

arrow::Compression::type compression_of_int(int compression) {
  arrow::Compression::type compression_ = arrow::Compression::UNCOMPRESSED;
  if (compression == 1) compression_ = arrow::Compression::SNAPPY;
  else if (compression == 2) compression_ = arrow::Compression::GZIP;
  else if (compression == 3) compression_ = arrow::Compression::BROTLI;
  else if (compression == 4) compression_ = arrow::Compression::ZSTD;
  else if (compression == 5) compression_ = arrow::Compression::LZ4;
  else if (compression == 6) compression_ = arrow::Compression::LZ4_FRAME;
  else if (compression == 7) compression_ = arrow::Compression::LZO;
  else if (compression == 8) compression_ = arrow::Compression::BZ2;
  return compression_;
}

TablePtr *create_table(struct ArrowArray *array, struct ArrowSchema *schema) {
  OCAML_BEGIN_PROTECT_EXN

  auto record_batch = arrow::ImportRecordBatch(array, schema);
  auto table = arrow::Table::FromRecordBatches({ok_exn(record_batch)});
  return new std::shared_ptr<arrow::Table>(std::move(ok_exn(table)));

  OCAML_END_PROTECT_EXN
  return nullptr;
}

void parquet_write_file(char *filename, struct ArrowArray *array, struct ArrowSchema *schema, int chunk_size, int compression) {
  OCAML_BEGIN_PROTECT_EXN_RELEASE_LOCK

  auto file = arrow::io::FileOutputStream::Open(filename);
  auto outfile = ok_exn(file);
  auto record_batch = arrow::ImportRecordBatch(array, schema);
  auto table = arrow::Table::FromRecordBatches({ok_exn(record_batch)});
  arrow::Compression::type compression_ = compression_of_int(compression);
  arrow::Status st = parquet::arrow::WriteTable(*(ok_exn(table)),
                                                arrow::default_memory_pool(),
                                                outfile,
                                                chunk_size,
                                                parquet::WriterProperties::Builder().compression(compression_)->build(),
                                                parquet::ArrowWriterProperties::Builder().enable_deprecated_int96_timestamps()->build());
  status_exn(st);

  OCAML_END_PROTECT_EXN
}

void arrow_write_file(char *filename, struct ArrowArray *array, struct ArrowSchema *schema, int chunk_size) {
  OCAML_BEGIN_PROTECT_EXN_RELEASE_LOCK

  auto file = arrow::io::FileOutputStream::Open(filename);
  auto outfile = ok_exn(file);
  auto record_batch = arrow::ImportRecordBatch(array, schema);
  auto table = arrow::Table::FromRecordBatches({ok_exn(record_batch)});
  auto table_ = ok_exn(table);
  auto batch_writer = arrow::ipc::MakeFileWriter(&(*outfile), table_->schema());
  arrow::Status st = ok_exn(batch_writer)->WriteTable(*table_);
  status_exn(st);

  OCAML_END_PROTECT_EXN
}

void feather_write_file(char *filename, struct ArrowArray *array, struct ArrowSchema *schema, int chunk_size, int compression) {
  OCAML_BEGIN_PROTECT_EXN_RELEASE_LOCK

  auto file = arrow::io::FileOutputStream::Open(filename);
  auto outfile = ok_exn(file);
  auto record_batch = arrow::ImportRecordBatch(array, schema);
  auto table = arrow::Table::FromRecordBatches({ok_exn(record_batch)});
  struct arrow::ipc::feather::WriteProperties wp;
  wp.compression = compression_of_int(compression);
  wp.chunksize = chunk_size;
  arrow::Status st = arrow::ipc::feather::WriteTable(*(ok_exn(table)),
                                                &(*outfile),
                                                wp);
  status_exn(st);

  OCAML_END_PROTECT_EXN
}

void parquet_write_table(char *filename, TablePtr *table, int chunk_size, int compression) {
  OCAML_BEGIN_PROTECT_EXN_RELEASE_LOCK

  auto file = arrow::io::FileOutputStream::Open(filename);
  auto outfile = ok_exn(file);
  arrow::Compression::type compression_ = compression_of_int(compression);
  arrow::Status st = parquet::arrow::WriteTable(**table,
                                                arrow::default_memory_pool(),
                                                outfile,
                                                chunk_size,
                                                parquet::WriterProperties::Builder().compression(compression_)->build(),
                                                parquet::ArrowWriterProperties::Builder().enable_deprecated_int96_timestamps()->build());
  status_exn(st);

  OCAML_END_PROTECT_EXN
}

void feather_write_table(char *filename, TablePtr *table, int chunk_size, int compression) {
  OCAML_BEGIN_PROTECT_EXN_RELEASE_LOCK

  auto file = arrow::io::FileOutputStream::Open(filename);
  auto outfile = ok_exn(file);
  struct arrow::ipc::feather::WriteProperties wp;
  wp.compression = compression_of_int(compression);
  wp.chunksize = chunk_size;
  arrow::Status st = arrow::ipc::feather::WriteTable(**table, &(*outfile), wp);
  status_exn(st);

  OCAML_END_PROTECT_EXN
}

ParquetReader *parquet_reader_open(char *filename, int *col_idxs, int ncols, int use_threads, int mmap, int buffer_size, int batch_size) {
  OCAML_BEGIN_PROTECT_EXN_RELEASE_LOCK

  arrow::Status st;
  parquet::ReaderProperties prop = parquet::default_reader_properties();
  parquet::ArrowReaderProperties arrow_prop = parquet::default_arrow_reader_properties();
  if (buffer_size > 0) {
    prop.enable_buffered_stream();
    prop.set_buffer_size(buffer_size);
  }
  if (batch_size > 0) arrow_prop.set_batch_size(batch_size);
  std::unique_ptr<parquet::ParquetFileReader> preader = parquet::ParquetFileReader::OpenFile(filename, mmap, prop);
  std::unique_ptr<parquet::arrow::FileReader> reader;
  st = parquet::arrow::FileReader::Make(arrow::default_memory_pool(), std::move(preader), arrow_prop, &reader);
  status_exn(st);
  if (use_threads >= 0) reader->set_use_threads(use_threads);
  std::unique_ptr<arrow::RecordBatchReader> batch_reader;
  std::vector<int> all_groups(reader->num_row_groups());
  std::iota(all_groups.begin(), all_groups.end(), 0);
  if (ncols)
    st = reader->GetRecordBatchReader(
      all_groups,
      std::vector<int>(col_idxs, col_idxs+ncols),
      &batch_reader);
  else
    st = reader->GetRecordBatchReader(all_groups, &batch_reader);
  status_exn(st);
  return new ParquetReader{ std::move(reader), std::move(batch_reader)};

  OCAML_END_PROTECT_EXN
  return nullptr;
}

TablePtr *parquet_reader_next(ParquetReader *pr) {
  if (!pr->batch_reader) caml_failwith("reader has already been closed");

  OCAML_BEGIN_PROTECT_EXN_RELEASE_LOCK

  arrow::Status st;
  std::shared_ptr<arrow::RecordBatch> batch;
  st = pr->batch_reader->ReadNext(&batch);
  status_exn(st);
  if (batch == nullptr) {
    return nullptr;
  }
  auto table_ = arrow::Table::FromRecordBatches({batch});
  std::shared_ptr<arrow::Table> table = std::move(ok_exn(table_));
  return new std::shared_ptr<arrow::Table>(std::move(table));

  OCAML_END_PROTECT_EXN
  return nullptr;
}

void parquet_reader_close(ParquetReader *pr) {
  pr->batch_reader.reset();
  pr->reader.reset();
}

void parquet_reader_free(ParquetReader *pr) {
  delete pr;
}

TablePtr *parquet_read_table(char *filename, int *col_idxs, int ncols, int use_threads, int64_t only_first) {
  OCAML_BEGIN_PROTECT_EXN_RELEASE_LOCK

  arrow::Status st;
  auto file = arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool());
  std::shared_ptr<arrow::io::RandomAccessFile> infile = ok_exn(file);
  std::unique_ptr<parquet::arrow::FileReader> reader;
  st = parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader);
  status_exn(st);
  if (use_threads >= 0) reader->set_use_threads(use_threads);
  std::shared_ptr<arrow::Table> table;
  if (only_first < 0) {
    if (ncols)
      st = reader->ReadTable(std::vector<int>(col_idxs, col_idxs+ncols), &table);
    else
      st = reader->ReadTable(&table);
    status_exn(st);
  } else {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    for (int row_group_idx = 0; row_group_idx < reader->num_row_groups(); ++row_group_idx) {
      std::unique_ptr<arrow::RecordBatchReader> batch_reader;
      if (ncols)
        st = reader->GetRecordBatchReader({row_group_idx}, std::vector<int>(col_idxs, col_idxs+ncols), &batch_reader);
      else
        st = reader->GetRecordBatchReader({row_group_idx}, &batch_reader);
      status_exn(st);
      std::shared_ptr<arrow::RecordBatch> batch;
      while (only_first > 0) {
        st = batch_reader->ReadNext(&batch);
        status_exn(st);
        if (batch == nullptr) break;
        if (only_first <= batch->num_rows()) {
          batches.push_back(std::move(batch->Slice(0, only_first)));
          only_first = 0;
          break;
        }
        else {
          only_first -= batch->num_rows();
          batches.push_back(std::move(batch));
        }
      }
      if (only_first <= 0)
        break;
    }
    auto table_ = arrow::Table::FromRecordBatches(batches);
    table = std::move(ok_exn(table_));
  }
  return new std::shared_ptr<arrow::Table>(std::move(table));

  OCAML_END_PROTECT_EXN
  return nullptr;
}

TablePtr *feather_read_table(char *filename, int *col_idxs, int ncols) {
  OCAML_BEGIN_PROTECT_EXN_RELEASE_LOCK

  arrow::Status st;
  auto file = arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool());
  std::shared_ptr<arrow::io::RandomAccessFile> infile = ok_exn(file);
  auto reader = arrow::ipc::feather::Reader::Open(infile);
  std::shared_ptr<arrow::Table> table;
  if (ncols)
    st = ok_exn(reader)->Read(std::vector<int>(col_idxs, col_idxs+ncols), &table);
  else
    st = ok_exn(reader)->Read(&table);
  status_exn(st);
  return new std::shared_ptr<arrow::Table>(std::move(table));
  OCAML_END_PROTECT_EXN
  return nullptr;
}

TablePtr *csv_read_table(char *filename) {
  OCAML_BEGIN_PROTECT_EXN_RELEASE_LOCK

  auto file = arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool());
  std::shared_ptr<arrow::io::RandomAccessFile> infile = ok_exn(file);

  auto reader =
    arrow::csv::TableReader::Make(arrow::io::default_io_context(),
                                  infile,
                                  arrow::csv::ReadOptions::Defaults(),
                                  arrow::csv::ParseOptions::Defaults(),
                                  arrow::csv::ConvertOptions::Defaults());

  auto table = ok_exn(reader)->Read();
  return new std::shared_ptr<arrow::Table>(std::move(table.ValueOrDie()));

  OCAML_END_PROTECT_EXN
  return nullptr;
}

TablePtr *json_read_table(char *filename) {
  OCAML_BEGIN_PROTECT_EXN_RELEASE_LOCK

  auto file = arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool());
  std::shared_ptr<arrow::io::RandomAccessFile> infile = ok_exn(file);

  auto reader_ = arrow::json::TableReader::Make(
    arrow::default_memory_pool(),
    infile,
    arrow::json::ReadOptions::Defaults(),
    arrow::json::ParseOptions::Defaults());
  std::shared_ptr<arrow::json::TableReader> reader = ok_exn(reader_);
  auto table_ = reader->Read();
  std::shared_ptr<arrow::Table> table = ok_exn(table_);
  return new std::shared_ptr<arrow::Table>(std::move(table));

  OCAML_END_PROTECT_EXN
  return nullptr;
}

TablePtr *table_concatenate(TablePtr **tables, int ntables) {
  OCAML_BEGIN_PROTECT_EXN

  std::vector<std::shared_ptr<arrow::Table>> vec;
  for (int i = 0; i < ntables; ++i) vec.push_back(**(tables+i));
  auto table = arrow::ConcatenateTables(vec);
  return new std::shared_ptr<arrow::Table>(std::move(ok_exn(table)));

  OCAML_END_PROTECT_EXN
  return nullptr;
}

TablePtr *table_slice(TablePtr *table, int64_t offset, int64_t length) {
  if (offset < 0) caml_invalid_argument("negative offset");
  if (length < 0) caml_invalid_argument("negative length");
  auto slice = (*table)->Slice(offset, length);
  return new std::shared_ptr<arrow::Table>(std::move(slice));
}

int64_t table_num_rows(TablePtr *table) {
  if (table != NULL) return (*table)->num_rows();
  return 0;
}

struct ArrowSchema *table_schema(TablePtr *table) {
  std::shared_ptr<arrow::Schema> schema = (*table)->schema();
  struct ArrowSchema *out = (struct ArrowSchema*)malloc(sizeof *out);
  arrow::ExportSchema(*schema, out);
  return out;
}

void free_table(TablePtr *table) {
  if (table != NULL)
    delete table;
}

/* Builder bindings. */
Int64BuilderPtr *create_int64_builder() {
  auto builder = std::make_shared<arrow::Int64Builder>();
  return new Int64BuilderPtr(builder);
}
DoubleBuilderPtr *create_double_builder() {
  auto builder = std::make_shared<arrow::DoubleBuilder>();
  return new DoubleBuilderPtr(builder);
}
StringBuilderPtr *create_string_builder() {
  auto builder = std::make_shared<arrow::StringBuilder>();
  return new StringBuilderPtr(builder);
}

void append_int64_builder(Int64BuilderPtr* ptr, int64_t v) {
  OCAML_BEGIN_PROTECT_EXN

  arrow::Status st = (*ptr)->Append(v);
  status_exn(st);

  OCAML_END_PROTECT_EXN
}

void append_double_builder(DoubleBuilderPtr* ptr, double v) {
  OCAML_BEGIN_PROTECT_EXN

  arrow::Status st = (*ptr)->Append(v);
  status_exn(st);

  OCAML_END_PROTECT_EXN
}


void append_string_builder(StringBuilderPtr* ptr, char* v) {
  OCAML_BEGIN_PROTECT_EXN

  arrow::Status st = (*ptr)->Append(v);
  status_exn(st);

  OCAML_END_PROTECT_EXN
}


void append_null_int64_builder(Int64BuilderPtr* ptr, int n) {
  OCAML_BEGIN_PROTECT_EXN

  arrow::Status st = (*ptr)->AppendNulls(n);
  status_exn(st);

  OCAML_END_PROTECT_EXN
}


void append_null_double_builder(DoubleBuilderPtr* ptr, int n) {
  OCAML_BEGIN_PROTECT_EXN

  arrow::Status st = (*ptr)->AppendNulls(n);
  status_exn(st);

  OCAML_END_PROTECT_EXN
}


void append_null_string_builder(StringBuilderPtr* ptr, int n) {
  OCAML_BEGIN_PROTECT_EXN

  arrow::Status st = (*ptr)->AppendNulls(n);
  status_exn(st);

  OCAML_END_PROTECT_EXN
}


void free_int64_builder(Int64BuilderPtr* ptr) {
  if (ptr != nullptr) delete ptr;
}

void free_double_builder(DoubleBuilderPtr* ptr) {
  if (ptr != nullptr) delete ptr;
}

void free_string_builder(StringBuilderPtr* ptr) {
  if (ptr != nullptr) delete ptr;
}

int64_t length_int64_builder(Int64BuilderPtr* ptr) {
  return (*ptr)->length();
}
int64_t length_double_builder(DoubleBuilderPtr* ptr) {
  return (*ptr)->length();
}
int64_t length_string_builder(StringBuilderPtr* ptr) {
  return (*ptr)->length();
}

int64_t null_count_int64_builder(Int64BuilderPtr* ptr) {
  return (*ptr)->null_count();
}
int64_t null_count_double_builder(DoubleBuilderPtr* ptr) {
  return (*ptr)->null_count();
}
int64_t null_count_string_builder(StringBuilderPtr* ptr) {
  return (*ptr)->null_count();
}

TablePtr *make_table(BuilderPtr **builders, char **col_names, int n) {
  OCAML_BEGIN_PROTECT_EXN

  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  for (int i = 0; i < n; ++i) {
    std::shared_ptr<arrow::Array> array;
    arrow::Status st = (*builders[i])->Finish(&array);
    status_exn(st);
    schema_vector.push_back(arrow::field(col_names[i], array->type()));
    arrays.push_back(std::move(array));
  }
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  auto table = arrow::Table::Make(schema, arrays);
  return new std::shared_ptr<arrow::Table>(std::move(table));

  OCAML_END_PROTECT_EXN
  return nullptr;
}

char *table_to_string(TablePtr *table) {
  OCAML_BEGIN_PROTECT_EXN

  std::string str = (*table)->ToString();
  return strdup(str.c_str());

  OCAML_END_PROTECT_EXN
  return nullptr;
}

/* Below are the non ctypes bindings. */

#include "ctypes_cstubs_internals.h"

extern "C" {
  value fast_col_read(value tbl, value col_idx);
}

value fast_col_read(value tbl, value col_idx) {
  CAMLparam2(tbl, col_idx);
  CAMLlocal4(ocaml_array, ocaml_valid, some, result);

  OCAML_BEGIN_PROTECT_EXN

  TablePtr* table = (TablePtr*)CTYPES_ADDR_OF_FATPTR(tbl);
  long int index = Long_val(col_idx);
  bool has_valid = false;

  std::shared_ptr<arrow::ChunkedArray> array = (*table)->column(index);
  bool has_null = array->null_count();
  int64_t total_len = array->length();
  arrow::Type::type dt = array->type()->id();

  int tag = -1;
  if (arrow::Type::STRING == dt) {
    ocaml_array = caml_alloc_tuple(total_len);
    tag = has_null ? 1 : 0;
    long int res_index = 0;
    for (int chunk_idx = 0; chunk_idx < array->num_chunks(); ++chunk_idx) {
      std::shared_ptr<arrow::Array> chunk = array->chunk(chunk_idx);
      auto str_array = std::dynamic_pointer_cast<arrow::StringArray>(chunk);
      if (str_array == nullptr) throw std::invalid_argument("not a string array");
      int64_t chunk_len = str_array->length();
      if (has_null) {
        for (int64_t row_index = 0; row_index < chunk_len; ++row_index) {
          if (str_array->IsValid(row_index)) {
            some = caml_alloc_tuple(1);
            int len = 0;
            char *ptr = (char*)str_array->GetValue(row_index, &len);
            Store_field(some, 0, caml_alloc_initialized_string(len, ptr));
            Store_field(ocaml_array, res_index++, some);
          }
          else {
            Store_field(ocaml_array, res_index++, Val_int(0));
          }
        }
      }
      else {
        for (int64_t row_index = 0; row_index < chunk_len; ++row_index) {
          int len = 0;
          char *ptr = (char*)str_array->GetValue(row_index, &len);
          Store_field(ocaml_array, res_index++, caml_alloc_initialized_string(len, ptr));
        }
      }
    }
  }
  else if (arrow::Type::INT64 == dt) {
    uint8_t *valid_ptr = nullptr;
    if (has_null) {
      has_valid = true;
      ocaml_valid = caml_ba_alloc_dims(CAML_BA_UINT8 | CAML_BA_C_LAYOUT, 1, nullptr, (total_len+7)/8);
      valid_ptr = (uint8_t*)Caml_ba_data_val(ocaml_valid);
    }
    ocaml_array = caml_ba_alloc_dims(CAML_BA_INT64 | CAML_BA_C_LAYOUT, 1, nullptr, total_len);
    int64_t *data_ptr = (int64_t*)Caml_ba_data_val(ocaml_array);
    tag = has_null ? 3 : 2;
    long int res_index = 0;
    for (int chunk_idx = 0; chunk_idx < array->num_chunks(); ++chunk_idx) {
      std::shared_ptr<arrow::Array> chunk = array->chunk(chunk_idx);
      auto int64_array = std::dynamic_pointer_cast<arrow::Int64Array>(chunk);
      if (int64_array == nullptr) throw std::invalid_argument("not a int64 array");
      int64_t chunk_len = int64_array->length();
      if (has_null) {
        arrow::internal::CopyBitmap(
          int64_array->null_bitmap_data(),
          int64_array->offset(),
          chunk_len,
          valid_ptr,
          res_index);
      }
      // raw_values has the offset applied
      memcpy(data_ptr + res_index, int64_array->raw_values(), chunk_len * sizeof(int64_t));
      res_index += chunk_len;
    }
  }
  else if (arrow::Type::DOUBLE == dt) {
    uint8_t *valid_ptr = nullptr;
    if (has_null) {
      has_valid = true;
      ocaml_valid = caml_ba_alloc_dims(CAML_BA_UINT8 | CAML_BA_C_LAYOUT, 1, nullptr, (total_len+7)/8);
      valid_ptr = (uint8_t*)Caml_ba_data_val(ocaml_valid);
    }
    ocaml_array = caml_ba_alloc_dims(CAML_BA_FLOAT64 | CAML_BA_C_LAYOUT, 1, nullptr, total_len);
    double *data_ptr = (double*)Caml_ba_data_val(ocaml_array);
    tag = has_null ? 5 : 4;
    long int res_index = 0;
    for (int chunk_idx = 0; chunk_idx < array->num_chunks(); ++chunk_idx) {
      std::shared_ptr<arrow::Array> chunk = array->chunk(chunk_idx);
      auto double_array = std::dynamic_pointer_cast<arrow::DoubleArray>(chunk);
      if (double_array == nullptr) throw std::invalid_argument("not a double array");
      int64_t chunk_len = double_array->length();
      if (has_null) {
        arrow::internal::CopyBitmap(
          double_array->null_bitmap_data(),
          double_array->offset(),
          chunk_len,
          valid_ptr,
          res_index);
      }
      // raw_values has the offset applied
      memcpy(data_ptr + res_index, double_array->raw_values(), chunk_len * sizeof(double));
      res_index += chunk_len;
    }
  }

  if (tag == -1) {
    result = Val_int(0);
  }
  else if (has_valid) {
    result = caml_alloc_small(2, tag);
    Store_field(result, 0, ocaml_array);
    Store_field(result, 1, ocaml_valid);
  }
  else {
    result = caml_alloc_small(1, tag);
    Store_field(result, 0, ocaml_array);
  }

  OCAML_END_PROTECT_EXN

  CAMLreturn(result);
}
