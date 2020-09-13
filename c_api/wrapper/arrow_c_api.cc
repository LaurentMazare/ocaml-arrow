#include "arrow_c_api.h"

#include<iostream>

#include <caml/mlvalues.h>
#include <caml/threads.h>
#include<caml/fail.h>

struct ArrowSchema *feather_schema(char *filename) {
  arrow::Status st;
  auto file = arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool());
  if (!file.ok()) {
    caml_failwith(file.status().ToString().c_str());
  }
  std::shared_ptr<arrow::io::RandomAccessFile> infile = file.ValueOrDie();
  auto reader = arrow::ipc::feather::Reader::Open(infile);
  if (!reader.ok()) {
    caml_failwith(reader.status().ToString().c_str());
  }
  std::shared_ptr<arrow::Schema> schema = reader.ValueOrDie()->schema();
  struct ArrowSchema *out = (struct ArrowSchema*)malloc(sizeof *out);
  arrow::ExportSchema(*schema, out);
  return out;
}

struct ArrowSchema *parquet_schema(char *filename, int64_t *num_rows) {
  arrow::Status st;
  auto file = arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool());
  if (!file.ok()) {
    caml_failwith(file.status().ToString().c_str());
  }
  std::shared_ptr<arrow::io::RandomAccessFile> infile = file.ValueOrDie();
  std::unique_ptr<parquet::arrow::FileReader> reader;
  st = parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader);
  if (!st.ok()) {
    caml_failwith(st.ToString().c_str());
  }
  std::shared_ptr<arrow::Schema> schema;
  st = reader->GetSchema(&schema);
  if (!st.ok()) {
    caml_failwith(st.ToString().c_str());
  }
  *num_rows = reader->parquet_reader()->metadata()->num_rows();
  struct ArrowSchema *out = (struct ArrowSchema*)malloc(sizeof *out);
  arrow::ExportSchema(*schema, out);
  return out;
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

struct ArrowArray *table_chunked_column_(TablePtr *table, char *column_name, int column_idx, int *nchunks, int dt) {
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
    else {
      char err[128];
      snprintf(err, 127, "unknown datatype %d", dt);
      caml_failwith(err);
    }
    std::shared_ptr<arrow::ChunkedArray> array;
    if (column_name) {
      array = (*table)->GetColumnByName(std::string(column_name));
      if (!array) {
        char err[128];
        snprintf(err, 127, "cannot find column %s", column_name);
        caml_failwith(err);
      }
    }
    else {
      array = (*table)->column(column_idx);
    }
    if (!array) {
      caml_failwith("error finding column");
    }
    *nchunks = array->num_chunks();
    struct ArrowArray *out = (struct ArrowArray*)malloc(array->num_chunks() * sizeof *out);
    for (int i = 0; i < array->num_chunks(); ++i) {
      auto chunk = array->chunk(i);
      if (chunk->type()->id() != expected_type) {
        char err[128];
        snprintf(err, 127,
                 "expected type with %s (id %d) got %s",
                 expected_type_str,
                 expected_type,
                 chunk->type()->ToString().c_str());
        caml_failwith(err);
      }
      arrow::ExportArray(*chunk, out + i);
    }
    return out;
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
  auto record_batch = arrow::ImportRecordBatch(array, schema);
  if (!record_batch.ok()) {
    caml_failwith(record_batch.status().ToString().c_str());
  }
  auto table = arrow::Table::FromRecordBatches({record_batch.ValueOrDie()});
  if (!table.ok()) {
    caml_failwith(table.status().ToString().c_str());
  }
  return new std::shared_ptr<arrow::Table>(std::move(table.ValueOrDie()));
}

void parquet_write_file(char *filename, struct ArrowArray *array, struct ArrowSchema *schema, int chunk_size, int compression) {
  auto file = arrow::io::FileOutputStream::Open(filename);
  if (!file.ok()) {
    caml_failwith(file.status().ToString().c_str());
  }
  auto outfile = file.ValueOrDie();
  auto record_batch = arrow::ImportRecordBatch(array, schema);
  if (!record_batch.ok()) {
    caml_failwith(record_batch.status().ToString().c_str());
  }
  auto table = arrow::Table::FromRecordBatches({record_batch.ValueOrDie()});
  if (!table.ok()) {
    caml_failwith(table.status().ToString().c_str());
  }
  arrow::Compression::type compression_ = compression_of_int(compression);
  caml_release_runtime_system();
  arrow::Status st = parquet::arrow::WriteTable(*(table.ValueOrDie()),
                                                arrow::default_memory_pool(),
                                                outfile,
                                                chunk_size,
                                                parquet::WriterProperties::Builder().compression(compression_)->build(),
                                                parquet::ArrowWriterProperties::Builder().enable_deprecated_int96_timestamps()->build());
  caml_acquire_runtime_system();
  if (!st.ok()) {
    caml_failwith(st.ToString().c_str());
  }
}

void feather_write_file(char *filename, struct ArrowArray *array, struct ArrowSchema *schema, int chunk_size, int compression) {
  auto file = arrow::io::FileOutputStream::Open(filename);
  if (!file.ok()) {
    caml_failwith(file.status().ToString().c_str());
  }
  auto outfile = file.ValueOrDie();
  auto record_batch = arrow::ImportRecordBatch(array, schema);
  if (!record_batch.ok()) {
    caml_failwith(record_batch.status().ToString().c_str());
  }
  auto table = arrow::Table::FromRecordBatches({record_batch.ValueOrDie()});
  if (!table.ok()) {
    caml_failwith(table.status().ToString().c_str());
  }
  struct arrow::ipc::feather::WriteProperties wp;
  wp.compression = compression_of_int(compression);
  wp.chunksize = chunk_size;
  caml_release_runtime_system();
  arrow::Status st = arrow::ipc::feather::WriteTable(*(table.ValueOrDie()),
                                                &(*outfile),
                                                wp);
  caml_acquire_runtime_system();
  if (!st.ok()) {
    caml_failwith(st.ToString().c_str());
  }
}

void parquet_write_table(char *filename, TablePtr *table, int chunk_size, int compression) {
  auto file = arrow::io::FileOutputStream::Open(filename);
  if (!file.ok()) {
    caml_failwith(file.status().ToString().c_str());
  }
  auto outfile = file.ValueOrDie();
  arrow::Compression::type compression_ = compression_of_int(compression);
  caml_release_runtime_system();
  arrow::Status st = parquet::arrow::WriteTable(**table,
                                                arrow::default_memory_pool(),
                                                outfile,
                                                chunk_size,
                                                parquet::WriterProperties::Builder().compression(compression_)->build(),
                                                parquet::ArrowWriterProperties::Builder().enable_deprecated_int96_timestamps()->build());
  caml_acquire_runtime_system();
  if (!st.ok()) {
    caml_failwith(st.ToString().c_str());
  }
}

void feather_write_table(char *filename, TablePtr *table, int chunk_size, int compression) {
  auto file = arrow::io::FileOutputStream::Open(filename);
  if (!file.ok()) {
    caml_failwith(file.status().ToString().c_str());
  }
  auto outfile = file.ValueOrDie();
  struct arrow::ipc::feather::WriteProperties wp;
  wp.compression = compression_of_int(compression);
  wp.chunksize = chunk_size;
  caml_release_runtime_system();
  arrow::Status st = arrow::ipc::feather::WriteTable(**table, &(*outfile), wp);
  caml_acquire_runtime_system();
  if (!st.ok()) {
    caml_failwith(st.ToString().c_str());
  }
}

TablePtr *parquet_read_table(char *filename, int *col_idxs, int ncols, int use_threads, int64_t only_first) {
  arrow::Status st;
  auto file = arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool());
  if (!file.ok()) {
    caml_failwith(file.status().ToString().c_str());
  }
  std::shared_ptr<arrow::io::RandomAccessFile> infile = file.ValueOrDie();
  std::unique_ptr<parquet::arrow::FileReader> reader;
  st = parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader);
  if (!st.ok()) {
    caml_failwith(st.ToString().c_str());
  }
  if (use_threads >= 0) reader->set_use_threads(use_threads);
  std::shared_ptr<arrow::Table> table;
  caml_release_runtime_system();
  if (only_first < 0) {
    if (ncols)
      st = reader->ReadTable(std::vector<int>(col_idxs, col_idxs+ncols), &table);
    else
      st = reader->ReadTable(&table);
    if (!st.ok()) {
      caml_acquire_runtime_system();
      caml_failwith(st.ToString().c_str());
    }
  } else {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    for (int row_group_idx = 0; row_group_idx < reader->num_row_groups(); ++row_group_idx) {
      std::unique_ptr<arrow::RecordBatchReader> batch_reader;
      if (ncols)
        st = reader->GetRecordBatchReader({row_group_idx}, std::vector<int>(col_idxs, col_idxs+ncols), &batch_reader);
      else
        st = reader->GetRecordBatchReader({row_group_idx}, &batch_reader);
      if (!st.ok()) {
        caml_acquire_runtime_system();
        caml_failwith(st.ToString().c_str());
      }
      std::shared_ptr<arrow::RecordBatch> batch;
      while (only_first > 0) {
        st = batch_reader->ReadNext(&batch);
        if (batch == nullptr) break;
        if (!st.ok()) {
          caml_acquire_runtime_system();
          caml_failwith(st.ToString().c_str());
        }
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
    if (!table_.ok()) {
      caml_acquire_runtime_system();
      caml_failwith(table_.status().ToString().c_str());
    }
    table = std::move(table_.ValueOrDie());
  }
  caml_acquire_runtime_system();
  return new std::shared_ptr<arrow::Table>(std::move(table));
}

TablePtr *feather_read_table(char *filename, int *col_idxs, int ncols) {
  arrow::Status st;
  auto file = arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool());
  if (!file.ok()) {
    caml_failwith(file.status().ToString().c_str());
  }
  std::shared_ptr<arrow::io::RandomAccessFile> infile = file.ValueOrDie();
  auto reader = arrow::ipc::feather::Reader::Open(infile);
  if (!reader.ok()) {
    caml_failwith(reader.status().ToString().c_str());
  }
  std::shared_ptr<arrow::Table> table;
  caml_release_runtime_system();
  if (ncols)
    st = reader.ValueOrDie()->Read(std::vector<int>(col_idxs, col_idxs+ncols), &table);
  else
    st = reader.ValueOrDie()->Read(&table);
  caml_acquire_runtime_system();
  if (!st.ok()) {
    caml_failwith(st.ToString().c_str());
  }
  return new std::shared_ptr<arrow::Table>(std::move(table));
}

TablePtr *csv_read_table(char *filename) {
  auto file = arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool());
  if (!file.ok()) {
    caml_failwith(file.status().ToString().c_str());
  }
  std::shared_ptr<arrow::io::RandomAccessFile> infile = file.ValueOrDie();

  auto reader =
    arrow::csv::TableReader::Make(arrow::default_memory_pool(),
                                  infile,
                                  arrow::csv::ReadOptions::Defaults(),
                                  arrow::csv::ParseOptions::Defaults(),
                                  arrow::csv::ConvertOptions::Defaults());

  if (!reader.ok()) {
    caml_failwith(reader.status().ToString().c_str());
  }
  caml_release_runtime_system();
  auto table = reader.ValueOrDie()->Read();
  caml_acquire_runtime_system();
  if (!table.ok()) {
    caml_failwith(table.status().ToString().c_str());
  }
  return new std::shared_ptr<arrow::Table>(std::move(table.ValueOrDie()));
}

TablePtr *json_read_table(char *filename) {
  arrow::Status st;
  auto file = arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool());
  if (!file.ok()) {
    caml_failwith(file.status().ToString().c_str());
  }
  std::shared_ptr<arrow::io::RandomAccessFile> infile = file.ValueOrDie();

  std::shared_ptr<arrow::json::TableReader> reader;
  st = arrow::json::TableReader::Make(arrow::default_memory_pool(),
                                      infile,
                                      arrow::json::ReadOptions::Defaults(),
                                      arrow::json::ParseOptions::Defaults(),
                                      &reader);

  if (!st.ok()) {
    caml_failwith(st.ToString().c_str());
  }
  std::shared_ptr<arrow::Table> table;
  caml_release_runtime_system();
  st = reader->Read(&table);
  caml_acquire_runtime_system();
  if (!st.ok()) {
    caml_failwith(st.ToString().c_str());
  }
  return new std::shared_ptr<arrow::Table>(std::move(table));
}

TablePtr *table_concatenate(TablePtr **tables, int ntables) {
  std::vector<std::shared_ptr<arrow::Table>> vec;
  for (int i = 0; i < ntables; ++i) vec.push_back(**(tables+i));
  auto table = arrow::ConcatenateTables(vec);
  if (!table.ok()) {
    caml_failwith(table.status().ToString().c_str());
  }
  return new std::shared_ptr<arrow::Table>(std::move(table.ValueOrDie()));
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
