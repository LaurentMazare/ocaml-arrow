#include "arrow_c_api.h"

#include<iostream>

#include<caml/fail.h>

struct ArrowSchema *get_schema(FileReaderPtr *reader) {
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status st = (*reader)->GetSchema(&schema);
  if (!st.ok()) {
    caml_failwith(st.ToString().c_str());
  }
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

// Returns a pointer on the shared-ptr so that deletion can be handled down the
// line through the close_file function.
FileReaderPtr *read_file(char *filename) {
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
  return new std::shared_ptr<parquet::arrow::FileReader>(std::move(reader));
}

void close_file(FileReaderPtr *reader) {
  if (reader != NULL)
    delete reader;
}

int64_t num_rows_file(FileReaderPtr *reader) {
  if (reader == NULL || (*reader)->parquet_reader() == NULL) {
    caml_failwith("num_rows_file got a null ptr");
  }
  return (*reader)->parquet_reader()->metadata()->num_rows();
}

struct ArrowArray *chunked_column(FileReaderPtr *reader, int column_idx, int *nchunks, int dt) {
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
    else {
      char err[128];
      snprintf(err, 127, "unknown datatype %d", dt);
      caml_failwith(err);
    }

    std::shared_ptr<arrow::ChunkedArray> array;
    arrow::Status st = (*reader)->ReadColumn(column_idx, &array);
    if (!st.ok()) {
      caml_failwith(st.ToString().c_str());
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

void free_chunked_column(struct ArrowArray *arrays, int nchunks) {
  for (int i = 0; i < nchunks; ++i) {
    if (arrays[i].release != NULL) arrays[i].release(arrays + i);
  }
  free(arrays);
}

void write_file(char *filename, struct ArrowArray *array, struct ArrowSchema *schema, int chunk_size, int compression) {
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
  arrow::Compression::type compression_ = arrow::Compression::UNCOMPRESSED;
  if (compression == 1) compression_ = arrow::Compression::SNAPPY;
  else if (compression == 2) compression_ = arrow::Compression::GZIP;
  else if (compression == 3) compression_ = arrow::Compression::BROTLI;
  else if (compression == 4) compression_ = arrow::Compression::ZSTD;
  else if (compression == 5) compression_ = arrow::Compression::LZ4;
  else if (compression == 6) compression_ = arrow::Compression::LZ4_FRAME;
  else if (compression == 7) compression_ = arrow::Compression::LZO;
  else if (compression == 8) compression_ = arrow::Compression::BZ2;
  arrow::Status st = parquet::arrow::WriteTable(*(table.ValueOrDie()),
                                                arrow::default_memory_pool(),
                                                outfile,
                                                chunk_size,
                                                parquet::WriterProperties::Builder().compression(compression_)->build(),
                                                parquet::ArrowWriterProperties::Builder().enable_deprecated_int96_timestamps()->build());
  if (!st.ok()) {
    caml_failwith(st.ToString().c_str());
  }
}
