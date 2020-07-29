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

static void release_schema(struct ArrowSchema* schema) {
  for (int64_t i = 0; i < schema->n_children; ++i) {
    struct ArrowSchema* child = schema->children[i];
    if (child->release != NULL) {
      child->release(child);
      assert(child->release == NULL);
    }
    if (child != NULL) {
      free((void*)child);
      schema->children[i] = NULL;
    }
  }

  struct ArrowSchema* dict = schema->dictionary;
  if (dict != NULL && dict->release != NULL) {
    dict->release(dict);
    assert(dict->release == NULL);
  }

  if (schema->dictionary != NULL) {
    free((void*)schema->dictionary);
    schema->dictionary = NULL;
  }

  if (schema->children != NULL) {
    free((void*)schema->children);
    schema->children = NULL;
  }

  if (schema->format != NULL) {
    free((void*)schema->format);
    schema->format = NULL;
  }

  if (schema->name != NULL) {
    free((void*)schema->name);
    schema->name = NULL;
  }

  if (schema->metadata != NULL) {
    free((void*)schema->metadata);
    schema->name = NULL;
  }

  schema->release = NULL;
}

struct ArrowSchema* alloc_schema(char *format, char* name) {
  struct ArrowSchema *schema = (struct ArrowSchema*)malloc(sizeof *schema);
  schema->format = strdup(format);
  schema->name = strdup(name);
  schema->metadata = NULL;
  schema->flags = 0;
  schema->n_children = 0;
  schema->children = NULL;
  schema->dictionary = NULL;
  schema->release = &release_schema;
  return schema;
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

struct ArrowArray *chunked_column(FileReaderPtr *reader, int column_idx, int *nchunks, int dt) {
    std::shared_ptr<arrow::DataType> expected_type;
    if (dt == 0) {
      expected_type = arrow::int64();
    }
    else if (dt == 1) {
      expected_type = arrow::float64();
    }
    else if (dt == 2) {
      expected_type = arrow::utf8();
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
      if (chunk->type() != expected_type) {
        char err[128];
        snprintf(err, 127,
                 "expected type %s got %s",
                 expected_type->ToString().c_str(),
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

void write_file(char *filename, struct ArrowArray *array, struct ArrowSchema *schema, int chunk_size) {
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
  arrow::Status st = parquet::arrow::WriteTable(*(table.ValueOrDie()),
                                                arrow::default_memory_pool(),
                                                outfile,
                                                chunk_size);
  if (!st.ok()) {
    caml_failwith(st.ToString().c_str());
  }
}
