#ifndef __OCAML_ARROW_C_API__
#define __OCAML_ARROW_C_API__

#include<arrow/c/abi.h>

#ifdef __cplusplus
#include<arrow/c/bridge.h>
#include<arrow/api.h>
#include<arrow/csv/api.h>
#include<arrow/io/api.h>
#include<arrow/json/api.h>
#include<arrow/ipc/api.h>
#include<arrow/ipc/feather.h>
#include<arrow/util/bitmap_ops.h>
#include<parquet/arrow/reader.h>
#include<parquet/arrow/writer.h>
#include<parquet/exception.h>

typedef std::shared_ptr<arrow::Table> TablePtr;
typedef std::shared_ptr<arrow::StringBuilder> StringBuilderPtr;
typedef std::shared_ptr<arrow::Int64Builder> Int64BuilderPtr;
typedef std::shared_ptr<arrow::DoubleBuilder> DoubleBuilderPtr;

struct ParquetReader {
  std::unique_ptr<parquet::arrow::FileReader> reader;
  std::unique_ptr<arrow::RecordBatchReader> batch_reader;
};

extern "C" {
#else
typedef void TablePtr;
typedef void ParquetReader;
typedef void StringBuilderPtr;
typedef void Int64BuilderPtr;
typedef void DoubleBuilderPtr;
#endif

struct ArrowSchema *arrow_schema(char*);
struct ArrowSchema *feather_schema(char*);
struct ArrowSchema *parquet_schema(char*, int64_t *num_rows);
struct ArrowSchema *alloc_schema(char*, char*);
void free_schema(struct ArrowSchema*);

TablePtr *parquet_read_table(char *, int *col_idxs, int ncols, int use_threads, int64_t only_first);
TablePtr *feather_read_table(char *, int *col_idxs, int ncols);
TablePtr *csv_read_table(char *);
TablePtr *json_read_table(char *);
TablePtr *table_concatenate(TablePtr **tables, int ntables);
TablePtr *table_slice(TablePtr*, int64_t, int64_t);
int64_t table_num_rows(TablePtr*);
struct ArrowSchema *table_schema(TablePtr*);
void free_table(TablePtr*);

struct ArrowArray *table_chunked_column(TablePtr *reader, int column_idx, int *nchunks, int dt);
struct ArrowArray *table_chunked_column_by_name(TablePtr *reader, char *column_name, int *nchunks, int dt);
void free_chunked_column(struct ArrowArray *, int nchunks);

TablePtr *create_table(struct ArrowArray *array, struct ArrowSchema *schema);
void arrow_write_file(char *filename, struct ArrowArray *, struct ArrowSchema *, int chunk_size);
void parquet_write_file(char *filename, struct ArrowArray *, struct ArrowSchema *, int chunk_size, int compression);
void feather_write_file(char *filename, struct ArrowArray *, struct ArrowSchema *, int chunk_size, int compression);
void parquet_write_table(char *filename, TablePtr *table, int chunk_size, int compression);
void feather_write_table(char *filename, TablePtr *table, int chunk_size, int compression);

ParquetReader *parquet_reader_open(char *filename, int *col_idxs, int ncols, int use_threads, int mmap, int buffer_size, int batch_size);
TablePtr *parquet_reader_next(ParquetReader *pr);
void parquet_reader_close(ParquetReader *pr);
void parquet_reader_free(ParquetReader *pr);

Int64BuilderPtr *create_int64_builder();
DoubleBuilderPtr *create_double_builder();
StringBuilderPtr *create_string_builder();
void append_int64_builder(Int64BuilderPtr*, int64_t);
void append_double_builder(DoubleBuilderPtr*, double);
void append_string_builder(StringBuilderPtr*, char*);
void append_null_int64_builder(Int64BuilderPtr*, int);
void append_null_double_builder(DoubleBuilderPtr*, int);
void append_null_string_builder(StringBuilderPtr*, int);
void free_int64_builder(Int64BuilderPtr*);
void free_double_builder(DoubleBuilderPtr*);
void free_string_builder(StringBuilderPtr*);
int64_t length_int64_builder(Int64BuilderPtr*);
int64_t length_double_builder(DoubleBuilderPtr*);
int64_t length_string_builder(StringBuilderPtr*);
int64_t null_count_int64_builder(Int64BuilderPtr*);
int64_t null_count_double_builder(DoubleBuilderPtr*);
int64_t null_count_string_builder(StringBuilderPtr*);
#ifdef __cplusplus
}
#endif
#endif
