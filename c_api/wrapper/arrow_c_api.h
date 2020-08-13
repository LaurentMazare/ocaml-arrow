#ifndef __OCAML_ARROW_C_API__
#define __OCAML_ARROW_C_API__

#include<arrow/c/abi.h>

#ifdef __cplusplus
#include<arrow/c/bridge.h>
#include<arrow/api.h>
#include<arrow/csv/api.h>
#include<arrow/io/api.h>
#include<arrow/json/api.h>
#include<arrow/ipc/feather.h>
#include<parquet/arrow/reader.h>
#include<parquet/arrow/writer.h>
#include<parquet/exception.h>

typedef std::shared_ptr<arrow::Table> TablePtr;
extern "C" {
#else
typedef void TablePtr;
#endif

struct ArrowSchema *feather_schema(char*);
struct ArrowSchema *parquet_schema(char*, int64_t *num_rows);
struct ArrowSchema *alloc_schema(char*, char*);
void free_schema(struct ArrowSchema*);

TablePtr *parquet_read_table(char *, int *col_idxs, int ncols, int use_threads, int64_t only_first);
TablePtr *feather_read_table(char *, int *col_idxs, int ncols);
TablePtr *csv_read_table(char *);
TablePtr *json_read_table(char *);
TablePtr *table_slice(TablePtr*, int64_t, int64_t);
int64_t table_num_rows(TablePtr*);
struct ArrowSchema *table_schema(TablePtr*);
void free_table(TablePtr*);

struct ArrowArray *table_chunked_column(TablePtr *reader, int column_idx, int *nchunks, int dt);
struct ArrowArray *table_chunked_column_by_name(TablePtr *reader, char *column_name, int *nchunks, int dt);
void free_chunked_column(struct ArrowArray *, int nchunks);

void parquet_write_file(char *filename, struct ArrowArray *, struct ArrowSchema *, int chunk_size, int compression);
void feather_write_file(char *filename, struct ArrowArray *, struct ArrowSchema *, int chunk_size, int compression);
void parquet_write_table(char *filename, TablePtr *table, int chunk_size, int compression);
void feather_write_table(char *filename, TablePtr *table, int chunk_size, int compression);
#ifdef __cplusplus
}
#endif
#endif
