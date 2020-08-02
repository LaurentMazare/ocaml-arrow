#ifndef __OCAML_ARROW_C_API__
#define __OCAML_ARROW_C_API__

#include<arrow/c/abi.h>

#ifdef __cplusplus
#include<arrow/c/bridge.h>
#include<arrow/api.h>
#include<arrow/io/api.h>
#include<parquet/arrow/reader.h>
#include<parquet/arrow/writer.h>
#include<parquet/exception.h>

typedef std::shared_ptr<parquet::arrow::FileReader> FileReaderPtr;
extern "C" {
#else
typedef void FileReaderPtr;
#endif

struct ArrowSchema *get_schema(FileReaderPtr*);
struct ArrowSchema *alloc_schema(char*, char*);
void free_schema(struct ArrowSchema*);

FileReaderPtr *read_file(char *filename);
void close_file(FileReaderPtr*);
int64_t num_rows_file(FileReaderPtr*);

struct ArrowArray *chunked_column(FileReaderPtr *reader, int column_idx, int *nchunks, int dt);
void free_chunked_column(struct ArrowArray *, int nchunks);

void write_file(char *filename, struct ArrowArray *, struct ArrowSchema *, int chunk_size);
#ifdef __cplusplus
}
#endif
#endif
