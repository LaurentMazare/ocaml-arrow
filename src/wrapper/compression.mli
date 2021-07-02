type t =
  | Uncompressed
  | Snappy
  | Gzip
  | Brotli
  | Zstd
  | Lz4
  | Lz4_frame
  | Lzo
  | Bz2

val to_cint : t -> int
