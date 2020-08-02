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

let to_cint = function
  | Uncompressed -> 0
  | Snappy -> 1
  | Gzip -> 2
  | Brotli -> 3
  | Zstd -> 4
  | Lz4 -> 5
  | Lz4_frame -> 6
  | Lzo -> 7
  | Bz2 -> 8
