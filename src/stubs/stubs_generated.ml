open Ctypes
module C (F : Cstubs.FOREIGN) = struct
  open F
  type array_ = unit ptr
  let array_ : array_ typ = ptr void
  type array_builder = unit ptr
  let array_builder : array_builder typ = ptr void
  type binary_array = unit ptr
  let binary_array : binary_array typ = ptr void
  type binary_array_builder = unit ptr
  let binary_array_builder : binary_array_builder typ = ptr void
  type binary_data_type = unit ptr
  let binary_data_type : binary_data_type typ = ptr void
  type boolean_array = unit ptr
  let boolean_array : boolean_array typ = ptr void
  type boolean_array_builder = unit ptr
  let boolean_array_builder : boolean_array_builder typ = ptr void
  type boolean_data_type = unit ptr
  let boolean_data_type : boolean_data_type typ = ptr void
  type buffer = unit ptr
  let buffer : buffer typ = ptr void
  type buffer_input_stream = unit ptr
  let buffer_input_stream : buffer_input_stream typ = ptr void
  type buffer_output_stream = unit ptr
  let buffer_output_stream : buffer_output_stream typ = ptr void
  type csv_read_options = unit ptr
  let csv_read_options : csv_read_options typ = ptr void
  type csv_reader = unit ptr
  let csv_reader : csv_reader typ = ptr void
  type cast_options = unit ptr
  let cast_options : cast_options typ = ptr void
  type chunked_array = unit ptr
  let chunked_array : chunked_array typ = ptr void
  type codec = unit ptr
  let codec : codec typ = ptr void
  type column = unit ptr
  let column : column typ = ptr void
  type compressed_input_stream = unit ptr
  let compressed_input_stream : compressed_input_stream typ = ptr void
  type compressed_output_stream = unit ptr
  let compressed_output_stream : compressed_output_stream typ = ptr void
  type count_options = unit ptr
  let count_options : count_options typ = ptr void
  type data_type = unit ptr
  let data_type : data_type typ = ptr void
  type date32_array = unit ptr
  let date32_array : date32_array typ = ptr void
  type date32_array_builder = unit ptr
  let date32_array_builder : date32_array_builder typ = ptr void
  type date32_data_type = unit ptr
  let date32_data_type : date32_data_type typ = ptr void
  type date64_array = unit ptr
  let date64_array : date64_array typ = ptr void
  type date64_array_builder = unit ptr
  let date64_array_builder : date64_array_builder typ = ptr void
  type date64_data_type = unit ptr
  let date64_data_type : date64_data_type typ = ptr void
  type decimal128 = unit ptr
  let decimal128 : decimal128 typ = ptr void
  type decimal128_array = unit ptr
  let decimal128_array : decimal128_array typ = ptr void
  type decimal128_array_builder = unit ptr
  let decimal128_array_builder : decimal128_array_builder typ = ptr void
  type decimal128_data_type = unit ptr
  let decimal128_data_type : decimal128_data_type typ = ptr void
  type decimal_data_type = unit ptr
  let decimal_data_type : decimal_data_type typ = ptr void
  type dense_union_array = unit ptr
  let dense_union_array : dense_union_array typ = ptr void
  type dense_union_data_type = unit ptr
  let dense_union_data_type : dense_union_data_type typ = ptr void
  type dictionary_array = unit ptr
  let dictionary_array : dictionary_array typ = ptr void
  type dictionary_data_type = unit ptr
  let dictionary_data_type : dictionary_data_type typ = ptr void
  type double_array = unit ptr
  let double_array : double_array typ = ptr void
  type double_array_builder = unit ptr
  let double_array_builder : double_array_builder typ = ptr void
  type double_data_type = unit ptr
  let double_data_type : double_data_type typ = ptr void
  type feather_file_reader = unit ptr
  let feather_file_reader : feather_file_reader typ = ptr void
  type feather_file_writer = unit ptr
  let feather_file_writer : feather_file_writer typ = ptr void
  type field = unit ptr
  let field : field typ = ptr void
  type file_output_stream = unit ptr
  let file_output_stream : file_output_stream typ = ptr void
  type fixed_size_binary_array = unit ptr
  let fixed_size_binary_array : fixed_size_binary_array typ = ptr void
  type fixed_size_binary_data_type = unit ptr
  let fixed_size_binary_data_type : fixed_size_binary_data_type typ = ptr void
  type fixed_width_data_type = unit ptr
  let fixed_width_data_type : fixed_width_data_type typ = ptr void
  type float_array = unit ptr
  let float_array : float_array typ = ptr void
  type float_array_builder = unit ptr
  let float_array_builder : float_array_builder typ = ptr void
  type float_data_type = unit ptr
  let float_data_type : float_data_type typ = ptr void
  type floating_point_data_type = unit ptr
  let floating_point_data_type : floating_point_data_type typ = ptr void
  type gio_input_stream = unit ptr
  let gio_input_stream : gio_input_stream typ = ptr void
  type gio_output_stream = unit ptr
  let gio_output_stream : gio_output_stream typ = ptr void
  type input_stream = unit ptr
  let input_stream : input_stream typ = ptr void
  type int16_array = unit ptr
  let int16_array : int16_array typ = ptr void
  type int16_array_builder = unit ptr
  let int16_array_builder : int16_array_builder typ = ptr void
  type int16_data_type = unit ptr
  let int16_data_type : int16_data_type typ = ptr void
  type int32_array = unit ptr
  let int32_array : int32_array typ = ptr void
  type int32_array_builder = unit ptr
  let int32_array_builder : int32_array_builder typ = ptr void
  type int32_data_type = unit ptr
  let int32_data_type : int32_data_type typ = ptr void
  type int64_array = unit ptr
  let int64_array : int64_array typ = ptr void
  type int64_array_builder = unit ptr
  let int64_array_builder : int64_array_builder typ = ptr void
  type int64_data_type = unit ptr
  let int64_data_type : int64_data_type typ = ptr void
  type int8_array = unit ptr
  let int8_array : int8_array typ = ptr void
  type int8_array_builder = unit ptr
  let int8_array_builder : int8_array_builder typ = ptr void
  type int8_data_type = unit ptr
  let int8_data_type : int8_data_type typ = ptr void
  type int_array_builder = unit ptr
  let int_array_builder : int_array_builder typ = ptr void
  type integer_data_type = unit ptr
  let integer_data_type : integer_data_type typ = ptr void
  type list_array = unit ptr
  let list_array : list_array typ = ptr void
  type list_array_builder = unit ptr
  let list_array_builder : list_array_builder typ = ptr void
  type list_data_type = unit ptr
  let list_data_type : list_data_type typ = ptr void
  type memory_mapped_input_stream = unit ptr
  let memory_mapped_input_stream : memory_mapped_input_stream typ = ptr void
  type mutable_buffer = unit ptr
  let mutable_buffer : mutable_buffer typ = ptr void
  type null_array = unit ptr
  let null_array : null_array typ = ptr void
  type null_array_builder = unit ptr
  let null_array_builder : null_array_builder typ = ptr void
  type null_data_type = unit ptr
  let null_data_type : null_data_type typ = ptr void
  type numeric_array = unit ptr
  let numeric_array : numeric_array typ = ptr void
  type numeric_data_type = unit ptr
  let numeric_data_type : numeric_data_type typ = ptr void
  type output_stream = unit ptr
  let output_stream : output_stream typ = ptr void
  type primitive_array = unit ptr
  let primitive_array : primitive_array typ = ptr void
  type record_batch = unit ptr
  let record_batch : record_batch typ = ptr void
  type record_batch_builder = unit ptr
  let record_batch_builder : record_batch_builder typ = ptr void
  type record_batch_file_reader = unit ptr
  let record_batch_file_reader : record_batch_file_reader typ = ptr void
  type record_batch_file_writer = unit ptr
  let record_batch_file_writer : record_batch_file_writer typ = ptr void
  type record_batch_reader = unit ptr
  let record_batch_reader : record_batch_reader typ = ptr void
  type record_batch_stream_reader = unit ptr
  let record_batch_stream_reader : record_batch_stream_reader typ = ptr void
  type record_batch_stream_writer = unit ptr
  let record_batch_stream_writer : record_batch_stream_writer typ = ptr void
  type record_batch_writer = unit ptr
  let record_batch_writer : record_batch_writer typ = ptr void
  type resizable_buffer = unit ptr
  let resizable_buffer : resizable_buffer typ = ptr void
  type schema = unit ptr
  let schema : schema typ = ptr void
  type seekable_input_stream = unit ptr
  let seekable_input_stream : seekable_input_stream typ = ptr void
  type sparse_union_array = unit ptr
  let sparse_union_array : sparse_union_array typ = ptr void
  type sparse_union_data_type = unit ptr
  let sparse_union_data_type : sparse_union_data_type typ = ptr void
  type string_array = unit ptr
  let string_array : string_array typ = ptr void
  type string_array_builder = unit ptr
  let string_array_builder : string_array_builder typ = ptr void
  type string_data_type = unit ptr
  let string_data_type : string_data_type typ = ptr void
  type struct_array = unit ptr
  let struct_array : struct_array typ = ptr void
  type struct_array_builder = unit ptr
  let struct_array_builder : struct_array_builder typ = ptr void
  type struct_data_type = unit ptr
  let struct_data_type : struct_data_type typ = ptr void
  type table = unit ptr
  let table : table typ = ptr void
  type table_batch_reader = unit ptr
  let table_batch_reader : table_batch_reader typ = ptr void
  type tensor = unit ptr
  let tensor : tensor typ = ptr void
  type time32_array = unit ptr
  let time32_array : time32_array typ = ptr void
  type time32_array_builder = unit ptr
  let time32_array_builder : time32_array_builder typ = ptr void
  type time32_data_type = unit ptr
  let time32_data_type : time32_data_type typ = ptr void
  type time64_array = unit ptr
  let time64_array : time64_array typ = ptr void
  type time64_array_builder = unit ptr
  let time64_array_builder : time64_array_builder typ = ptr void
  type time64_data_type = unit ptr
  let time64_data_type : time64_data_type typ = ptr void
  type time_data_type = unit ptr
  let time_data_type : time_data_type typ = ptr void
  type timestamp_array = unit ptr
  let timestamp_array : timestamp_array typ = ptr void
  type timestamp_array_builder = unit ptr
  let timestamp_array_builder : timestamp_array_builder typ = ptr void
  type timestamp_data_type = unit ptr
  let timestamp_data_type : timestamp_data_type typ = ptr void
  type u_int16_array = unit ptr
  let u_int16_array : u_int16_array typ = ptr void
  type u_int16_array_builder = unit ptr
  let u_int16_array_builder : u_int16_array_builder typ = ptr void
  type u_int16_data_type = unit ptr
  let u_int16_data_type : u_int16_data_type typ = ptr void
  type u_int32_array = unit ptr
  let u_int32_array : u_int32_array typ = ptr void
  type u_int32_array_builder = unit ptr
  let u_int32_array_builder : u_int32_array_builder typ = ptr void
  type u_int32_data_type = unit ptr
  let u_int32_data_type : u_int32_data_type typ = ptr void
  type u_int64_array = unit ptr
  let u_int64_array : u_int64_array typ = ptr void
  type u_int64_array_builder = unit ptr
  let u_int64_array_builder : u_int64_array_builder typ = ptr void
  type u_int64_data_type = unit ptr
  let u_int64_data_type : u_int64_data_type typ = ptr void
  type u_int8_array = unit ptr
  let u_int8_array : u_int8_array typ = ptr void
  type u_int8_array_builder = unit ptr
  let u_int8_array_builder : u_int8_array_builder typ = ptr void
  type u_int8_data_type = unit ptr
  let u_int8_data_type : u_int8_data_type typ = ptr void
  type u_int_array_builder = unit ptr
  let u_int_array_builder : u_int_array_builder typ = ptr void
  type union_array = unit ptr
  let union_array : union_array typ = ptr void
  type union_data_type = unit ptr
  let union_data_type : union_data_type typ = ptr void

  module Array = struct
    type t = array_
    let t : t typ = array_

    let cast = foreign "garrow_array_cast"
      (t @-> data_type @-> cast_options @-> ptr (ptr void) @-> returning array_)
    let count = foreign "garrow_array_count"
      (t @-> count_options @-> ptr (ptr void) @-> returning int64_t)
    let count_values = foreign "garrow_array_count_values"
      (t @-> ptr (ptr void) @-> returning struct_array)
    let dictionary_encode = foreign "garrow_array_dictionary_encode"
      (t @-> ptr (ptr void) @-> returning dictionary_array)
    let equal = foreign "garrow_array_equal"
      (t @-> array_ @-> returning bool)
    let equal_approx = foreign "garrow_array_equal_approx"
      (t @-> array_ @-> returning bool)
    let equal_range = foreign "garrow_array_equal_range"
      (t @-> int64_t @-> array_ @-> int64_t @-> int64_t @-> returning bool)
    let get_length = foreign "garrow_array_get_length"
      (t @-> returning int64_t)
    let get_n_nulls = foreign "garrow_array_get_n_nulls"
      (t @-> returning int64_t)
    let get_null_bitmap = foreign "garrow_array_get_null_bitmap"
      (t @-> returning buffer)
    let get_offset = foreign "garrow_array_get_offset"
      (t @-> returning int64_t)
    let get_value_data_type = foreign "garrow_array_get_value_data_type"
      (t @-> returning data_type)
    let get_value_type = foreign "garrow_array_get_value_type"
      (t @-> returning type_)
    let is_null = foreign "garrow_array_is_null"
      (t @-> int64_t @-> returning bool)
    let is_valid = foreign "garrow_array_is_valid"
      (t @-> int64_t @-> returning bool)
    let slice = foreign "garrow_array_slice"
      (t @-> int64_t @-> int64_t @-> returning array_)
    let unique = foreign "garrow_array_unique"
      (t @-> ptr (ptr void) @-> returning array_)
  end

  module ArrayBuilder = struct
    type t = array_builder
    let t : t typ = array_builder

    let finish = foreign "garrow_array_builder_finish"
      (t @-> ptr (ptr void) @-> returning array_)
    let get_value_data_type = foreign "garrow_array_builder_get_value_data_type"
      (t @-> returning data_type)
    let get_value_type = foreign "garrow_array_builder_get_value_type"
      (t @-> returning type_)
  end

  module BinaryArray = struct
    type t = binary_array
    let t : t typ = binary_array

    let new_ = foreign "garrow_binary_array_new"
      (int64_t @-> buffer @-> buffer @-> buffer @-> int64_t @-> returning t)
    let get_buffer = foreign "garrow_binary_array_get_buffer"
      (t @-> returning buffer)
    let get_offsets_buffer = foreign "garrow_binary_array_get_offsets_buffer"
      (t @-> returning buffer)
    let get_value = foreign "garrow_binary_array_get_value"
      (t @-> int64_t @-> returning bytes)
  end

  module BinaryArrayBuilder = struct
    type t = binary_array_builder
    let t : t typ = binary_array_builder

    let new_ = foreign "garrow_binary_array_builder_new"
      (returning t)
    let append_null = foreign "garrow_binary_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
  end

  module BinaryDataType = struct
    type t = binary_data_type
    let t : t typ = binary_data_type

    let new_ = foreign "garrow_binary_data_type_new"
      (returning t)
  end

  module BooleanArray = struct
    type t = boolean_array
    let t : t typ = boolean_array

    let new_ = foreign "garrow_boolean_array_new"
      (int64_t @-> buffer @-> buffer @-> int64_t @-> returning t)
    let and_ = foreign "garrow_boolean_array_and"
      (t @-> boolean_array @-> ptr (ptr void) @-> returning boolean_array)
    let get_value = foreign "garrow_boolean_array_get_value"
      (t @-> int64_t @-> returning bool)
    let invert = foreign "garrow_boolean_array_invert"
      (t @-> ptr (ptr void) @-> returning boolean_array)
    let or_ = foreign "garrow_boolean_array_or"
      (t @-> boolean_array @-> ptr (ptr void) @-> returning boolean_array)
    let xor = foreign "garrow_boolean_array_xor"
      (t @-> boolean_array @-> ptr (ptr void) @-> returning boolean_array)
  end

  module BooleanArrayBuilder = struct
    type t = boolean_array_builder
    let t : t typ = boolean_array_builder

    let new_ = foreign "garrow_boolean_array_builder_new"
      (returning t)
    let append_null = foreign "garrow_boolean_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_boolean_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
    let append_value = foreign "garrow_boolean_array_builder_append_value"
      (t @-> bool @-> ptr (ptr void) @-> returning bool)
  end

  module BooleanDataType = struct
    type t = boolean_data_type
    let t : t typ = boolean_data_type

    let new_ = foreign "garrow_boolean_data_type_new"
      (returning t)
  end

  module Buffer = struct
    type t = buffer
    let t : t typ = buffer

    let new_bytes = foreign "garrow_buffer_new_bytes"
      (bytes @-> returning t)
    let copy = foreign "garrow_buffer_copy"
      (t @-> int64_t @-> int64_t @-> ptr (ptr void) @-> returning buffer)
    let equal = foreign "garrow_buffer_equal"
      (t @-> buffer @-> returning bool)
    let equal_n_bytes = foreign "garrow_buffer_equal_n_bytes"
      (t @-> buffer @-> int64_t @-> returning bool)
    let get_capacity = foreign "garrow_buffer_get_capacity"
      (t @-> returning int64_t)
    let get_data = foreign "garrow_buffer_get_data"
      (t @-> returning bytes)
    let get_mutable_data = foreign "garrow_buffer_get_mutable_data"
      (t @-> returning bytes)
    let get_parent = foreign "garrow_buffer_get_parent"
      (t @-> returning buffer)
    let get_size = foreign "garrow_buffer_get_size"
      (t @-> returning int64_t)
    let is_mutable = foreign "garrow_buffer_is_mutable"
      (t @-> returning bool)
    let slice = foreign "garrow_buffer_slice"
      (t @-> int64_t @-> int64_t @-> returning buffer)
  end

  module BufferInputStream = struct
    type t = buffer_input_stream
    let t : t typ = buffer_input_stream

    let new_ = foreign "garrow_buffer_input_stream_new"
      (buffer @-> returning t)
    let get_buffer = foreign "garrow_buffer_input_stream_get_buffer"
      (t @-> returning buffer)
  end

  module BufferOutputStream = struct
    type t = buffer_output_stream
    let t : t typ = buffer_output_stream

    let new_ = foreign "garrow_buffer_output_stream_new"
      (resizable_buffer @-> returning t)
  end

  module CSVReadOptions = struct
    type t = csv_read_options
    let t : t typ = csv_read_options

    let new_ = foreign "garrow_csv_read_options_new"
      (returning t)
  end

  module CSVReader = struct
    type t = csv_reader
    let t : t typ = csv_reader

    let new_ = foreign "garrow_csv_reader_new"
      (input_stream @-> csv_read_options @-> ptr (ptr void) @-> returning t)
    let read = foreign "garrow_csv_reader_read"
      (t @-> ptr (ptr void) @-> returning table)
  end

  module CastOptions = struct
    type t = cast_options
    let t : t typ = cast_options

    let new_ = foreign "garrow_cast_options_new"
      (returning t)
  end

  module ChunkedArray = struct
    type t = chunked_array
    let t : t typ = chunked_array

    let equal = foreign "garrow_chunked_array_equal"
      (t @-> chunked_array @-> returning bool)
    let get_value_data_type = foreign "garrow_chunked_array_get_value_data_type"
      (t @-> returning data_type)
    let get_value_type = foreign "garrow_chunked_array_get_value_type"
      (t @-> returning type_)
  end

  module Codec = struct
    type t = codec
    let t : t typ = codec

    let new_ = foreign "garrow_codec_new"
      (compression_type @-> ptr (ptr void) @-> returning t)
  end

  module Column = struct
    type t = column
    let t : t typ = column

    let new_array = foreign "garrow_column_new_array"
      (field @-> array_ @-> returning t)
    let new_chunked_array = foreign "garrow_column_new_chunked_array"
      (field @-> chunked_array @-> returning t)
    let equal = foreign "garrow_column_equal"
      (t @-> column @-> returning bool)
    let get_data = foreign "garrow_column_get_data"
      (t @-> returning chunked_array)
    let get_data_type = foreign "garrow_column_get_data_type"
      (t @-> returning data_type)
    let get_field = foreign "garrow_column_get_field"
      (t @-> returning field)
  end

  module CompressedInputStream = struct
    type t = compressed_input_stream
    let t : t typ = compressed_input_stream

    let new_ = foreign "garrow_compressed_input_stream_new"
      (codec @-> input_stream @-> ptr (ptr void) @-> returning t)
  end

  module CompressedOutputStream = struct
    type t = compressed_output_stream
    let t : t typ = compressed_output_stream

    let new_ = foreign "garrow_compressed_output_stream_new"
      (codec @-> output_stream @-> ptr (ptr void) @-> returning t)
  end

  module CountOptions = struct
    type t = count_options
    let t : t typ = count_options

    let new_ = foreign "garrow_count_options_new"
      (returning t)
  end

  module DataType = struct
    type t = data_type
    let t : t typ = data_type

    let equal = foreign "garrow_data_type_equal"
      (t @-> data_type @-> returning bool)
    let get_id = foreign "garrow_data_type_get_id"
      (t @-> returning type_)
  end

  module Date32Array = struct
    type t = date32_array
    let t : t typ = date32_array

    let new_ = foreign "garrow_date32_array_new"
      (int64_t @-> buffer @-> buffer @-> int64_t @-> returning t)
  end

  module Date32ArrayBuilder = struct
    type t = date32_array_builder
    let t : t typ = date32_array_builder

    let new_ = foreign "garrow_date32_array_builder_new"
      (returning t)
    let append_null = foreign "garrow_date32_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_date32_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
  end

  module Date32DataType = struct
    type t = date32_data_type
    let t : t typ = date32_data_type

    let new_ = foreign "garrow_date32_data_type_new"
      (returning t)
  end

  module Date64Array = struct
    type t = date64_array
    let t : t typ = date64_array

    let new_ = foreign "garrow_date64_array_new"
      (int64_t @-> buffer @-> buffer @-> int64_t @-> returning t)
    let get_value = foreign "garrow_date64_array_get_value"
      (t @-> int64_t @-> returning int64_t)
  end

  module Date64ArrayBuilder = struct
    type t = date64_array_builder
    let t : t typ = date64_array_builder

    let new_ = foreign "garrow_date64_array_builder_new"
      (returning t)
    let append_null = foreign "garrow_date64_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_date64_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
    let append_value = foreign "garrow_date64_array_builder_append_value"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
  end

  module Date64DataType = struct
    type t = date64_data_type
    let t : t typ = date64_data_type

    let new_ = foreign "garrow_date64_data_type_new"
      (returning t)
  end

  module Decimal128 = struct
    type t = decimal128
    let t : t typ = decimal128

    let new_integer = foreign "garrow_decimal128_new_integer"
      (int64_t @-> returning t)
    let divide = foreign "garrow_decimal128_divide"
      (t @-> decimal128 @-> decimal128 @-> ptr (ptr void) @-> returning decimal128)
    let equal = foreign "garrow_decimal128_equal"
      (t @-> decimal128 @-> returning bool)
    let greater_than = foreign "garrow_decimal128_greater_than"
      (t @-> decimal128 @-> returning bool)
    let greater_than_or_equal = foreign "garrow_decimal128_greater_than_or_equal"
      (t @-> decimal128 @-> returning bool)
    let less_than = foreign "garrow_decimal128_less_than"
      (t @-> decimal128 @-> returning bool)
    let less_than_or_equal = foreign "garrow_decimal128_less_than_or_equal"
      (t @-> decimal128 @-> returning bool)
    let minus = foreign "garrow_decimal128_minus"
      (t @-> decimal128 @-> returning decimal128)
    let multiply = foreign "garrow_decimal128_multiply"
      (t @-> decimal128 @-> returning decimal128)
    let not_equal = foreign "garrow_decimal128_not_equal"
      (t @-> decimal128 @-> returning bool)
    let plus = foreign "garrow_decimal128_plus"
      (t @-> decimal128 @-> returning decimal128)
    let to_integer = foreign "garrow_decimal128_to_integer"
      (t @-> returning int64_t)
  end

  module Decimal128Array = struct
    type t = decimal128_array
    let t : t typ = decimal128_array

    let get_value = foreign "garrow_decimal128_array_get_value"
      (t @-> int64_t @-> returning decimal128)
  end

  module Decimal128ArrayBuilder = struct
    type t = decimal128_array_builder
    let t : t typ = decimal128_array_builder

    let new_ = foreign "garrow_decimal128_array_builder_new"
      (decimal128_data_type @-> returning t)
    let append_null = foreign "garrow_decimal128_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_value = foreign "garrow_decimal128_array_builder_append_value"
      (t @-> decimal128 @-> ptr (ptr void) @-> returning bool)
  end

  module Decimal128DataType = struct
    type t = decimal128_data_type
    let t : t typ = decimal128_data_type

  end

  module DecimalDataType = struct
    type t = decimal_data_type
    let t : t typ = decimal_data_type

  end

  module DenseUnionArray = struct
    type t = dense_union_array
    let t : t typ = dense_union_array

  end

  module DenseUnionDataType = struct
    type t = dense_union_data_type
    let t : t typ = dense_union_data_type

  end

  module DictionaryArray = struct
    type t = dictionary_array
    let t : t typ = dictionary_array

    let new_ = foreign "garrow_dictionary_array_new"
      (data_type @-> array_ @-> returning t)
    let get_dictionary = foreign "garrow_dictionary_array_get_dictionary"
      (t @-> returning array_)
    let get_dictionary_data_type = foreign "garrow_dictionary_array_get_dictionary_data_type"
      (t @-> returning dictionary_data_type)
    let get_indices = foreign "garrow_dictionary_array_get_indices"
      (t @-> returning array_)
  end

  module DictionaryDataType = struct
    type t = dictionary_data_type
    let t : t typ = dictionary_data_type

    let new_ = foreign "garrow_dictionary_data_type_new"
      (data_type @-> array_ @-> bool @-> returning t)
    let get_dictionary = foreign "garrow_dictionary_data_type_get_dictionary"
      (t @-> returning array_)
    let get_index_data_type = foreign "garrow_dictionary_data_type_get_index_data_type"
      (t @-> returning data_type)
    let is_ordered = foreign "garrow_dictionary_data_type_is_ordered"
      (t @-> returning bool)
  end

  module DoubleArray = struct
    type t = double_array
    let t : t typ = double_array

    let new_ = foreign "garrow_double_array_new"
      (int64_t @-> buffer @-> buffer @-> int64_t @-> returning t)
    let get_value = foreign "garrow_double_array_get_value"
      (t @-> int64_t @-> returning double)
    let sum = foreign "garrow_double_array_sum"
      (t @-> ptr (ptr void) @-> returning double)
  end

  module DoubleArrayBuilder = struct
    type t = double_array_builder
    let t : t typ = double_array_builder

    let new_ = foreign "garrow_double_array_builder_new"
      (returning t)
    let append_null = foreign "garrow_double_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_double_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
    let append_value = foreign "garrow_double_array_builder_append_value"
      (t @-> double @-> ptr (ptr void) @-> returning bool)
  end

  module DoubleDataType = struct
    type t = double_data_type
    let t : t typ = double_data_type

    let new_ = foreign "garrow_double_data_type_new"
      (returning t)
  end

  module FeatherFileReader = struct
    type t = feather_file_reader
    let t : t typ = feather_file_reader

    let new_ = foreign "garrow_feather_file_reader_new"
      (seekable_input_stream @-> ptr (ptr void) @-> returning t)
    let get_n_columns = foreign "garrow_feather_file_reader_get_n_columns"
      (t @-> returning int64_t)
    let get_n_rows = foreign "garrow_feather_file_reader_get_n_rows"
      (t @-> returning int64_t)
    let has_description = foreign "garrow_feather_file_reader_has_description"
      (t @-> returning bool)
    let read = foreign "garrow_feather_file_reader_read"
      (t @-> ptr (ptr void) @-> returning table)
  end

  module FeatherFileWriter = struct
    type t = feather_file_writer
    let t : t typ = feather_file_writer

    let new_ = foreign "garrow_feather_file_writer_new"
      (output_stream @-> ptr (ptr void) @-> returning t)
    let close = foreign "garrow_feather_file_writer_close"
      (t @-> ptr (ptr void) @-> returning bool)
    let write = foreign "garrow_feather_file_writer_write"
      (t @-> table @-> ptr (ptr void) @-> returning bool)
  end

  module Field = struct
    type t = field
    let t : t typ = field

    let equal = foreign "garrow_field_equal"
      (t @-> field @-> returning bool)
    let get_data_type = foreign "garrow_field_get_data_type"
      (t @-> returning data_type)
    let is_nullable = foreign "garrow_field_is_nullable"
      (t @-> returning bool)
  end

  module FileOutputStream = struct
    type t = file_output_stream
    let t : t typ = file_output_stream

  end

  module FixedSizeBinaryArray = struct
    type t = fixed_size_binary_array
    let t : t typ = fixed_size_binary_array

  end

  module FixedSizeBinaryDataType = struct
    type t = fixed_size_binary_data_type
    let t : t typ = fixed_size_binary_data_type

  end

  module FixedWidthDataType = struct
    type t = fixed_width_data_type
    let t : t typ = fixed_width_data_type

  end

  module FloatArray = struct
    type t = float_array
    let t : t typ = float_array

    let new_ = foreign "garrow_float_array_new"
      (int64_t @-> buffer @-> buffer @-> int64_t @-> returning t)
    let get_value = foreign "garrow_float_array_get_value"
      (t @-> int64_t @-> returning float)
    let sum = foreign "garrow_float_array_sum"
      (t @-> ptr (ptr void) @-> returning double)
  end

  module FloatArrayBuilder = struct
    type t = float_array_builder
    let t : t typ = float_array_builder

    let new_ = foreign "garrow_float_array_builder_new"
      (returning t)
    let append_null = foreign "garrow_float_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_float_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
    let append_value = foreign "garrow_float_array_builder_append_value"
      (t @-> float @-> ptr (ptr void) @-> returning bool)
  end

  module FloatDataType = struct
    type t = float_data_type
    let t : t typ = float_data_type

    let new_ = foreign "garrow_float_data_type_new"
      (returning t)
  end

  module FloatingPointDataType = struct
    type t = floating_point_data_type
    let t : t typ = floating_point_data_type

  end

  module GIOInputStream = struct
    type t = gio_input_stream
    let t : t typ = gio_input_stream

    let new_ = foreign "garrow_gio_input_stream_new"
      (input_stream @-> returning t)
  end

  module GIOOutputStream = struct
    type t = gio_output_stream
    let t : t typ = gio_output_stream

    let new_ = foreign "garrow_gio_output_stream_new"
      (output_stream @-> returning t)
  end

  module InputStream = struct
    type t = input_stream
    let t : t typ = input_stream

    let advance = foreign "garrow_input_stream_advance"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
    let read_tensor = foreign "garrow_input_stream_read_tensor"
      (t @-> ptr (ptr void) @-> returning tensor)
  end

  module Int16Array = struct
    type t = int16_array
    let t : t typ = int16_array

    let new_ = foreign "garrow_int16_array_new"
      (int64_t @-> buffer @-> buffer @-> int64_t @-> returning t)
    let sum = foreign "garrow_int16_array_sum"
      (t @-> ptr (ptr void) @-> returning int64_t)
  end

  module Int16ArrayBuilder = struct
    type t = int16_array_builder
    let t : t typ = int16_array_builder

    let new_ = foreign "garrow_int16_array_builder_new"
      (returning t)
    let append_null = foreign "garrow_int16_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_int16_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
  end

  module Int16DataType = struct
    type t = int16_data_type
    let t : t typ = int16_data_type

    let new_ = foreign "garrow_int16_data_type_new"
      (returning t)
  end

  module Int32Array = struct
    type t = int32_array
    let t : t typ = int32_array

    let new_ = foreign "garrow_int32_array_new"
      (int64_t @-> buffer @-> buffer @-> int64_t @-> returning t)
    let sum = foreign "garrow_int32_array_sum"
      (t @-> ptr (ptr void) @-> returning int64_t)
  end

  module Int32ArrayBuilder = struct
    type t = int32_array_builder
    let t : t typ = int32_array_builder

    let new_ = foreign "garrow_int32_array_builder_new"
      (returning t)
    let append_null = foreign "garrow_int32_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_int32_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
  end

  module Int32DataType = struct
    type t = int32_data_type
    let t : t typ = int32_data_type

    let new_ = foreign "garrow_int32_data_type_new"
      (returning t)
  end

  module Int64Array = struct
    type t = int64_array
    let t : t typ = int64_array

    let new_ = foreign "garrow_int64_array_new"
      (int64_t @-> buffer @-> buffer @-> int64_t @-> returning t)
    let get_value = foreign "garrow_int64_array_get_value"
      (t @-> int64_t @-> returning int64_t)
    let sum = foreign "garrow_int64_array_sum"
      (t @-> ptr (ptr void) @-> returning int64_t)
  end

  module Int64ArrayBuilder = struct
    type t = int64_array_builder
    let t : t typ = int64_array_builder

    let new_ = foreign "garrow_int64_array_builder_new"
      (returning t)
    let append_null = foreign "garrow_int64_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_int64_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
    let append_value = foreign "garrow_int64_array_builder_append_value"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
  end

  module Int64DataType = struct
    type t = int64_data_type
    let t : t typ = int64_data_type

    let new_ = foreign "garrow_int64_data_type_new"
      (returning t)
  end

  module Int8Array = struct
    type t = int8_array
    let t : t typ = int8_array

    let new_ = foreign "garrow_int8_array_new"
      (int64_t @-> buffer @-> buffer @-> int64_t @-> returning t)
    let sum = foreign "garrow_int8_array_sum"
      (t @-> ptr (ptr void) @-> returning int64_t)
  end

  module Int8ArrayBuilder = struct
    type t = int8_array_builder
    let t : t typ = int8_array_builder

    let new_ = foreign "garrow_int8_array_builder_new"
      (returning t)
    let append_null = foreign "garrow_int8_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_int8_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
  end

  module Int8DataType = struct
    type t = int8_data_type
    let t : t typ = int8_data_type

    let new_ = foreign "garrow_int8_data_type_new"
      (returning t)
  end

  module IntArrayBuilder = struct
    type t = int_array_builder
    let t : t typ = int_array_builder

    let new_ = foreign "garrow_int_array_builder_new"
      (returning t)
    let append_null = foreign "garrow_int_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_int_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
    let append_value = foreign "garrow_int_array_builder_append_value"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
  end

  module IntegerDataType = struct
    type t = integer_data_type
    let t : t typ = integer_data_type

  end

  module ListArray = struct
    type t = list_array
    let t : t typ = list_array

    let new_ = foreign "garrow_list_array_new"
      (data_type @-> int64_t @-> buffer @-> array_ @-> buffer @-> int64_t @-> returning t)
    let get_value = foreign "garrow_list_array_get_value"
      (t @-> int64_t @-> returning array_)
    let get_value_type = foreign "garrow_list_array_get_value_type"
      (t @-> returning data_type)
  end

  module ListArrayBuilder = struct
    type t = list_array_builder
    let t : t typ = list_array_builder

    let new_ = foreign "garrow_list_array_builder_new"
      (list_data_type @-> ptr (ptr void) @-> returning t)
    let append_null = foreign "garrow_list_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_value = foreign "garrow_list_array_builder_append_value"
      (t @-> ptr (ptr void) @-> returning bool)
    let get_value_builder = foreign "garrow_list_array_builder_get_value_builder"
      (t @-> returning array_builder)
  end

  module ListDataType = struct
    type t = list_data_type
    let t : t typ = list_data_type

    let new_ = foreign "garrow_list_data_type_new"
      (field @-> returning t)
    let get_field = foreign "garrow_list_data_type_get_field"
      (t @-> returning field)
  end

  module MemoryMappedInputStream = struct
    type t = memory_mapped_input_stream
    let t : t typ = memory_mapped_input_stream

  end

  module MutableBuffer = struct
    type t = mutable_buffer
    let t : t typ = mutable_buffer

    let new_bytes = foreign "garrow_mutable_buffer_new_bytes"
      (bytes @-> returning t)
    let slice = foreign "garrow_mutable_buffer_slice"
      (t @-> int64_t @-> int64_t @-> returning mutable_buffer)
  end

  module NullArray = struct
    type t = null_array
    let t : t typ = null_array

    let new_ = foreign "garrow_null_array_new"
      (int64_t @-> returning t)
  end

  module NullArrayBuilder = struct
    type t = null_array_builder
    let t : t typ = null_array_builder

    let new_ = foreign "garrow_null_array_builder_new"
      (returning t)
    let append_null = foreign "garrow_null_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_null_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
  end

  module NullDataType = struct
    type t = null_data_type
    let t : t typ = null_data_type

    let new_ = foreign "garrow_null_data_type_new"
      (returning t)
  end

  module NumericArray = struct
    type t = numeric_array
    let t : t typ = numeric_array

    let mean = foreign "garrow_numeric_array_mean"
      (t @-> ptr (ptr void) @-> returning double)
  end

  module NumericDataType = struct
    type t = numeric_data_type
    let t : t typ = numeric_data_type

  end

  module OutputStream = struct
    type t = output_stream
    let t : t typ = output_stream

    let write_tensor = foreign "garrow_output_stream_write_tensor"
      (t @-> tensor @-> ptr (ptr void) @-> returning int64_t)
  end

  module PrimitiveArray = struct
    type t = primitive_array
    let t : t typ = primitive_array

    let get_buffer = foreign "garrow_primitive_array_get_buffer"
      (t @-> returning buffer)
  end

  module RecordBatch = struct
    type t = record_batch
    let t : t typ = record_batch

    let equal = foreign "garrow_record_batch_equal"
      (t @-> record_batch @-> returning bool)
    let get_n_rows = foreign "garrow_record_batch_get_n_rows"
      (t @-> returning int64_t)
    let get_schema = foreign "garrow_record_batch_get_schema"
      (t @-> returning schema)
    let slice = foreign "garrow_record_batch_slice"
      (t @-> int64_t @-> int64_t @-> returning record_batch)
  end

  module RecordBatchBuilder = struct
    type t = record_batch_builder
    let t : t typ = record_batch_builder

    let new_ = foreign "garrow_record_batch_builder_new"
      (schema @-> ptr (ptr void) @-> returning t)
    let flush = foreign "garrow_record_batch_builder_flush"
      (t @-> ptr (ptr void) @-> returning record_batch)
    let get_initial_capacity = foreign "garrow_record_batch_builder_get_initial_capacity"
      (t @-> returning int64_t)
    let get_schema = foreign "garrow_record_batch_builder_get_schema"
      (t @-> returning schema)
  end

  module RecordBatchFileReader = struct
    type t = record_batch_file_reader
    let t : t typ = record_batch_file_reader

    let new_ = foreign "garrow_record_batch_file_reader_new"
      (seekable_input_stream @-> ptr (ptr void) @-> returning t)
    let get_schema = foreign "garrow_record_batch_file_reader_get_schema"
      (t @-> returning schema)
    let get_version = foreign "garrow_record_batch_file_reader_get_version"
      (t @-> returning metadata_version)
  end

  module RecordBatchFileWriter = struct
    type t = record_batch_file_writer
    let t : t typ = record_batch_file_writer

    let new_ = foreign "garrow_record_batch_file_writer_new"
      (output_stream @-> schema @-> ptr (ptr void) @-> returning t)
  end

  module RecordBatchReader = struct
    type t = record_batch_reader
    let t : t typ = record_batch_reader

    let get_schema = foreign "garrow_record_batch_reader_get_schema"
      (t @-> returning schema)
    let read_next = foreign "garrow_record_batch_reader_read_next"
      (t @-> ptr (ptr void) @-> returning record_batch)
  end

  module RecordBatchStreamReader = struct
    type t = record_batch_stream_reader
    let t : t typ = record_batch_stream_reader

    let new_ = foreign "garrow_record_batch_stream_reader_new"
      (input_stream @-> ptr (ptr void) @-> returning t)
  end

  module RecordBatchStreamWriter = struct
    type t = record_batch_stream_writer
    let t : t typ = record_batch_stream_writer

    let new_ = foreign "garrow_record_batch_stream_writer_new"
      (output_stream @-> schema @-> ptr (ptr void) @-> returning t)
  end

  module RecordBatchWriter = struct
    type t = record_batch_writer
    let t : t typ = record_batch_writer

    let close = foreign "garrow_record_batch_writer_close"
      (t @-> ptr (ptr void) @-> returning bool)
    let write_record_batch = foreign "garrow_record_batch_writer_write_record_batch"
      (t @-> record_batch @-> ptr (ptr void) @-> returning bool)
    let write_table = foreign "garrow_record_batch_writer_write_table"
      (t @-> table @-> ptr (ptr void) @-> returning bool)
  end

  module ResizableBuffer = struct
    type t = resizable_buffer
    let t : t typ = resizable_buffer

    let new_ = foreign "garrow_resizable_buffer_new"
      (int64_t @-> ptr (ptr void) @-> returning t)
    let reserve = foreign "garrow_resizable_buffer_reserve"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
    let resize = foreign "garrow_resizable_buffer_resize"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
  end

  module Schema = struct
    type t = schema
    let t : t typ = schema

    let equal = foreign "garrow_schema_equal"
      (t @-> schema @-> returning bool)
  end

  module SeekableInputStream = struct
    type t = seekable_input_stream
    let t : t typ = seekable_input_stream

    let get_support_zero_copy = foreign "garrow_seekable_input_stream_get_support_zero_copy"
      (t @-> returning bool)
    let peek = foreign "garrow_seekable_input_stream_peek"
      (t @-> int64_t @-> returning bytes)
    let read_at = foreign "garrow_seekable_input_stream_read_at"
      (t @-> int64_t @-> int64_t @-> ptr (ptr void) @-> returning buffer)
  end

  module SparseUnionArray = struct
    type t = sparse_union_array
    let t : t typ = sparse_union_array

  end

  module SparseUnionDataType = struct
    type t = sparse_union_data_type
    let t : t typ = sparse_union_data_type

  end

  module StringArray = struct
    type t = string_array
    let t : t typ = string_array

    let new_ = foreign "garrow_string_array_new"
      (int64_t @-> buffer @-> buffer @-> buffer @-> int64_t @-> returning t)
  end

  module StringArrayBuilder = struct
    type t = string_array_builder
    let t : t typ = string_array_builder

    let new_ = foreign "garrow_string_array_builder_new"
      (returning t)
  end

  module StringDataType = struct
    type t = string_data_type
    let t : t typ = string_data_type

    let new_ = foreign "garrow_string_data_type_new"
      (returning t)
  end

  module StructArray = struct
    type t = struct_array
    let t : t typ = struct_array

  end

  module StructArrayBuilder = struct
    type t = struct_array_builder
    let t : t typ = struct_array_builder

    let new_ = foreign "garrow_struct_array_builder_new"
      (struct_data_type @-> ptr (ptr void) @-> returning t)
    let append_null = foreign "garrow_struct_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_value = foreign "garrow_struct_array_builder_append_value"
      (t @-> ptr (ptr void) @-> returning bool)
  end

  module StructDataType = struct
    type t = struct_data_type
    let t : t typ = struct_data_type

  end

  module Table = struct
    type t = table
    let t : t typ = table

    let equal = foreign "garrow_table_equal"
      (t @-> table @-> returning bool)
    let get_schema = foreign "garrow_table_get_schema"
      (t @-> returning schema)
  end

  module TableBatchReader = struct
    type t = table_batch_reader
    let t : t typ = table_batch_reader

    let new_ = foreign "garrow_table_batch_reader_new"
      (table @-> returning t)
  end

  module Tensor = struct
    type t = tensor
    let t : t typ = tensor

    let equal = foreign "garrow_tensor_equal"
      (t @-> tensor @-> returning bool)
    let get_buffer = foreign "garrow_tensor_get_buffer"
      (t @-> returning buffer)
    let get_size = foreign "garrow_tensor_get_size"
      (t @-> returning int64_t)
    let get_value_data_type = foreign "garrow_tensor_get_value_data_type"
      (t @-> returning data_type)
    let get_value_type = foreign "garrow_tensor_get_value_type"
      (t @-> returning type_)
    let is_column_major = foreign "garrow_tensor_is_column_major"
      (t @-> returning bool)
    let is_contiguous = foreign "garrow_tensor_is_contiguous"
      (t @-> returning bool)
    let is_mutable = foreign "garrow_tensor_is_mutable"
      (t @-> returning bool)
    let is_row_major = foreign "garrow_tensor_is_row_major"
      (t @-> returning bool)
  end

  module Time32Array = struct
    type t = time32_array
    let t : t typ = time32_array

    let new_ = foreign "garrow_time32_array_new"
      (time32_data_type @-> int64_t @-> buffer @-> buffer @-> int64_t @-> returning t)
  end

  module Time32ArrayBuilder = struct
    type t = time32_array_builder
    let t : t typ = time32_array_builder

    let new_ = foreign "garrow_time32_array_builder_new"
      (time32_data_type @-> returning t)
    let append_null = foreign "garrow_time32_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_time32_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
  end

  module Time32DataType = struct
    type t = time32_data_type
    let t : t typ = time32_data_type

    let new_ = foreign "garrow_time32_data_type_new"
      (time_unit @-> ptr (ptr void) @-> returning t)
  end

  module Time64Array = struct
    type t = time64_array
    let t : t typ = time64_array

    let new_ = foreign "garrow_time64_array_new"
      (time64_data_type @-> int64_t @-> buffer @-> buffer @-> int64_t @-> returning t)
    let get_value = foreign "garrow_time64_array_get_value"
      (t @-> int64_t @-> returning int64_t)
  end

  module Time64ArrayBuilder = struct
    type t = time64_array_builder
    let t : t typ = time64_array_builder

    let new_ = foreign "garrow_time64_array_builder_new"
      (time64_data_type @-> returning t)
    let append_null = foreign "garrow_time64_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_time64_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
    let append_value = foreign "garrow_time64_array_builder_append_value"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
  end

  module Time64DataType = struct
    type t = time64_data_type
    let t : t typ = time64_data_type

    let new_ = foreign "garrow_time64_data_type_new"
      (time_unit @-> ptr (ptr void) @-> returning t)
  end

  module TimeDataType = struct
    type t = time_data_type
    let t : t typ = time_data_type

    let get_unit = foreign "garrow_time_data_type_get_unit"
      (t @-> returning time_unit)
  end

  module TimestampArray = struct
    type t = timestamp_array
    let t : t typ = timestamp_array

    let new_ = foreign "garrow_timestamp_array_new"
      (timestamp_data_type @-> int64_t @-> buffer @-> buffer @-> int64_t @-> returning t)
    let get_value = foreign "garrow_timestamp_array_get_value"
      (t @-> int64_t @-> returning int64_t)
  end

  module TimestampArrayBuilder = struct
    type t = timestamp_array_builder
    let t : t typ = timestamp_array_builder

    let new_ = foreign "garrow_timestamp_array_builder_new"
      (timestamp_data_type @-> returning t)
    let append_null = foreign "garrow_timestamp_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_timestamp_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
    let append_value = foreign "garrow_timestamp_array_builder_append_value"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
  end

  module TimestampDataType = struct
    type t = timestamp_data_type
    let t : t typ = timestamp_data_type

    let new_ = foreign "garrow_timestamp_data_type_new"
      (time_unit @-> returning t)
    let get_unit = foreign "garrow_timestamp_data_type_get_unit"
      (t @-> returning time_unit)
  end

  module UInt16Array = struct
    type t = u_int16_array
    let t : t typ = u_int16_array

    let new_ = foreign "garrow_uint16_array_new"
      (int64_t @-> buffer @-> buffer @-> int64_t @-> returning t)
  end

  module UInt16ArrayBuilder = struct
    type t = u_int16_array_builder
    let t : t typ = u_int16_array_builder

    let new_ = foreign "garrow_uint16_array_builder_new"
      (returning t)
    let append_null = foreign "garrow_uint16_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_uint16_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
  end

  module UInt16DataType = struct
    type t = u_int16_data_type
    let t : t typ = u_int16_data_type

    let new_ = foreign "garrow_uint16_data_type_new"
      (returning t)
  end

  module UInt32Array = struct
    type t = u_int32_array
    let t : t typ = u_int32_array

    let new_ = foreign "garrow_uint32_array_new"
      (int64_t @-> buffer @-> buffer @-> int64_t @-> returning t)
  end

  module UInt32ArrayBuilder = struct
    type t = u_int32_array_builder
    let t : t typ = u_int32_array_builder

    let new_ = foreign "garrow_uint32_array_builder_new"
      (returning t)
    let append_null = foreign "garrow_uint32_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_uint32_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
  end

  module UInt32DataType = struct
    type t = u_int32_data_type
    let t : t typ = u_int32_data_type

    let new_ = foreign "garrow_uint32_data_type_new"
      (returning t)
  end

  module UInt64Array = struct
    type t = u_int64_array
    let t : t typ = u_int64_array

    let new_ = foreign "garrow_uint64_array_new"
      (int64_t @-> buffer @-> buffer @-> int64_t @-> returning t)
  end

  module UInt64ArrayBuilder = struct
    type t = u_int64_array_builder
    let t : t typ = u_int64_array_builder

    let new_ = foreign "garrow_uint64_array_builder_new"
      (returning t)
    let append_null = foreign "garrow_uint64_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_uint64_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
  end

  module UInt64DataType = struct
    type t = u_int64_data_type
    let t : t typ = u_int64_data_type

    let new_ = foreign "garrow_uint64_data_type_new"
      (returning t)
  end

  module UInt8Array = struct
    type t = u_int8_array
    let t : t typ = u_int8_array

    let new_ = foreign "garrow_uint8_array_new"
      (int64_t @-> buffer @-> buffer @-> int64_t @-> returning t)
  end

  module UInt8ArrayBuilder = struct
    type t = u_int8_array_builder
    let t : t typ = u_int8_array_builder

    let new_ = foreign "garrow_uint8_array_builder_new"
      (returning t)
    let append_null = foreign "garrow_uint8_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_uint8_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
  end

  module UInt8DataType = struct
    type t = u_int8_data_type
    let t : t typ = u_int8_data_type

    let new_ = foreign "garrow_uint8_data_type_new"
      (returning t)
  end

  module UIntArrayBuilder = struct
    type t = u_int_array_builder
    let t : t typ = u_int_array_builder

    let new_ = foreign "garrow_uint_array_builder_new"
      (returning t)
    let append_null = foreign "garrow_uint_array_builder_append_null"
      (t @-> ptr (ptr void) @-> returning bool)
    let append_nulls = foreign "garrow_uint_array_builder_append_nulls"
      (t @-> int64_t @-> ptr (ptr void) @-> returning bool)
  end

  module UnionArray = struct
    type t = union_array
    let t : t typ = union_array

  end

  module UnionDataType = struct
    type t = union_data_type
    let t : t typ = union_data_type

  end

end