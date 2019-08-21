open Base
module W = Arrow_core.Wrapper

module type S = sig
  type elt_t
  type ml_t
  type t

  module Builder : sig
    type t

    val new_ : unit -> t

    (* val append_value : t -> elt_t -> bool *)
    val append_values : t -> elt_t list -> bool list -> bool
  end

  val finish : Builder.t -> W.Array.t
  val of_array : W.Array.t -> t
  val data_type : (elt_t, ml_t) Data_type.t
  val get : t -> Int64.t -> ml_t
end

module DoubleArray = struct
  type elt_t = float
  type ml_t = float
  type t = W.DoubleArray.t

  module Builder = W.DoubleArrayBuilder

  let data_type = Data_type.Double
  let finish = W.ArrayBuilder.finish
  let of_array array = Option.value_exn (W.DoubleArray.of_gobject array)
  let get = W.DoubleArray.get_value
end

module FloatArray = struct
  type elt_t = float
  type ml_t = float
  type t = W.FloatArray.t

  module Builder = W.FloatArrayBuilder

  let data_type = Data_type.Float
  let finish = W.ArrayBuilder.finish
  let of_array array = Option.value_exn (W.FloatArray.of_gobject array)
  let get = W.FloatArray.get_value
end

module Int64Array = struct
  type elt_t = Int64.t
  type ml_t = int
  type t = W.Int64Array.t

  module Builder = W.Int64ArrayBuilder

  let data_type = Data_type.Int64
  let finish = W.ArrayBuilder.finish
  let of_array array = Option.value_exn (W.Int64Array.of_gobject array)
  let get t i = W.Int64Array.get_value t i |> Int64.to_int_exn
end

module Int32Array = struct
  type elt_t = Int32.t
  type ml_t = int
  type t = W.Int32Array.t

  module Builder = W.Int32ArrayBuilder

  let data_type = Data_type.Int32
  let finish = W.ArrayBuilder.finish
  let of_array array = Option.value_exn (W.Int32Array.of_gobject array)
  let get t i = W.Int32Array.get_value t i |> Int32.to_int_exn
end

module BoolArray = struct
  type elt_t = bool
  type ml_t = bool
  type t = W.BooleanArray.t

  module Builder = W.BooleanArrayBuilder

  let data_type = Data_type.Bool
  let finish = W.ArrayBuilder.finish
  let of_array array = Option.value_exn (W.BooleanArray.of_gobject array)
  let get = W.BooleanArray.get_value
end

module StringArray = struct
  type elt_t = string
  type ml_t = string
  type t = W.StringArray.t

  module Builder = W.StringArrayBuilder

  let data_type = Data_type.String
  let finish = W.ArrayBuilder.finish
  let of_array array = Option.value_exn (W.StringArray.of_gobject array)
  let get = W.StringArray.get_string
end

type ('a, 'b) t =
  { mod_ : (module S with type elt_t = 'a and type ml_t = 'b)
  ; data : W.Array.t
  }

let data t = t.data

let module_of_data_type
    : type a b. (a, b) Data_type.t -> (module S with type elt_t = a and type ml_t = b)
  = function
  | Data_type.Double -> (module DoubleArray)
  | Data_type.Float -> (module FloatArray)
  | Data_type.Int64 -> (module Int64Array)
  | Data_type.Int32 -> (module Int32Array)
  | Data_type.Bool -> (module BoolArray)
  | Data_type.String -> (module StringArray)

let length t = W.Array.get_length t.data |> Int64.to_int_exn
let to_string t = W.Array.to_string t.data

let slice t ~start ~length =
  let data = W.Array.slice t.data (Int64.of_int start) (Int64.of_int length) in
  { mod_ = t.mod_; data }

let get : type a b. (a, b) t -> int -> b =
 fun t index ->
  let index = Int64.of_int index in
  let (module M) = t.mod_ in
  M.get (M.of_array t.data) index

type packed = P : _ t -> packed

let pack t = P t

let unpack : type a b. packed -> (a, b) Data_type.t -> (a, b) t option =
 fun (P t) data_type ->
  let (module M) = t.mod_ in
  match M.data_type, data_type with
  | Data_type.Double, Data_type.Double -> Some t
  | Data_type.Float, Data_type.Float -> Some t
  | Data_type.Int64, Data_type.Int64 -> Some t
  | Data_type.Int32, Data_type.Int32 -> Some t
  | Data_type.String, Data_type.String -> Some t
  | _, _ -> None

let packed_length (P t) = length t
let packed_to_string (P t) = to_string t
let packed_slice (P t) ~start ~length = P (slice t ~start ~length)
let packed_data (P t) = t.data

let of_list : type a b. a list -> (a, b) Data_type.t -> (a, b) t =
 fun list data_type ->
  let mod_ = module_of_data_type data_type in
  let (module M) = mod_ in
  let builder = M.Builder.new_ () in
  if not (M.Builder.append_values builder list []) then failwith "cannot append";
  { mod_; data = M.finish builder }

let of_data : type a b. W.Array.t -> (a, b) Data_type.t -> (a, b) t Or_error.t =
 fun data data_type ->
  let mod_ = module_of_data_type data_type in
  let (module M) = mod_ in
  let data_type' = W.Array.get_value_data_type data in
  Data_type.check_equal data_type data_type'
  |> Or_error.map ~f:(fun () -> { mod_; data })

let of_data_exn : type a b. W.Array.t -> (a, b) Data_type.t -> (a, b) t =
 fun data data_type -> Or_error.ok_exn (of_data data data_type)
