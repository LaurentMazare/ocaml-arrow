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
  val kind : (elt_t, ml_t) Kind.t
  val get : t -> Int64.t -> ml_t
end

module DoubleArray = struct
  type elt_t = float
  type ml_t = float
  type t = W.DoubleArray.t

  module Builder = W.DoubleArrayBuilder

  let kind = Kind.Double
  let finish = W.ArrayBuilder.finish
  let of_array array = Option.value_exn (W.DoubleArray.of_gobject array)
  let get = W.DoubleArray.get_value
end

module FloatArray = struct
  type elt_t = float
  type ml_t = float
  type t = W.FloatArray.t

  module Builder = W.FloatArrayBuilder

  let kind = Kind.Float
  let finish = W.ArrayBuilder.finish
  let of_array array = Option.value_exn (W.FloatArray.of_gobject array)
  let get = W.FloatArray.get_value
end

module Int64Array = struct
  type elt_t = Int64.t
  type ml_t = int
  type t = W.Int64Array.t

  module Builder = W.Int64ArrayBuilder

  let kind = Kind.Int64
  let finish = W.ArrayBuilder.finish
  let of_array array = Option.value_exn (W.Int64Array.of_gobject array)
  let get t i = W.Int64Array.get_value t i |> Int64.to_int_exn
end

module Int32Array = struct
  type elt_t = Int32.t
  type ml_t = int
  type t = W.Int32Array.t

  module Builder = W.Int32ArrayBuilder

  let kind = Kind.Int32
  let finish = W.ArrayBuilder.finish
  let of_array array = Option.value_exn (W.Int32Array.of_gobject array)
  let get t i = W.Int32Array.get_value t i |> Int32.to_int_exn
end

module BoolArray = struct
  type elt_t = bool
  type ml_t = bool
  type t = W.BooleanArray.t

  module Builder = W.BooleanArrayBuilder

  let kind = Kind.Bool
  let finish = W.ArrayBuilder.finish
  let of_array array = Option.value_exn (W.BooleanArray.of_gobject array)
  let get = W.BooleanArray.get_value
end

module StringArray = struct
  type elt_t = string
  type ml_t = string
  type t = W.StringArray.t

  module Builder = W.StringArrayBuilder

  let kind = Kind.String
  let finish = W.ArrayBuilder.finish
  let of_array array = Option.value_exn (W.StringArray.of_gobject array)
  let get = W.StringArray.get_string
end

type ('a, 'b) t =
  { mod_ : (module S with type elt_t = 'a and type ml_t = 'b)
  ; data : W.Array.t
  }

let module_of_kind : type a b.
    (a, b) Kind.t -> (module S with type elt_t = a and type ml_t = b)
  = function
  | Kind.Double -> (module DoubleArray)
  | Kind.Float -> (module FloatArray)
  | Kind.Int64 -> (module Int64Array)
  | Kind.Int32 -> (module Int32Array)
  | Kind.Bool -> (module BoolArray)
  | Kind.String -> (module StringArray)

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

let unpack : type a b. packed -> (a, b) Kind.t -> (a, b) t option =
 fun (P t) kind ->
  let (module M) = t.mod_ in
  match M.kind, kind with
  | Kind.Double, Kind.Double -> Some t
  | Kind.Float, Kind.Float -> Some t
  | Kind.Int64, Kind.Int64 -> Some t
  | Kind.Int32, Kind.Int32 -> Some t
  | Kind.String, Kind.String -> Some t
  | _, _ -> None

let packed_length (P t) = length t
let packed_to_string (P t) = to_string t
let packed_slice (P t) ~start ~length = P (slice t ~start ~length)

let of_list : type a b. a list -> (a, b) Kind.t -> (a, b) t =
 fun list kind ->
  let mod_ = module_of_kind kind in
  let (module M) = mod_ in
  let builder = M.Builder.new_ () in
  if not (M.Builder.append_values builder list []) then failwith "cannot append";
  { mod_; data = M.finish builder }
