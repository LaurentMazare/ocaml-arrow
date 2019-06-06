open Base
module W = Arrow_core.Wrapper

module type S = sig
  type elt_t
  type ml_t
  type t

  val of_array : W.Array.t -> t
  val kind : (elt_t, ml_t) Kind.t
  val get : t -> Int64.t -> ml_t
  val set : t -> Int64.t -> ml_t -> unit
end

module DoubleArray = struct
  type elt_t = [ `double ]
  type ml_t = float
  type t = W.DoubleArray.t

  let kind = Kind.Double
  let of_array array = Option.value_exn (W.DoubleArray.of_gobject array)
  let get = W.DoubleArray.get_value
  let set _array _index _v = failwith "TODO"
end

module FloatArray = struct
  type elt_t = [ `float ]
  type ml_t = float
  type t = W.FloatArray.t

  let kind = Kind.Float
  let of_array array = Option.value_exn (W.FloatArray.of_gobject array)
  let get = W.FloatArray.get_value
  let set _array _index _v = failwith "TODO"
end

module Int64Array = struct
  type elt_t = [ `int64 ]
  type ml_t = int
  type t = W.Int64Array.t

  let kind = Kind.Int64
  let of_array array = Option.value_exn (W.Int64Array.of_gobject array)
  let get t i = W.Int64Array.get_value t i |> Int64.to_int_exn
  let set _array _index _v = failwith "TODO"
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
  | _ -> failwith "TODO"

let of_list : type a b. b list -> (a, b) Kind.t -> (a, b) t =
 fun _list kind ->
  let mod_ = module_of_kind kind in
  { mod_; data = failwith "TODO" }

let length t = W.Array.get_length t.data |> Int64.to_int_exn

let slice t ~start ~length =
  let data = W.Array.slice t.data (Int64.of_int start) (Int64.of_int length) in
  { mod_ = t.mod_; data }

let get : type a b. (a, b) t -> int -> b =
 fun t index ->
  let index = Int64.of_int index in
  let (module M) = t.mod_ in
  M.get (M.of_array t.data) index

let set : type a b. (a, b) t -> int -> b -> unit =
 fun t index v ->
  let index = Int64.of_int index in
  let (module M) = t.mod_ in
  M.set (M.of_array t.data) index v

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
  | Kind.Int16, Kind.Int16 -> Some t
  | Kind.Int8, Kind.Int8 -> Some t
  | Kind.Uint64, Kind.Uint64 -> Some t
  | Kind.Uint32, Kind.Uint32 -> Some t
  | Kind.Uint16, Kind.Uint16 -> Some t
  | Kind.Uint8, Kind.Uint8 -> Some t
  | Kind.String, Kind.String -> Some t
  | _, _ -> None

let packed_length (P t) = length t
let packed_slice (P t) ~start ~length = P (slice t ~start ~length)
