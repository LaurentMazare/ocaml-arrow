open Base
module W = Arrow_core.Wrapper

type (_, _) t =
  | Double : (float, float) t
  | Float : (float, float) t
  | Int64 : (Int64.t, int) t
  | Int32 : (Int32.t, int) t
  | Bool : (bool, bool) t
  | String : (string, string) t

type packed = P : _ t -> packed

let dt_double = W.DoubleDataType.new_ ()
let dt_float = W.FloatDataType.new_ ()
let dt_int64 = W.Int64DataType.new_ ()
let dt_int32 = W.Int32DataType.new_ ()
let dt_bool = W.BooleanDataType.new_ ()
let dt_string = W.StringDataType.new_ ()

let to_wrapper : type a b. (a, b) t -> W.DataType.t = function
  | Double -> (dt_double :> W.DataType.t)
  | Float -> (dt_float :> W.DataType.t)
  | Int64 -> (dt_int64 :> W.DataType.t)
  | Int32 -> (dt_int32 :> W.DataType.t)
  | Bool -> (dt_bool :> W.DataType.t)
  | String -> (dt_string :> W.DataType.t)

let equal_dt : type a b. (a, b) t -> W.DataType.t -> bool =
 fun t dt' ->
  let dt = to_wrapper t in
  W.DataType.equal dt' dt

let check_equal : type a b. (a, b) t -> W.DataType.t -> unit Or_error.t =
 fun t dt' ->
  let dt = to_wrapper t in
  if W.DataType.equal dt' dt
  then Ok ()
  else
    Or_error.errorf
      "different data types: %s %s"
      (W.DataType.to_string dt)
      (W.DataType.to_string dt')

module Public = struct
  type nonrec ('a, 'b) t = ('a, 'b) t
  type nonrec packed = packed

  let double = Double
  let float = Float
  let int64 = Int64
  let int32 = Int32
  let bool = Bool
  let string = String
end
