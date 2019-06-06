open Base

type (_, _) t =
  | Double : (float, float) t
  | Float : (float, float) t
  | Int64 : (Int64.t, int) t
  | Int32 : (Int32.t, int) t
  | Bool : (bool, bool) t
  | String : (string, string) t

type packed = P : _ t -> packed

let double = Double
let float = Float
let int64 = Int64
let int32 = Int32
let bool = Bool
let string = String
