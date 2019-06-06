open Base

(* TODO: support more types, e.g. UInt, Int16, Int8, ... *)
type (_, _) t =
  | Double : (float, float) t
  | Float : (float, float) t
  | Int64 : (Int64.t, int) t
  | Int32 : (Int32.t, int) t
  | Bool : (bool, bool) t
  | String : (string, string) t

type packed = P : _ t -> packed

val double : (float, float) t
val float : (float, float) t
val int64 : (Int64.t, int) t
val int32 : (Int32.t, int) t
val bool : (bool, bool) t
val string : (string, string) t
