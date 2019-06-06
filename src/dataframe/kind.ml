type (_, _) t =
  | Double : ([ `double ], float) t
  | Float : ([ `float ], float) t
  | Int64 : ([ `int64 ], int) t
  | Int32 : ([ `int32 ], int) t
  | Int16 : ([ `int16 ], int) t
  | Int8 : ([ `int8 ], int) t
  | Uint64 : ([ `uint64 ], int) t
  | Uint32 : ([ `uint32 ], int) t
  | Uint16 : ([ `uint16 ], int) t
  | Uint8 : ([ `uint8 ], int) t
  | Bool : ([ `bool ], bool) t
  | String : ([ `string ], string) t

type packed = P : _ t -> packed

let double = Double
let float = Float
let int64 = Int64
let int32 = Int32
let int16 = Int16
let int8 = Int8
let uint64 = Uint64
let uint32 = Uint32
let uint16 = Uint16
let uint8 = Uint8
let bool = Bool
let string = String
