open! Base
include Wrapper.Writer

let col (type a) (data : a array) (type_ : a Table.col_type) ~name =
  match type_ with
  | Int -> int data ~name
  | Float -> float data ~name
  | Utf8 -> utf8 data ~name
  | Date -> date data ~name
  | Time_ns -> time_ns data ~name
  | Bool ->
    let bs = Valid.create_all_valid (Array.length data) in
    Array.iteri data ~f:(fun i a -> if not a then Valid.set bs i a);
    bitset bs ~name

let col_opt (type a) (data : a option array) (type_ : a Table.col_type) ~name =
  match type_ with
  | Int -> int_opt data ~name
  | Float -> float_opt data ~name
  | Utf8 -> utf8_opt data ~name
  | Date -> date_opt data ~name
  | Time_ns -> time_ns_opt data ~name
  | Bool ->
    let bs = Valid.create_all_valid (Array.length data) in
    let valid = Valid.create_all_valid (Array.length data) in
    Array.iteri data ~f:(fun i a ->
        match a with
        | None -> Valid.set valid i false
        | Some false -> Valid.set bs i false
        | Some true -> ());
    bitset valid ~name
