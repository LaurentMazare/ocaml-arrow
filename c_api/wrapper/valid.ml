open! Base

type t =
  { length : int
  ; data : (int, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
  }

let create_all_valid length =
  let data = Bigarray.Array1.create Int8_unsigned C_layout ((length + 7) / 8) in
  Bigarray.Array1.fill data 255;
  { length; data }

let mask i = 1 lsl (i land 0b111)
let unmask i = lnot (mask i) land 255
let get t i = t.data.{i / 8} land mask i <> 0

let set t i b =
  let index = i / 8 in
  if b
  then t.data.{index} <- t.data.{index} lor mask i
  else t.data.{index} <- t.data.{index} land unmask i

let length t = t.length

let num_true t =
  (* TODO: optimize or memoize. *)
  let res = ref 0 in
  for i = 0 to length t - 1 do
    if get t i then Int.incr res
  done;
  !res

let num_false t = length t - num_true t
let bigarray t = t.data

let%expect_test _ =
  let round_trip bool_list =
    let t = create_all_valid (List.length bool_list) in
    List.iteri bool_list ~f:(fun i b -> set t i b);
    List.init (List.length bool_list) ~f:(get t)
  in
  List.iter
    ~f:(fun s ->
      let round_trip_s =
        String.to_list s
        |> List.map ~f:(function
               | '1' -> true
               | '0' -> false
               | _ -> assert false)
        |> round_trip
        |> List.map ~f:(function
               | true -> "1"
               | false -> "0")
        |> String.concat ~sep:""
      in
      if String.( <> ) s round_trip_s then Stdio.printf "<%s> <%s>\n%!" s round_trip_s)
    [ ""
    ; "0"
    ; "1"
    ; "00"
    ; "01"
    ; "10"
    ; "11"
    ; "1110"
    ; "1111"
    ; "11110"
    ; "0000000"
    ; "0000001"
    ; "00000000"
    ; "11111111"
    ; "10100001"
    ; "01011011"
    ; "11111111111101111111"
    ; "11101010101011111"
    ];
  [%expect {| |}]
