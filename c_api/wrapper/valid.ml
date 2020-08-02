open! Base

type t = (int, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

let create_all_valid dim =
  let t = Bigarray.Array1.create Int8_unsigned C_layout ((dim + 7) / 8) in
  Bigarray.Array1.fill t 255;
  t

let mask i = 1 lsl (i land 0b111)
let unmask i = lnot (mask i) land 255
let get t i = t.{i / 8} land mask i <> 0

let set t i b =
  let index = i / 8 in
  if b then t.{index} <- t.{index} lor mask i else t.{index} <- t.{index} land unmask i

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
