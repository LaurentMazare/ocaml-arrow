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
  let res = ref 0 in
  let length = t.length in
  for byte_index = 0 to (length / 8) - 1 do
    res := !res + Int.popcount t.data.{byte_index}
  done;
  let last_byte_index = length / 8 in
  let last_bits = length % 8 in
  if last_bits > 0
  then res := !res + Int.popcount (t.data.{last_byte_index} land ((1 lsl last_bits) - 1));
  !res

let num_false t = length t - num_true t
let bigarray t = t.data

let%expect_test _ =
  let of_bool_list bool_list =
    let t = create_all_valid (List.length bool_list) in
    List.iteri bool_list ~f:(fun i b -> set t i b);
    t
  in
  let to_bool_list t = List.init t.length ~f:(get t) in
  let rec gen len =
    if len = 0
    then [ "" ]
    else (
      let strs = gen (len - 1) in
      List.map ~f:(( ^ ) "0") strs @ List.map ~f:(( ^ ) "1") strs)
  in
  let strs = List.init 18 ~f:gen |> List.concat in
  List.iter
    ~f:(fun s ->
      let t =
        String.to_list s
        |> List.map ~f:(function
               | '1' -> true
               | '0' -> false
               | _ -> assert false)
        |> of_bool_list
      in
      let round_trip_s =
        to_bool_list t
        |> List.map ~f:(function
               | true -> "1"
               | false -> "0")
        |> String.concat ~sep:""
      in
      let num_true_bis = String.count s ~f:(Char.( = ) '1') in
      let num_false_bis = String.count s ~f:(Char.( = ) '0') in
      if num_true t <> num_true_bis || num_false t <> num_false_bis
      then Stdio.printf "%s %d %d\n" s (num_true t) (num_false t);
      if String.( <> ) s round_trip_s then Stdio.printf "<%s> <%s>\n%!" s round_trip_s)
    (strs @ [ "11111111111101111111"; "11101010101011111" ]);
  [%expect {| |}]
