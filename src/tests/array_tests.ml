open Base
open Arrow

let%expect_test _ =
  let array = Array.of_list [ 1.0; 2.0; 3.0 ] Data_type.double in
  Stdio.printf "%d\n%!" (Array.length array);
  [%expect {|
        3
      |}]
