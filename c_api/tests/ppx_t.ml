open! Core_kernel

type t =
  { x : int
  ; y : float
  ; z : string
  ; x_opt : int option
  ; y_opt : float option
  ; z_opt : string option
  }
[@@deriving arrow, sexp_of]
