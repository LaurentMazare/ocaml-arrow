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

type t2 =
  { x : int
  ; t : Time_ns.t
  ; t_opt : Time_ns.t option
  ; s : Time_ns.Span.t
  ; s_opt : Time_ns.Span.t option
  ; o : Time_ns.Ofday.t
  ; o_opt : Time_ns.Ofday.t option
  }
[@@deriving arrow]
