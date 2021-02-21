open! Base

module type Intf = sig
  type t
  type elem

  val create : unit -> t
  val append : t -> elem -> unit
  val append_null : ?n:int -> t -> unit
  val append_opt : t -> elem option -> unit
  val length : t -> int
  val null_count : t -> int
end

module Double = struct
  include Wrapper.DoubleBuilder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = length t |> Int64.to_int_exn
  let null_count t = null_count t |> Int64.to_int_exn
end

module String = struct
  include Wrapper.StringBuilder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = length t |> Int64.to_int_exn
  let null_count t = null_count t |> Int64.to_int_exn
end

module NativeInt = struct
  include Wrapper.Int64Builder

  let append t v = append t (Int64.of_int v)

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = length t |> Int64.to_int_exn
  let null_count t = null_count t |> Int64.to_int_exn
end

module Int64 = struct
  include Wrapper.Int64Builder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = length t |> Int64.to_int_exn
  let null_count t = null_count t |> Int64.to_int_exn
end
