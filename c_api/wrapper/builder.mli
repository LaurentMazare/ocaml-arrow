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

module Double : sig
  include Intf with type elem := float
end

module Int64 : sig
  include Intf with type elem := Int64.t
end

module NativeInt : sig
  include Intf with type elem := int
end

module String : sig
  include Intf with type elem := string
end
