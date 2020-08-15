open Ppxlib

val arrow : Deriving.t

module Reader : sig
  val deriver : Deriving.t
end

module Writer : sig
  val deriver : Deriving.t
end
