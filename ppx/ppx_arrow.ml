open Base
open Ppxlib
open Ast_builder.Default

let raise_errorf ~loc fmt = Location.raise_errorf ~loc (Caml.( ^^ ) "ppx_arrow: " fmt)

let floatable =
  Attribute.declare
    "arrow.floatable"
    Attribute.Context.label_declaration
    Ast_pattern.(pstr nil)
    (fun x -> x)

let intable =
  Attribute.declare
    "arrow.intable"
    Attribute.Context.label_declaration
    Ast_pattern.(pstr nil)
    (fun x -> x)

let stringable =
  Attribute.declare
    "arrow.stringable"
    Attribute.Context.label_declaration
    Ast_pattern.(pstr nil)
    (fun x -> x)

module Attr = struct
  type t =
    { kind : [ `intable | `stringable | `floatable ]
    ; is_option : bool
    }
end

let attribute field ~loc =
  let intable = Attribute.get intable field in
  let floatable = Attribute.get floatable field in
  let stringable = Attribute.get stringable field in
  let is_option =
    match field.pld_type.ptyp_desc with
    | Ptyp_constr ({ txt = Lident "option"; _ }, [ _ ]) -> true
    | _ -> false
  in
  match intable, floatable, stringable with
  | Some _, None, None -> Some { Attr.kind = `intable; is_option }
  | None, Some _, None -> Some { Attr.kind = `floatable; is_option }
  | None, None, Some _ -> Some { Attr.kind = `stringable; is_option }
  | None, None, None -> None
  | _ ->
    raise_errorf "cannot have more than one of intable, floatable, or stringable" ~loc

let lident ~loc str = Loc.make ~loc (Lident str)

let fresh_label =
  let counter = ref 0 in
  fun ~loc ->
    Int.incr counter;
    let label = Printf.sprintf "_lbl_%d" !counter in
    ppat_var (Loc.make ~loc label) ~loc, pexp_ident (lident ~loc label) ~loc

let closure_of_fn (fn : expression -> expression) ~loc : expression =
  let loc = { loc with loc_ghost = true } in
  let arg_pat, arg_expr = fresh_label ~loc in
  pexp_fun Nolabel ~loc None arg_pat (fn arg_expr)

let open_runtime expr ~loc =
  [%expr
    let open! Ppx_arrow_runtime in
    [%e expr]]

(* If a field has type [A.B.t] return [A.B.fn_name] as an expr. *)
let fn_from_field_module field ~fn_name ~loc =
  let ident =
    match field.pld_type.ptyp_desc with
    | Ptyp_constr
        ({ txt = Lident "option"; _ }, [ { ptyp_desc = Ptyp_constr ({ txt; _ }, []); _ } ])
    | Ptyp_constr ({ loc = _; txt }, []) ->
      (match txt with
      | Lident _ -> lident ~loc fn_name
      | Ldot (modl, _) -> Ldot (modl, fn_name) |> Loc.make ~loc
      | Lapply _ -> raise_errorf ~loc "'%s' apply not supported" (Longident.name txt))
    | _ ->
      raise_errorf
        ~loc
        "cannot extract module name from '%a'"
        Pprintast.core_type
        field.pld_type
  in
  pexp_ident ident ~loc

(* Generated function names. *)
(* TODO: maybe generate some to_table/of_table functions? *)
let arrow_read tname = "arrow_read_" ^ tname
let arrow_write tname = "arrow_write_" ^ tname

module Signature : sig
  val gen
    :  [ `read | `write | `both ]
    -> (signature, rec_flag * type_declaration list) Deriving.Generator.t
end = struct
  let read_type td ~loc =
    [%type: string -> [%t Ppxlib.core_type_of_type_declaration td] array]

  let write_type td ~loc =
    [%type: string -> [%t Ppxlib.core_type_of_type_declaration td] array -> unit]

  let of_td ~kind td : signature_item list =
    let { Location.loc; txt = tname } = td.ptype_name in
    if not (List.is_empty td.ptype_params)
    then raise_errorf "parametered types are not supported" ~loc;
    let psig_value ~name ~type_ =
      psig_value ~loc (value_description ~loc ~name:(Loc.make name ~loc) ~type_ ~prim:[])
    in
    let read_type = read_type td ~loc in
    let write_type = write_type td ~loc in
    match kind with
    | `both ->
      [ psig_value ~name:(arrow_read tname) ~type_:read_type
      ; psig_value ~name:(arrow_write tname) ~type_:write_type
      ]
    | `read -> [ psig_value ~name:(arrow_read tname) ~type_:read_type ]
    | `write -> [ psig_value ~name:(arrow_write tname) ~type_:write_type ]

  let gen kind =
    Deriving.Generator.make_noarg (fun ~loc:_ ~path:_ (_rec_flag, tds) ->
        List.concat_map tds ~f:(of_td ~kind))
end

module Structure : sig
  val gen
    :  [ `read | `write | `both ]
    -> (structure, rec_flag * type_declaration list) Deriving.Generator.t
end = struct
  let expr_of_tds ~loc ~record tds =
    let exprs =
      List.map tds ~f:(fun td ->
          let { Location.loc; txt = _ } = td.ptype_name in
          if not (List.is_empty td.ptype_params)
          then raise_errorf "parametered types are not supported" ~loc;
          let expr arg_t =
            match td.ptype_kind with
            | Ptype_abstract -> raise_errorf ~loc "abstract types not supported"
            | Ptype_variant _ -> raise_errorf ~loc "variant types not supported"
            | Ptype_record fields -> record fields ~loc arg_t
            | Ptype_open -> raise_errorf ~loc "open types not supported"
          in
          closure_of_fn expr ~loc)
    in
    pexp_tuple ~loc exprs

  let extract_ident ident ~loc =
    let rec loop = function
      | Lident ident -> [ ident ]
      | Ldot (modl, ident) -> ident :: loop modl
      | Lapply _ -> raise_errorf ~loc "'%s' apply not supported" (Longident.name ident)
    in
    loop ident

  let runtime_fn field ~fn_name ~loc =
    let modl =
      match attribute field ~loc with
      | Some { kind = `intable; is_option = false } -> "Int_col"
      | Some { kind = `intable; is_option = true } -> "Int_option_col"
      | Some { kind = `floatable; is_option = false } -> "Float_col"
      | Some { kind = `floatable; is_option = true } -> "Float_option_col"
      | Some { kind = `stringable; is_option = false } -> "String_col"
      | Some { kind = `stringable; is_option = true } -> "String_option_col"
      | None ->
        (match field.pld_type.ptyp_desc with
        | Ptyp_constr ({ loc = _; txt }, []) ->
          (match extract_ident txt ~loc with
          | [] -> assert false
          | [ ident ] -> String.capitalize ident ^ "_col"
          | "t" :: modl :: _ -> modl ^ "_col"
          | _ -> raise_errorf ~loc "'%s' base type not supported" (Longident.name txt))
        | Ptyp_constr
            ( { txt = Lident "option"; _ }
            , [ { ptyp_desc = Ptyp_constr ({ txt; _ }, []); _ } ] ) ->
          (match extract_ident txt ~loc with
          | [] -> assert false
          | [ ident ] -> String.capitalize ident ^ "_option_col"
          | "t" :: modl :: _ -> modl ^ "_option_col"
          | _ -> raise_errorf ~loc "'%s' option type not supported" (Longident.name txt))
        | _ -> raise_errorf ~loc "'%a' not supported" Pprintast.core_type field.pld_type)
    in
    pexp_ident (Loc.make (Ldot (Lident modl, fn_name)) ~loc) ~loc

  let read_fields fields ~loc args =
    let record_fields =
      List.map fields ~f:(fun field ->
          let get = runtime_fn field ~fn_name:"get" ~loc in
          let expr = [%expr [%e get] [%e evar field.pld_name.txt ~loc] idx] in
          let apply_fn ~fn_name ~is_option =
            let fn = fn_from_field_module field ~fn_name ~loc in
            if is_option
            then [%expr Option.map ~f:[%e fn] [%e expr]]
            else pexp_apply fn ~loc [ Nolabel, expr ]
          in
          let expr =
            match attribute field ~loc with
            | Some { kind = `stringable; is_option } ->
              apply_fn ~fn_name:"of_string" ~is_option
            | Some { kind = `floatable; is_option } ->
              apply_fn ~fn_name:"of_float" ~is_option
            | Some { kind = `intable; is_option } ->
              apply_fn ~fn_name:"of_int_exn" ~is_option
            | None -> expr
          in
          lident field.pld_name.txt ~loc, expr)
    in
    let pat str = ppat_var (Loc.make ~loc str) ~loc in
    let create_columns =
      List.map fields ~f:(fun field ->
          let name_as_string = estring ~loc field.pld_name.txt in
          let of_table = runtime_fn field ~fn_name:"of_table" ~loc in
          let expr = [%expr [%e of_table] table [%e name_as_string]] in
          value_binding ~loc ~pat:(pat field.pld_name.txt) ~expr)
    in
    [%expr
      Caml.Array.init (Arrow_c_api.Table.num_rows table) (fun idx ->
          [%e pexp_record record_fields ~loc None])]
    |> pexp_let ~loc Nonrecursive create_columns
    |> pexp_let
         ~loc
         Nonrecursive
         (* TODO: only select the appropriate columns. *)
         [ value_binding
             ~loc
             ~pat:(pat "table")
             ~expr:[%expr Arrow_c_api.File_reader.table [%e args]]
         ]
    |> open_runtime ~loc

  let write_fields fields ~loc args =
    let pat str = ppat_var (Loc.make ~loc str) ~loc in
    let create_columns =
      List.map fields ~f:(fun field ->
          let init = runtime_fn field ~fn_name:"init" ~loc in
          let expr = [%expr [%e init] __arrow_len] in
          value_binding ~loc ~pat:(pat field.pld_name.txt) ~expr)
    in
    let set_columns =
      List.map fields ~f:(fun field ->
          let set = runtime_fn field ~fn_name:"set" ~loc in
          let array = evar field.pld_name.txt ~loc in
          let value =
            pexp_field
              [%expr [%e args].(__arrow_idx)]
              (lident ~loc field.pld_name.txt)
              ~loc
          in
          let apply_fn ~fn_name ~is_option =
            let fn = fn_from_field_module field ~fn_name ~loc in
            if is_option
            then [%expr Option.map ~f:[%e fn] [%e value]]
            else pexp_apply fn ~loc [ Nolabel, value ]
          in
          let value =
            match attribute field ~loc with
            | Some { kind = `stringable; is_option } ->
              apply_fn ~fn_name:"to_string" ~is_option
            | Some { kind = `floatable; is_option } ->
              apply_fn ~fn_name:"to_float" ~is_option
            | Some { kind = `intable; is_option } ->
              apply_fn ~fn_name:"to_int_exn" ~is_option
            | None -> value
          in
          [%expr [%e set] [%e array] __arrow_idx [%e value]])
    in
    let col_list =
      List.map fields ~f:(fun field ->
          let name_as_string = estring ~loc field.pld_name.txt in
          let writer_col = runtime_fn field ~fn_name:"writer_col" ~loc in
          let array = evar field.pld_name.txt ~loc in
          [%expr [%e writer_col] [%e array] [%e name_as_string]])
    in
    closure_of_fn ~loc (fun filename ->
        [%expr
          for __arrow_idx = 0 to __arrow_len - 1 do
            [%e esequence set_columns ~loc]
          done;
          Arrow_c_api.Writer.write [%e filename] ~cols:[%e elist col_list ~loc]]
        |> pexp_let ~loc Nonrecursive create_columns
        |> pexp_let
             ~loc
             Nonrecursive
             [ value_binding
                 ~loc
                 ~pat:(pat "__arrow_len")
                 ~expr:[%expr Caml.Array.length [%e args]]
             ]
        |> open_runtime ~loc)

  let gen kind =
    let attributes =
      [ Attribute.T intable; Attribute.T floatable; Attribute.T stringable ]
    in
    Deriving.Generator.make_noarg ~attributes (fun ~loc ~path:_ (rec_flag, tds) ->
        let mk_pat mk_ =
          let pats =
            List.map tds ~f:(fun td ->
                let { Location.loc; txt = tname } = td.ptype_name in
                let name = mk_ tname in
                ppat_var ~loc (Loc.make name ~loc))
          in
          ppat_tuple ~loc pats
        in
        let tds = List.map tds ~f:name_type_params_in_td in
        let read_expr = expr_of_tds ~loc ~record:read_fields in
        let write_expr = expr_of_tds ~loc ~record:write_fields in
        let bindings =
          match kind with
          | `both ->
            [ value_binding ~loc ~pat:(mk_pat arrow_read) ~expr:(read_expr tds)
            ; value_binding ~loc ~pat:(mk_pat arrow_write) ~expr:(write_expr tds)
            ]
          | `read -> [ value_binding ~loc ~pat:(mk_pat arrow_read) ~expr:(read_expr tds) ]
          | `write ->
            [ value_binding ~loc ~pat:(mk_pat arrow_write) ~expr:(write_expr tds) ]
        in
        [ pstr_value ~loc (really_recursive rec_flag tds) bindings ])
end

let arrow =
  Deriving.add
    "arrow"
    ~str_type_decl:(Structure.gen `both)
    ~sig_type_decl:(Signature.gen `both)

module Reader = struct
  let name = "arrow_reader"

  let deriver =
    Deriving.add
      name
      ~str_type_decl:(Structure.gen `read)
      ~sig_type_decl:(Signature.gen `read)
end

module Writer = struct
  let name = "arrow_writer"

  let deriver =
    Deriving.add
      name
      ~str_type_decl:(Structure.gen `write)
      ~sig_type_decl:(Signature.gen `write)
end
