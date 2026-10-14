//! rustdoc の `Type` と制約を、`rs.json` が書く形に合わせた文字列にする。
//!
//! 寿命は捨てる。標準ライブラリの型は最後の名前だけ、ほかのクレートの型は `<クレート名>::<名前>` で表す
//! (例: `Option`、`chrono::DateTime`)。クレート内の型は最後の名前だけで表す (例: `SeqNr`)。

use rustdoc_types::{
  AssocItemConstraintKind, Crate, GenericArg, GenericArgs, GenericBound, Path, Term, TraitBoundModifier, Type,
};

/// 型を正規化した文字列にする。
pub fn render_type(krate: &Crate, ty: &Type) -> String {
  match ty {
    Type::ResolvedPath(path) => render_path(krate, path),
    Type::Generic(name) | Type::Primitive(name) => name.clone(),
    Type::BorrowedRef { is_mutable, type_, .. } => {
      let mutability = if *is_mutable { "mut " } else { "" };
      format!("&{mutability}{}", render_type(krate, type_))
    }
    Type::Tuple(types) => {
      let rendered: Vec<String> = types.iter().map(|inner| render_type(krate, inner)).collect();
      format!("({})", rendered.join(", "))
    }
    Type::Slice(inner) => format!("[{}]", render_type(krate, inner)),
    Type::Array { type_, len } => format!("[{}; {len}]", render_type(krate, type_)),
    Type::RawPointer { is_mutable, type_ } => {
      let mutability = if *is_mutable { "mut" } else { "const" };
      format!("*{mutability} {}", render_type(krate, type_))
    }
    Type::ImplTrait(bounds) => format!("impl {}", render_bounds(krate, bounds)),
    Type::DynTrait(dyn_trait) => {
      let traits: Vec<String> = dyn_trait
        .traits
        .iter()
        .map(|poly_trait| render_path(krate, &poly_trait.trait_))
        .collect();
      format!("dyn {}", traits.join(" + "))
    }
    Type::QualifiedPath {
      name,
      self_type,
      trait_,
      ..
    } => match (self_type.as_ref(), trait_) {
      (Type::Generic(owner), _) if owner == "Self" => format!("Self::{name}"),
      (_, Some(trait_path)) => format!(
        "<{} as {}>::{name}",
        render_type(krate, self_type),
        render_path(krate, trait_path)
      ),
      (_, None) => format!("{}::{name}", render_type(krate, self_type)),
    },
    Type::FunctionPointer(_) => "fn".to_string(),
    Type::Pat { type_, .. } => render_type(krate, type_),
    Type::Infer => "_".to_string(),
  }
}

/// 制約を `+` でつないだ文字列にする。
pub fn render_bounds(krate: &Crate, bounds: &[GenericBound]) -> String {
  let rendered: Vec<String> = bounds.iter().map(|bound| render_bound(krate, bound)).collect();
  rendered.join(" + ")
}

/// 制約 1 つを文字列にする。トレイトの制約は名前 (と型の引数)、寿命の制約は寿命の名前 (`'static`)。
pub fn render_bound(krate: &Crate, bound: &GenericBound) -> String {
  match bound {
    GenericBound::TraitBound { trait_, modifier, .. } => {
      let prefix = match modifier {
        TraitBoundModifier::None => "",
        TraitBoundModifier::Maybe => "?",
        TraitBoundModifier::MaybeConst => "~const ",
      };
      format!("{prefix}{}", render_path(krate, trait_))
    }
    GenericBound::Outlives(lifetime) => lifetime.clone(),
    GenericBound::Use(_) => "use<..>".to_string(),
  }
}

/// 名前と型の引数を合わせた文字列にする。
fn render_path(krate: &Crate, path: &Path) -> String {
  let name = path_name(krate, path);
  match path.args.as_deref() {
    Some(args) => format!("{name}{}", render_generic_args(krate, args)),
    None => name,
  }
}

/// クレート内の型と標準ライブラリの型は最後の名前、ほかのクレートの型は `<クレート名>::<名前>` にする。
fn path_name(krate: &Crate, path: &Path) -> String {
  let written_last = path.path.rsplit("::").next().unwrap_or(&path.path);
  let Some(summary) = krate.paths.get(&path.id) else {
    return written_last.to_string();
  };
  let last = summary.path.last().map_or(written_last, String::as_str);
  if summary.crate_id == 0 {
    return last.to_string();
  }
  match krate
    .external_crates
    .get(&summary.crate_id)
    .map(|external| external.name.as_str())
  {
    Some("core" | "alloc" | "std") | None => last.to_string(),
    Some(crate_name) => format!("{crate_name}::{last}"),
  }
}

fn render_generic_args(krate: &Crate, args: &GenericArgs) -> String {
  match args {
    GenericArgs::AngleBracketed { args, constraints } => {
      let mut rendered: Vec<String> = args
        .iter()
        .filter_map(|arg| match arg {
          GenericArg::Lifetime(_) => None,
          GenericArg::Type(ty) => Some(render_type(krate, ty)),
          GenericArg::Const(constant) => Some(constant.expr.clone()),
          GenericArg::Infer => Some("_".to_string()),
        })
        .collect();
      for constraint in constraints {
        rendered.push(match &constraint.binding {
          AssocItemConstraintKind::Equality(Term::Type(ty)) => {
            format!("{} = {}", constraint.name, render_type(krate, ty))
          }
          AssocItemConstraintKind::Equality(Term::Constant(constant)) => {
            format!("{} = {}", constraint.name, constant.expr)
          }
          AssocItemConstraintKind::Constraint(bounds) => {
            format!("{}: {}", constraint.name, render_bounds(krate, bounds))
          }
        });
      }
      if rendered.is_empty() {
        String::new()
      } else {
        format!("<{}>", rendered.join(", "))
      }
    }
    GenericArgs::Parenthesized { inputs, output } => {
      let rendered: Vec<String> = inputs.iter().map(|ty| render_type(krate, ty)).collect();
      match output {
        Some(output) => format!("({}) -> {}", rendered.join(", "), render_type(krate, output)),
        None => format!("({})", rendered.join(", ")),
      }
    }
    GenericArgs::ReturnTypeNotation => "(..)".to_string(),
  }
}

/// `#[async_trait]` が書き換えた戻り値 `Pin<Box<dyn Future<Output = X> + Send + 'async_trait>>` から `X` を取り出す。
/// その形でなければ `None`。
pub fn unwrap_async_output<'a>(krate: &Crate, ty: &'a Type) -> Option<&'a Type> {
  let pinned = single_type_argument(krate, ty, "Pin")?;
  let boxed = single_type_argument(krate, pinned, "Box")?;
  let Type::DynTrait(dyn_trait) = boxed else {
    return None;
  };
  dyn_trait.traits.iter().find_map(|poly_trait| {
    if path_name(krate, &poly_trait.trait_) != "Future" {
      return None;
    }
    let GenericArgs::AngleBracketed { constraints, .. } = poly_trait.trait_.args.as_deref()? else {
      return None;
    };
    constraints.iter().find_map(|constraint| match &constraint.binding {
      AssocItemConstraintKind::Equality(Term::Type(output)) if constraint.name == "Output" => Some(output),
      _ => None,
    })
  })
}

/// `#[async_trait]` が書き換えた戻り値の `dyn Future` が `Send` を持つか。`#[async_trait(?Send)]` なら持たない。
pub fn async_output_is_send(krate: &Crate, ty: &Type) -> bool {
  let Some(boxed) =
    single_type_argument(krate, ty, "Pin").and_then(|pinned| single_type_argument(krate, pinned, "Box"))
  else {
    return false;
  };
  let Type::DynTrait(dyn_trait) = boxed else {
    return false;
  };
  dyn_trait
    .traits
    .iter()
    .any(|poly_trait| path_name(krate, &poly_trait.trait_) == "Send")
}

/// 名前が `expected_name` のパスで、型の引数が 1 つのとき、その型の引数を返す。
fn single_type_argument<'a>(krate: &Crate, ty: &'a Type, expected_name: &str) -> Option<&'a Type> {
  let Type::ResolvedPath(path) = ty else {
    return None;
  };
  if path_name(krate, path) != expected_name {
    return None;
  }
  let GenericArgs::AngleBracketed { args, .. } = path.args.as_deref()? else {
    return None;
  };
  let mut types = args.iter().filter_map(|arg| match arg {
    GenericArg::Type(inner) => Some(inner),
    _ => None,
  });
  let first = types.next()?;
  types.next().is_none().then_some(first)
}
