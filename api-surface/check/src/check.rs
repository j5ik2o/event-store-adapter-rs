//! 公開 API (rustdoc の JSON) を `surface.json` と `rs.json` で照合する。
//!
//! 期待する形は `surface.json` と `rs.json` から導く。ずれは `Problem` の列で返し、飛ばさない。
//! 余分なメソッド・列挙の値・操作は、形が定めていないので報告しない。

use std::collections::BTreeMap;
use std::fmt;

use rustdoc_types::{Crate, Function, GenericBound, Generics, Id, Impl, Item, ItemEnum, Type, WherePredicate};

use crate::mapping::{ConstructorMapping, Mapping, TypeMapping};
use crate::render::{async_output_is_send, render_bound, render_type, unwrap_async_output};
use crate::surface::{Surface, SurfaceElement, SurfaceOperation, SurfaceType, TypeExpr};

/// 照合で見つけたずれ 1 件。`path` はずれた要素の場所、`message` は期待する形と実際の形。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Problem {
  pub path: String,
  pub message: String,
}

impl fmt::Display for Problem {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}: {}", self.path, self.message)
  }
}

/// `module_path` のモジュール (空ならクレートのルート) の公開 API を照合する。
///
/// ルートを照合するときは、クレートの名前も `rs.json` の `crate` と照合する。
pub fn check_api(krate: &Crate, module_path: &[&str], surface: &Surface, mapping: &Mapping) -> Vec<Problem> {
  let mut checker = Checker {
    krate,
    surface,
    mapping,
    module_items: Vec::new(),
    problems: Vec::new(),
  };
  checker.run(module_path);
  let mut problems: Vec<Problem> = Vec::new();
  for problem in checker.problems {
    if !problems.contains(&problem) {
      problems.push(problem);
    }
  }
  problems
}

/// 型の引数の書き方が変わる場所。データの型の中ではそのまま (`AID`)、ストアの中では `Self::AID`。
#[derive(Clone, Copy, PartialEq, Eq)]
enum Context {
  Data,
  Store,
}

/// 型の使われ方。取得のメソッドの戻り値、ストアの操作の入力、構築の引数、操作の出力。
#[derive(Clone, Copy)]
enum Position {
  Getter,
  Input,
  Build,
  Output,
}

struct Checker<'a> {
  krate: &'a Crate,
  surface: &'a Surface,
  mapping: &'a Mapping,
  /// 照合するモジュールの項目。
  module_items: Vec<Id>,
  problems: Vec<Problem>,
}

impl<'a> Checker<'a> {
  fn report(&mut self, path: impl Into<String>, message: impl Into<String>) {
    self.problems.push(Problem {
      path: path.into(),
      message: message.into(),
    });
  }

  fn run(&mut self, module_path: &[&str]) {
    if !self.locate_module(module_path) {
      return;
    }
    if module_path.is_empty() {
      self.check_crate_name();
    }
    self.check_conventions();
    self.check_coverage();
    for surface_type in &self.surface.types {
      self.check_type(surface_type);
    }
    self.check_store();
    self.check_error_categories();
  }

  // ---- 項目の探し方 ----

  fn locate_module(&mut self, module_path: &[&str]) -> bool {
    let Some(mut module) = self.krate.index.get(&self.krate.root) else {
      self.report("crate", "rustdoc の JSON にルートのモジュールがない");
      return false;
    };
    for segment in module_path {
      let Some(next) = self
        .child_items(module)
        .into_iter()
        .find(|item| item.name.as_deref() == Some(segment) && matches!(item.inner, ItemEnum::Module(_)))
      else {
        self.report("crate", format!("モジュール {segment} がない"));
        return false;
      };
      module = next;
    }
    let ItemEnum::Module(module_data) = &module.inner else {
      self.report("crate", "ルートが Module でない");
      return false;
    };
    self.module_items = module_data.items.clone();
    true
  }

  fn child_items(&self, module: &'a Item) -> Vec<&'a Item> {
    let ItemEnum::Module(module_data) = &module.inner else {
      return Vec::new();
    };
    module_data
      .items
      .iter()
      .filter_map(|id| self.krate.index.get(id))
      .collect()
  }

  /// 照合するモジュールの項目を名前で探す。`use` による再公開は元の項目までたどる。
  fn lookup(&self, name: &str) -> Option<&'a Item> {
    self
      .module_items
      .iter()
      .filter_map(|id| self.krate.index.get(id))
      .find_map(|item| match &item.inner {
        ItemEnum::Use(use_item) if !use_item.is_glob && use_item.name == name => {
          use_item.id.as_ref().and_then(|id| self.resolve_use(id))
        }
        ItemEnum::Use(_) => None,
        _ if item.name.as_deref() == Some(name) => Some(item),
        _ => None,
      })
  }

  fn resolve_use(&self, id: &Id) -> Option<&'a Item> {
    let target = self.krate.index.get(id)?;
    match &target.inner {
      ItemEnum::Use(use_item) => use_item.id.as_ref().and_then(|next| self.resolve_use(next)),
      _ => Some(target),
    }
  }

  fn item(&self, id: &Id) -> Option<&'a Item> {
    self.krate.index.get(id)
  }

  // ---- 対応表の決まり ----

  fn check_crate_name(&mut self) {
    let actual = self
      .krate
      .index
      .get(&self.krate.root)
      .and_then(|root| root.name.clone());
    if actual.as_deref() != Some(self.mapping.crate_name.as_str()) {
      self.report(
        "crate",
        format!(
          "crate is {}, rs.json says {}",
          actual.as_deref().unwrap_or("(unnamed)"),
          self.mapping.crate_name
        ),
      );
    }
  }

  fn check_conventions(&mut self) {
    let conventions = &self.mapping.conventions;
    if conventions.receiver != "&self" {
      self.report(
        "rs.json",
        format!(
          "unsupported receiver convention {:?} (supported: \"&self\")",
          conventions.receiver
        ),
      );
    }
    if conventions.async_style != "async_trait" {
      self.report(
        "rs.json",
        format!(
          "unsupported async convention {:?} (supported: \"async_trait\")",
          conventions.async_style
        ),
      );
    }
    if self.surface.version != self.mapping.surface_version {
      self.report(
        "rs.json",
        format!(
          "rs.json is for surface {}, surface.json is {}",
          self.mapping.surface_version, self.surface.version
        ),
      );
    }
    for (name, value_type) in &self.mapping.value_types {
      if value_type.type_parameter && value_type.bound.is_none() {
        self.report(
          format!("value_types.{name}"),
          "rs.json gives a type parameter without a bound",
        );
      }
    }
  }

  /// 対応表にない共通の項目を報告する (飛ばさない)。
  fn check_coverage(&mut self) {
    for name in self.surface.type_parameters.keys() {
      if !self.mapping.type_parameters.contains_key(name) {
        self.report(
          format!("type_parameters.{name}"),
          format!("rs.json has no mapping for type parameter {name}"),
        );
      }
    }
    for name in self.surface.value_types.keys() {
      if !self.mapping.value_types.contains_key(name) {
        self.report(
          format!("value_types.{name}"),
          format!("rs.json has no mapping for value type {name}"),
        );
      }
    }
  }

  /// `surface.json` の型の書き方を、`rs.json` の決まりで rs の型の文字列にする。
  fn render_surface(&self, text: &str, context: Context, position: Position) -> Result<String, String> {
    let expr = TypeExpr::parse(text)?;
    self.render_expr(&expr, context, position)
  }

  fn render_expr(&self, expr: &TypeExpr, context: Context, position: Position) -> Result<String, String> {
    let mapping = self.mapping;
    let conventions = &mapping.conventions;
    let name = expr.name.as_str();
    match name {
      "none" => Ok(conventions.none.clone()),
      "optional" | "list" => {
        let [inner] = expr.args.as_slice() else {
          return Err(format!("{name} takes one type argument"));
        };
        let template = if name == "optional" {
          &conventions.optional
        } else {
          &conventions.list
        };
        Ok(template.replace("{T}", &self.render_expr(inner, context, position)?))
      }
      _ => {
        if let Some(value_type) = mapping.value_types.get(name) {
          let rs = if value_type.type_parameter {
            self.type_parameter_name(&value_type.rs, context)
          } else {
            value_type.rs.clone()
          };
          let template = match position {
            Position::Getter => &value_type.getter,
            Position::Input => &value_type.input,
            Position::Build => &value_type.build,
            Position::Output => "{rs}",
          };
          Ok(template.replace("{rs}", &rs))
        } else if let Some(type_parameter) = mapping.type_parameters.get(name) {
          let rs = self.type_parameter_name(&type_parameter.rs, context);
          let template = match position {
            Position::Getter => &type_parameter.getter,
            Position::Input => &type_parameter.input,
            Position::Build => &type_parameter.build,
            Position::Output => "{rs}",
          };
          Ok(template.replace("{rs}", &rs))
        } else if let Some(type_mapping) = mapping.types.get(name) {
          let rs = self.instantiate_data_type(expr, type_mapping, context)?;
          let template = match position {
            Position::Getter => &conventions.data_type.getter,
            Position::Input => &conventions.data_type.input,
            Position::Build => &conventions.data_type.build,
            Position::Output => "{rs}",
          };
          Ok(template.replace("{rs}", &rs))
        } else {
          Err(format!("rs.json has no mapping for {name}"))
        }
      }
    }
  }

  /// データの型に型の引数を与えた rs の型 (例: `EventEnvelope<Self::AID, Self::P>`)。
  fn instantiate_data_type(
    &self,
    expr: &TypeExpr,
    type_mapping: &TypeMapping,
    context: Context,
  ) -> Result<String, String> {
    let surface_type = self
      .surface
      .types
      .iter()
      .find(|candidate| candidate.name == expr.name)
      .ok_or_else(|| format!("surface.json has no type {}", expr.name))?;
    if surface_type.type_parameters.len() != expr.args.len() {
      return Err(format!(
        "{} takes {} type argument(s), got {}",
        expr.name,
        surface_type.type_parameters.len(),
        expr.args.len()
      ));
    }
    let mut given: BTreeMap<&str, String> = BTreeMap::new();
    for (surface_parameter, argument) in surface_type.type_parameters.iter().zip(&expr.args) {
      let rs_parameter = self
        .mapping
        .type_parameters
        .get(surface_parameter)
        .ok_or_else(|| format!("rs.json has no mapping for type parameter {surface_parameter}"))?;
      given.insert(&rs_parameter.rs, self.render_expr(argument, context, Position::Output)?);
    }
    let arguments: Vec<String> = type_mapping
      .type_parameters
      .iter()
      .map(|rs_parameter| {
        given
          .get(rs_parameter.as_str())
          .cloned()
          .unwrap_or_else(|| self.type_parameter_name(rs_parameter, context))
      })
      .collect();
    Ok(format!("{}<{}>", type_mapping.rs, arguments.join(", ")))
  }

  fn type_parameter_name(&self, rs_name: &str, context: Context) -> String {
    match context {
      Context::Data => rs_name.to_string(),
      Context::Store => self.mapping.conventions.store_type_parameter.replace("{name}", rs_name),
    }
  }

  fn result_of(&self, success: &str) -> String {
    self
      .mapping
      .conventions
      .result
      .replace("{T}", success)
      .replace("{error}", &self.mapping.error_categories.rs)
  }

  // ---- 制約 ----

  /// 型の引数に付いた制約のうち、許していないものの一覧。
  fn disallowed_bounds<'b>(&self, bounds: impl IntoIterator<Item = &'b GenericBound>) -> Vec<String> {
    bounds
      .into_iter()
      .map(|bound| render_bound(self.krate, bound))
      .filter(|rendered| !self.mapping.allowed_type_parameter_bounds.contains(rendered))
      .collect()
  }

  fn allowed_bounds_text(&self) -> String {
    self.mapping.allowed_type_parameter_bounds.join(", ")
  }

  /// 共通の型の引数 (`P`・`A`) に対応する rs の型の引数の名前なら、共通の名前を返す。
  fn constrained_surface_parameter(&self, rs_name: &str) -> Option<&'a str> {
    self
      .surface
      .type_parameters
      .keys()
      .find(|surface_name| {
        self
          .mapping
          .type_parameters
          .get(surface_name.as_str())
          .is_some_and(|mapped| mapped.rs == rs_name)
      })
      .map(String::as_str)
  }

  // ---- データの型 ----

  fn check_type(&mut self, surface_type: &SurfaceType) {
    let type_name = surface_type.name.as_str();
    let Some(type_mapping) = self.mapping.types.get(type_name) else {
      self.report(type_name, format!("rs.json has no mapping for type {type_name}"));
      return;
    };
    let rs_name = type_mapping.rs.as_str();
    let Some(item) = self.lookup(rs_name) else {
      self.report(type_name, format!("{rs_name} is not exported"));
      return;
    };
    let ItemEnum::Struct(struct_item) = &item.inner else {
      self.report(type_name, format!("{rs_name} is not a struct"));
      return;
    };

    let actual_parameters = type_parameter_names(&struct_item.generics);
    if actual_parameters != type_mapping.type_parameters {
      self.report(
        type_name,
        format!(
          "{rs_name} has type parameters <{}>, want <{}>",
          actual_parameters.join(", "),
          type_mapping.type_parameters.join(", ")
        ),
      );
    }
    for surface_parameter in &surface_type.type_parameters {
      match self.mapping.type_parameters.get(surface_parameter) {
        None => self.report(
          format!("{type_name}.type_parameters.{surface_parameter}"),
          format!("rs.json has no mapping for type parameter {surface_parameter}"),
        ),
        Some(mapped) if !type_mapping.type_parameters.contains(&mapped.rs) => self.report(
          format!("{type_name}.type_parameters.{surface_parameter}"),
          format!(
            "{rs_name} has no type parameter {} (for {surface_parameter})",
            mapped.rs
          ),
        ),
        Some(_) => {}
      }
    }

    let impls = self.inherent_impls(&struct_item.impls);
    self.check_type_parameter_bounds(type_name, type_mapping, &struct_item.generics, &impls);

    let methods = self.impl_functions(&impls);
    for element in &surface_type.elements {
      self.check_getter(surface_type, type_mapping, element, &methods);
    }
    self.check_constructor(surface_type, type_mapping, &methods);
  }

  /// 型の定義と inherent の `impl` で、`P`・`A` に許していない制約を付けていないか。
  fn check_type_parameter_bounds(
    &mut self,
    type_name: &str,
    type_mapping: &TypeMapping,
    definition: &Generics,
    impls: &[&'a Impl],
  ) {
    let rs_name = type_mapping.rs.as_str();
    for (index, rs_parameter) in type_mapping.type_parameters.iter().enumerate() {
      let Some(surface_parameter) = self.constrained_surface_parameter(rs_parameter) else {
        continue;
      };
      let mut disallowed = self.disallowed_bounds(bounds_of(definition, rs_parameter));
      for impl_item in impls {
        if let Some(local_name) = impl_parameter_name(impl_item, index) {
          disallowed.extend(self.disallowed_bounds(bounds_of(&impl_item.generics, &local_name)));
        }
      }
      for bound in disallowed {
        self.report(
          format!("{type_name}.type_parameters.{surface_parameter}"),
          format!(
            "{rs_name}::{rs_parameter} has bound {bound}; allowed: {}",
            self.allowed_bounds_text()
          ),
        );
      }
    }
  }

  fn inherent_impls(&self, impl_ids: &[Id]) -> Vec<&'a Impl> {
    impl_ids
      .iter()
      .filter_map(|id| self.item(id))
      .filter_map(|item| match &item.inner {
        ItemEnum::Impl(impl_item)
          if impl_item.trait_.is_none() && !impl_item.is_synthetic && impl_item.blanket_impl.is_none() =>
        {
          Some(impl_item)
        }
        _ => None,
      })
      .collect()
  }

  fn impl_functions(&self, impls: &[&'a Impl]) -> BTreeMap<String, &'a Function> {
    let mut functions = BTreeMap::new();
    for impl_item in impls {
      for id in &impl_item.items {
        if let Some(Item {
          name: Some(name),
          inner: ItemEnum::Function(function),
          ..
        }) = self.item(id)
        {
          functions.entry(name.clone()).or_insert(function);
        }
      }
    }
    functions
  }

  fn check_getter(
    &mut self,
    surface_type: &SurfaceType,
    type_mapping: &TypeMapping,
    element: &SurfaceElement,
    methods: &BTreeMap<String, &'a Function>,
  ) {
    let path = format!("{}.{}", surface_type.name, element.name);
    let rs_name = type_mapping.rs.as_str();
    let Some(method) = type_mapping.getters.get(&element.name) else {
      self.report(path, format!("rs.json has no mapping for element {}", element.name));
      return;
    };
    let want = match self.render_surface(&element.type_, Context::Data, Position::Getter) {
      Ok(want) => want,
      Err(message) => {
        self.report(path, message);
        return;
      }
    };
    let Some(function) = methods.get(method) else {
      self.report(
        path,
        format!("{rs_name} has no method {method} (want fn {method}(&self) -> {want})"),
      );
      return;
    };
    if !takes_only_shared_self(&function.sig.inputs) {
      self.report(
        path,
        format!(
          "{rs_name}::{method} takes ({}), want (&self)",
          self.render_inputs(&function.sig.inputs)
        ),
      );
      return;
    }
    let actual = self.render_output(function);
    if actual != want {
      self.report(path, format!("{rs_name}::{method} returns {actual}, want {want}"));
    }
  }

  // ---- 構築 ----

  fn check_constructor(
    &mut self,
    surface_type: &SurfaceType,
    type_mapping: &TypeMapping,
    methods: &BTreeMap<String, &'a Function>,
  ) {
    let path = format!("{}.constructor", surface_type.name);
    let Some(constructor) = &type_mapping.constructor else {
      if surface_type.elements.iter().any(|element| element.required.is_some()) {
        self.report(
          path,
          format!("rs.json has no mapping for the constructor of {}", surface_type.name),
        );
      }
      return;
    };
    if !self.constructor_mapping_agrees(&path, surface_type, constructor) {
      return;
    }
    let rs_name = type_mapping.rs.as_str();

    // 構築の関数が必須の要素をすべて受け取る。
    let function_name = constructor.function.as_str();
    match methods.get(function_name) {
      None => self.report(path.clone(), format!("{rs_name} has no function {function_name}")),
      Some(function) => {
        let expected = self.expected_inputs(surface_type, &constructor.params);
        self.check_constructor_inputs(&path, rs_name, function_name, function, &expected);
        self.check_returns_self(&path, rs_name, function_name, function, type_mapping);
      }
    }

    // 省略できる要素は、足すメソッドで与える。
    for (element_name, builder_name) in &constructor.builders {
      let Some(element) = surface_type
        .elements
        .iter()
        .find(|candidate| &candidate.name == element_name)
      else {
        continue;
      };
      match methods.get(builder_name) {
        None => self.report(
          path.clone(),
          format!("{rs_name} has no builder {builder_name} for optional element {element_name}"),
        ),
        Some(function) => {
          let want_input = match self.render_surface(&element.type_, Context::Data, Position::Build) {
            Ok(want) => want,
            Err(message) => {
              self.report(path.clone(), message);
              continue;
            }
          };
          let actual: Vec<String> = function
            .sig
            .inputs
            .iter()
            .map(|(_, ty)| render_type(self.krate, ty))
            .collect();
          let want = vec!["Self".to_string(), want_input];
          if actual != want {
            self.report(
              path.clone(),
              format!(
                "{rs_name}::{builder_name} takes ({}), want ({})",
                actual.join(", "),
                want.join(", ")
              ),
            );
          }
          self.check_returns_self(&path, rs_name, builder_name, function, type_mapping);
        }
      }
    }
  }

  /// `rs.json` の構築の対応が `surface.json` の `required` と食い違っていないか。食い違いがなければ `true`。
  fn constructor_mapping_agrees(
    &mut self,
    path: &str,
    surface_type: &SurfaceType,
    constructor: &ConstructorMapping,
  ) -> bool {
    let before = self.problems.len();
    for element in &surface_type.elements {
      let name = element.name.as_str();
      match element.required {
        Some(true) if !constructor.params.iter().any(|param| param == name) => {
          self.report(
            path,
            format!("rs.json constructor does not take required element {name}"),
          );
        }
        Some(false) => {
          if constructor.params.iter().any(|param| param == name) {
            self.report(path, format!("rs.json constructor takes optional element {name}"));
          }
          if !constructor.builders.contains_key(name) {
            self.report(
              path,
              format!("rs.json has no mapping for the builder of optional element {name}"),
            );
          }
        }
        _ => {}
      }
    }
    for param in &constructor.params {
      if !surface_type.elements.iter().any(|element| &element.name == param) {
        self.report(
          path,
          format!(
            "rs.json constructor param {param} is not an element of {}",
            surface_type.name
          ),
        );
      }
    }
    for builder in constructor.builders.keys() {
      if !surface_type.elements.iter().any(|element| &element.name == builder) {
        self.report(
          path,
          format!("rs.json builder {builder} is not an element of {}", surface_type.name),
        );
      }
    }
    self.problems.len() == before
  }

  /// 構築が受け取るべき要素と、その rs の型。
  fn expected_inputs(&mut self, surface_type: &SurfaceType, params: &[String]) -> Vec<(String, String)> {
    let mut expected = Vec::new();
    for param in params {
      let Some(element) = surface_type.elements.iter().find(|candidate| &candidate.name == param) else {
        continue;
      };
      match self.render_surface(&element.type_, Context::Data, Position::Build) {
        Ok(want) => expected.push((param.clone(), want)),
        Err(message) => self.report(format!("{}.constructor", surface_type.name), message),
      }
    }
    expected
  }

  /// 必須の要素を受け取らない構築、必須でない要素を必須にした構築を、それぞれ報告する。
  fn check_constructor_inputs(
    &mut self,
    path: &str,
    rs_name: &str,
    function_name: &str,
    function: &Function,
    expected: &[(String, String)],
  ) {
    let actual: Vec<String> = function
      .sig
      .inputs
      .iter()
      .map(|(_, ty)| render_type(self.krate, ty))
      .collect();
    let mut remaining = actual.clone();
    let mut missing = Vec::new();
    for (element, want) in expected {
      match remaining.iter().position(|candidate| candidate == want) {
        Some(index) => {
          remaining.remove(index);
        }
        None => missing.push((element, want)),
      }
    }
    for (element, want) in &missing {
      self.report(path, format!("required element not accepted: {element} ({want})"));
    }
    let expected_text: Vec<&str> = expected.iter().map(|(_, want)| want.as_str()).collect();
    for extra in &remaining {
      self.report(
        path,
        format!(
          "{rs_name}::{function_name} takes an extra parameter {extra}; it may take only the required elements ({})",
          expected_text.join(", ")
        ),
      );
    }
    let want: Vec<String> = expected.iter().map(|(_, want)| want.clone()).collect();
    if missing.is_empty() && remaining.is_empty() && actual != want {
      self.report(
        path,
        format!(
          "{rs_name}::{function_name} takes ({}), want ({}) in this order",
          actual.join(", "),
          want.join(", ")
        ),
      );
    }
  }

  fn check_returns_self(
    &mut self,
    path: &str,
    rs_name: &str,
    function_name: &str,
    function: &Function,
    type_mapping: &TypeMapping,
  ) {
    let actual = self.render_output(function);
    let explicit = format!("{rs_name}<{}>", type_mapping.type_parameters.join(", "));
    if actual != "Self" && actual != explicit {
      self.report(path, format!("{rs_name}::{function_name} returns {actual}, want Self"));
    }
  }

  // ---- ストア ----

  fn check_store(&mut self) {
    let store_mapping = &self.mapping.store;
    let store_name = store_mapping.rs.as_str();
    let Some(item) = self.lookup(store_name) else {
      self.report("store", format!("{store_name} is not exported"));
      return;
    };
    let ItemEnum::Trait(trait_item) = &item.inner else {
      self.report("store", format!("{store_name} is not a trait"));
      return;
    };
    let trait_items: Vec<&'a Item> = trait_item.items.iter().filter_map(|id| self.item(id)).collect();

    self.check_associated_types(store_name, &trait_item.generics, &trait_items);

    let functions: BTreeMap<&str, &Function> = trait_items
      .iter()
      .filter_map(|item| match (&item.name, &item.inner) {
        (Some(name), ItemEnum::Function(function)) => Some((name.as_str(), function)),
        _ => None,
      })
      .collect();
    for operation in &self.surface.store.operations {
      self.check_operation(store_name, operation, &functions);
    }
  }

  fn check_associated_types(&mut self, store_name: &str, trait_generics: &Generics, trait_items: &[&'a Item]) {
    let associated_types = &self.mapping.store.associated_types;
    let aggregate_id_bounds: Vec<(&str, &str)> = self
      .mapping
      .value_types
      .values()
      .filter(|value_type| value_type.type_parameter)
      .filter_map(|value_type| value_type.bound.as_deref().map(|bound| (value_type.rs.as_str(), bound)))
      .collect();

    // 対応表にないものを飛ばさない。
    let mut required: Vec<(String, String)> = Vec::new();
    for surface_parameter in &self.surface.store.type_parameters {
      match self.mapping.type_parameters.get(surface_parameter) {
        Some(mapped) => required.push((surface_parameter.clone(), mapped.rs.clone())),
        None => self.report(
          format!("store.type_parameters.{surface_parameter}"),
          format!("rs.json has no mapping for type parameter {surface_parameter}"),
        ),
      }
    }
    for (rs, _) in &aggregate_id_bounds {
      required.push(((*rs).to_string(), (*rs).to_string()));
    }
    for (label, rs) in &required {
      if !associated_types.contains(rs) {
        self.report(
          format!("store.type_parameters.{label}"),
          format!("rs.json has no mapping for associated type {rs}"),
        );
      }
    }

    for name in associated_types {
      let path = format!(
        "store.type_parameters.{}",
        self.constrained_surface_parameter(name).unwrap_or(name)
      );
      let Some(associated) = trait_items
        .iter()
        .find(|item| item.name.as_deref() == Some(name.as_str()))
      else {
        self.report(path, format!("{store_name} has no associated type {name}"));
        continue;
      };
      let ItemEnum::AssocType { bounds, .. } = &associated.inner else {
        self.report(path, format!("{store_name}::{name} is not an associated type"));
        continue;
      };
      let mut all_bounds: Vec<&GenericBound> = bounds.iter().collect();
      all_bounds.extend(self.where_bounds_of_associated_type(trait_generics, name));

      if self.constrained_surface_parameter(name).is_some() {
        for bound in self.disallowed_bounds(all_bounds.iter().copied()) {
          self.report(
            path.clone(),
            format!(
              "{store_name}::{name} has bound {bound}; allowed: {}",
              self.allowed_bounds_text()
            ),
          );
        }
      } else if let Some((_, required_bound)) = aggregate_id_bounds.iter().find(|(rs, _)| rs == name) {
        let rendered: Vec<String> = all_bounds.iter().map(|bound| render_bound(self.krate, bound)).collect();
        if !rendered.iter().any(|candidate| candidate == required_bound) {
          self.report(
            path,
            format!(
              "{store_name}::{name} has no bound {required_bound} (has: {})",
              rendered.join(", ")
            ),
          );
        }
      }
    }
  }

  /// `where Self::P: Clone` のように、トレイトの `where` 節で関連型に付けた制約。
  fn where_bounds_of_associated_type(&self, trait_generics: &'a Generics, name: &str) -> Vec<&'a GenericBound> {
    let want = format!("Self::{name}");
    trait_generics
      .where_predicates
      .iter()
      .filter_map(|predicate| match predicate {
        WherePredicate::BoundPredicate { type_, bounds, .. } if render_type(self.krate, type_) == want => Some(bounds),
        _ => None,
      })
      .flatten()
      .collect()
  }

  fn check_operation(&mut self, store_name: &str, operation: &SurfaceOperation, functions: &BTreeMap<&str, &Function>) {
    let path = format!("store.{}", operation.name);
    let Some(method) = self.mapping.store.operations.get(&operation.name) else {
      self.report(path, format!("rs.json has no mapping for operation {}", operation.name));
      return;
    };
    let Some(function) = functions.get(method.as_str()) else {
      self.report(path, format!("{store_name} has no method {method}"));
      return;
    };

    let mut want_inputs = Vec::new();
    for input in &operation.inputs {
      match self.render_surface(&input.type_, Context::Store, Position::Input) {
        Ok(want) => want_inputs.push(want),
        Err(message) => {
          self.report(path.clone(), message);
          return;
        }
      }
    }
    let want_output = match self.render_surface(&operation.output, Context::Store, Position::Output) {
      Ok(want) => self.result_of(&want),
      Err(message) => {
        self.report(path, message);
        return;
      }
    };

    let inputs = &function.sig.inputs;
    if !inputs.first().is_some_and(|(_, ty)| is_shared_self(ty)) {
      self.report(path.clone(), format!("{store_name}::{method} must take &self"));
      return;
    }
    let actual_inputs: Vec<String> = inputs
      .iter()
      .skip(1)
      .map(|(_, ty)| render_type(self.krate, ty))
      .collect();
    if actual_inputs != want_inputs {
      self.report(
        path.clone(),
        format!(
          "{store_name}::{method} takes ({}), want ({})",
          actual_inputs.join(", "),
          want_inputs.join(", ")
        ),
      );
    }

    let Some(output) = &function.sig.output else {
      self.report(
        path,
        format!("{store_name}::{method} is not async (it returns nothing; want {want_output})"),
      );
      return;
    };
    let Some(awaited) = unwrap_async_output(self.krate, output) else {
      self.report(
        path,
        format!(
          "{store_name}::{method} is not async (returns {}; want a #[async_trait] method that returns {want_output})",
          render_type(self.krate, output)
        ),
      );
      return;
    };
    if !async_output_is_send(self.krate, output) {
      self.report(
        path.clone(),
        format!(
          "{store_name}::{method} returns a future without Send (want #[async_trait], not #[async_trait(?Send)])"
        ),
      );
    }
    let actual_output = render_type(self.krate, awaited);
    if actual_output != want_output {
      self.report(
        path,
        format!("{store_name}::{method} returns {actual_output}, want {want_output}"),
      );
    }
  }

  // ---- エラーの分類 ----

  fn check_error_categories(&mut self) {
    let error_mapping = &self.mapping.error_categories;
    let enum_name = error_mapping.rs.as_str();
    let Some(item) = self.lookup(enum_name) else {
      self.report("error_categories", format!("{enum_name} is not exported"));
      return;
    };
    let ItemEnum::Enum(enum_item) = &item.inner else {
      self.report("error_categories", format!("{enum_name} is not an enum"));
      return;
    };
    let variant_names: Vec<&str> = enum_item
      .variants
      .iter()
      .filter_map(|id| self.item(id))
      .filter_map(|variant| variant.name.as_deref())
      .collect();
    for category in &self.surface.error_categories.names {
      let path = format!("error_categories.{category}");
      let Some(variant) = error_mapping.variants.get(category) else {
        self.report(path, format!("rs.json has no mapping for error category {category}"));
        continue;
      };
      if !variant_names.contains(&variant.as_str()) {
        self.report(path, format!("{enum_name} has no variant {variant}"));
      }
    }
  }

  // ---- 表示 ----

  fn render_inputs(&self, inputs: &[(String, Type)]) -> String {
    let rendered: Vec<String> = inputs.iter().map(|(_, ty)| render_type(self.krate, ty)).collect();
    rendered.join(", ")
  }

  fn render_output(&self, function: &Function) -> String {
    function
      .sig
      .output
      .as_ref()
      .map_or_else(|| "()".to_string(), |ty| render_type(self.krate, ty))
  }
}

/// 型の定義が持つ型の引数の名前の並び (寿命と定数の引数は数えない)。
fn type_parameter_names(generics: &Generics) -> Vec<String> {
  generics
    .params
    .iter()
    .filter(|param| matches!(param.kind, rustdoc_types::GenericParamDefKind::Type { .. }))
    .map(|param| param.name.clone())
    .collect()
}

/// 型の引数 `name` に付いた制約。定義の `<P: X>` と `where P: Y` の両方を集める。
fn bounds_of<'g>(generics: &'g Generics, name: &str) -> Vec<&'g GenericBound> {
  let mut bounds: Vec<&GenericBound> = Vec::new();
  for param in &generics.params {
    if let rustdoc_types::GenericParamDefKind::Type { bounds: declared, .. } = &param.kind {
      if param.name == name {
        bounds.extend(declared);
      }
    }
  }
  for predicate in &generics.where_predicates {
    if let WherePredicate::BoundPredicate {
      type_: Type::Generic(constrained),
      bounds: declared,
      ..
    } = predicate
    {
      if constrained == name {
        bounds.extend(declared);
      }
    }
  }
  bounds
}

/// inherent の `impl` が、型の `index` 番目の型の引数に付けた名前 (`impl<X, Y> T<X, Y>` の `X`)。
fn impl_parameter_name(impl_item: &Impl, index: usize) -> Option<String> {
  let Type::ResolvedPath(path) = &impl_item.for_ else {
    return None;
  };
  let rustdoc_types::GenericArgs::AngleBracketed { args, .. } = path.args.as_deref()? else {
    return None;
  };
  let type_arguments: Vec<&Type> = args
    .iter()
    .filter_map(|arg| match arg {
      rustdoc_types::GenericArg::Type(ty) => Some(ty),
      _ => None,
    })
    .collect();
  match type_arguments.get(index)? {
    Type::Generic(name) => Some(name.clone()),
    _ => None,
  }
}

fn is_shared_self(ty: &Type) -> bool {
  matches!(
    ty,
    Type::BorrowedRef { is_mutable: false, type_, .. } if matches!(type_.as_ref(), Type::Generic(name) if name == "Self")
  )
}

fn takes_only_shared_self(inputs: &[(String, Type)]) -> bool {
  matches!(inputs, [(_, receiver)] if is_shared_self(receiver))
}
