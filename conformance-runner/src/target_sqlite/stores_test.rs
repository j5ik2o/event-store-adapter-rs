use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use event_store_adapter_rs::{initialize_sqlite_store, SqliteOptions};

use crate::target_sqlite::stores::ScenarioFile;

/// 連番で名前を決める割り当てを、この試験どうしが乱さないようにする。
static SERIAL: Mutex<()> = Mutex::new(());

fn serialized() -> std::sync::MutexGuard<'static, ()> {
  SERIAL.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// ファイルの置き場のディレクトリ名の末尾の数字（連番）を 1 つ進めた名前の場所を返す。
fn next_allocation_path(previous: &Path) -> PathBuf {
  let directory = previous.parent().expect("ファイルはディレクトリの下にある");
  let name = directory
    .file_name()
    .and_then(|name| name.to_str())
    .expect("ディレクトリ名は文字列");
  let digits = name.chars().rev().take_while(char::is_ascii_digit).count();
  assert!(digits > 0, "ディレクトリ名 {name} は連番で終わる");
  let (prefix, sequence) = name.split_at(name.len() - digits);
  let next = sequence.parse::<u64>().expect("連番は整数") + 1;
  directory
    .parent()
    .expect("ディレクトリは親を持つ")
    .join(format!("{prefix}{next}"))
    .join(previous.file_name().expect("ファイル名がある"))
}

#[test]
fn should_scenario_file_give_each_scene_a_different_new_file_that_initializes() {
  let _serial = serialized();

  let first = ScenarioFile::new().expect("1 つ目の一時ファイルを用意できる");
  let second = ScenarioFile::new().expect("2 つ目の一時ファイルを用意できる");

  assert_ne!(first.path(), second.path());
  initialize_sqlite_store(first.path(), &SqliteOptions::default()).expect("1 つ目を初期化できる");
  initialize_sqlite_store(second.path(), &SqliteOptions::default()).expect("2 つ目を初期化できる");
}

#[test]
fn should_scenario_file_remove_a_stale_file_left_under_the_next_name_before_initialization() {
  let _serial = serialized();
  let probe = ScenarioFile::new().expect("次の名前を知るための一時ファイルを用意できる");
  let stale_path = next_allocation_path(probe.path());
  fs::create_dir_all(stale_path.parent().expect("親がある")).expect("古いファイルの置き場を作れる");
  initialize_sqlite_store(&stale_path, &SqliteOptions::default()).expect("テーブルを持つ古いファイルを用意できる");
  assert!(stale_path.exists(), "試験の前提: 古いファイルが残っている");

  let scene = ScenarioFile::new().expect("古いファイルが残っていても用意できる");

  assert_eq!(
    scene.path(),
    stale_path,
    "試験の前提: 古いファイルと同じ名前が割り当てられた"
  );
  initialize_sqlite_store(scene.path(), &SqliteOptions::default())
    .expect("古いファイルは消されていて、初期化は設定エラーにならずに成功する");
}
