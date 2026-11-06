use std::future::Future;
use std::sync::mpsc::channel;
use std::thread::JoinHandle;

use dockertest::{DockerOperations, DockerTest, Image, Source, TestBodySpecification};
use tokio::sync::{mpsc, oneshot};

/// `DockerTest` の中で DynamoDB Local を指すハンドル名。
pub const DYNAMODB_LOCAL_HANDLE: &str = "dynamodb-local";

// dockertest も name:tag の形で参照を作るため、name に "@sha256" を含めて digest 参照にする（DynamoDB Local 3.3.1）
const DYNAMODB_LOCAL_IMAGE_NAME: &str = "amazon/dynamodb-local@sha256";
const DYNAMODB_LOCAL_IMAGE_DIGEST: &str = "ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab";

/// DynamoDB Local がコンテナ内で待ち受けるポート。
pub const DYNAMODB_LOCAL_PORT: u16 = 8000;

// Bigtable エミュレーターは google-cloud-cli の 587.0.0-emulators（2026-09-29 公開）を digest で固定する。
// 動くタグ（`emulators` など）は使わない。
const BIGTABLE_EMULATOR_IMAGE_NAME: &str = "gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256";
const BIGTABLE_EMULATOR_IMAGE_DIGEST: &str = "8d8573471c489a409f03891d2c24d177fa531276184f7002b9a2d853d1b3ed54";

/// Bigtable エミュレーターがコンテナ内で待ち受けるポート。
pub const BIGTABLE_EMULATOR_PORT: u16 = 8086;

const BIGTABLE_EMULATOR_HANDLE: &str = "bigtable-emulator";

// Spanner エミュレーターは 1.5.58（2026-09-15 公開）を digest で固定する。動くタグ（`latest` など）は使わない。
const SPANNER_EMULATOR_IMAGE_NAME: &str = "gcr.io/cloud-spanner-emulator/emulator@sha256";
const SPANNER_EMULATOR_IMAGE_DIGEST: &str = "c6f3402f2599684f295a0fdefb6fbbbfb18a0e43e309ff5456ccb452a4570a79";

/// Spanner エミュレーターがコンテナ内で待ち受ける gRPC のポート。
pub const SPANNER_EMULATOR_PORT: u16 = 9010;

const SPANNER_EMULATOR_HANDLE: &str = "spanner-emulator";

/// dockertest が起動したコンテナを、呼び出し側が保持している間だけ生かす。
///
/// dockertest はコンテナの寿命を `DockerTest::run` のスコープに閉じる。このリポジトリの
/// 呼び出し側は複数の操作にまたがってコンテナを保持し、drop で削除する。そのため専用の
/// スレッドで `run` を回し、公開ポートと ID だけを渡して、drop までコンテナを持つ。
pub struct LocalContainer {
  id: String,
  port: u16,
  commands: Option<mpsc::UnboundedSender<Command>>,
  thread: Option<JoinHandle<()>>,
}

/// 保持しているスレッドにコンテナの操作を頼む。
enum Command {
  /// コンテナを停止する。停止が終わったら合図を返す。
  Kill(oneshot::Sender<()>),
}

impl LocalContainer {
  /// Docker が付けたコンテナ ID。
  pub fn id(&self) -> &str {
    &self.id
  }

  /// ホストから見たコンテナの公開ポート。
  pub fn port(&self) -> u16 {
    self.port
  }

  /// コンテナを停止する（削除は drop のとき）。
  pub async fn stop(&self) {
    let (ack, done) = oneshot::channel();
    if let Some(commands) = &self.commands {
      if commands.send(Command::Kill(ack)).is_ok() {
        let _ = done.await;
      }
    }
  }
}

impl Drop for LocalContainer {
  fn drop(&mut self) {
    drop(self.commands.take());
    if let Some(thread) = self.thread.take() {
      let _ = thread.join();
    }
  }
}

/// `spec` のコンテナを専用スレッドで起動し、`port_of` が成功するまで待ってから返す。
fn run_held<P, F>(spec: TestBodySpecification, handle: &'static str, port_of: P) -> anyhow::Result<LocalContainer>
where
  P: FnOnce(DockerOperations) -> F + Send + 'static,
  F: Future<Output = anyhow::Result<u16>> + Send,
{
  let (port_sender, port_receiver) = channel::<anyhow::Result<(u16, String)>>();
  let (commands, mut command_receiver) = mpsc::unbounded_channel();
  let thread = std::thread::Builder::new()
    .name("dockertest-container".to_owned())
    .spawn(move || {
      let mut test = DockerTest::new();
      test.provide_container(spec);
      test.run(move |ops| async move {
        let id = ops.handle(handle).id().to_owned();
        let _ = port_sender.send(port_of(ops.clone()).await.map(|port| (port, id)));
        // 呼び出し側が全ていなくなる（送信側が落ちる）まで、コンテナを持ち続ける。
        while let Some(command) = command_receiver.recv().await {
          match command {
            Command::Kill(ack) => {
              ops.handle(handle).kill().await;
              let _ = ack.send(());
            }
          }
        }
      });
    })?;
  match port_receiver.recv() {
    Ok(Ok((port, id))) => Ok(LocalContainer {
      id,
      port,
      commands: Some(commands),
      thread: Some(thread),
    }),
    Ok(Err(error)) => {
      drop(commands);
      let _ = thread.join();
      Err(error)
    }
    Err(_) => {
      drop(commands);
      let _ = thread.join();
      Err(anyhow::anyhow!("コンテナを起動するスレッドが、公開ポートを返す前に終了した"))
    }
  }
}

fn exposed_port(ops: &DockerOperations, handle: &str, container_port: u16) -> anyhow::Result<u16> {
  ops
    .handle(handle)
    .host_port(u32::from(container_port))
    .map(|(_, port)| *port as u16)
    .ok_or_else(|| anyhow::anyhow!("{handle} の公開ポート {container_port} が見つからない"))
}

/// DynamoDB Local 3.3.1 を digest 固定のイメージで起動し、`ListTables` が成功してから返す。
pub fn dynamodb_local() -> anyhow::Result<LocalContainer> {
  let spec = TestBodySpecification::with_image(
    Image::with_repository(DYNAMODB_LOCAL_IMAGE_NAME)
      .tag(DYNAMODB_LOCAL_IMAGE_DIGEST)
      .source(Source::DockerHub),
  )
  .set_handle(DYNAMODB_LOCAL_HANDLE)
  .set_publish_all_ports(true)
  .replace_cmd(
    [
      "-jar",
      "DynamoDBLocal.jar",
      "-inMemory",
      "-sharedDb",
      "-disableTelemetry",
    ]
    .map(str::to_string)
    .to_vec(),
  );
  run_held(spec, DYNAMODB_LOCAL_HANDLE, |ops| async move {
    let port = exposed_port(&ops, DYNAMODB_LOCAL_HANDLE, DYNAMODB_LOCAL_PORT)?;
    crate::dynamodb::wait_until_list_tables_succeeds(&crate::dynamodb::create_dynamodb_local_client(port)).await?;
    Ok(port)
  })
}

/// 起動したコンテナが使う Bigtable エミュレーターのイメージ参照（名前, digest）。
pub fn bigtable_emulator_image() -> (&'static str, &'static str) {
  (BIGTABLE_EMULATOR_IMAGE_NAME, BIGTABLE_EMULATOR_IMAGE_DIGEST)
}

/// Bigtable エミュレーターを digest 固定のイメージで起動し、管理 API の `ListTables` が成功してから返す。
pub fn bigtable_emulator() -> anyhow::Result<LocalContainer> {
  // このイメージはポートを EXPOSE しないため、ホスト側の空きポートを明示して割り当てる。
  let host_port = free_host_port()?;
  let mut spec = TestBodySpecification::with_image(
    Image::with_repository(BIGTABLE_EMULATOR_IMAGE_NAME)
      .tag(BIGTABLE_EMULATOR_IMAGE_DIGEST)
      .source(Source::DockerHub),
  )
  .set_handle(BIGTABLE_EMULATOR_HANDLE)
  .replace_env(std::collections::HashMap::from([(
    "CLOUDSDK_CORE_DISABLE_PROMPTS".to_owned(),
    "1".to_owned(),
  )]))
  .replace_cmd(
    [
      "gcloud",
      "beta",
      "emulators",
      "bigtable",
      "start",
      "--host-port=0.0.0.0:8086",
    ]
    .map(str::to_string)
    .to_vec(),
  );
  spec.modify_port_map(u32::from(BIGTABLE_EMULATOR_PORT), host_port);
  run_held(spec, BIGTABLE_EMULATOR_HANDLE, |ops| async move {
    let port = exposed_port(&ops, BIGTABLE_EMULATOR_HANDLE, BIGTABLE_EMULATOR_PORT)?;
    crate::bigtable::wait_until_list_tables_succeeds(port).await?;
    Ok(port)
  })
}

/// 起動したコンテナが使う Spanner エミュレーターのイメージ参照（名前, digest）。
pub fn spanner_emulator_image() -> (&'static str, &'static str) {
  (SPANNER_EMULATOR_IMAGE_NAME, SPANNER_EMULATOR_IMAGE_DIGEST)
}

/// Spanner エミュレーターを digest 固定のイメージで起動し、インスタンス構成の一覧（`ListInstanceConfigs`）が成功してから返す。
pub fn spanner_emulator() -> anyhow::Result<LocalContainer> {
  let spec = TestBodySpecification::with_image(
    Image::with_repository(SPANNER_EMULATOR_IMAGE_NAME)
      .tag(SPANNER_EMULATOR_IMAGE_DIGEST)
      .source(Source::DockerHub),
  )
  .set_handle(SPANNER_EMULATOR_HANDLE)
  .set_publish_all_ports(true);
  run_held(spec, SPANNER_EMULATOR_HANDLE, |ops| async move {
    let port = exposed_port(&ops, SPANNER_EMULATOR_HANDLE, SPANNER_EMULATOR_PORT)?;
    crate::spanner::wait_until_list_instance_configs_succeeds(port).await?;
    Ok(port)
  })
}

/// ホストの空きポートを 1 つ選ぶ。Docker がそのポートを bind するまでの間に他が使う可能性は残る。
fn free_host_port() -> anyhow::Result<u32> {
  let listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
  let port = listener.local_addr()?.port();
  drop(listener);
  Ok(u32::from(port))
}
