use std::fs::{File, OpenOptions};
use std::future::Future;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::thread::JoinHandle;

use bollard::image::{CreateImageOptions, TagImageOptions};
use bollard::Docker;
use dockertest::{DockerOperations, DockerTest, Image, PullPolicy, TestBodySpecification};
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};

/// `DockerTest` の中で DynamoDB Local を指すハンドル名。
pub const DYNAMODB_LOCAL_HANDLE: &str = "dynamodb-local";

const DYNAMODB_LOCAL_IMAGE_REPOSITORY: &str = "amazon/dynamodb-local";
const DYNAMODB_LOCAL_IMAGE_DIGEST: &str = "ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab";

/// DynamoDB Local がコンテナ内で待ち受けるポート。
pub const DYNAMODB_LOCAL_PORT: u16 = 8000;

// Bigtable エミュレーターは google-cloud-cli の 587.0.0-emulators（2026-09-29 公開）を digest で固定する。
// 動くタグ（`emulators` など）は使わない。
const BIGTABLE_EMULATOR_IMAGE_REPOSITORY: &str = "gcr.io/google.com/cloudsdktool/google-cloud-cli";
const BIGTABLE_EMULATOR_IMAGE_DIGEST: &str = "8d8573471c489a409f03891d2c24d177fa531276184f7002b9a2d853d1b3ed54";

/// Bigtable エミュレーターがコンテナ内で待ち受けるポート。
pub const BIGTABLE_EMULATOR_PORT: u16 = 8086;

const BIGTABLE_EMULATOR_HANDLE: &str = "bigtable-emulator";

// Spanner エミュレーターは 1.5.58（2026-09-15 公開）を digest で固定する。動くタグ（`latest` など）は使わない。
const SPANNER_EMULATOR_IMAGE_REPOSITORY: &str = "gcr.io/cloud-spanner-emulator/emulator";
const SPANNER_EMULATOR_IMAGE_DIGEST: &str = "c6f3402f2599684f295a0fdefb6fbbbfb18a0e43e309ff5456ccb452a4570a79";

/// Spanner エミュレーターがコンテナ内で待ち受ける gRPC のポート。
pub const SPANNER_EMULATOR_PORT: u16 = 9010;

const SPANNER_EMULATOR_HANDLE: &str = "spanner-emulator";

/// コンテナの起動と停止の窓を、プロセスをまたいで直列化するロックファイル。
///
/// 停止したコンテナの公開ポートは Docker が解放する。その直後に別のテストがコンテナを
/// 起動すると、同じホストポートが割り当てられ得る。停止の窓の間は起動を待たせ、停止した
/// コンテナのポートを新しいコンテナが受け取らないようにする。ロックはファイルに置くので、
/// テストバイナリを並列に走らせても効く。
fn gate_path() -> PathBuf {
  std::env::temp_dir().join("event-store-adapter-rs-dockertest.lock")
}

fn open_gate() -> File {
  OpenOptions::new()
    .create(true)
    .read(true)
    .write(true)
    .open(gate_path())
    .expect("起動ゲートのファイルを開ける")
}

/// 起動の窓を開いている間だけ存在する。ポートを渡したら落とす。
struct StartTicket(File);

impl StartTicket {
  fn new() -> Self {
    let file = open_gate();
    file.lock_shared().expect("起動ゲートの共有ロック");
    Self(file)
  }
}

impl Drop for StartTicket {
  fn drop(&mut self) {
    let _ = self.0.unlock();
  }
}

/// `LocalContainer::stop` が開けた停止の窓。呼び出し側は、停止したあとの要求が
/// 終わるまでこれを保持する。
pub struct StopGuard(File);

impl StopGuard {
  fn new() -> Self {
    let file = open_gate();
    file.lock().expect("停止ゲートの排他ロック");
    Self(file)
  }
}

impl Drop for StopGuard {
  fn drop(&mut self) {
    let _ = self.0.unlock();
  }
}

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
  ///
  /// 返した [`StopGuard`] は、停止したあとの要求が終わるまで保持する。その間、
  /// ほかの起動は待つので、停止したコンテナの公開ポートを別のコンテナが受け取らない。
  ///
  /// dockertest の `kill` は、公開ポートが閉じる前に返る。Docker の `stop` と同じく、
  /// ホスト側のポートが接続を受け付けなくなるまで待ってから返す。
  pub async fn stop(&self) -> StopGuard {
    let guard = StopGuard::new();
    let (ack, done) = oneshot::channel();
    if let Some(commands) = &self.commands {
      if commands.send(Command::Kill(ack)).is_ok() {
        let _ = done.await;
      }
    }
    self.wait_until_port_closed().await;
    guard
  }

  async fn wait_until_port_closed(&self) {
    let address = std::net::SocketAddr::from(([127, 0, 0, 1], self.port));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
      let connected = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        tokio::net::TcpStream::connect(address),
      )
      .await;
      match connected {
        // 接続を拒否したら、公開ポートは閉じている。
        Ok(Err(_)) => return,
        _ if tokio::time::Instant::now() >= deadline => return,
        _ => tokio::time::sleep(std::time::Duration::from_millis(25)).await,
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

/// digest 参照のイメージをローカルに用意し、dockertest に渡す決定的な alias タグを返す。
///
/// dockertest 0.5.0 は pull のときに repository と tag を分けて渡すため、`name@sha256` の
/// 参照を正しく組み立てられない。ここで完全な digest 参照を pull し、`name:eswa-<先頭 12 桁>`
/// にタグ付けしてから、dockertest にはその alias を `Source::Local` で渡す。
async fn ensure_image(repository: &str, digest: &str) -> anyhow::Result<String> {
  let prefix = digest
    .get(..12)
    .ok_or_else(|| anyhow::anyhow!("digest が 12 桁より短い: {digest}"))?;
  let alias = format!("eswa-{prefix}");
  let digest_reference = format!("{repository}@sha256:{digest}");
  let docker = Docker::connect_with_local_defaults()?;
  if docker.inspect_image(&format!("{repository}:{alias}")).await.is_ok() {
    return Ok(alias);
  }
  if docker.inspect_image(&digest_reference).await.is_err() {
    let options = CreateImageOptions::<&str> {
      from_image: &digest_reference,
      ..Default::default()
    };
    let mut stream = docker.create_image(Some(options), None, None);
    while let Some(item) = stream.next().await {
      let info = item?;
      if let Some(error) = info.error {
        anyhow::bail!("{digest_reference} を pull できない: {error}");
      }
    }
  }
  let image = docker.inspect_image(&digest_reference).await?;
  let id = image
    .id
    .ok_or_else(|| anyhow::anyhow!("{digest_reference} のイメージ ID が無い"))?;
  docker
    .tag_image(
      &id,
      Some(TagImageOptions {
        repo: repository,
        tag: alias.as_str(),
      }),
    )
    .await?;
  Ok(alias)
}

/// `spec` のコンテナを専用スレッドで起動し、`port_of` が成功するまで待ってから返す。
fn run_held<S, P, F>(
  repository: &'static str,
  digest: &'static str,
  handle: &'static str,
  spec: S,
  port_of: P,
) -> anyhow::Result<LocalContainer>
where
  S: FnOnce(String) -> TestBodySpecification + Send + 'static,
  P: FnOnce(DockerOperations) -> F + Send + 'static,
  F: Future<Output = anyhow::Result<u16>> + Send,
{
  let (port_sender, port_receiver) = channel::<anyhow::Result<(u16, String)>>();
  let (commands, mut command_receiver) = mpsc::unbounded_channel();
  let thread = std::thread::Builder::new()
    .name("dockertest-container".to_owned())
    .spawn(move || {
      let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
          let _ = port_sender.send(Err(anyhow::anyhow!("実行基盤を作れない: {error}")));
          return;
        }
      };
      let alias = match runtime.block_on(ensure_image(repository, digest)) {
        Ok(alias) => alias,
        Err(error) => {
          let _ = port_sender.send(Err(error));
          return;
        }
      };
      drop(runtime);
      let spec = spec(alias);
      let ticket = StartTicket::new();
      let mut test = DockerTest::new();
      test.provide_container(spec);
      test.run(move |ops| {
        let ticket = ticket;
        async move {
          let id = ops.handle(handle).id().to_owned();
          let _ = port_sender.send(port_of(ops.clone()).await.map(|port| (port, id)));
          // ポートを渡したので、起動の窓を閉じる。
          drop(ticket);
          // 呼び出し側が全ていなくなる（送信側が落ちる）まで、コンテナを持ち続ける。
          while let Some(command) = command_receiver.recv().await {
            match command {
              Command::Kill(ack) => {
                ops.handle(handle).kill().await;
                let _ = ack.send(());
              }
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
      Err(anyhow::anyhow!("コンテナを起動できなかった。イメージの取得を確認する"))
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
  run_held(
    DYNAMODB_LOCAL_IMAGE_REPOSITORY,
    DYNAMODB_LOCAL_IMAGE_DIGEST,
    DYNAMODB_LOCAL_HANDLE,
    |alias| {
      TestBodySpecification::with_image(
        Image::with_repository(DYNAMODB_LOCAL_IMAGE_REPOSITORY)
          .tag(alias)
          .pull_policy(PullPolicy::Never),
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
      )
    },
    |ops| async move {
      let port = exposed_port(&ops, DYNAMODB_LOCAL_HANDLE, DYNAMODB_LOCAL_PORT)?;
      crate::dynamodb::wait_until_list_tables_succeeds(&crate::dynamodb::create_dynamodb_local_client(port)).await?;
      Ok(port)
    },
  )
}

/// 起動したコンテナが使う Bigtable エミュレーターのイメージ参照（repository, digest）。
pub fn bigtable_emulator_image() -> (&'static str, &'static str) {
  (BIGTABLE_EMULATOR_IMAGE_REPOSITORY, BIGTABLE_EMULATOR_IMAGE_DIGEST)
}

/// Bigtable エミュレーターを digest 固定のイメージで起動し、管理 API の `ListTables` が成功してから返す。
pub fn bigtable_emulator() -> anyhow::Result<LocalContainer> {
  run_held(
    BIGTABLE_EMULATOR_IMAGE_REPOSITORY,
    BIGTABLE_EMULATOR_IMAGE_DIGEST,
    BIGTABLE_EMULATOR_HANDLE,
    |alias| {
      // このイメージはポートを EXPOSE しないため、ホスト側のポートは Docker に自動で割り当てさせる。
      let mut spec = TestBodySpecification::with_image(
        Image::with_repository(BIGTABLE_EMULATOR_IMAGE_REPOSITORY)
          .tag(alias)
          .pull_policy(PullPolicy::Never),
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
      spec.modify_port_map(u32::from(BIGTABLE_EMULATOR_PORT), 0);
      spec
    },
    |ops| async move {
      let port = exposed_port(&ops, BIGTABLE_EMULATOR_HANDLE, BIGTABLE_EMULATOR_PORT)?;
      crate::bigtable::wait_until_list_tables_succeeds(port).await?;
      Ok(port)
    },
  )
}

/// 起動したコンテナが使う Spanner エミュレーターのイメージ参照（repository, digest）。
pub fn spanner_emulator_image() -> (&'static str, &'static str) {
  (SPANNER_EMULATOR_IMAGE_REPOSITORY, SPANNER_EMULATOR_IMAGE_DIGEST)
}

/// Spanner エミュレーターを digest 固定のイメージで起動し、インスタンス構成の一覧（`ListInstanceConfigs`）が成功してから返す。
pub fn spanner_emulator() -> anyhow::Result<LocalContainer> {
  run_held(
    SPANNER_EMULATOR_IMAGE_REPOSITORY,
    SPANNER_EMULATOR_IMAGE_DIGEST,
    SPANNER_EMULATOR_HANDLE,
    |alias| {
      TestBodySpecification::with_image(
        Image::with_repository(SPANNER_EMULATOR_IMAGE_REPOSITORY)
          .tag(alias)
          .pull_policy(PullPolicy::Never),
      )
      .set_handle(SPANNER_EMULATOR_HANDLE)
      .set_publish_all_ports(true)
    },
    |ops| async move {
      let port = exposed_port(&ops, SPANNER_EMULATOR_HANDLE, SPANNER_EMULATOR_PORT)?;
      crate::spanner::wait_until_list_instance_configs_succeeds(port).await?;
      Ok(port)
    },
  )
}
