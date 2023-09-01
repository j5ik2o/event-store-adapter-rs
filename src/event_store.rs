use std::fmt::Debug;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem, Update};
use serde::{Deserialize, Serialize};

use crate::key_resolver::{DefaultPartitionKeyResolver, KeyResolver};
use crate::serializer::EventSerializer;
use crate::types::{Aggregate, AggregateId, Event, EventPersistenceGateway};

#[derive(Debug, Clone)]
pub struct EventStore {
    client: Client,
    journal_table_name: String,
    journal_aid_index_name: String,
    snapshot_table_name: String,
    snapshot_aid_index_name: String,
    shard_count: u64,
    key_resolver: Arc<dyn KeyResolver>,
}

unsafe impl Sync for EventStore {}

unsafe impl Send for EventStore {}

#[async_trait]
impl EventPersistenceGateway for EventStore {
    async fn get_snapshot_by_id<T, AID: AggregateId>(&self, aid: &AID) -> Result<(T, usize, usize)>
        where
            T: ?Sized + Serialize + Aggregate + for<'de> Deserialize<'de>,
    {
        let response = self
            .client
            .query()
            .table_name(self.snapshot_table_name.clone())
            .index_name(self.snapshot_aid_index_name.clone())
            .key_condition_expression("#aid = :aid AND #seq_nr > :seq_nr")
            .expression_attribute_names("#aid", "aid")
            .expression_attribute_names("#seq_nr", "seq_nr")
            .expression_attribute_values(":aid", AttributeValue::S(aid.to_string()))
            .expression_attribute_values(":seq_nr", AttributeValue::N(0.to_string()))
            .scan_index_forward(false)
            .limit(1)
            .send()
            .await?;
        if let Some(items) = response.items {
            if items.len() == 1 {
                let item = items[0].clone();
                let payload = item.get("payload").unwrap().as_s().unwrap();
                let aggregate = serde_json::from_str::<T>(payload).unwrap();
                let seq_nr = item.get("seq_nr").unwrap().as_n().unwrap().parse::<usize>().unwrap();
                let version = item.get("version").unwrap().as_n().unwrap().parse::<usize>().unwrap();
                Ok((aggregate, seq_nr, version))
            } else {
                Err(anyhow::anyhow!("No snapshot found for aggregate id: {}", aid))
            }
        } else {
            Err(anyhow::anyhow!("No snapshot found for aggregate id: {}", aid))
        }
    }

    async fn get_events_by_id_and_seq_nr<T, AID: AggregateId>(&self, aid: &AID, seq_nr: usize) -> anyhow::Result<Vec<T>>
        where
            T: Debug + for<'de> Deserialize<'de>,
    {
        let response = self
            .client
            .query()
            .table_name(self.journal_table_name.clone())
            .index_name(self.journal_aid_index_name.clone())
            .key_condition_expression("#aid = :aid AND #seq_nr > :seq_nr")
            .expression_attribute_names("#aid", "aid")
            .expression_attribute_values(":aid", AttributeValue::S(aid.to_string()))
            .expression_attribute_names("#seq_nr", "seq_nr")
            .expression_attribute_values(":seq_nr", AttributeValue::N(seq_nr.to_string()))
            .send()
            .await?;
        let mut events = Vec::new();
        if let Some(items) = response.items {
            for item in items {
                let payload = item.get("payload").unwrap();
                let str = payload.as_s().unwrap();
                let event = serde_json::from_str::<T>(str).unwrap();
                events.push(event);
            }
        }
        Ok(events)
    }

    async fn store_event_with_snapshot_opt<A, E>(
        &mut self,
        event: &E,
        version: usize,
        aggregate: Option<&A>,
    ) -> anyhow::Result<()>
        where
            A: ?Sized + Serialize + Aggregate,
            E: ?Sized + Serialize + Event,
    {
        match (event.is_created(), aggregate) {
            (true, Some(ar)) => {
                let _ = self
                    .client
                    .transact_write_items()
                    .transact_items(TransactWriteItem::builder().put(self.put_snapshot(event, ar)?).build())
                    .transact_items(TransactWriteItem::builder().put(self.put_journal(event)?).build())
                    .send()
                    .await?;
            }
            (true, None) => {
                panic!("Aggregate is not found");
            }
            (false, ar) => {
                let _ = self
                    .client
                    .transact_write_items()
                    .transact_items(
                        TransactWriteItem::builder()
                            .update(self.update_snapshot(event, version, ar)?)
                            .build(),
                    )
                    .transact_items(TransactWriteItem::builder().put(self.put_journal(event)?).build())
                    .send()
                    .await?;
            }
        }
        Ok(())
    }
}

impl EventStore {
    pub fn new(
        client: Client,
        journal_table_name: String,
        journal_aid_index_name: String,
        snapshot_table_name: String,
        snapshot_aid_index_name: String,
        shard_count: u64,
    ) -> Self {
        Self::new_with_key_resolver(
            client,
            journal_table_name,
            journal_aid_index_name,
            snapshot_table_name,
            snapshot_aid_index_name,
            shard_count,
            Arc::new(DefaultPartitionKeyResolver),
        )
    }

    pub fn new_with_key_resolver(
        client: Client,
        journal_table_name: String,
        journal_aid_index_name: String,
        snapshot_table_name: String,
        snapshot_aid_index_name: String,
        shard_count: u64,
        key_resolver: Arc<dyn KeyResolver>,
    ) -> Self {
        Self {
            client,
            journal_table_name,
            journal_aid_index_name,
            snapshot_table_name,
            snapshot_aid_index_name,
            shard_count,
            key_resolver,
        }
    }

    fn put_snapshot<E, A>(&mut self, event: &E, ar: &A) -> Result<Put>
        where
            A: ?Sized + Serialize + Aggregate,
            E: ?Sized + Serialize + Event,
    {
        let put_snapshot = Put::builder()
            .table_name(self.snapshot_table_name.clone())
            .item(
                "pkey",
                AttributeValue::S(self.resolve_pkey(event.aggregate_id(), self.shard_count)),
            )
            // ロックを取る場合は常にskey=resolve_skey(aid, 0)で行う
            .item("skey", AttributeValue::S(self.resolve_skey(event.aggregate_id(), 0)))
            .item("payload", AttributeValue::S(serde_json::to_string(ar)?))
            .item("aid", AttributeValue::S(event.aggregate_id().to_string()))
            .item("seq_nr", AttributeValue::N(ar.seq_nr().to_string()))
            .item("version", AttributeValue::N("1".to_string()))
            .condition_expression("attribute_not_exists(pkey) AND attribute_not_exists(skey)")
            .build();
        Ok(put_snapshot)
    }

    fn update_snapshot<E, A>(&mut self, event: &E, version: usize, ar_opt: Option<&A>) -> Result<Update>
        where
            A: ?Sized + Serialize + Aggregate,
            E: ?Sized + Serialize + Event,
    {
        let mut update_snapshot = Update::builder()
            .table_name(self.snapshot_table_name.clone())
            .update_expression("SET #version=:after_version")
            .key(
                "pkey",
                AttributeValue::S(self.resolve_pkey(event.aggregate_id(), self.shard_count)),
            )
            // ロックを取る場合は常にskey=resolve_skey(aid, 0)で行う
            .key("skey", AttributeValue::S(self.resolve_skey(event.aggregate_id(), 0)))
            .expression_attribute_names("#version", "version")
            .expression_attribute_values(":before_version", AttributeValue::N(version.to_string()))
            .expression_attribute_values(":after_version", AttributeValue::N((version + 1).to_string()))
            .condition_expression("#version=:before_version");
        if let Some(ar) = ar_opt {
            update_snapshot = update_snapshot
                .update_expression("SET #payload=:payload, #seq_nr=:seq_nr, #version=:after_version")
                .expression_attribute_names("#seq_nr", "seq_nr")
                .expression_attribute_names("#payload", "payload")
                .expression_attribute_values(":seq_nr", AttributeValue::N(ar.seq_nr().to_string()))
                .expression_attribute_values(":payload", AttributeValue::S(serde_json::to_string(ar)?));
        }
        Ok(update_snapshot.build())
    }

    fn resolve_pkey<AID: AggregateId>(&self, id: &AID, shard_count: u64) -> String {
        self
            .key_resolver
            .resolve_pkey(&id.type_name(), &id.value(), shard_count)
    }

    fn resolve_skey<AID: AggregateId>(&self, id: &AID, seq_nr: usize) -> String {
        self.key_resolver.resolve_skey(&id.type_name(), &id.value(), seq_nr)
    }

    fn put_journal<E>(&mut self, event: &E) -> Result<Put>
        where
            E: ?Sized + Serialize + Event,
    {
        let pkey = self.resolve_pkey(event.aggregate_id(), self.shard_count);
        let skey = self.resolve_skey(event.aggregate_id(), event.seq_nr());
        let aid = event.aggregate_id().to_string();
        let seq_nr = event.seq_nr().to_string();
        let payload = serde_json::to_string(event)?;
        let occurred_at = event.occurred_at().timestamp_millis().to_string();

        let put_journal = Put::builder()
            .table_name(self.journal_table_name.clone())
            .item("pkey", AttributeValue::S(pkey))
            .item("skey", AttributeValue::S(skey))
            .item("aid", AttributeValue::S(aid))
            .item("seq_nr", AttributeValue::N(seq_nr))
            .item("payload", AttributeValue::S(payload))
            .item("occurred_at", AttributeValue::N(occurred_at))
            .build();

        Ok(put_journal)
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fmt::{Display, Formatter};
    use std::thread::sleep;
    use std::time::Duration;

    use anyhow::Result;
    use aws_sdk_dynamodb::Client;
    use aws_sdk_dynamodb::config::{Credentials, Region};
    use aws_sdk_dynamodb::operation::create_table::CreateTableOutput;
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection, ProjectionType,
        ProvisionedThroughput, ScalarAttributeType,
    };
    use chrono::{DateTime, Utc};
    use once_cell::sync::Lazy;
    use serde::{Deserialize, Serialize};
    use testcontainers::clients::Cli;
    use testcontainers::Container;
    use testcontainers::core::WaitFor;
    use testcontainers::images::generic::GenericImage;
    use tokio::sync::Mutex;
    use ulid_generator_rs::{ULID, ULIDGenerator};

    use crate::event_store::EventStore;
    use crate::types::{Aggregate, AggregateId, Event, EventPersistenceGateway};

    static DOCKER: Lazy<Mutex<Cli>> = Lazy::new(|| Mutex::new(Cli::default()));

    pub async fn create_journal_table(client: &Client, table_name: &str, gsi_name: &str) -> Result<CreateTableOutput> {
        let pkey_attribute_definition = AttributeDefinition::builder()
            .attribute_name("pkey")
            .attribute_type(ScalarAttributeType::S)
            .build();

        let skey_attribute_definition = AttributeDefinition::builder()
            .attribute_name("skey")
            .attribute_type(ScalarAttributeType::S)
            .build();

        let pkey_schema = KeySchemaElement::builder()
            .attribute_name("pkey")
            .key_type(KeyType::Hash)
            .build();

        let skey_schema = KeySchemaElement::builder()
            .attribute_name("skey")
            .key_type(KeyType::Range)
            .build();

        let aid_attribute_definition = AttributeDefinition::builder()
            .attribute_name("aid")
            .attribute_type(ScalarAttributeType::S)
            .build();

        let seq_nr_attribute_definition = AttributeDefinition::builder()
            .attribute_name("seq_nr")
            .attribute_type(ScalarAttributeType::N)
            .build();

        let provisioned_throughput = ProvisionedThroughput::builder()
            .read_capacity_units(10)
            .write_capacity_units(5)
            .build();

        let gsi = GlobalSecondaryIndex::builder()
            .index_name(gsi_name)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("aid")
                    .key_type(KeyType::Hash)
                    .build(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("seq_nr")
                    .key_type(KeyType::Range)
                    .build(),
            )
            .projection(Projection::builder().projection_type(ProjectionType::All).build())
            .provisioned_throughput(provisioned_throughput.clone())
            .build();

        let result = client
            .create_table()
            .table_name(table_name)
            .attribute_definitions(pkey_attribute_definition)
            .attribute_definitions(skey_attribute_definition)
            .attribute_definitions(aid_attribute_definition)
            .attribute_definitions(seq_nr_attribute_definition)
            .key_schema(pkey_schema)
            .key_schema(skey_schema)
            .global_secondary_indexes(gsi)
            .provisioned_throughput(provisioned_throughput)
            .send()
            .await?;

        Ok(result)
    }

    pub async fn create_snapshot_table(client: &Client, table_name: &str, gsi_name: &str) -> Result<CreateTableOutput> {
        let pkey_attribute_definition = AttributeDefinition::builder()
            .attribute_name("pkey")
            .attribute_type(ScalarAttributeType::S)
            .build();

        let pkey_schema = KeySchemaElement::builder()
            .attribute_name("pkey")
            .key_type(KeyType::Hash)
            .build();

        let skey_attribute_definition = AttributeDefinition::builder()
            .attribute_name("skey")
            .attribute_type(ScalarAttributeType::S)
            .build();

        let skey_schema = KeySchemaElement::builder()
            .attribute_name("skey")
            .key_type(KeyType::Range)
            .build();

        let aid_attribute_definition = AttributeDefinition::builder()
            .attribute_name("aid")
            .attribute_type(ScalarAttributeType::S)
            .build();

        let seq_nr_attribute_definition = AttributeDefinition::builder()
            .attribute_name("seq_nr")
            .attribute_type(ScalarAttributeType::N)
            .build();

        let provisioned_throughput = ProvisionedThroughput::builder()
            .read_capacity_units(10)
            .write_capacity_units(5)
            .build();

        let gsi = GlobalSecondaryIndex::builder()
            .index_name(gsi_name)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("aid")
                    .key_type(KeyType::Hash)
                    .build(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("seq_nr")
                    .key_type(KeyType::Range)
                    .build(),
            )
            .projection(Projection::builder().projection_type(ProjectionType::All).build())
            .provisioned_throughput(provisioned_throughput.clone())
            .build();

        let result = client
            .create_table()
            .table_name(table_name)
            .attribute_definitions(pkey_attribute_definition)
            .attribute_definitions(skey_attribute_definition)
            .attribute_definitions(aid_attribute_definition)
            .attribute_definitions(seq_nr_attribute_definition)
            .key_schema(pkey_schema)
            .key_schema(skey_schema)
            .global_secondary_indexes(gsi)
            .provisioned_throughput(provisioned_throughput)
            .send()
            .await?;

        Ok(result)
    }

    pub fn create_client(dynamodb_port: u16) -> Client {
        let region = Region::new("us-west-1");
        let config = aws_sdk_dynamodb::Config::builder()
            .region(Some(region))
            .endpoint_url(format!("http://localhost:{}", dynamodb_port))
            .credentials_provider(Credentials::new("x", "x", None, None, "default"))
            .build();

        Client::from_conf(config)
    }

    async fn wait_table(client: &Client, target_table_name: &str) -> bool {
        let lto = client.list_tables().send().await;
        match lto {
            Ok(lto) => {
                log::info!("table_names: {:?}", lto.table_names());
                match lto.table_names() {
                    Some(table_names) => table_names.iter().any(|tn| tn == target_table_name),
                    None => false,
                }
            }
            Err(e) => {
                println!("Error: {}", e);
                false
            }
        }
    }


    struct ULIDGeneratorState {
        generator: ULIDGenerator,
        last_id: Option<ULID>,
    }

    impl ULIDGeneratorState {
        fn new() -> Self {
            Self {
                generator: ULIDGenerator::new(),
                last_id: None,
            }
        }
    }

    static ID_GENERATOR_STATE: Lazy<std::sync::Mutex<ULIDGeneratorState>> = Lazy::new(|| std::sync::Mutex::new(ULIDGeneratorState::new()));

    /// 初回以降の採番が衝突しない単調増加するIDを生成する。
    pub fn id_generate() -> ULID {
        let mut state = ID_GENERATOR_STATE.lock().unwrap();
        match state.last_id {
            None => {
                let id = state.generator.generate().unwrap();
                state.last_id = Some(id);
                id
            }
            Some(last_id) => {
                let id = state.generator.generate_monotonic(&last_id).unwrap();
                state.last_id = Some(id);
                id
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct UserAccountId {
        value: String,
    }

    impl Display for UserAccountId {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.value)
        }
    }

    impl AggregateId for UserAccountId {
        fn type_name(&self) -> String {
            "UserAccount".to_string()
        }

        fn value(&self) -> String {
            self.value.clone()
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub enum UserAccountEvent {
        Created {
            id: ULID,
            aggregate_id: UserAccountId,
            seq_nr: usize,
            name: String,
            occurred_at: chrono::DateTime<chrono::Utc>,
        },
        Renamed {
            id: ULID,
            aggregate_id: UserAccountId,
            seq_nr: usize,
            name: String,
            occurred_at: chrono::DateTime<chrono::Utc>,
        },
    }

    impl Event for UserAccountEvent {
        type ID = ULID;
        type AggregateID = UserAccountId;

        fn id(&self) -> &Self::ID {
            match self {
                UserAccountEvent::Created { id, .. } => id,
                UserAccountEvent::Renamed { id, .. } => id,
            }
        }

        fn aggregate_id(&self) -> &Self::AggregateID {
            match self {
                UserAccountEvent::Created { aggregate_id, .. } => aggregate_id,
                UserAccountEvent::Renamed { aggregate_id, .. } => aggregate_id,
            }
        }

        fn seq_nr(&self) -> usize {
            match self {
                UserAccountEvent::Created { seq_nr, .. } => *seq_nr,
                UserAccountEvent::Renamed { seq_nr, .. } => *seq_nr,
            }
        }

        fn occurred_at(&self) -> &DateTime<Utc> {
            match self {
                UserAccountEvent::Created { occurred_at, .. } => occurred_at,
                UserAccountEvent::Renamed { occurred_at, .. } => occurred_at,
            }
        }

        fn is_created(&self) -> bool {
            match self {
                UserAccountEvent::Created { .. } => true,
                UserAccountEvent::Renamed { .. } => false,
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct UserAccount {
        id: UserAccountId,
        name: String,
        seq_nr: usize,
        version: usize,
    }

    impl UserAccount {
        fn new(id: UserAccountId, name: String) -> Result<(Self, UserAccountEvent)> {
            let mut my_self = Self {
                id: id.clone(),
                name,
                seq_nr: 0,
                version: 1,
            };
            my_self.seq_nr += 1;
            let event = UserAccountEvent::Created {
                id: id_generate(),
                aggregate_id: id,
                seq_nr: my_self.seq_nr,
                name: my_self.name.clone(),
                occurred_at: chrono::Utc::now(),
            };
            Ok((my_self, event))
        }

        fn replay(events: impl IntoIterator<Item=UserAccountEvent>, snapshot_opt: Option<UserAccount>, version: usize) -> Self {
            let mut result = events.into_iter().fold(snapshot_opt, |result, event| match (result, event) {
                (Some(mut this), event) => {
                    this.apply_event(event.clone());
                    Some(this)
                },
                (..) => None,
            }).unwrap();
            result.version = version;
            result
        }

        fn apply_event(&mut self, event: UserAccountEvent) {
            match event {
                UserAccountEvent::Renamed {
                    name, ..
                } => {
                    self.name = name;
                }
                _ => {}
            }
        }

        pub fn rename(&mut self, name: &str) -> Result<UserAccountEvent> {
            self.name = name.to_string();
            self.seq_nr += 1;
            let event = UserAccountEvent::Renamed {
                id: id_generate(),
                aggregate_id: self.id.clone(),
                seq_nr: self.seq_nr,
                name: name.to_string(),
                occurred_at: chrono::Utc::now(),
            };
            Ok(event)
        }
    }

    impl Aggregate for UserAccount {
        type ID = UserAccountId;

        fn id(&self) -> &Self::ID {
            &self.id
        }

        fn seq_nr(&self) -> usize {
            self.seq_nr
        }

        fn version(&self) -> usize {
            self.version
        }

        fn set_version(&mut self, version: usize) {
            self.version = version
        }
    }

    pub struct UserAccountRepository {
        event_store: EventStore,
    }

    impl UserAccountRepository {
        fn new(event_store: EventStore) -> Self {
            Self {
                event_store,
            }
        }

        async fn store(&mut self, event: &UserAccountEvent, version: usize, snapshot_opt: Option<&UserAccount>) -> Result<()> {
            self.event_store.store_event_with_snapshot_opt(event, version, snapshot_opt).await
        }

        async fn find_by_id(&self, id: &UserAccountId) -> Result<UserAccount> {
            let (snapshot, seq_nr, version) = self.event_store.get_snapshot_by_id(id).await?;
            let events = self.event_store.get_events_by_id_and_seq_nr(id, seq_nr).await?;
            Ok(UserAccount::replay(events, Some(snapshot), version))
        }
    }

    #[tokio::test]
    async fn test() {
        env::set_var("RUST_LOG", "debug");
        let _ = env_logger::builder().is_test(true).try_init();

        let wait_for = WaitFor::message_on_stdout("Ready.");
        let image = GenericImage::new("localstack/localstack", "2.1.0")
            .with_env_var("SERVICES", "dynamodb")
            .with_env_var("DEFAULT_REGION", "us-west-1")
            .with_env_var("EAGER_SERVICE_LOADING", "1")
            .with_env_var("DYNAMODB_SHARED_DB", "1")
            .with_env_var("DYNAMODB_IN_MEMORY", "1")
            .with_wait_for(wait_for);
        let binding = DOCKER.lock().await;
        let dynamodb_node: Container<GenericImage> = binding.run::<GenericImage>(image);
        let port = dynamodb_node.get_host_port_ipv4(4566);
        log::debug!("DynamoDB port: {}", port);

        let test_time_factor = env::var("TEST_TIME_FACTOR")
            .unwrap_or("1".to_string())
            .parse::<f32>()
            .unwrap();

        sleep(Duration::from_millis((1000f32 * test_time_factor) as u64));

        let client = create_client(port);

        let journal_table_name = "journal";
        let journal_aid_index_name = "journal-aid-index";
        let _journal_table_output = create_journal_table(&client, journal_table_name, journal_aid_index_name).await;

        let snapshot_table_name = "snapshot";
        let snapshot_aid_index_name = "snapshot-aid-index";
        let _snapshot_table_output = create_snapshot_table(&client, snapshot_table_name, snapshot_aid_index_name).await;

        while !(wait_table(&client, journal_table_name).await) {
            log::info!("Waiting for journal table to be created");
            sleep(Duration::from_millis((1000f32 * test_time_factor) as u64));
        }

        while !(wait_table(&client, snapshot_table_name).await) {
            log::info!("Waiting for snapshot table to be created");
            sleep(Duration::from_millis((1000f32 * test_time_factor) as u64));
        }

        let event_store = EventStore::new(
            client.clone(),
            journal_table_name.to_string(),
            journal_aid_index_name.to_string(),
            snapshot_table_name.to_string(),
            snapshot_aid_index_name.to_string(),
            64,
        );

        let mut user_account_repository = UserAccountRepository::new(event_store);
        let (user_account, event) = UserAccount::new(UserAccountId { value: "1".to_string() }, "test".to_string()).unwrap();
        user_account_repository.store(&event, user_account.version(), Some(&user_account)).await.unwrap();

        let mut user_account = user_account_repository.find_by_id(&UserAccountId { value: "1".to_string() }).await.unwrap();

        let event = user_account.rename("test2").unwrap();

        user_account_repository.store(&event, user_account.version(), None).await.unwrap();
        let user_account = user_account_repository.find_by_id(&UserAccountId { value: "1".to_string() }).await.unwrap();

        assert_eq!(user_account.name, "test2");
    }
}
