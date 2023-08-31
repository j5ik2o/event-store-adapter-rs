use anyhow::Result;
use serde::{de, Serialize};
use crate::types::Event;

pub trait EventSerializer {
    fn serialize<T>(&mut self, event: &T) -> Result<Vec<u8>> where T: ?Sized + Serialize + Event;
    fn deserialize<T>(&mut self, data: &[u8]) -> Result<T> where for<'a> T: de::Deserialize<'a>;
}

pub struct JsonEventSerializer;

impl EventSerializer for JsonEventSerializer {
    fn serialize<T>(&mut self, event: &T) -> Result<Vec<u8>> where T: ?Sized + Serialize + Event {
        serde_json::to_vec(event).map_err(|e| e.into())
    }

    fn deserialize<T>(&mut self, data: &[u8]) -> Result<T> where for<'a> T: de::Deserialize<'a> {
        serde_json::from_slice::<T>(data).map_err(|e| e.into())
    }
}