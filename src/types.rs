use std::{cell::RefCell, rc::Rc};

use crate::error::Error;

pub type SequenceNumber = u64;
pub const MAX_SEQUENCE_NUMBER: SequenceNumber = (1 << 56) - 1;

type Shared<T> = Rc<RefCell<T>>;
pub fn shared<T>(t: T) -> Rc<RefCell<T>> {
    Rc::new(RefCell::new(t))
}

#[derive(Copy, Clone)]
pub enum ValueType {
    Deletetion = 0,
    Value = 1,
}

impl TryFrom<u8> for ValueType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0u8 => Ok(ValueType::Deletetion),
            1u8 => Ok(ValueType::Value),
            _ => Err(Error::Corruption("wrong tag type".into())),
        }
    }
}
