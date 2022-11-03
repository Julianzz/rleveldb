use crate::{consts::NUM_LEVELS, filenames::FileMeta};

pub struct Version {
    pub files: [Vec<FileMeta>; NUM_LEVELS],
}

impl Version {
    pub fn new() -> Version {
        Version {
            files: Default::default(),
        }
    }
}
