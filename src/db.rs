use anyhow::{bail, Context, Result};
use atomicwrites::AtomicFile;
use bincode::Options;
use float_ord::FloatOrd;
use serde::{Deserialize, Serialize};

use std::fmt::{self, Display, Formatter};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DbVersion(u32);

pub struct Db {
    pub dirs: Vec<Dir>,
    pub modified: bool,
    path: PathBuf,
}

impl Db {
    const CURRENT_VERSION: DbVersion = DbVersion(3);
    const MAX_SIZE: u64 = 8 * 1024 * 1024; // 8 MiB

    pub fn open(data_dir: PathBuf) -> Result<Db> {
        fs::create_dir_all(&data_dir)
            .with_context(|| format!("unable to create data directory: {}", data_dir.display()))?;

        let path = data_dir.join("db.zo");

        let buffer = match fs::read(&path) {
            Ok(buffer) => buffer,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Ok(Db {
                    dirs: Vec::new(),
                    modified: false,
                    path,
                })
            }
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("could not read from database: {}", path.display()))
            }
        };

        if buffer.is_empty() {
            return Ok(Db {
                dirs: Vec::new(),
                modified: false,
                path,
            });
        }

        let deserializer = &mut bincode::options()
            .with_fixint_encoding()
            .with_limit(Self::MAX_SIZE);

        let version_size = deserializer
            .serialized_size(&Self::CURRENT_VERSION)
            .context("could not determine size of database version field")?
            as _;

        if buffer.len() < version_size {
            bail!("database is corrupted: {}", path.display());
        }

        let (buffer_version, buffer_dirs) = buffer.split_at(version_size);

        let version = deserializer.deserialize(buffer_version).with_context(|| {
            format!("could not deserialize database version: {}", path.display())
        })?;

        let dirs = match version {
            Self::CURRENT_VERSION => deserializer
                .deserialize(buffer_dirs)
                .with_context(|| format!("could not deserialize database: {}", path.display()))?,
            DbVersion(version_num) => bail!(
                "zoxide {} does not support schema v{}: {}",
                env!("ZOXIDE_VERSION"),
                version_num,
                path.display(),
            ),
        };

        Ok(Db {
            dirs,
            modified: false,
            path,
        })
    }

    pub fn save(&mut self) -> Result<()> {
        if !self.modified {
            return Ok(());
        }

        let (buffer, buffer_size) = (|| -> bincode::Result<_> {
            let version_size = bincode::serialized_size(&Self::CURRENT_VERSION)?;
            let dirs_size = bincode::serialized_size(&self.dirs)?;

            let buffer_size = version_size + dirs_size;
            let mut buffer = Vec::with_capacity(buffer_size as _);

            bincode::serialize_into(&mut buffer, &Self::CURRENT_VERSION)?;
            bincode::serialize_into(&mut buffer, &self.dirs)?;

            Ok((buffer, buffer_size))
        })()
        .context("could not serialize database")?;

        AtomicFile::new(&self.path, atomicwrites::AllowOverwrite)
            .write(|db_file| {
                let _ = db_file.set_len(buffer_size);
                db_file.write_all(&buffer)
            })
            .with_context(|| format!("could not write to database: {}", self.path.display()))?;

        self.modified = false;

        Ok(())
    }

    pub fn matches<'a>(&'a mut self, now: Epoch, keywords: &[String]) -> DbMatches<'a> {
        DbMatches::new(self, now, keywords)
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if let Err(e) = self.save() {
            eprintln!("{:#}", e);
        }
    }
}

/// Streaming iterator for matching entries
pub struct DbMatches<'a> {
    db: &'a mut Db,
    idxs: std::iter::Rev<std::ops::Range<usize>>,
    keywords: Vec<String>,
}

impl<'a> DbMatches<'a> {
    pub fn new(db: &'a mut Db, now: Epoch, keywords: &[String]) -> DbMatches<'a> {
        db.dirs
            .sort_unstable_by_key(|dir| FloatOrd(dir.get_score(now)));

        let idxs = (0..db.dirs.len()).rev();
        let keywords = keywords
            .iter()
            .map(|keyword| keyword.to_lowercase())
            .collect();

        DbMatches { db, idxs, keywords }
    }

    pub fn next(&mut self) -> Option<&Dir> {
        for idx in &mut self.idxs {
            let dir = &self.db.dirs[idx];

            if !dir.is_match(&self.keywords) {
                continue;
            }

            if !dir.is_valid() {
                self.db.dirs.swap_remove(idx);
                self.db.modified = true;
                continue;
            }

            let dir = &self.db.dirs[idx];
            return Some(dir);
        }

        None
    }
}

pub type Rank = f64;
pub type Epoch = i64; // use a signed integer so subtraction can be performed on it

#[derive(Debug, Deserialize, Serialize)]
pub struct Dir {
    pub path: String,
    pub rank: Rank,
    pub last_accessed: Epoch,
}

impl Dir {
    pub fn is_valid(&self) -> bool {
        self.rank.is_finite() && self.rank >= 1.0 && Path::new(&self.path).is_dir()
    }

    pub fn is_match(&self, query: &[String]) -> bool {
        let path_lower = self.path.to_lowercase();

        let get_filenames = || {
            let query_name = Path::new(query.last()?).file_name()?.to_str()?;
            let dir_name = Path::new(&path_lower).file_name()?.to_str()?;
            Some((query_name, dir_name))
        };

        if let Some((query_name, dir_name)) = get_filenames() {
            if !dir_name.contains(query_name) {
                return false;
            }
        }

        let mut subpath = path_lower.as_str();

        for subquery in query.iter() {
            match subpath.find(subquery) {
                Some(idx) => subpath = &subpath[idx + subquery.len()..],
                None => return false,
            }
        }

        true
    }

    pub fn get_score(&self, now: Epoch) -> Rank {
        const HOUR: Epoch = 60 * 60;
        const DAY: Epoch = 24 * HOUR;
        const WEEK: Epoch = 7 * DAY;

        let duration = now - self.last_accessed;
        if duration < HOUR {
            self.rank * 4.0
        } else if duration < DAY {
            self.rank * 2.0
        } else if duration < WEEK {
            self.rank * 0.5
        } else {
            self.rank * 0.25
        }
    }

    pub fn display(&self) -> DirDisplay {
        DirDisplay { dir: self }
    }

    pub fn display_score(&self, now: Epoch) -> DirScoreDisplay {
        DirScoreDisplay { dir: self, now }
    }
}

pub struct DirDisplay<'a> {
    dir: &'a Dir,
}

impl Display for DirDisplay<'_> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.dir.path)
    }
}

pub struct DirScoreDisplay<'a> {
    dir: &'a Dir,
    now: Epoch,
}

impl Display for DirScoreDisplay<'_> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        const SCORE_MIN: Rank = 0.0;
        const SCORE_MAX: Rank = 9999.0;

        let score = self.dir.get_score(self.now);

        let score_clamped = if score > SCORE_MAX {
            SCORE_MAX
        } else if score > SCORE_MIN {
            score
        } else {
            SCORE_MIN
        };

        write!(f, "{:>4.0} {}", score_clamped, self.dir.path)
    }
}
