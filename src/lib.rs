use std::collections::{BTreeSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{stream::BoxStream, StreamExt};

use hdfs::hdfs::{get_hdfs_by_full_path, FileStatus, HdfsErr, HdfsFile, HdfsFs};
use hdfs::walkdir::HdfsWalkDir;
use object_store::{
    path::{self, Path}, Error, GetOptions, GetResult, GetResultPayload, ListResult, ObjectMeta, MultipartId,
    ObjectStore, PutOptions, PutResult, payload::PutPayload, PutMode, Result,
    util::maybe_spawn_blocking, upload::MultipartUpload,
};

#[derive(Debug)]
pub struct HadoopFileSystem {
    hdfs: Arc<HdfsFs>,
}

impl std::fmt::Display for HadoopFileSystem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HadoopFileSystem({})", self.get_path_root())
    }
}

impl Default for HadoopFileSystem {
    fn default() -> Self {
        Self::new()
    }
}

impl HadoopFileSystem {
    /// Create new HadoopFileSystem by getting HdfsFs from default path
    pub fn new() -> Self {
        Self {
            hdfs: get_hdfs_by_full_path("default").expect("Fail to get default HdfsFs"),
        }
    }

    /// Create new HadoopFileSystem by getting HdfsFs from the full path, 
    /// e.g. full_path == hdfs://localhost:8020/xxx/xxx
    pub fn new(full_path: &str) -> Option<Self> {
        get_hdfs_by_full_path(full_path)
            .map(|hdfs| Some(Self { hdfs }))
            .unwrap_or(None)
    }

    pub fn get_path_root(&self) -> String {
        self.hdfs.url().to_owned()
    }

    pub fn get_path(&self, full_path: &str) -> Path {
        get_path(full_path, self.hdfs.url())
    }

    fn read_range(range: &Range<usize>, file: &HdfsFile) -> Result<Bytes> {
        // Set lenght to read
        let to_read = range.end - range.start;
        
        // Create buffer to safe what was read
        let mut buf = vec![0; to_read];
        
        // Read specifying the range
        let read = file
            .read_with_pos(range.start as i64, buf.as_mut_slice())
            .map_err(match_error)?;

        // Verify read
        assert_eq!(
            to_read as i32,
            read,
            "Read path {} from {} with expected size {} and actual size {}",
            file.path(),
            range.start,
            to_read,
            read
        );

        Ok(buf.into())
    }
}


#[async_trait]
impl ObjectStore for HadoopFileSystem {
    /// Save the provided `payload` to `location` with the given options
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        if matches!(opts.mode, PutMode::Update(_)) {
            return Err(Error::NotImplemented);
        }

        if !opts.attributes.is_empty() {
            return Err(Error::NotImplemented);
        }

        let hdfs = self.hdfs.clone();
        // The following variable will shadow location: &Path
        let location = String::from(location);
        maybe_spawn_blocking(move || {
            // Note that here the return is made explicit for clarity but removing the ; it can be removed
            let file = match opts.mode {
                PutMode::Overwrite => {
                    return match hdfs.create_with_overwrite(&location, true) {
                        Ok(f) => f,
                        Err(e) => Err(match_error(e)),
                    };
                }
                PutMode::Create => {
                    return match hdfs.create(&location) {
                        Ok(f) => f,
                        Err(e) => Err(match_error(e)),
                    };
                }
                PutMode::Update(_) => unreachable!(),
            };

            file.write(payload.as_ref()).map_err(match_error)?;
            file.close().map_err(match_error);

            return Ok(PutResult {
                e_tag: None,
                version: None,
            });
        })
        .await
    }

    /// Perform a multipart upload
    ///
    /// Client should prefer [`ObjectStore::put`] for small payloads, as streaming uploads
    /// typically require multiple separate requests. See [`MultipartUpload`] for more information
    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        // VERSION 0.10
        //self.put_multipart_opts(location, PutMultipartOpts::default())
        //    .await

        todo!()
    }

    /// Cleanup an aborted upload.
    ///
    /// See documentation for individual stores for exact behavior, as capabilities
    /// vary by object store.
    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {

        todo!()
    }
    
    // VERSION 0.10
    /// Perform a multipart upload with options
    ///
    /// Client should prefer [`ObjectStore::put`] for small payloads, as streaming uploads
    /// typically require multiple separate requests. See [`MultipartUpload`] for more information
    //async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOpts) -> Result<Box<dyn MultipartUpload>> {
    //  todo!()
    //}

    /// Perform a get request with options
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.if_match.is_some() || options.if_none_match.is_some() {
            return Err(Error::Generic {
                store: "HadoopFileSystem",
                source: Box::new(HdfsErr::Generic("ETags not supported".to_string())),
            });
        }

        let hdfs = self.hdfs.clone();
        let hdfs_root = self.get_path_root();
        // The following variable will shadow location: &Path
        let location = String::from(location);

        let (blob, object_metadata, range) = maybe_spawn_blocking(move || {
            let file = hdfs.open(&location).map_err(match_error)?;
            let file_status = file.get_file_status().map_err(match_error)?;

            // Check Modified
            if options.if_unmodified_since.is_some() || options.if_modified_since.is_some() {
                check_modified(&options, &location, last_modified(&file_status))?;
            }

            // Set GetRange
            let range = if let Some(range) = options.range {
                range
            } else {
                Range {
                    start: 0,
                    end: file_status.len(),
                }
            };

            // Read Buffer
            let buf = Self::read_range(&range, &file)?;

            // Close file
            file.close().map_err(match_error)?;

            // Convert Metadata
            let object_metadata = convert_metadata(file_status, &hdfs_root);

            Ok((buf, object_metadata, range))
        })
        .await?;

        Ok(GetResult {
            payload: GetResultPayload::Stream(
                futures::stream::once(async move { Ok(blob) }).boxed(),
            ),
            meta: object_metadata,
            range,
        })
    }

    /// Return the bytes that are stored at the specified location
    /// in the given byte range.
    ///
    /// See [`GetRange::Bounded`] for more details on how `range` gets interpreted
    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let hdfs = self.hdfs.clone();
        // The following variable will shadow location: &Path
        let location = String::from(location);

        maybe_spawn_blocking(move || {
            let file = hdfs.open(&location).map_err(match_error)?;
            let buf = Self::read_range(&range, &file)?;
            file.close().map_err(match_error)?;

            Ok(buf)
        })
        .await
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let hdfs = self.hdfs.clone();
        let hdfs_root = self.get_path_root();
        // The following variable will shadow location: &Path
        let location = String::from(location);

        maybe_spawn_blocking(move || {
            let file_status = hdfs.get_file_status(&location).map_err(match_error)?;
            Ok(convert_metadata(file_status, &hdfs_root))
        })
        .await
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> Result<()> {
        let hdfs = self.hdfs.clone();
        // The following variable will shadow location: &Path
        let location = String::from(location);

        maybe_spawn_blocking(move || {
            hdfs.delete(&location, false).map_err(match_error)?;

            Ok(())
        })
        .await
    }

    /// List all of the leaf files under the prefix path.
    /// It will recursively search leaf files whose depth is larger than 1
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        let default_path = Path::from(self.get_path_root());
        let prefix = prefix.unwrap_or(&default_path);
        let hdfs = self.hdfs.clone();
        let hdfs_root = self.get_path_root();
        let walkdir =
            HdfsWalkDir::new_with_hdfs(String::from(prefix), hdfs)
                .min_depth(1);

        let s =
            walkdir.into_iter().flat_map(move |result_dir_entry| {
                match convert_walkdir_result(result_dir_entry) {
                    Err(e) => Some(Err(e)),
                    Ok(None) => None,
                    Ok(entry @ Some(_)) => entry
                        .filter(|dir_entry| dir_entry.is_file())
                        .map(|entry| Ok(convert_metadata(entry, &hdfs_root))),
                }
            });

        // If no tokio context, return iterator directly as no
        // need to perform chunked spawn_blocking reads
        if tokio::runtime::Handle::try_current().is_err() {
            return futures::stream::iter(s).boxed();
        }

        // Otherwise list in batches of CHUNK_SIZE
        const CHUNK_SIZE: usize = 1024;

        let buffer = VecDeque::with_capacity(CHUNK_SIZE);
        let stream = futures::stream::try_unfold((s, buffer), |(mut s, mut buffer)| async move {
            if buffer.is_empty() {
                (s, buffer) = tokio::task::spawn_blocking(move || {
                    for _ in 0..CHUNK_SIZE {
                        match s.next() {
                            Some(r) => buffer.push_back(r),
                            None => break,
                        }
                    }
                    (s, buffer)
                })
                .await?;
            }

            match buffer.pop_front() {
                Some(Err(e)) => Err(e),
                Some(Ok(meta)) => Ok(Some((meta, (s, buffer)))),
                None => Ok(None),
            }
        });

        stream.boxed()
    }

    /// List files and directories directly under the prefix path.
    /// It will not recursively search leaf files whose depth is larger than 1
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let default_path = Path::from(self.get_path_root());
        let prefix = prefix.unwrap_or(&default_path);
        let hdfs = self.hdfs.clone();
        let hdfs_root = self.get_path_root();
        let walkdir =
            HdfsWalkDir::new_with_hdfs(String::from(prefix), hdfs)
                .min_depth(1)
                .max_depth(1);

        let prefix = prefix.clone();
        maybe_spawn_blocking(move || {
            let mut common_prefixes = BTreeSet::new();
            let mut objects = Vec::new();

            for entry_res in walkdir.into_iter().map(convert_walkdir_result) {
                if let Some(entry) = entry_res? {
                    let is_directory = entry.is_directory();
                    let entry_location = get_path(entry.name(), &hdfs_root);

                    let mut parts = match entry_location.prefix_match(&prefix) {
                        Some(parts) => parts,
                        None => continue,
                    };

                    let common_prefix = match parts.next() {
                        Some(p) => p,
                        None => continue,
                    };

                    drop(parts);

                    if is_directory {
                        common_prefixes.insert(prefix.child(common_prefix));
                    } else {
                        objects.push(convert_metadata(entry, &hdfs_root));
                    }
                }
            }

            Ok(ListResult {
                common_prefixes: common_prefixes.into_iter().collect(),
                objects,
            })
        })
        .await
    }

    /// Copy an object from one path to another.
    /// If there exists an object at the destination, it will be overwritten.
    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let hdfs = self.hdfs.clone();
        // The following two variables will shadow from: &Path and to: &Path
        let from = String::from(from);
        let to = String::from(to);

        maybe_spawn_blocking(move || {
            // We need to make sure the source exist
            if !hdfs.exist(&from) {
                return Err(Error::NotFound {
                    path: from.clone(),
                    source: Box::new(HdfsErr::FileNotFound(from)),
                });
            }
            // Delete destination if exists
            if hdfs.exist(&to) {
                hdfs.delete(&to, false).map_err(match_error)?;
            }

            hdfs::util::HdfsUtil::copy(hdfs.as_ref(), &from, hdfs.as_ref(), &to)
                .map_err(match_error)?;

            Ok(())
        })
        .await
    }

    /// Move an object from one path to another in the same object store (HDFS)
    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let hdfs = self.hdfs.clone();
        // The following two variables will shadow the from and to &Path
        let from = String::from(from);
        let to = String::from(to);

        maybe_spawn_blocking(move || {
            hdfs.rename(&from, &to).map_err(match_error)?;

            Ok(())
        })
        .await
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object. 
    /// This is done performing an atomic operation
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let hdfs = self.hdfs.clone();
        // The following two variables will shadow the from and to &Path
        let from = String::from(from);
        let to = String::from(to);

        maybe_spawn_blocking(move || {
            if hdfs.exist(&to) {
                return Err(Error::AlreadyExists {
                    path: from,
                    source: Box::new(HdfsErr::FileAlreadyExists(to)),
                });
            }

            hdfs::util::HdfsUtil::copy(hdfs.as_ref(), &from, hdfs.as_ref(), &to)
                .map_err(match_error)?;

            Ok(())
        })
        .await
    }
}

/// Matches HdfsErr to its corresponding ObjectStoreError
fn match_error(err: HdfsErr) -> Error {
    match err {
        HdfsErr::FileNotFound(path) => Error::NotFound {
            path: path.clone(),
            source: Box::new(HdfsErr::FileNotFound(path)),
        },
        HdfsErr::FileAlreadyExists(path) => Error::AlreadyExists {
            path: path.clone(),
            source: Box::new(HdfsErr::FileAlreadyExists(path)),
        },
        HdfsErr::InvalidUrl(path) => Error::InvalidPath {
            source: path::Error::InvalidPath {
                path: PathBuf::from(path),
            },
        },
        HdfsErr::CannotConnectToNameNode(namenode_uri) => Error::Generic {
            store: "HadoopFileSystem",
            source: Box::new(HdfsErr::CannotConnectToNameNode(namenode_uri)),
        },
        HdfsErr::Generic(err_str) => Error::Generic {
            store: "HadoopFileSystem",
            source: Box::new(HdfsErr::Generic(err_str)),
        },
    }
}

/// Create Path without prefix
pub fn get_path(full_path: &str, prefix: &str) -> Path {
    let partial_path = full_path.strip_prefix(prefix).unwrap();
    Path::from(partial_path)
}

/// Convert HDFS file status to ObjectMeta
pub fn convert_metadata(file: FileStatus, prefix: &str) -> ObjectMeta {
    ObjectMeta {
        location: get_path(file.name(), prefix),
        last_modified: last_modified(&file),
        size: file.len(),
        e_tag: None,
        version: None,
    }
}

/// Gets the last_modified date time on a file
fn last_modified(file: &FileStatus) -> DateTime<Utc> {
    DateTime::from_timestamp(file.last_modified(), 0).unwrap()
}

/// Checks if the file at the specified location was modified since a specific date
fn check_modified(
    get_options: &GetOptions,
    location: &str,
    last_modified: DateTime<Utc>,
) -> Result<()> {
    if let Some(date) = get_options.if_modified_since {
        if last_modified <= date {
            return Err(Error::NotModified {
                path: location.to_string(),
                source: format!("{} >= {}", date, last_modified).into(),
            });
        }
    }

    if let Some(date) = get_options.if_unmodified_since {
        if last_modified > date {
            return Err(Error::Precondition {
                path: location.to_string(),
                source: format!("{} < {}", date, last_modified).into(),
            });
        }
    }
    Ok(())
}

/// Convert walkdir results and converts not-found errors into `None`.
fn convert_walkdir_result(
    res: std::result::Result<FileStatus, HdfsErr>,
) -> Result<Option<FileStatus>> {
    match res {
        Ok(entry) => Ok(Some(entry)),
        Err(walkdir_err) => match walkdir_err {
            HdfsErr::FileNotFound(_) => Ok(None),
            _ => Err(match_error(HdfsErr::Generic(
                "Fail to walk hdfs directory".to_owned(),
            ))),
        },
    }
}
