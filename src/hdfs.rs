use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};

use hdfs::hdfs::{get_hdfs_by_full_path, HdfsErr, HdfsFile, HdfsFs};
use object_store::{
    path::{self, Path}, Error, GetOptions, GetResult, GetResultPayload, ListResult, ObjectMeta,
    ObjectStore, PutOptions, PutResult, PutPayload, PutMode, Result, maybe_spawn_blocking,
};

#[derive(Debug)]
pub struct HadoopFileSystem {
    hdfs: Arc<HdfsFs>,
}

impl std::fmt::Display for HadoopFileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HadoopFileSystem({})", self.hdfs.url())
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
}


#[async_trait]
impl ObjectStore for HadoopFileSystem {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        if matches!(opts.mode, PutMode::Update(_)) {
            return Err(Error::NotImplemented);
        }

        if !opts.attributes.is_empty() {
            return Err(Error::NotImplemented);
        }

        let hdfs = self.hdfs.clone();
        let path = String::from(location);
        maybe_spawn_blocking(move || {
            //let mut e_tag = None;

            let file = match opts.mode {
                PutMode::Overwrite => {
                    return match hdfs.create_with_overwrite(&path, true) {
                        Ok(f) => f,
                        Err(e) => Err(match_to_local_error(e)),
                    };
                }
                PutMode::Create => 
                    return match hdfs.create(&path) {
                        Ok(f) => f,
                        Err(e) => Err(match_to_local_error(e)),
                    };
                PutMode::Update(_) => unreachable!(),
            }

            file.write(payload.as_ref()).map_err(match_to_local_error)?;
            file.close().map_err(match_to_local_error);

            return Ok(PutResult {
                e_tag: None,
                version: None,
            });
        })
        .await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOpts) -> Result<Box<dyn MultipartUpload>> {
        
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'_, Result<ObjectMeta>> {

    }


    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
       
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {

    }
}

fn match_to_local_error(err: HdfsErr) -> Error {
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

















    /// Gets the hdfs address of root (e.g. hdfs://localhost:8020/xxx/xxx)
    pub fn get_path_root(&self) -> String {
        self.hdfs.url().to_owned()
    }

    /// Removes the prefix from the path, transforming an address 
    /// such as hdfs://localhost:8020/xxx/xxx into /localhost:8020/xxx/xxx
    pub fn remove_prefix(&self, location: &Path) -> Result<PathBuf> {
        let mut url = self.hdfs.url().clone();
        url.path_segments_mut()
            .expect("hdfs path")
            // technically not necessary as Path ignores empty segments
            // but avoids creating paths with "//" which look odd in error messages.
            .pop_if_empty()
            .extend(location.parts());

        url.to_file_path()
            .map_err(|_| Error::InvalidUrl { url }.into())
    }