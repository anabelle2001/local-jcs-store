use color_eyre::eyre::{self, eyre, Result};
use openssl::{base64, sha::Sha256};
use serde_jcs::to_vec as to_jcs;
use serde_json::Value;
use std::path::PathBuf;
use tokio::{fs, io::AsyncWriteExt};

/// Computes the SHA-256 hash of the input, and encodes the result in Base64.
/// '/' characters are replaced by '+'.
///
/// # Example
/// ```
/// # use local_jcs_store::b64sha256;
/// let bytes: &[u8] = "Hello, World!".as_bytes();
/// let hash: String = b64sha256(bytes);
/// assert!(hash=="3+1gIbsr1bCvZ2KQgJ7DpTGR3YHH9wpLKGiKNiGCmG8=")
/// ```
pub fn b64sha256(bytes: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(&bytes);
    let hash = base64::encode_block(&hash.finish());
    let hash = hash.replace('/', "+");
    return hash;
}

/// Stores many `Item`s in a directory.
///
/// # Example
/// ```
/// # use color_eyre::eyre::Result;
/// # use local_jcs_store::Database;
/// #
/// # #[tokio::main]
/// # async fn main() {
/// #
/// use serde_json::json;
///
/// let tmp_dir = std::env::temp_dir();
/// let mut my_db = Database::open(tmp_dir).unwrap();
/// let item = my_db.put_obj(&json! ({"Hello":"World!"}) ).await.unwrap();
/// # }
/// ```

pub struct Database {
    path: PathBuf,
}

/// A JSON object and its hash.
///
/// # Properties
///
/// `json_utf8` - The cannonical JSON [JCS / RFC 8785] representation of an object
///
/// `hash_b64` - The object's sha256 hash, encoded in base64, but with slashes replaced with pluses.
///
/// [rfc8785]: https://tools.ietf.org/html/rfc8785

pub struct Item {
    pub hash_b64: String,
    pub json_utf8: String,
}

impl Database {
    /// Opens a database, creating it if the path does not exist
    ///
    /// # Errors
    /// - A file is present at the path
    /// - The folder could not be created
    pub fn open(path: PathBuf) -> Result<Self> {
        use std::fs;

        //if the path does not exist, try to create it as a dir
        if !path.try_exists()? {
            fs::create_dir_all(&path)?;
        };

        //it's possible that the path exists, but is a file.
        if path.is_dir() {
            Ok(Self { path })
        } else {
            Err(eyre!("File exists at DB Directory"))
        }
    }

    /// Saves an `Item` to the database. Does nothing if the item exists
    pub async fn put_item(
        &mut self,
        item: &Item,
    ) -> Result<()> {
        let item_path = self.path.join(&item.hash_b64);

        if !item_path.try_exists()? {
            fs::File::create(&item_path)
                .await?
                .write_all(item.json_utf8.as_bytes())
                .await?;
        }

        Ok(())
    }

    /// Saves a JSON `Value` to the database. Does nothing if the item exists
    pub async fn put_obj(
        &mut self,
        object: &Value,
    ) -> Result<Item> {
        let item: Item = mk_item(object)?;
        self.put_item(&item).await?;

        Ok(item)
    }

    /// Attempts to read an item from disk
    pub async fn get_item(
        &self,
        hash_b64: &str,
    ) -> Result<Item> {
        let path = self.path.join(hash_b64);
        let json_utf8 = fs::read(path).await?;
        let json_utf8 = String::from_utf8(json_utf8)?;

        let item = Item {
            hash_b64: hash_b64.to_string(),
            json_utf8,
        };

        item.check_hash()?;

        return Ok(item);
    }

    /// Attempts to read an item from disk & parse it into a JSON object
    pub async fn get_obj(
        &self,
        hash_b64: &str,
    ) -> Result<Value> {
        use std::str::FromStr;

        let item = self.get_item(hash_b64).await?;
        Ok(Value::from_str(&item.json_utf8)?)
    }
}

impl core::fmt::Debug for Item {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        writeln!(
            f,
            "DISPLAY: Item {{ hash_b64: {}, json_utf8: {} }}",
            self.hash_b64, self.json_utf8,
        )
    }
}

impl Item {
    /// Verify that the item has a valid hash.
    ///  
    /// # Returns
    /// - `Ok(&self)` if the hash is valid
    /// - `Err()` if the hash is **not** valid
    fn check_hash(&self) -> Result<&Self> {
        let is_valid = self.hash_b64 == b64sha256(self.json_utf8.as_bytes());

        if is_valid {
            Ok(self)
        } else {
            Err(eyre!("Invalid Hash"))
        }
    }
}

impl TryFrom<Value> for Item {
    type Error = eyre::Error;

    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        mk_item(&value)
    }
}

impl TryFrom<Item> for Value {
    fn try_from(value: Item) -> std::result::Result<Self, Self::Error> {
        value.check_hash()?;
        Ok(serde_json::from_str(&value.json_utf8)?)
    }
    type Error = eyre::Error;
}

/// Converts a JSON `Value` into an Item
///
/// # Example
/// ```
/// # use local_jcs_store::mk_item;
/// use serde_json::json;
/// let json = json!({"Hello":"World!"});
/// let item = mk_item(&json).unwrap();
/// println!("{:?}",item);
/// ```
pub fn mk_item(obj: &Value) -> Result<Item> {
    let json_utf8: Vec<u8> = to_jcs(&obj)?;
    let hash_b64 = b64sha256(&json_utf8);
    let json_utf8: String = String::from_utf8(json_utf8)?;

    Ok(Item {
        json_utf8,
        hash_b64,
    })
}
