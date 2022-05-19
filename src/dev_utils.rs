use nix::sys::statfs::statfs;
use nix::NixPath;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

fn get_property(properties: &HashMap<String, String>, property: &str) -> anyhow::Result<String> {
    properties
        .get(property)
        .cloned()
        .ok_or_else(|| anyhow!("Unable to get property: {property}"))
}

pub struct Disk {
    serial_number: String,
}

impl Disk {
    fn from_properties(properties: &HashMap<String, String>) -> anyhow::Result<Self> {
        Ok(Self {
            serial_number: get_property(properties, "ID_SERIAL")?,
        })
    }

    pub fn serial_number(&self) -> &str {
        &self.serial_number
    }
}

pub struct Partition {
    uuid: String,
    mount_point: PathBuf,
    capacity: u64,
}

impl Partition {
    pub fn uuid(&self) -> &str {
        &self.uuid
    }

    pub fn mount_point(&self) -> &Path {
        &self.mount_point
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    fn get_capacity<P: ?Sized + NixPath>(path: &P) -> anyhow::Result<u64> {
        let data = statfs(path)?;
        let size: u64 = data.block_size().try_into()?;
        Ok(size * data.blocks_free())
    }

    fn make<P: ?Sized + NixPath>(
        path: &P,
        properties: &HashMap<String, String>,
    ) -> anyhow::Result<Self> {
        let dev_name = get_property(properties, "DEVNAME")?;
        Ok(Self {
            uuid: get_property(properties, "ID_FS_UUID")?,
            mount_point: block_utils::get_mountpoint(&dev_name)?
                .ok_or_else(|| anyhow!("Device not mounted: {dev_name}"))?,
            capacity: Self::get_capacity(path)?,
        })
    }
}

pub fn get_disk_for_path(path: impl AsRef<Path>) -> anyhow::Result<Disk> {
    let path = path.as_ref();
    log::debug!(
        "Looking up block device properties for: {}",
        path.to_string_lossy()
    );

    let properties = block_utils::get_block_dev_properties(path)?;
    match get_property(&properties, "DEVTYPE")?.as_ref() {
        "disk" => Disk::from_properties(&properties),
        "partition" => match block_utils::get_parent_devpath_from_path(path)? {
            Some(parent) => Err(anyhow!(
                concat!(
                    "This device at {} is a partition and not a disk. ",
                    "Perhaps you meant the disk at {}"
                ),
                path.to_string_lossy(),
                parent.to_string_lossy()
            )),
            None => Err(anyhow!(
                "This device at {} is a partition and not a disk. ",
                path.to_string_lossy()
            )),
        },
        typ => Err(anyhow!("Unknown device type: {typ}")),
    }
}

pub fn get_partition_for_path(path: impl AsRef<Path>) -> anyhow::Result<(Disk, Partition)> {
    let path = path.as_ref();
    log::debug!(
        "Looking up block device properties for: {}",
        path.to_string_lossy()
    );
    let properties = block_utils::get_block_dev_properties(path)?;
    match &*get_property(&properties, "DEVTYPE")? {
        "partition" => match block_utils::get_parent_devpath_from_path(path)? {
            Some(parent) => Ok((
                get_disk_for_path(parent)?,
                Partition::make(path, &properties)?,
            )),
            None => Err(anyhow!(
                "Unable to get parent disk for partition at path {}",
                path.to_string_lossy()
            )),
        },
        "disk" => Err(anyhow!(
            "This device is a disk. Try running `lsblk` to determine the partition: {}",
            path.to_string_lossy()
        )),
        typ => Err(anyhow!("Unknown device type: {typ}")),
    }
}

pub fn get_partition_for_uuid(uuid: &str) -> anyhow::Result<Partition> {
    block_utils::get_block_partitions()?
        .iter()
        .flat_map(|path| {
            block_utils::get_block_dev_properties(path)
                .ok()
                .map(|props| (path, props))
        })
        // TODO slightly sloppy to assume only one device with a UUID exists
        .find(|(_, props)| props.get("ID_FS_UUID").map(|i| &**i) == Some(uuid))
        .ok_or_else(|| {
            anyhow!("Unable to find partition with UUID {uuid}. Is your disk plugged in?")
        })
        .and_then(|(path, props)| Partition::make(path, &props))
}

pub fn get_all_partitions() -> anyhow::Result<Vec<Partition>> {
    Ok(block_utils::get_block_partitions()?
        .iter()
        .flat_map(|path| {
            block_utils::get_block_dev_properties(path)
                .ok()
                .map(|props| (path, props))
        })
        .flat_map(|(path, props)| Partition::make(path, &props).ok())
        .collect::<Vec<_>>())
}
