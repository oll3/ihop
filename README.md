An experimental tool/service for performing efficient Linux device software updates.

#### Example usage
1. Create a software release image (or anything you want to mount as a block device). It could be a rootfs image (like ext4/squashfs) or an image containing multiple partitions and file systems.
2. Compress the image with [bita](https://github.com/oll3/bita): `bita compress -i release_v2.ext4 release_v2.ext4.cba` 
3. Upload the compressed image to some http file service.
4. Clone the image to your device's chunks store: `ihop clone https://server/release_v2.ext4.cba /path/to/chunk/store/release_v2`.
5. Mount the image at a NBD (Network Block Device) with _ihop_: `ihop mount /path/to/chunk/store/release_v2 /dev/nbd1`.
6. Mount and enjoy your new filesystem at some mount point: `mount -o ro -t ext4 /dev/nbd1 /my/new/rootfs`.

#### Compressing and chunking
The image given to _bita_ (step 1.) is splitted into chunks and a description of how to rebuild the image from those chunks. See [bita](https://github.com/oll3/bita) for more details on the chunking process.

#### Cloning
On clone _ihop_ will check which chunks are already present in the chunk store and only download and write the new ones to disk.
Together with the chunks a description of how to rebuild the original image is also stored. In the example above the description would be the file `/path/to/chunk/store/release_v2`, while the chunks which belong to the release will be stored in subdirectories based on the chunk hash under `/path/to/chunk/store/chunks`. Chunk data is stored uncompressed.

Since _ihop_ will only download and write the diff between currently available chunks and a new ones this makes for a very quick, low bandwidth and write efficient update. Avoiding unnecessary network traffic and avoiding unnecessary flash memory wear. This at a cost of potentially reduced read speed from the block device. Also potentially more fragile than for example a simple 1:1 write of an image to a partition.

![chunk-store1](chunk-store-1.png?raw=true "two release images sharing some chunks")

#### Mounting a block device
To use `ihop mount` the kernel needs to support NBD (`CONFIG_BLK_DEV_NBD`). Even though the name has 'Network' in it, in this case it's  just a way of having a block device driver run in userspace.

While running `ihop mount` the NBD block device will act just like any regular (read only mode) block device. The device created is put together from the description and chunk files where _ihop_ maps between block requests and seeking into chunk files. _ihop_ needs to run for as long as the device should stay mounted since the block -> chunk mappning is done in this process.

#### Verified/Secure boot
The mounted image will be a bit-perfect clone of the original release file (`release_v1.ext4` in the example), hence it should be possible to combine with integrity checking using dm-verity or a boot time full integrity check.
