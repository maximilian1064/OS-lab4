// vim: noet:ts=4:sts=4:sw=4:et
#define FUSE_USE_VERSION 26
#define _GNU_SOURCE

#include <assert.h>
#include <endian.h>
#include <err.h>
#include <errno.h>
#include <fuse.h>
#include <fcntl.h>
#include <iconv.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "vfat.h"
#include "util.h"
#include "debugfs.h"

#define DEBUG_PRINT(...) printf(__VA_ARGS__)

/* constants: bit masks, etc */
#define IS_NON_MIRRORED_FAT 1u << 7
#define FAT_ID_MASK 0xf
#define FAT_ENTRY_MASK 0x0fffffff
#define SMALLEST_EOC_MARK 0x0ffffff8
#define VFAT_ATTR_READONLY 0x1

iconv_t iconv_utf16;
char* DEBUGFS_PATH = "/.debug";


static void
vfat_init(const char *dev)
{
    struct fat_boot_header s;

    iconv_utf16 = iconv_open("utf-8", "utf-16"); // from utf-16 to utf-8
    // These are useful so that we can setup correct permissions in the mounted directories
    vfat_info.mount_uid = getuid();
    vfat_info.mount_gid = getgid();

    // Use mount time as mtime and ctime for the filesystem root entry (e.g. "/")
    vfat_info.mount_time = time(NULL);

    vfat_info.fd = open(dev, O_RDONLY);
    if (vfat_info.fd < 0)
        err(1, "open(%s)", dev);
    if (pread(vfat_info.fd, &s, sizeof(s), 0) != sizeof(s))
        err(1, "read super block");
    
    /* 
     * Parsing and storing BPB information
     */
    /* [1] general info */
    vfat_info.bytes_per_sector = s.bytes_per_sector;
    vfat_info.total_sectors = (s.total_sectors_small == 0) ? s.total_sectors : s.total_sectors_small;

    /* [2] reserved region */
    vfat_info.reserved_sectors = s.reserved_sectors;

    /* [3] root dir region, not exist in FAT32 */
    vfat_info.root_dir_sectors = (s.root_max_entries * 32 + vfat_info.bytes_per_sector - 1) /
                                vfat_info.bytes_per_sector;

    /* [4] FAT region */
    vfat_info.fat_count = s.fat_count;
    // active FAT ID
    vfat_info.fat_in_use = (s.fat_flags & IS_NON_MIRRORED_FAT) ? s.fat_flags & FAT_ID_MASK : 0;
    vfat_info.sectors_per_fat = (s.sectors_per_fat_small == 0) ? s.sectors_per_fat : s.sectors_per_fat_small;
    // FAT size in bytes
    vfat_info.fat_size = vfat_info.sectors_per_fat * vfat_info.bytes_per_sector;
    // first byte of active FAT
    vfat_info.fat_begin_offset = (vfat_info.reserved_sectors + vfat_info.fat_in_use * vfat_info.sectors_per_fat) *
                                    vfat_info.bytes_per_sector;

    /* [5] cluster/data region */
    vfat_info.sectors_per_cluster = s.sectors_per_cluster;
    // cluster size in bytes
    vfat_info.cluster_size = vfat_info.sectors_per_cluster * vfat_info.bytes_per_sector;
    vfat_info.direntry_per_cluster = vfat_info.cluster_size / 32;
    // first byte of first cluster 
    vfat_info.cluster_begin_offset = (vfat_info.reserved_sectors + vfat_info.fat_count * vfat_info.sectors_per_fat) * 
                                        vfat_info.bytes_per_sector;

    /* Check if the filesystem mounted is FAT32 */
    size_t data_sectors = vfat_info.total_sectors - (vfat_info.reserved_sectors + 
                            vfat_info.fat_count * vfat_info.sectors_per_fat + vfat_info.root_dir_sectors);
    size_t count_of_clusters = data_sectors / vfat_info.sectors_per_cluster;
    if (count_of_clusters < 65525)
        err(1, "not a FAT32 drive!");

    // number of valid entries in FAT
    vfat_info.fat_entries = count_of_clusters + 2;
    /* retrieve FAT */
    vfat_info.fat = mmap_file(vfat_info.fd, vfat_info.fat_begin_offset, vfat_info.fat_entries * 4);

    /* XXX add your code here */
    vfat_info.root_inode.st_ino = le32toh(s.root_cluster);
    vfat_info.root_inode.st_mode = 0555 | S_IFDIR;
    vfat_info.root_inode.st_nlink = 1;
    vfat_info.root_inode.st_uid = vfat_info.mount_uid;
    vfat_info.root_inode.st_gid = vfat_info.mount_gid;
    vfat_info.root_inode.st_size = 0;
    vfat_info.root_inode.st_atime = vfat_info.root_inode.st_mtime = vfat_info.root_inode.st_ctime = vfat_info.mount_time;

}

/* TODO: XXX add your code here */

int vfat_next_cluster(uint32_t cluster_num)
{
    /* TODO: Read FAT to actually get the next cluster */
    return vfat_info.fat[cluster_num] & FAT_ENTRY_MASK;
}

int vfat_readdir(uint32_t first_cluster, fuse_fill_dir_t callback, void *callbackdata)
{
    struct stat st; // we can reuse same stat entry over and over again

    memset(&st, 0, sizeof(st));
    st.st_uid = vfat_info.mount_uid;
    st.st_gid = vfat_info.mount_gid;
    st.st_nlink = 1;

    // go through cluster chains and collect directory entries
    // TODO: long filename support
    uint32_t cluster;
    struct fat32_direntry *cluster_buf;
    int last_entry_found = 0;

    for (cluster = first_cluster; cluster < SMALLEST_EOC_MARK; cluster = vfat_next_cluster(cluster)) {

        // bring in entire cluster
        cluster_buf = mmap_file(vfat_info.fd, vfat_info.cluster_begin_offset + (cluster - 2) * vfat_info.cluster_size,
                    vfat_info.cluster_size);

        // collect valid entries
        struct fat32_direntry *dir_entry;
        for (dir_entry = cluster_buf; dir_entry < cluster_buf + vfat_info.direntry_per_cluster; dir_entry++) {
            // check if entry is long file name entry or invalid entry
            if (dir_entry->attr == VFAT_ATTR_LFN) {
                ;
            } else if (dir_entry->attr & VFAT_ATTR_INVAL) {
                ;
            } else {
                // check if entry is free
                uint8_t dir_name0 = dir_entry->name[0];
                if (dir_name0 == 0) {
                    last_entry_found = 1;
                    break;
                } else if (dir_name0 == 0xE5) {
                    continue;
                } else if (dir_name0 == 0x05) {
                    dir_entry->name[0] = 0xE5;
                }

                // parse entry and update stat
                st.st_ino = ((uint32_t)dir_entry->cluster_hi << 16) | (uint32_t)dir_entry->cluster_lo;
                st.st_size = dir_entry->size;
                st.st_mode = (dir_entry->attr & VFAT_ATTR_READONLY) ? 0555 : 0777
                            | ((dir_entry->attr & VFAT_ATTR_DIR) ? S_IFDIR : S_IFREG);

                // file name
                char filename[13];
                int i, k;
                // name
                for (i = 0; i < 8; i++) {
                    if (dir_entry->name[i] == 0x20)
                        break;
                    filename[i] = dir_entry->name[i]; 
                }
                // possible dot
                filename[i] = '.'; 
                i++;
                // ext
                for (k = 0; k < 3; k++) {
                    if (dir_entry->ext[k] == 0x20)
                        break;
                    filename[i + k] = dir_entry->ext[k];
                }
                // string terminator
                if (k == 0) {
                    filename[i - 1] = '\0';
                } else {
                    filename[i + k] = '\0';
                }
                // DEBUG
                // printf("%s\n", filename);
            }

        }

        unmap(cluster_buf, vfat_info.cluster_size); 

        // finish operation once last entry found
        if (last_entry_found)
            break;
    }

    /* XXX add your code here */
    return 0;
}


// Used by vfat_search_entry()
struct vfat_search_data {
    const char*  name;
    int          found;
    struct stat* st;
};


// You can use this in vfat_resolve as a callback function for vfat_readdir
// This way you can get the struct stat of the subdirectory/file.
int vfat_search_entry(void *data, const char *name, const struct stat *st, off_t offs)
{
    struct vfat_search_data *sd = data;

    if (strcmp(sd->name, name) != 0) return 0;

    sd->found = 1;
    *sd->st = *st;

    return 1;
}

/**
 * Fills in stat info for a file/directory given the path
 * @path full path to a file, directories separated by slash
 * @st file stat structure
 * @returns 0 iff operation completed succesfully -errno on error
*/
int vfat_resolve(const char *path, struct stat *st)
{
    /* TODO: Add your code here.
        You should tokenize the path (by slash separator) and then
        for each token search the directory for the file/dir with that name.
        You may find it useful to use following functions:
        - strtok to tokenize by slash. See manpage
        - vfat_readdir in conjuction with vfat_search_entry
    */
    int res = -ENOENT; // Not Found
    char *str = malloc(strlen(path) + 1);
    char *token;
    uint32_t first_cluster_curr_dir = vfat_info.root_inode.st_ino;
    // struct vfat_search_data search_data = {;

    strcpy(str, path);

    if (strcmp("/", path) == 0) {
        *st = vfat_info.root_inode;
        res = 0;
    } else {
        for (; ; str = NULL) {
            token = strtok(str, "/");
            if (!token)
                break;
            // vfat_readdir(first_cluster_curr_dir, vfat_search_entry, &call_back_data);
        }
    }
    
    free(str);

    return res;
}

// Get file attributes
int vfat_fuse_getattr(const char *path, struct stat *st)
{
    if (strncmp(path, DEBUGFS_PATH, strlen(DEBUGFS_PATH)) == 0) {
        // This is handled by debug virtual filesystem
        return debugfs_fuse_getattr(path + strlen(DEBUGFS_PATH), st);
    } else {
        // Normal file
        return vfat_resolve(path, st);
    }
}

// Extended attributes useful for debugging
int vfat_fuse_getxattr(const char *path, const char* name, char* buf, size_t size)
{
    struct stat st;
    int ret = vfat_resolve(path, &st);
    if (ret != 0) return ret;
    if (strcmp(name, "debug.cluster") != 0) return -ENODATA;

    if (buf == NULL) {
        ret = snprintf(NULL, 0, "%u", (unsigned int) st.st_ino);
        if (ret < 0) err(1, "WTF?");
        return ret + 1;
    } else {
        ret = snprintf(buf, size, "%u", (unsigned int) st.st_ino);
        if (ret >= size) return -ERANGE;
        return ret;
    }
}

int vfat_fuse_readdir(
        const char *path, void *callback_data,
        fuse_fill_dir_t callback, off_t unused_offs, struct fuse_file_info *unused_fi)
{
    if (strncmp(path, DEBUGFS_PATH, strlen(DEBUGFS_PATH)) == 0) {
        // This is handled by debug virtual filesystem
        return debugfs_fuse_readdir(path + strlen(DEBUGFS_PATH), callback_data, callback, unused_offs, unused_fi);
    }
    /* TODO: Add your code here. You should reuse vfat_readdir and vfat_resolve functions
    */
    return 0;
}

int vfat_fuse_read(
        const char *path, char *buf, size_t size, off_t offs,
        struct fuse_file_info *unused)
{
    if (strncmp(path, DEBUGFS_PATH, strlen(DEBUGFS_PATH)) == 0) {
        // This is handled by debug virtual filesystem
        return debugfs_fuse_read(path + strlen(DEBUGFS_PATH), buf, size, offs, unused);
    }
    /* TODO: Add your code here. Look at debugfs_fuse_read for example interaction.
    */
    return 0;
}

////////////// No need to modify anything below this point
int vfat_opt_args(void *data, const char *args, int key, struct fuse_args *oargs)
{
    if (key == FUSE_OPT_KEY_NONOPT && !vfat_info.dev) {
        vfat_info.dev = strdup(args);
        return (0);
    }
    return (1);
}

struct fuse_operations vfat_available_ops = {
    .getattr = vfat_fuse_getattr,
    .getxattr = vfat_fuse_getxattr,
    .readdir = vfat_fuse_readdir,
    .read = vfat_fuse_read,
};

int main(int argc, char **argv)
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    fuse_opt_parse(&args, NULL, NULL, vfat_opt_args);

    if (!vfat_info.dev)
        errx(1, "missing file system parameter");

    vfat_init(vfat_info.dev);
    // DEBUG
    // vfat_readdir(2, NULL, NULL);

    return (fuse_main(args.argc, args.argv, &vfat_available_ops, NULL));
}
