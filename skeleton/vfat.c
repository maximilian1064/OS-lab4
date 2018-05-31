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
/* checksum calculation */
uint8_t checksum(char *short_name)
{
    int i;
    uint8_t sum = 0;
    for (i = 11; i != 0; i--) {
        sum = ((sum & 1) ? 0x80 : 0) + (sum >> 1) + (uint8_t)(*short_name++);
    }

    return sum;
}

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
    int filler_return_one = 0;

    struct fat32_direntry_long *dir_entry_long_set;
    uint8_t curr_ord;
    int possible_long_name = 0;
    uint8_t dir_entry_long_set_size;


    for (cluster = first_cluster; cluster < SMALLEST_EOC_MARK; cluster = vfat_next_cluster(cluster)) {

        // bring in entire cluster
        cluster_buf = mmap_file(vfat_info.fd, vfat_info.cluster_begin_offset + (cluster - 2) * vfat_info.cluster_size,
                    vfat_info.cluster_size);

        // collect valid entries
        struct fat32_direntry *dir_entry;
        struct fat32_direntry_long *dir_entry_long;

        for (dir_entry = cluster_buf; dir_entry < cluster_buf + vfat_info.direntry_per_cluster; dir_entry++) {
            // get a pointer of long entry type
            dir_entry_long = dir_entry;

            // entry is "free" or "last" entry ?
            // free the long entry set buffer if preceded by long entries
            // they are orphans in these cases
            if (dir_entry_long->seq == 0) {
                if (possible_long_name) {
                    possible_long_name = 0;
                    free(dir_entry_long_set);
                }
                last_entry_found = 1;
                break;
            } else if (dir_entry_long->seq == 0xE5) {
                if (possible_long_name) {
                    possible_long_name = 0;
                    free(dir_entry_long_set);
                }
                continue;
            }

            // long filename entry? 
            if (dir_entry->attr == VFAT_ATTR_LFN) {
                // first long entry in set?
                if ((dir_entry_long->seq & VFAT_LFN_SEQ_START) == VFAT_LFN_SEQ_START) {
                    curr_ord = dir_entry_long->seq & VFAT_LFN_SEQ_MASK;
                    possible_long_name = 1;
                    dir_entry_long_set_size = curr_ord;
                    dir_entry_long_set = malloc(sizeof(struct fat32_direntry_long) * curr_ord);
                    dir_entry_long_set[curr_ord - 1] = *dir_entry_long;
                // normal long entry in set
                } else if (possible_long_name) {
                    curr_ord--;
                    // non-contiguous ord number, ORPHAN
                    if ((curr_ord == 0) || (dir_entry_long->seq != curr_ord)) {
                        possible_long_name = 0;
                        free(dir_entry_long_set);
                        // continue;
                    } else {
                        dir_entry_long_set[curr_ord - 1] = *dir_entry_long;
                    }
                // standalone long entry, ORPHAN
                } else {
                    // continue;
                }
            // invalid entry?
            // free the long entry set buffer if preceded by long entries
            // they are orphans in these cases
            } else if (dir_entry->attr & VFAT_ATTR_INVAL) {
                if (possible_long_name) {
                    possible_long_name = 0;
                    free(dir_entry_long_set);
                }
                // continue;
            // short name entry
            } else {
                char *filename;

                // get long name into filename if entries valid
                if (possible_long_name) {
                    // get checksum of short entry
                    // TODO: change all these shitty mallocs to static array!!!
                    uint8_t checksum_short = checksum(dir_entry->nameext);
                    uint8_t curr_entry;
                    char *curr_name_piece = malloc(26);
                    char *curr_name_piece_utf8 = malloc(13);
                    char *curr_name_piece_tmp;
                    char *curr_name_piece_utf8_tmp;
                    filename = malloc((dir_entry_long_set_size * 13) + 1);
                    filename[dir_entry_long_set_size * 13] = '\0';
                    size_t utf16len, utf8len;

                    for (curr_entry = 0; curr_entry < dir_entry_long_set_size; curr_entry++) {
                        // check checksum
                        if (checksum_short != dir_entry_long_set[curr_entry].csum) {
                            possible_long_name = 0;
                            break;
                        } else {
                            // append the name piece in current long entry to filename
                            utf16len = 26;
                            utf8len = 13;
                            curr_name_piece_tmp = curr_name_piece;
                            curr_name_piece_utf8_tmp = curr_name_piece_utf8;
                            memcpy(curr_name_piece, dir_entry_long_set[curr_entry].name1, 10);
                            memcpy(curr_name_piece + 10, dir_entry_long_set[curr_entry].name2, 12);
                            memcpy(curr_name_piece + 22, dir_entry_long_set[curr_entry].name3, 4);
                            iconv(iconv_utf16, &curr_name_piece_tmp, &utf16len,
                                    &curr_name_piece_utf8_tmp, &utf8len);
                            memcpy(filename + curr_entry * 13, curr_name_piece_utf8, 13);
                        }
                    }

                    free(curr_name_piece);
                    free(curr_name_piece_utf8);

                    // DEBUG
                    // printf("%s\n", filename);
                }

                // get short file name if long name entries are not valid
                if (!possible_long_name) {

                } else {
                    possible_long_name = 0;
                    free(dir_entry_long_set);
                }
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
                // use first cluster number as st_ino
                st.st_ino = ((uint32_t)dir_entry->cluster_hi << 16) | (uint32_t)dir_entry->cluster_lo;
                // directory is sized by following its cluster chain to EOC mark 
                // TODO: why ls total still zero
                if (dir_entry->attr & VFAT_ATTR_DIR) {
                    size_t num_cluster = 0;
                    uint32_t curr_cluster = st.st_ino;
                    for (; curr_cluster < SMALLEST_EOC_MARK; curr_cluster = vfat_next_cluster(curr_cluster))
                        num_cluster++;
                    st.st_size = num_cluster * vfat_info.cluster_size;
                } else {
                    st.st_size = dir_entry->size;
                }
                // we are doing read-only system
                st.st_mode = 0555 | ((dir_entry->attr & VFAT_ATTR_DIR) ? S_IFDIR : S_IFREG);

                // file name
                filename = malloc(13);
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
                // printf("ino: %d\n", st.st_ino);
                // printf("size: %d\n", st.st_size);
                // printf("DIR?: %d\n", st.st_mode & S_IFDIR);


                // make a copy of st
                // struct stat *st_copy = malloc(sizeof(struct stat)); 
                // memcpy(st_copy, &st, sizeof(struct stat));

                // send filename and st to filler 
                if (callback(callbackdata, filename, &st, 0) == 1) {
                    filler_return_one = 1;
                    break;
                }
            }

        }

        unmap(cluster_buf, vfat_info.cluster_size); 

        // finish operation once last entry found or filler returns 1 
        if (last_entry_found)
            break;
        if (filler_return_one)
            return 1;
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
    struct vfat_search_data search_data = {NULL, 0, st};

    strcpy(str, path);

    if (strcmp("/", path) == 0) {
        *st = vfat_info.root_inode;
        res = 0;
    } else {
        for (; ; str = NULL) {
            // get token in current layer of path
            // TODO: rm token
            // TODO: use reentrant strtok?
            token = strtok(str, "/");
            if (!token)
                break;

            // search token in current directory
            search_data.name = token;
            search_data.found = 0;
            vfat_readdir(first_cluster_curr_dir, vfat_search_entry, &search_data);
            if (search_data.found == 0) {
                // exit if token not found
                break;
            } else {
                // update current directory
                first_cluster_curr_dir = search_data.st->st_ino;
            }

            // DEBUG
            // printf("%s\n", token);
        }
        if (search_data.found == 1)
            res = 0;
    }
    
    free(str);

    return res;
}

// Get file attributes
int vfat_fuse_getattr(const char *path, struct stat *st)
{
    // DEBUG
    // printf("getattr: %s\n", path);

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
    // DEBUG
    // printf("getxattr, %s\n", path);

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
    // DEBUG
    // printf("readdir, %s\n", path);

    if (strncmp(path, DEBUGFS_PATH, strlen(DEBUGFS_PATH)) == 0) {
        // This is handled by debug virtual filesystem
        return debugfs_fuse_readdir(path + strlen(DEBUGFS_PATH), callback_data, callback, unused_offs, unused_fi);
    }
    /* TODO: Add your code here. You should reuse vfat_readdir and vfat_resolve functions
    */
    struct stat *st = malloc(sizeof(struct stat));
    int res = vfat_resolve(path, st);
    // clever unix tool like "ls" will check the file stat before calling readdir
    // but still check here for safety
    if (res) {
        free(st);
        return res;
    }
    if (st->st_mode & S_IFREG) {
        free(st);
        return -ENOTDIR;
    }
    if (vfat_readdir(st->st_ino, callback, callback_data)) {
        free(st);
        // DEBUG
        // printf("-1\n");
        return -1;
    }
    free(st);
    // DEBUG
    // if (strcmp(path, "/") == 0) {
    //     return vfat_readdir(vfat_info.root_inode.st_ino, callback, callback_data);
    // }
    return 0;
}

int vfat_fuse_read(
        const char *path, char *buf, size_t size, off_t offs,
        struct fuse_file_info *unused)
{
    // DEBUG
    // printf("read, %s\n", path);

    if (strncmp(path, DEBUGFS_PATH, strlen(DEBUGFS_PATH)) == 0) {
        // This is handled by debug virtual filesystem
        return debugfs_fuse_read(path + strlen(DEBUGFS_PATH), buf, size, offs, unused);
    }
    /* TODO: Add your code here. Look at debugfs_fuse_read for example interaction.
    */
    // resolve path
    struct stat *st = malloc(sizeof(struct stat));
    int res = vfat_resolve(path, st);
    if (res) {
        free(st);
        return res;
    }
    if (st->st_mode & S_IFDIR) {
        free(st);
        return -EISDIR;
    }

    // actual read 
    off_t eof = st->st_size;
    size_t len;
    off_t first_cluster, last_cluster;
    // get actual length of bytes to be read
    if (offs >= eof) {
        free(st);
        return 0;
    } else if (offs + size > eof) {
        len = eof - offs;
    } else {
        len = size;
    }
    // relevant clusters
    first_cluster = offs / vfat_info.cluster_size;
    last_cluster = (offs + len - 1) / vfat_info.cluster_size;

    // put relevant clusters into a temporay buffer
    char *tmp_buf = malloc(vfat_info.cluster_size * (last_cluster - first_cluster + 1));
    char *cluster_buf;
    off_t curr_cluster = 0;
    uint32_t curr_cluster_num = st->st_ino;
    for (; curr_cluster < first_cluster; curr_cluster_num = vfat_next_cluster(curr_cluster_num), curr_cluster++)
        ;
    for (; curr_cluster <= last_cluster; curr_cluster_num = vfat_next_cluster(curr_cluster_num), curr_cluster++) {
        cluster_buf = mmap_file(vfat_info.fd, vfat_info.cluster_begin_offset + (curr_cluster_num - 2) * vfat_info.cluster_size,
                    vfat_info.cluster_size);
        memcpy(tmp_buf + (curr_cluster - first_cluster) * vfat_info.cluster_size, cluster_buf, 
                vfat_info.cluster_size);
        unmap(cluster_buf, vfat_info.cluster_size);
    }

    // copy relevant bytes to buf
    memcpy(buf, tmp_buf + offs - (first_cluster * vfat_info.cluster_size), len);

    free(tmp_buf);

    return len;
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
