// Finder uses the catalog APIs, which require more metadata than POSIX stat.
// Run against a real mount: terminal-only checks missed this regression.
#include <CoreServices/CoreServices.h>
#include <sys/mount.h>
#include <stdio.h>
#include <string.h>

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: mounted-tests MOUNT [CHILD_PATH ...]\n");
        return 2;
    }
    struct statfs fs;
    if (statfs(argv[1], &fs) || strcmp(fs.f_fstypename, "gyit") != 0) {
        fprintf(stderr, "expected an active gyit mount\n");
        return 1;
    }
    for (int i = 1; i < argc; i++) {
        FSRef ref;
        Boolean directory;
        FSCatalogInfo info;
        OSStatus error = FSPathMakeRef((const UInt8 *)argv[i], &ref, &directory);
        if (!error) {
            error = FSGetCatalogInfo(&ref, kFSCatInfoGettableInfo, &info, NULL, NULL, NULL);
        }
        if (error) {
            fprintf(stderr, "Finder catalog lookup failed for %s: %d\n", argv[i], (int)error);
            return 1;
        }
    }
    puts("mounted Finder catalog checks passed");
    return 0;
}
