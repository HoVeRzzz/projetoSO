#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "constants.h"
#include "parser.h"
#include "operations.h"

int main(int argc, char *argv[]) {
    if (kvs_init()) {
        fprintf(stderr, "Failed to initialize KVS\n");
        return 1;
    }

    if (argc == 1) {
        process_commands(STDIN_FILENO, NULL, 0);
    } else if (argc == 3) {
        char *directory_path = argv[1];
        int max_backups = atoi(argv[2]);
        if (max_backups <= 0) {
            fprintf(stderr, "Invalid value for <max_backups>: %d\n", max_backups);
            return 1;
        }
        if (process_job_files(directory_path, max_backups, 1) == 0) return 1;
    } else if (argc == 4) {
        char *directory_path = argv[1];
        int max_backups = atoi(argv[2]);
        int max_threads = atoi(argv[3]);
        if (max_backups <= 0 || max_threads <= 0) {
            fprintf(stderr, "Invalid value for <max_backups> or <max_threads>\n");
            return 1;
        }
        if (process_job_files(directory_path, max_backups, max_threads) == 0) return 1;
    } else {
        fprintf(stderr, "Usage: %s [directory_path max_backups [max_threads]]\n", argv[0]);
        return 1;
    }

    kvs_terminate();
    return 0;
}