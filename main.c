#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "constants.h"
#include "parser.h"
#include "operations.h"

int g_max_backups = 0;

int main(int argc, char *argv[]) {
    if (kvs_init()) {
        fprintf(stderr, "Failed to initialize KVS\n");
        return 1;
    }

    if (argc == 1) {
        process_commands(STDIN_FILENO, NULL);
    } else if (argc == 3) {
        char *directory_path = argv[1];
        g_max_backups = atoi(argv[2]);
        if (g_max_backups <= 0) {
            fprintf(stderr, "Invalid value for <max_backups>: %d\n", g_max_backups);
            return 1;
        }
        if (process_job_files(directory_path) == 0) return 1;
    }

    kvs_terminate();
    return 0;
}