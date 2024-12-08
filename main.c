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
        // Corre quando nao sao dados argumentos
        process_terminal_commands();
    } else if (argc == 3) {
        // Corre caso sejam dados 2 argumentos

        //const char *directory_path = argv[1];
        int max_backups = atoi(argv[2]);
        if (max_backups <= 0) {
            fprintf(stderr, "Invalid value for <max_backups>: %d\n", max_backups);
            return 1;
        }

    }
}