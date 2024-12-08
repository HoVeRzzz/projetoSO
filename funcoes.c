#include "funcoes.h"
#include "constants.h"
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <stdio.h>

#define MAX_FILES 256

size_t list_job_files(const char *dir_path, char files[][MAX_JOB_FILE_NAME_SIZE]) {
    DIR *dir = opendir(dir_path);
    if (!dir) {
        perror("Failed to open directory");
        return 0;
    }

    struct dirent *entry;
    size_t count = 0;
    while ((entry = readdir(dir)) != NULL) {
        if (strstr(entry->d_name, ".job") != NULL) { // Verifica extensão
            snprintf(files[count++], MAX_JOB_FILE_NAME_SIZE, "%s/%s", dir_path, entry->d_name);
        }
    }
    closedir(dir);
    return count;
}

int count_job_files(const char *dir_path) {
    DIR *dir = opendir(dir_path);
    if (!dir) {
        perror("Failed to open directory");
        return 0;
    }
    struct dirent *entry;
    int count = 0;
    while ((entry = readdir(dir)) != NULL) {
        if (strstr(entry->d_name, ".job") != NULL) count++;
    }
    closedir(dir);
    return count;
}

char process_job_files(char *directory) {
    int num_files = count_job_files(directory);
    char job_files[num_files][MAX_JOB_FILE_NAME_SIZE];
    int num_files = list_job_files(directory, job_files);
    DIR *dir = opendir(directory);
    if (!dir) {
        perror("Failed to open directory");
        return 0;
    }
    for (int i = 0; i < num_files; i++) {
        char output_file[MAX_JOB_FILE_NAME_SIZE];
        // Substituir extensão .job por .out
        strncpy(output_file, job_files[i], MAX_JOB_FILE_NAME_SIZE - 1);
        output_file[MAX_JOB_FILE_NAME_SIZE - 1] = '\0';
        char *ext = strstr(output_file, ".job");
        strcpy(ext, ".out");
        int input_fd = open(job_files[i], O_RDONLY);
        if (input_fd < 0) {
            perror("Failed to open job file");
            continue;
        }

        // Abertura do ficheiro de saída
        int output_fd = open(output_file, O_WRONLY | O_CREAT | O_TRUNC, 0666);
        if (output_fd < 0) {
            perror("Failed to create output file");
            close(input_fd);
            continue;
        }
    }
}
