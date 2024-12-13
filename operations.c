#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <pthread.h>
#include "kvs.h"
#include "constants.h"
#include "parser.h"

static struct HashTable* kvs_table = NULL;
static int backup_count = 0;


typedef struct {
    char job_file[MAX_JOB_FILE_NAME_SIZE];
    int max_backups;
} thread_data_t;

pthread_mutex_t kvs_mutex = PTHREAD_MUTEX_INITIALIZER;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
    return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
    if (kvs_table != NULL) {
        fprintf(stderr, "KVS state has already been initialized\n");
        return 1;
    }

    kvs_table = create_hash_table();
    return kvs_table == NULL;
}

int kvs_terminate() {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }

    free_table(kvs_table);
    return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }

    for (size_t i = 0; i < num_pairs; i++) {
        if (write_pair(kvs_table, keys[i], values[i]) != 0) {
            fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
        }
    }

    return 0;
}

typedef struct {
    char key[MAX_STRING_SIZE];
    char value[MAX_STRING_SIZE];
} KeyValuePair;

int compare_key_value_pairs(const void* a, const void* b) {
    return strcmp(((KeyValuePair*)a)->key, ((KeyValuePair*)b)->key);
}

int kvs_read(int fd, size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }

    KeyValuePair pairs[num_pairs];
    size_t pair_count = 0;

    for (size_t i = 0; i < num_pairs; i++) {
        char* result = read_pair(kvs_table, keys[i]);
        if (result == NULL) {
            snprintf(pairs[pair_count].key, MAX_STRING_SIZE, "%s", keys[i]);
            snprintf(pairs[pair_count].value, MAX_STRING_SIZE, "KVSERROR");
        } else {
            snprintf(pairs[pair_count].key, MAX_STRING_SIZE, "%s", keys[i]);
            snprintf(pairs[pair_count].value, MAX_STRING_SIZE, "%s", result);
            free(result);
        }
        pair_count++;
    }

    // Sort pairs by key
    qsort(pairs, pair_count, sizeof(KeyValuePair), compare_key_value_pairs);

    write(fd, "[", 1);
    for (size_t i = 0; i < pair_count; i++) {
        char buffer[MAX_STRING_SIZE * 2 + 10];
        int len = snprintf(buffer, sizeof(buffer), "(%s,%s)", pairs[i].key, pairs[i].value);
        write(fd, buffer, (size_t)len);
    }
    write(fd, "]\n", 2);

    return 0;
}


int kvs_delete(int fd, size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }
    int aux = 0;

    for (size_t i = 0; i < num_pairs; i++) {
        if (delete_pair(kvs_table, keys[i]) != 0) {
            if (!aux) {
                write(fd, "[", 1);
                aux = 1;
            }
            char buffer[MAX_STRING_SIZE + 12];
            int len = snprintf(buffer, sizeof(buffer), "(%s,KVSMISSING)", keys[i]);
            write(fd, buffer, (size_t)len);
        }
    }
    if (aux) {
        write(fd, "]\n", 2);
    }

    return 0;
}


void kvs_show(int fd) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = kvs_table->table[i];
        while (keyNode != NULL) {
            char buffer[MAX_STRING_SIZE * 2 + 10];
            int len = snprintf(buffer, sizeof(buffer), "(%s, %s)\n", keyNode->key, keyNode->value);
            write(fd, buffer, (size_t)len);
            keyNode = keyNode->next;
        }
    }
}

int kvs_backup(const char *job_file, int max_backups) {
    if (backup_count >= max_backups) {
        // Esperar que um processo filho termine
        wait(NULL);
        backup_count--;
    }

    pid_t pid = fork();
    if (pid < 0) {
        perror("Failed to fork");
        return 1;
    } else if (pid == 0) {
        // Processo filho
        char backup_file[MAX_JOB_FILE_NAME_SIZE];
        strncpy(backup_file, job_file, MAX_JOB_FILE_NAME_SIZE - 1);
        backup_file[MAX_JOB_FILE_NAME_SIZE - 1] = '\0';

        char *ext = strstr(backup_file, ".job");
        if (ext != NULL) {
            snprintf(ext, (size_t)(MAX_JOB_FILE_NAME_SIZE - (ext - backup_file)), "-%d.bck", backup_count + 1);
        } else {
            snprintf(backup_file, MAX_JOB_FILE_NAME_SIZE, "%s-%d.bck", job_file, backup_count + 1);
        }

        int fd = open(backup_file, O_WRONLY | O_CREAT | O_TRUNC, 0666);
        if (fd < 0) {
            perror("Failed to create backup file");
            _exit(1);
        }

        for (int i = 0; i < TABLE_SIZE; i++) {
            KeyNode *keyNode = kvs_table->table[i];
            while (keyNode != NULL) {
                char buffer[MAX_STRING_SIZE * 2 + 10];
                int len = snprintf(buffer, sizeof(buffer), "(%s, %s)\n", keyNode->key, keyNode->value);
                if (len < 0) {
                    perror("Failed to format string");
                    close(fd);
                    _exit(1);
                }
                write(fd, buffer, (size_t)len);
                keyNode = keyNode->next;
            }
        }

        close(fd);
        _exit(0);
    } else {
        // Processo pai
        backup_count++;
    }

    return 0;
}

void kvs_wait(unsigned int delay_ms) {
    struct timespec delay = delay_to_timespec(delay_ms);
    nanosleep(&delay, NULL);
}

size_t list_job_files(const char *dir_path, char files[][MAX_JOB_FILE_NAME_SIZE]) {
    DIR *dir = opendir(dir_path);
    if (!dir) {
        perror("Failed to open directory");
        return 0;
    }

    struct dirent *entry;
    size_t count = 0;

    // Determinar se é necessário adicionar uma barra ao final
    int add_slash = (dir_path[strlen(dir_path) - 1] != '/');

    while ((entry = readdir(dir)) != NULL) {
        if (strstr(entry->d_name, ".job") != NULL) { // Verifica extensão
            // Adicionar "/" ao caminho se necessário
            if (add_slash) {
                if (snprintf(files[count], MAX_JOB_FILE_NAME_SIZE, "%s/%s", dir_path, entry->d_name) >= MAX_JOB_FILE_NAME_SIZE) {
                    fprintf(stderr, "Filename too long, skipping: %s/%s\n", dir_path, entry->d_name);
                    continue;
                }
            } else {
                if (snprintf(files[count], MAX_JOB_FILE_NAME_SIZE, "%s%s", dir_path, entry->d_name) >= MAX_JOB_FILE_NAME_SIZE) {
                    fprintf(stderr, "Filename too long, skipping: %s%s\n", dir_path, entry->d_name);
                    continue;
                }
            }
            count++;
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

void process_commands(int source, int output_fd, const char *job_file, int max_backups) {
    while (1) {
        char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        unsigned int delay;
        size_t num_pairs;
        const char *help_msg =
                    "Available commands:\n"
                    "  WRITE [(key,value)(key2,value2),...]\n"
                    "  READ [key,key2,...]\n"
                    "  DELETE [key,key2,...]\n"
                    "  SHOW\n"
                    "  WAIT <delay_ms>\n"
                    "  BACKUP\n"
                    "  HELP\n";
        switch (get_next(source)) {
            case CMD_WRITE:
                num_pairs = parse_write(source, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (kvs_write(num_pairs, keys, values)) {
                    fprintf(stderr, "Failed to write pair\n");
                }
                break;
            case CMD_READ:
                num_pairs = parse_read_delete(source, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (kvs_read(output_fd, num_pairs, keys)) {
                    fprintf(stderr, "Failed to read pair\n");
                }
                break;
            case CMD_DELETE:
                num_pairs = parse_read_delete(source, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (kvs_delete(output_fd, num_pairs, keys)) {
                    fprintf(stderr, "Failed to delete pair\n");
                }
                break;
            case CMD_SHOW:
                kvs_show(output_fd);
                break;
            case CMD_WAIT:
                if (parse_wait(source, &delay, NULL) == -1) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (delay > 0) {
                    printf("Waiting...\n");
                    kvs_wait(delay);
                }
                break;
            case CMD_BACKUP:
                if (kvs_backup(job_file, max_backups)) {
                    fprintf(stderr, "Failed to perform backup.\n");
                }
                break;
            case CMD_INVALID:
                fprintf(stderr, "Invalid command. See HELP for usage\n");
                break;
            case CMD_HELP:
                write(output_fd, help_msg, strlen(help_msg));
                break;
            case CMD_EMPTY:
                break;
            case EOC:
                return;
        }
    }
}

void* process_job_file_thread(void* arg) {
    thread_data_t* data = (thread_data_t*)arg;
    char output_file[MAX_JOB_FILE_NAME_SIZE];
    strncpy(output_file, data->job_file, MAX_JOB_FILE_NAME_SIZE - 1);
    output_file[MAX_JOB_FILE_NAME_SIZE - 1] = '\0';
    char *ext = strstr(output_file, ".job");
    if (ext != NULL) {
        strcpy(ext, ".out");
    }

    int input_fd = open(data->job_file, O_RDONLY);
    if (input_fd < 0) {
        fprintf(stderr, "Failed to open job file: %s\n", data->job_file);
        pthread_exit(NULL);
    }

    int output_fd = open(output_file, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (output_fd < 0) {
        fprintf(stderr, "Failed to create output file: %s\n", output_file);
        close(input_fd);
        pthread_exit(NULL);
    }

    process_commands(input_fd, output_fd, data->job_file, data->max_backups);

    close(input_fd);
    close(output_fd);

    pthread_exit(NULL);
}

char process_job_files(char *directory, int max_backups, int max_threads) {
    int num_files = count_job_files(directory);
    if (num_files <= 0) {
        fprintf(stderr, "No .job files found in directory: %s\n", directory);
        return 0;
    }

    char job_files[num_files][MAX_JOB_FILE_NAME_SIZE];
    list_job_files(directory, job_files);

    pthread_t threads[max_threads];
    thread_data_t thread_data[max_threads];
    int thread_count = 0;

    for (int i = 0; i < num_files; i++) {
        strncpy(thread_data[thread_count].job_file, job_files[i], MAX_JOB_FILE_NAME_SIZE - 1);
        thread_data[thread_count].job_file[MAX_JOB_FILE_NAME_SIZE - 1] = '\0';
        thread_data[thread_count].max_backups = max_backups;

        if (pthread_create(&threads[thread_count], NULL, process_job_file_thread, &thread_data[thread_count]) != 0) {
            perror("Failed to create thread");
            return 0;
        }

        thread_count++;
        if (thread_count == max_threads) {
            for (int j = 0; j < thread_count; j++) {
                pthread_join(threads[j], NULL);
            }
            thread_count = 0;
        }
    }

    for (int j = 0; j < thread_count; j++) {
        pthread_join(threads[j], NULL);
    }

    return 1;
}
