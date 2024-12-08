#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>

#include "kvs.h"
#include "constants.h"
#include "parser.h"

static struct HashTable* kvs_table = NULL;


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

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  printf("[");
  for (size_t i = 0; i < num_pairs; i++) {
    char* result = read_pair(kvs_table, keys[i]);
    if (result == NULL) {
      printf("(%s,KVSERROR)", keys[i]);
    } else {
      printf("(%s,%s)", keys[i], result);
    }
    free(result);
  }
  printf("]\n");
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  int aux = 0;

  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        printf("[");
        aux = 1;
      }
      printf("(%s,KVSMISSING)", keys[i]);
    }
  }
  if (aux) {
    printf("]\n");
  }

  return 0;
}

void kvs_show() {
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      printf("(%s, %s)\n", keyNode->key, keyNode->value);
      keyNode = keyNode->next; // Move to the next node
    }
  }
}

int kvs_backup() {
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}


//==============================================================================

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
            if (snprintf(files[count], MAX_JOB_FILE_NAME_SIZE, "%s/%s", dir_path, entry->d_name) >= MAX_JOB_FILE_NAME_SIZE) {
                fprintf(stderr, "Filename too long, skipping: %s/%s\n", dir_path, entry->d_name);
                continue;
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

char process_job_files(char *directory) {
    int num_files = count_job_files(directory);
    char job_files[num_files][MAX_JOB_FILE_NAME_SIZE];
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
        if (ext != NULL) {
            strcpy(ext, ".out");
        }
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
        // Add code to process the files here
        close(input_fd);
        close(output_fd);
    }
    closedir(dir);
    return 1;
}


void process_terminal_commands() {
    while (1) {
        printf("> ");
        fflush(stdout);

        switch (get_next(STDIN_FILENO)) {
            case CMD_WRITE: {
                char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                size_t num_pairs = parse_write(STDIN_FILENO, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs > 0) {
                    kvs_write(num_pairs, keys, values);
                } else {
                    fprintf(stderr, "Invalid WRITE command\n");
                }
                break;
            }

            case CMD_READ: {
                char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                size_t num_pairs = parse_read_delete(STDIN_FILENO, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs > 0) {
                    kvs_read(num_pairs, keys);
                } else {
                    fprintf(stderr, "Invalid READ command\n");
                }
                break;
            }

            case CMD_DELETE: {
                char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                size_t num_pairs = parse_read_delete(STDIN_FILENO, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs > 0) {
                    kvs_delete(num_pairs, keys);
                } else {
                    fprintf(stderr, "Invalid DELETE command\n");
                }
                break;
            }

            case CMD_SHOW:
                kvs_show();
                break;

            case CMD_WAIT: {
                unsigned int delay;
                if (parse_wait(STDIN_FILENO, &delay, NULL) == 0) {
                    kvs_wait(delay);
                } else {
                    fprintf(stderr, "Invalid WAIT command\n");
                }
                break;
            }

            case CMD_BACKUP:
                fprintf(stderr, "BACKUP not implemented yet\n");
                break;

            case CMD_HELP:
                printf(
                    "Available commands:\n"
                    "  WRITE [(key,value)(key2,value2),...]\n"
                    "  READ [key,key2,...]\n"
                    "  DELETE [key,key2,...]\n"
                    "  SHOW\n"
                    "  WAIT <delay_ms>\n"
                    "  BACKUP\n"
                    "  HELP\n"
                );
                break;

            case CMD_EMPTY:
                break;

            case CMD_INVALID:
                fprintf(stderr, "Invalid command\n");
                break;

            case EOC:
                return;
        }
    }
}
