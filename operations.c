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
//==============================================================================
//==============================================================================
//==============================================================================
//==============================================================================
//==============================================================================

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


void process_commands(int source) {
    while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;
    if (source == STDIN_FILENO) {
        printf("> ");
        fflush(stdout);
    }
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

        if (kvs_read(num_pairs, keys)) {
          fprintf(stderr, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(source, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_delete(num_pairs, keys)) {
          fprintf(stderr, "Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:

        kvs_show();
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

        if (kvs_backup()) {
          fprintf(stderr, "Failed to perform backup.\n");
        }
        break;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:
        printf( 
            "Available commands:\n"
            "  WRITE [(key,value)(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n" // Not implemented
            "  HELP\n"
        );

        break;
        
      case CMD_EMPTY:
        break;

      case EOC:
        return;
    }
  }
}



char process_job_files(char *directory) {
    // Contar número de arquivos .job no diretório
    int num_files = count_job_files(directory);
    if (num_files <= 0) {
        fprintf(stderr, "No .job files found in directory: %s\n", directory);
        return 0;
    }

    // Criar array para armazenar os caminhos dos arquivos .job
    char job_files[num_files][MAX_JOB_FILE_NAME_SIZE];
    list_job_files(directory, job_files);

    // Processar cada arquivo .job
    for (int i = 0; i < num_files; i++) {
        char output_file[MAX_JOB_FILE_NAME_SIZE];
        strncpy(output_file, job_files[i], MAX_JOB_FILE_NAME_SIZE - 1);
        output_file[MAX_JOB_FILE_NAME_SIZE - 1] = '\0';

        // Substituir extensão .job por .out
        char *ext = strstr(output_file, ".job");
        if (ext != NULL) {
            strcpy(ext, ".out");
        }

        // Abrir o arquivo .job para leitura
        int input_fd = open(job_files[i], O_RDONLY);
        if (input_fd < 0) {
            fprintf(stderr, "Failed to open job file: %s\n", job_files[i]);
            continue;
        }

        // Abrir o arquivo .out para escrita
        int output_fd = open(output_file, O_WRONLY | O_CREAT | O_TRUNC, 0666);
        if (output_fd < 0) {
            fprintf(stderr, "Failed to create output file: %s\n", output_file);
            close(input_fd);
            continue;
        }

        // Salvar o stdout original
        int stdout_fd = dup(STDOUT_FILENO);
        if (stdout_fd < 0) {
            perror("Failed to save stdout");
            close(input_fd);
            close(output_fd);
            continue;
        }

        // Redirecionar stdout para o arquivo .out
        if (dup2(output_fd, STDOUT_FILENO) < 0) {
            perror("Failed to redirect stdout");
            close(input_fd);
            close(output_fd);
            close(stdout_fd);
            continue;
        }

        // Garantir que o buffer seja esvaziado após o redirecionamento
        fflush(stdout);

        // Processar os comandos no arquivo .job
        process_commands(input_fd);

        // Restaurar o stdout original
        fflush(stdout);
        if (dup2(stdout_fd, STDOUT_FILENO) < 0) {
            perror("Failed to restore stdout");
        }
        close(stdout_fd);

        // Fechar os descritores de arquivo
        close(input_fd);
        close(output_fd);
    }

    return 1;
}
