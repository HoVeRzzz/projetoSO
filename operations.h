#ifndef KVS_OPERATIONS_H
#define KVS_OPERATIONS_H

#include <stddef.h>

/// Initializes the KVS state.
/// @return 0 if the KVS state was initialized successfully, 1 otherwise.
int kvs_init();

/// Destroys the KVS state.
/// @return 0 if the KVS state was terminated successfully, 1 otherwise.
int kvs_terminate();

/// Writes a key value pair to the KVS. If key already exists it is updated.
/// @param num_pairs Number of pairs being written.
/// @param keys Array of keys' strings.
/// @param values Array of values' strings.
/// @return 0 if the pairs were written successfully, 1 otherwise.
int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]);

/// Reads values from the KVS.
/// @param fd File descriptor for the output
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @return 0 if the pairs were read successfully, 1 otherwise.
int kvs_read(int fd, size_t num_pairs, char keys[][MAX_STRING_SIZE]);
/// Deletes key value pairs from the KVS.
/// @param fd File descriptor for the output
/// @param num_pairs Number of pairs to delete.
/// @param keys Array of keys' strings.
/// @return 0 if the pairs were deleted successfully, 1 otherwise.
int kvs_delete(int fd, size_t num_pairs, char keys[][MAX_STRING_SIZE]);

/// Writes the state of the KVS.
/// @param fd File descriptor to write the output.
void kvs_show(int fd);

/// Creates a backup of the KVS state and stores it in the correspondent
/// backup file
/// @param job_file The job file name.
/// @param max_backups The maximum number of backups allowed.
/// @return 0 if the backup was successful, 1 otherwise.
int kvs_backup(const char *job_file, int max_backups);

/// Waits for a given amount of time.
/// @param delay_ms Delay in milliseconds.
void kvs_wait(unsigned int delay_ms);

/// Lists job files in a directory.
/// @param dir_path Path to the directory.
/// @param files Array to store the file names.
/// @return Number of job files found.
size_t list_job_files(const char *dir_path, char files[][MAX_JOB_FILE_NAME_SIZE]);

/// Counts the number of job files in a directory.
/// @param dir_path Path to the directory.
/// @return Number of job files found.
int count_job_files(const char *dir_path);

/// Processes job files in a directory.
/// @param directory Path to the directory.
/// @param max_backups Maximum number of backups allowed.
/// @param max_threads Maximum number of threads to use.
/// @return 1 if the job files were processed successfully, 0 otherwise.
char process_job_files(char *directory, int max_backups, int max_threads);

/// Processes commands from a job file.
/// @param source File descriptor for the input.
/// @param job_file Name of the job file.
/// @param max_backups Maximum number of backups allowed.
void process_commands(int source, const char *job_file, int max_backups);

#endif  // KVS_OPERATIONS_H