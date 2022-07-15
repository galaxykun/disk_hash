#ifndef _DISk_HASH_H_
#define _DISk_HASH_H_

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>


#define BLOCK_SIZE         1 << 13
#define BLOCK_TOTAL_SIZE   sizeof(long)
#define NEXT_SIZE          sizeof(long)
#define TABLE_NUMBER       10000019
#define TABLE_SIZE         TABLE_NUMBER * NEXT_SIZE

#define FNV_prime          16777619
#define offset_basis       2166136261

#define TYPE_INT           'I'
#define TYPE_CHAR          'C'
#define TYPE_BINARY        'B'
#define DIR_NAME           "data"
#define TABLE_FILE_NAME    "data/table"
#define DATA_FILE_NAME     "data/data"
#define NEWDATA_FILE_NAME  "data/data_temp"

#define KEY_MAX            256
#define VAL_MAX            1 << 12

#define SUCCESS            0
#define ERR_BASE           0
#define ERR_DIR            ERR_BASE - 1
#define ERR_WRITE          ERR_BASE - 2
#define ERR_OPEN_FILE      ERR_BASE - 3
#define ERR_MY_MALLOC      ERR_BASE - 4
#define ERR_TABLE_SIZE     ERR_BASE - 5
#define ERR_READ_FILE      ERR_BASE - 6
#define ERR_PARAMETER      ERR_BASE - 7
#define ERR_RENAME         ERR_BASE - 8
#define ERR_CLOSE          ERR_BASE - 9

#define NOT_FOUND          ERR_BASE - 100


typedef struct _DATA{
   size_t   total_size;
   char     key[KEY_MAX];
   size_t   val_size;
   char     val[VAL_MAX];
   char     type;
} _DATA;

typedef struct _BLOCK{
   int      table_offset;
   long     data_offset;
   long     block_offset;
   long     pre_block_offset;
} _BLOCK;

typedef struct _BUFFER{
   FILE     *table;
   FILE     *data;
   long     *table_buf;
   void     *block_buf;
} _BUFFER;

typedef struct _DISK_HASH{
   _BLOCK  block;
   _BUFFER buf;
} _DISK_HASH;


int open_table(_DISK_HASH *dh);
int close_table(_DISK_HASH *dh);
int add(const char *key, const void *val, const size_t val_size, const char type, _DISK_HASH *dh);
int del(const char *key, _DISK_HASH *dh);
int find(const char *key, _DISK_HASH *dh, _DATA *result);
int search_data(const char *key, _DISK_HASH *dh);
int search_enough_space_block(int space, _DISK_HASH *dh);
int add_new_block_to_data(_DISK_HASH *dh);
int copy_to_data(void *ptr, _DATA *result);
int copy_to_buffer( void *ptr, size_t total_size, const char *key, const void *val, const size_t val_size, const char type);
int hash_func (const char* key);
int reorganize(_DISK_HASH *dh);
int reorganize_data(_DISK_HASH *dh, FILE *new_data, void *data_block_buf, long *new_data_ptr, int i);
int write_to_new_data(void *buffer, long next, FILE *new_data);

#endif
