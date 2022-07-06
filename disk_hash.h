#ifndef _FILE_H_
#define _FILE_H_

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <time.h>


#define BLOCK_SIZE         1 << 14
#define BLOCK_TOTAL_SIZE   sizeof(size_t)
#define NEXT_SIZE          sizeof(size_t)
#define TABLE_SIZE         10000019 * NEXT_SIZE

#define FNV_prime          16777619
#define offset_basis       2166136261

#define TYPE_INT           'I'
#define TYPE_CHAR          'C'
#define TYPE_BINARY        'B'


#define SUCCESS            0
#define ERR_BASE           0
#define ERR_DIR            ERR_BASE - 1
#define ERR_WRITE          ERR_BASE - 2
#define ERR_OPEN_FILE      ERR_BASE - 3
#define ERR_MY_MALLOC      ERR_BASE - 4
#define ERR_TABLE_SIZE     ERR_BASE - 5
#define ERR_READ_FILE      ERR_BASE - 6


#define NOT_FOUND          ERR_BASE - 100


typedef struct _DATA{
   size_t   total_size;
   char     *key;
   size_t   val_size;
   char     *val;
   char     type;
} _DATA;

typedef struct _files{
   FILE *table;
   FILE *data;
   void *table_buf;
} _files;


int open_table(_files *fp);
int close_table(_files *fp);
int add();
int del();
int find(const char *key, _files *fp, size_t *result, _DATA *result_data);
int reorganize();
int hash_func();

#endif