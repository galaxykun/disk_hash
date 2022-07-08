#ifndef _FILE_H_
#define _FILE_H_

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>


#define BLOCK_SIZE         1 << 14
#define BLOCK_TOTAL_SIZE   sizeof(size_t)
#define NEXT_SIZE          sizeof(size_t)
#define TABLE_SIZE         10000019 * NEXT_SIZE

#define FNV_prime          16777619
#define offset_basis       2166136261

#define TYPE_INT           'I'
#define TYPE_CHAR          'C'
#define TYPE_BINARY        'B'

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


#define NOT_FOUND          -100
// #define NOT_FOUND_TABLE    NOT_FOUND - 1
// #define NOT_FOUND_DATA     NOT_FOUND - 2


typedef struct _DATA{
   size_t   total_size;
   char     key[KEY_MAX];
   size_t   val_size;
   char     val[VAL_MAX];
   char     type;
   int      table_ptr;
   size_t   data_ptr;
   size_t   block_ptr;
   size_t   pre_block_ptr;
   //int      hash_num;
} _DATA;

typedef struct _files{
   FILE     *table;
   FILE     *data;
   size_t   *table_buf;
} _files;


int open_table(_files *fp);
int close_table(_files *fp);
int add(const char *key, const void *val, const size_t val_size, const char type, _files *fp, _DATA *result_data);
int del(const char *key, _files *fp, _DATA *result_data);
int find(const char *key, _files *fp, _DATA *result_data);
int hash_func (const char* key);
int reorganize();
//int inquire();

#endif