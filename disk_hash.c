#include "disk_hash.h"
#include "my_malloc.c"

#ifdef _PRINTF_TIME
   clock_t start, stop;
#endif


int main(){
   #ifdef _PRINTF_TIME
      start = clock();
   #endif
   #ifdef _PRINTF_TIME
      stop = clock();
      printf("first time : %f\n", ((double)(stop - start)/CLOCKS_PER_SEC));
   #endif

   _files fp;

   #ifdef _PRINTF_TIME
      start = clock();
   #endif

   open_table(&fp);

   #ifdef _PRINTF_TIME
      stop = clock();
      printf("open_table time : %f\n", ((double)(stop - start)/CLOCKS_PER_SEC));
   #endif

   /* _DATA data;
   data.key = my_malloc(10);
   data.key = "asdqwe";
   size_t res = 0;
   int test = -1;
   test = find(data.key, &fp, &res, &data); */

   #ifdef _DEBUG
      //printf("find return : %d\n", test);
   #endif

   close_table(&fp);


   return 0;
}

int open_table(_files *fp){
   struct stat info;

   //dir exist?
   if(stat("data", &info)){
      if(errno == ENOENT){
         umask(002);
         if(mkdir("data", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH)){
            #ifdef _DEBUG
            printf("%s\n", strerror(errno));
            #endif

            return errno;
         }
      }
      else{
         #ifdef _DEBUG
         printf("%s\n", strerror(errno));
         #endif

         return errno;
      }
   }
   //is dir?
   else if(!S_ISDIR(info.st_mode)){
      #ifdef _DEBUG
      printf("Not dir!\n");
      #endif

      return ERR_DIR;
   }

   //file exist?
   if(stat("data/table", &info)){
      if(errno == ENOENT){
         #ifdef _DEBUG
         printf("Not table!\n");
         #endif

         fp->table = fopen("data/table", "w+");
         if(!fp->table){
            #ifdef _DEBUG
            printf("ERROR open file!\n");
            #endif

            return ERR_OPEN_FILE;
         }

         fp->table_buf = (size_t*)my_malloc(TABLE_SIZE);
         if(!fp->table_buf){
             #ifdef _DEBUG
               printf("ERROR my_malloc!\n");
            #endif

            close_table(fp);
            return ERR_MY_MALLOC;
         }

         #ifdef _PRINTF_TIME
            start = clock();
         #endif

         size_t *ptr = fp->table_buf;
         for(; ptr <= fp->table_buf + (TABLE_SIZE / NEXT_SIZE); ptr++){
            //memset(ptr, '-', NEXT_SIZE);
            *ptr = -1;
            //ptr += NEXT_SIZE;
            //*(char*)(ptr - 1) = '\n';
         }

         #ifdef _DEBUG
            *(size_t*)fp->table_buf = 12345678;
         #endif

         #ifdef _PRINTF_TIME
            stop = clock();
            printf("for time : %f\n", ((double)(stop - start)/CLOCKS_PER_SEC));
         #endif

         #ifdef _PRINTF_TIME
            start = clock();
         #endif

         if(TABLE_SIZE != fwrite(fp->table_buf, 1, TABLE_SIZE, fp->table)){
            #ifdef _DEBUG
            printf("ERROR WRITE!\n");
            #endif

            close_table(fp);
            return ERR_WRITE;
         }

         #ifdef _PRINTF_TIME
            stop = clock();
            printf("fwrite time : %f\n", ((double)(stop - start)/CLOCKS_PER_SEC));
         #endif

         
      }
      else{
         #ifdef _DEBUG
         printf("%s\n", strerror(errno));
         #endif

         return errno;
      }
   }
   else{
      if(info.st_size != TABLE_SIZE){
         #ifdef _DEBUG
            printf("ERROR TABLE SIZE!\n");
         #endif

         return ERR_TABLE_SIZE;
      }

      fp->table = fopen("data/table", "r+");
      if(!fp->table){
         #ifdef _DEBUG
         printf("ERROR open file!\n");
         #endif

         return ERR_OPEN_FILE;
      }

      fp->table_buf = (size_t*)my_malloc(TABLE_SIZE);
      if(!fp->table_buf){
         #ifdef _DEBUG
            printf("ERROR my_malloc!\n");
         #endif

         close_table(fp);
         return ERR_MY_MALLOC;
      }

      #ifdef _PRINTF_TIME
         start = clock();
      #endif

      size_t *ptr = fp->table_buf;
      if(TABLE_SIZE != fread(ptr, 1, TABLE_SIZE, fp->table)){
         #ifdef _DEBUG
            printf("ERROR fread!\n");
         #endif

         close_table(fp);
         return ERR_READ_FILE;
      }

      #ifdef _PRINTF_TIME
         stop = clock();
         printf("fread time : %f\n", ((double)(stop - start)/CLOCKS_PER_SEC));
      #endif

      #ifdef _DEBUG
         printf("------\n");
         if(*(fp->table_buf) == -1){
            printf("<->");
         }
         else{
            printf("%d", fp->table_buf);
         }
         printf("\n------\n");
      #endif
   }

   if(stat("data/data", &info) && errno != ENOENT){
      #ifdef _DEBUG
         printf("%s\n", strerror(errno));
      #endif

      return errno;
   }
   
   fp->data = fopen("data/data", "r+");
   if(!fp->data){
      #ifdef _DEBUG
         printf("ERROR open file!\n");
      #endif

      return ERR_OPEN_FILE;
   }
   
   
   return SUCCESS;
}

int close_table(_files *fp){
   fclose(fp->table);
   fclose(fp->data);
   my_free(fp->table_buf);


   return SUCCESS;
}

int find(const char *key, _files *fp, _DATA *result_data){
   if(!result_data || !fp){
      #ifdef _DEBUG
         printf("ERROR find Parameter!\n");
      #endif

      return ERR_PARAMETER;
   }

   //result_data->hash_num = hash_func(key);
   result_data->table_ptr  = hash_func(key);
   result_data->block_ptr  = *(fp->table_buf + result_data->table_ptr);
   result_data->data_ptr   = -1;

   if(result_data->block_ptr == -1){
      #ifdef _DEBUG
         printf("NOT FOUND TABLE\n");
      #endif

      return NOT_FOUND;
   }
   else{
      #ifdef _DEBUG
         printf("CAN FOUND\n");
         //會不會有table連到一個空資料的位置?
      #endif

      size_t block_ptr = result_data->block_ptr;

      void *data = my_malloc(BLOCK_SIZE);
      if(!data){
         #ifdef _DEBUG
            printf("find my_malloc error!\n");
         #endif

         my_free(data);

         return ERR_MY_MALLOC;
      }
 
      while(block_ptr != -1){
         fseek(fp->data, block_ptr, SEEK_SET);
         fflush(fp->data);
         if(BLOCK_SIZE != fread(data, 1, BLOCK_SIZE, fp->data)){
            #ifdef _DEBUG
               printf("find read error!\n");
            #endif

            my_free(data);

            return ERR_READ_FILE;
         }

         size_t block_total = *(size_t*)data;
         size_t next = *(size_t*)(data + BLOCK_TOTAL_SIZE);
         void *data_ptr = data + BLOCK_TOTAL_SIZE + NEXT_SIZE;

         for(; data_ptr <= data + block_total;){
            void *ptr = data_ptr;

            if(!strcmp(key, (char*)(ptr + sizeof(size_t)))){
               result_data->total_size = *(size_t*)ptr;
               ptr += sizeof(size_t);

               int tmp = strlen((char*)ptr) + 1;
               strncpy(result_data->key, (char*)ptr, tmp);
               ptr += tmp;

               result_data->val_size = *(size_t*)ptr;
               ptr += sizeof(size_t);

               memcpy(result_data->val, ptr, result_data->val_size);
               ptr += result_data->val_size;

               result_data->type = *(char*)ptr;
               ptr++;

               result_data->block_ptr = block_ptr;
               result_data->data_ptr = data_ptr - data;

               my_free(data);

               return SUCCESS;
            }

            data_ptr += *(size_t*)ptr;
         }   

         block_ptr = next;
      }

      my_free(data);

      return NOT_FOUND;
   }


   return SUCCESS;
}

int del(const char *key, _files *fp, _DATA *result_data){
   if(!result_data || !fp){
      #ifdef _DEBUG
         printf("ERROR del Parameter!\n");
      #endif

      return ERR_PARAMETER;
   }

   int return_num = find(key, fp, result_data);

   if(!return_num){
      result_data->data_ptr   = -1;
      result_data->block_ptr  = -1;

      void *block_buf = my_malloc(BLOCK_SIZE);
      if(!block_buf){
         #ifdef _DEBUG
            printf("ERROR del MALLOC!\n");
         #endif

         return ERR_MY_MALLOC;
      }

      fseek(fp->data, result_data->block_ptr, SEEK_SET);
      fflush(fp->data);
      if(BLOCK_SIZE != fread(block_buf, 1, BLOCK_SIZE, fp->data)){
         #ifdef _DEBUG
            printf("ERROR del READ!\n");
         #endif

         return ERR_READ_FILE;
      }

      *(size_t*)block_buf -= result_data->total_size;

      fseek(fp->data, result_data->block_ptr, SEEK_SET);
      void *ptr = block_buf;
      if(result_data->data_ptr != fwrite(ptr, 1, result_data->data_ptr, fp->data)){
         #ifdef _DEBUG
            printf("del ERROR add WRITE 1\n");
         #endif

         return ERR_WRITE;
      }
      size_t temp = result_data->data_ptr + result_data->total_size;
      ptr += temp;
      if(BLOCK_SIZE - temp != fwrite(ptr, 1, BLOCK_SIZE - temp, fp->data)){
         #ifdef _DEBUG
            printf("del ERROR add WRITE 2\n");
         #endif

         return ERR_WRITE;
      }  
   }


   return return_num;
}

int add(const char *key, const void *val, const size_t val_size, const char type, _files *fp, _DATA *result_data){
   if(!result_data || !fp){
      #ifdef _DEBUG
         printf("ERROR del Parameter!\n");
      #endif

      return ERR_PARAMETER;
   }

   int return_num = del(key, fp, result_data);
   if(return_num && return_num != NOT_FOUND){
      #ifdef _DEBUG
         printf("ERROR add return number : %d !\n", return_num);
      #endif

      return return_num;
   }

   void *block_buf = my_malloc(BLOCK_SIZE);
   if(!block_buf){
      #ifdef _DEBUG
         printf("ERROR ADD MALLOC!\n");
      #endif

      return ERR_MY_MALLOC;
   }

   //total + key + val_size + val + type;
   size_t total_size = sizeof(size_t) + strlen(key) + 1 + sizeof(size_t) + val_size + sizeof(char);

   void *table_ptr      = fp->table_buf;
   *(size_t*)table_ptr   += result_data->table_ptr;

   size_t block_ptr     = *(size_t*)result_data->block_ptr;
   size_t pre_block_ptr = -1;

   for(; block_ptr != -1;){
      fflush(fp->data);
      fseek(fp->data, block_ptr, SEEK_SET);
      if(BLOCK_SIZE != fread(block_buf, 1, BLOCK_SIZE, fp->data)){
         #ifdef _DEBUG
            printf("ERROR ADD READ!\n");
         #endif

         my_free(block_buf);
         return ERR_READ_FILE;
      }

      if(*(size_t*)block_buf + total_size <= BLOCK_SIZE){
         break;
      }
      
      pre_block_ptr  = block_ptr;
      block_ptr      = *(size_t*)(block_buf + BLOCK_TOTAL_SIZE);
   }

   if(block_ptr == -1){
      struct stat info;

      fflush(fp->data);
      if(stat("data/data", &info)){
         #ifdef _DEBUG
            printf("%s\n", strerror(errno));
         #endif
         
         my_free(block_buf);
         return errno;
      }

      block_ptr = info.st_size;

      if(pre_block_ptr == -1){
         *(size_t*)table_ptr = block_ptr;

         fseek(fp->table, 0, SEEK_SET);
         if(TABLE_SIZE != fwrite(fp->table_buf, 1, TABLE_SIZE, fp->table)){
            #ifdef _DEBUG
               printf("ERROR add WRITE TABLE\n");
            #endif

            my_free(block_buf);
            return ERR_WRITE;
         }
      }
      else{
         *(size_t*)(block_buf + BLOCK_TOTAL_SIZE) = block_ptr;

         fseek(fp->data, pre_block_ptr, SEEK_SET);
         if(BLOCK_SIZE != fwrite(block_buf, 1, BLOCK_SIZE, fp->data)){
            #ifdef _DEBUG
               printf("ERROR add WRITE DATA\n");
            #endif

            my_free(block_buf);
            return ERR_WRITE;
         }
      }

      *(size_t*)block_buf = sizeof(size_t) * 2;
      *(size_t*)(block_buf + BLOCK_TOTAL_SIZE) = -1;  
   }

   //add data
   result_data->block_ptr  =  block_ptr;
   *(size_t*)block_buf     += total_size;

   void *data_ptr          = block_buf + *(size_t*)block_buf;
   result_data->data_ptr   = *(size_t*)block_buf;

   *(size_t*)data_ptr      =  total_size;
   result_data->total_size =  total_size;
   data_ptr                += sizeof(total_size);

   int temp = strlen(key) + 1;
   strncpy(data_ptr, key, temp);
   strncpy(result_data->key, key, temp);
   data_ptr += temp;

   *(size_t*)data_ptr      =  val_size;
   result_data->val_size   =  val_size;
   data_ptr                += sizeof(val_size);

   strncpy(data_ptr, val, val_size);
   strncpy(result_data->val, val, val_size);
   data_ptr += val_size;

   *(char*)data_ptr++   = type;
   result_data->type    = type;

   //write data
   fseek(fp->data, block_ptr, SEEK_SET);
   if(TABLE_SIZE != fwrite(block_buf, 1, BLOCK_SIZE, fp->data)){
      #ifdef _DEBUG
         printf("ERROR add WRITE TABLE\n");
      #endif

      my_free(block_buf);
      return ERR_WRITE;
   }
   

   return SUCCESS;
}

int hash_func (const char* key){
   char *str = (char*)key;
   unsigned hash = offset_basis;
   while(*str){
      hash = hash * FNV_prime;
      hash = hash ^ *str++;
   }	


   return (int)(hash % (TABLE_SIZE / NEXT_SIZE));
}
