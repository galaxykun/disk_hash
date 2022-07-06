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

   _DATA data;
   data.key = my_malloc(10);
   data.key = "asdqwe";
   size_t res = 0;
   int test = -1;
   test = find(data.key, &fp, &res, &data);

   #ifdef _DEBUG
      printf("find return : %d\n", test);
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

         fp->table_buf = my_malloc(TABLE_SIZE);
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

         void *ptr = fp->table_buf;
         for(; ptr <= fp->table_buf + TABLE_SIZE;){
            //memset(ptr, '-', NEXT_SIZE);
            *(size_t*)ptr = -1;
            ptr += NEXT_SIZE;
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

         if(TABLE_SIZE != fwrite(fp->table_buf, sizeof(char), TABLE_SIZE, fp->table)){
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

      fp->table_buf = my_malloc(TABLE_SIZE);
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

      void *ptr = fp->table_buf;
      if(TABLE_SIZE != fread(ptr, sizeof(char), TABLE_SIZE, fp->table)){
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
         if(*(size_t*)fp->table_buf == -1){
            printf("<->");
         }
         else{
            printf("%d", *(int*)fp->table_buf);
         }
         printf("\n------\n");
      #endif
   }

   if(stat("data/data", &info)){
      if(errno == ENOENT){
         fp->data = fopen("data/data", "w+");
         if(!fp->data){
            #ifdef _DEBUG
            printf("ERROR open file!\n");
            #endif

            return ERR_OPEN_FILE;
         }
      }
      else{
         #ifdef _DEBUG
            printf("%s\n", strerror(errno));
         #endif

         return errno;
      }
   }
   else{
      fp->data = fopen("data/data", "r+");
      if(!fp->data){
         #ifdef _DEBUG
            printf("ERROR open file!\n");
         #endif

         return ERR_OPEN_FILE;
      }
   }

   
   return SUCCESS;
}

int close_table(_files *fp){
   fclose(fp->table);
   fclose(fp->data);
   my_free(fp->table_buf);


   return SUCCESS;
}

int find(const char *key, _files *fp, size_t *result, _DATA *result_data){
   int hash_num = hash_func(key);

   void *table_ptr = (fp->table_buf + (hash_num * NEXT_SIZE));

   if(*(size_t*)table_ptr == -1){
      #ifdef _DEBUG
         printf("NOT FOUND\n");
         //*(int*)table_ptr = 654321;
         //fseek(fp->table, 0, SEEK_SET);
         //fp->table->_IO_write_ptr = fp->table->_IO_write_base;
         //fwrite(fp->table_buf, sizeof(char), TABLE_SIZE, fp->table);
      #endif
      
      *result = -1;
      result_data = NULL;

      return NOT_FOUND;
   }
   else{
      #ifdef _DEBUG
         printf("CAN FOUND\n");
         //會不會有table連到一個空資料的位置?
      #endif

      size_t block_ptr = *(size_t*)table_ptr;

      void *data = my_malloc(BLOCK_SIZE);
      if(!data){
         #ifdef _DEBUG
            printf("find my_malloc error!\n");
         #endif

         return ERR_MY_MALLOC;
      }

      
      while(block_ptr != -1){
         fseek(fp->data, block_ptr, SEEK_SET);
         if(BLOCK_SIZE != fread(data, sizeof(char), BLOCK_SIZE, fp->data)){
            #ifdef _DEBUG
               printf("find read error!\n");
            #endif

            return ERR_READ_FILE;
         }

         size_t block_total = *(size_t*)data;
         size_t next = *(size_t*)(data + sizeof(size_t));
         void *data_ptr = data + sizeof(size_t) * 2;
         for(; data_ptr <= data + block_total;){
            void *ptr = data_ptr;
            _DATA data_temp;
            data_temp.total_size = *(size_t*)ptr;
            ptr += sizeof(size_t);
            data_temp.key = (char*)ptr;
            ptr += strlen((char*)ptr) + 1;
            data_temp.val_size = *(size_t*)ptr;
            ptr += sizeof(size_t);
            data_temp.val = (char*)ptr;
            ptr += data_temp.val_size;
            data_temp.type = *(char*)ptr;
            ptr++;

            

            if(strcmp(key, data_temp.key)){
               data_ptr = ptr;
            }
            else{
               //*result = *(int*)block_ptr;
               *result = block_ptr + (data_ptr - data);
               result_data = &data_temp;

               my_free(data);
               return SUCCESS;
            }
         }   

         block_ptr = next;
      }


      *result = -1;
      result_data = NULL;
      return NOT_FOUND;
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
