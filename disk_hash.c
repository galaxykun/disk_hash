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
         if(fp->table_buf == -1){
            printf("<->");
         }
         else{
            printf("%d", fp->table_buf);
         }
         printf("\n------\n");
      #endif
   }

   if(stat("data/data", &info)){
      if(errno == ENOENT){
         /* fp->data = fopen("data/data", "w+");
         if(!fp->data){
            #ifdef _DEBUG
            printf("ERROR open file!\n");
            #endif

            return ERR_OPEN_FILE;
         } */
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

int find(const char *key, _files *fp, _DATA *result_data){
   if(!result_data){
      return ERR_PARAMETER;
   }

   int hash_num = hash_func(key);

   size_t table_ptr = *(fp->table_buf + hash_num);

   if(table_ptr == -1){
      #ifdef _DEBUG
         printf("NOT FOUND\n");
         //*(int*)table_ptr = 654321;
         //fseek(fp->table, 0, SEEK_SET);
         //fp->table->_IO_write_ptr = fp->table->_IO_write_base;
         //fwrite(fp->table_buf, sizeof(char), TABLE_SIZE, fp->table);
      #endif

      return NOT_FOUND_TABLE;
   }
   else{
      #ifdef _DEBUG
         printf("CAN FOUND\n");
         //會不會有table連到一個空資料的位置?
      #endif

      size_t block_ptr = table_ptr;

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

            if(strcmp(key, (char*)(ptr + sizeof(size_t)))){
               data_ptr += *(size_t*)ptr;
            }
            else{
               result_data.total_size = *(size_t*)ptr;
               ptr += sizeof(size_t);

               int tmp = strlen((char*)ptr) + 1;
               strncpy(result_data.key, (char*)ptr, tmp);
               ptr += tmp;

               result_data.val_size = *(size_t*)ptr;
               ptr += sizeof(size_t);

               memcpy(result_data.val, ptr, result_data.val_size);
               ptr += result_data.val_size;

               result_data.type = *(char*)ptr;
               ptr++;

               result_data->block_ptr = block_ptr;
               result_data->data_ptr = data_ptr - data;

               my_free(data);

               return SUCCESS;
            }
         }   

         block_ptr = next;
      }

      my_free(data);

      return NOT_FOUND_DATA;
   }

   my_free(data);


   return SUCCESS;
}

int del(const char *key, _files *fp, _DATA *result_data){
   if(!result_data){
      return ERR_PARAMETER;
   }
   
   int return_num = find(key, fp, result_data);

   if(!return_num){
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
            printf("ERROR add WRITE 1\n");
         #endif

         return ERR_WRITE;
      }
      size_t temp = result_data->data_ptr + result_data->total_size;
      ptr += temp;
      if(BLOCK_SIZE - temp != fwrite(ptr, 1, BLOCK_SIZE - temp, fp->data)){
         #ifdef _DEBUG
            printf("ERROR add WRITE 2\n");
         #endif

         return ERR_WRITE;
      }  
   }


   return return_num;
}

/* int add(const char *key, const char *val, const size_t val_size, const char type, _files *fp){
   //total_size + key_size + val_size + val + type;
   size_t total = sizeof(size_t) + strlen(key) + 1 + sizeof(size_t) + val_size + 1;
   _DATA *data = NULL;
   size_t data_res = -1;
   size_t block_ptr = -1;

   void *block_buf = my_malloc(BLOCK_SIZE);
   if(!block_buf){
      #ifdef _DEBUG
         printf("ERROR ADD MALLOC!\n");
      #endif

      return ERR_MY_MALLOC;
   }

   int return_num = find(key, fp, &data_res, &block_ptr, data);

   //如果沒有table就不用找直接跳到移到data最後步驟
   //先把舊資料拿出來，
   //再找到一個夠大的block，
   //都沒找到就移到data最後
   //更新好block_buff
   //寫回去對應的block_ptr

   if(return_num == NOT_FOUND_TABLE){
      //更新table，指向data最後的位置
      //更新block_buf
   }
   else if(return_num == NOT_FOUND_DATA){
      //找尋一個合適大小的block或是data最後
      //更新block_buf
   }
   else if(!return_num){
      //檢查新舊資料長度
      //查看那個block夠不夠放
      //不夠就從頭找一個夠的，都不夠就放data最後
      //更新block_buf
   }
   else{
      #ifdef _DEBUG
         printf("ERROR : %d\n", return_num);
      #endif

      return return_num;
   }

   //寫進去
   if(BLOCK_SIZE != fwrite(block_buf, sizeof(char), BLOCK_SIZE, fp->data)){
      #ifdef _DEBUG
         printf("ERROR add WRITE 3\n");
      #endif

      return ERR_WRITE;
   }
   
   //found?
   if(data){
      fseek(fp->data, block_ptr, SEEK_SET);
      if(BLOCK_SIZE != fread(block_buf, sizeof(char), BLOCK_SIZE, fp->data)){
         #ifdef _DEBUG
            printf("ERROR ADD READ!\n");
         #endif

         return ERR_READ_FILE;
      }

      //剩餘空間夠不夠
      if(total - data->total_size > 0 && *(size_t)block_buf + (total - data->total_size) > BLOCK_SIZE){
      //if(total - data->total_size > 0 && *(size_t*)block_buf < total - data->total_size){
         //有沒有下一個block
         if(*(size_t*)(block_buf + BLOCK_TOTAL_SIZE) == -1){
            //拿取data大小，在最後面開個block空間出來
            fflush(fp->data);
            struct stat info;
            if(stat("data/data", &info)){
               #ifdef _DEBUG
                  printf("%s\n", strerror(errno));
               #endif

               return errno;
            }

            //連結next，更改block rest
            *(size_t*)block_buf -= data->total_size;
            *(size_t*)(block_buf + BLOCK_TOTAL_SIZE) = (size_t)info.st_size + 1;

            //把這筆資料拿起來寫回去
            fseek(fp->data, block_ptr, SEEK_SET);
            void *data_ptr = block_buf + data_res;
            if(data_ptr - block_buf != fwrite(block_buf, sizeof(char), data_ptr - block_buf, fp->data)){
               #ifdef _DEBUG
                  printf("ERROR add WRITE 1\n");
               #endif

               return ERR_WRITE;
            }
            data_ptr += data->total_size;
            if((block_buf + BLOCK_SIZE) - data_ptr != fwrite(data_ptr, sizeof(char), (block_buf + BLOCK_SIZE) - data_ptr, fp->data)){
               #ifdef _DEBUG
                  printf("ERROR add WRITE 2\n");
               #endif

               return ERR_WRITE;
            }

            //初始化block buffer，整筆資料放進去
            *(size_t*)block_buf = total;
            *(size_t*)(block_buf + BLOCK_TOTAL_SIZE) = -1;
            data_ptr = block_buf + BLOCK_TOTAL_SIZE + NEXT_SIZE;
            *(size_t*)data_ptr = total;
            data_ptr += sizeof(size_t) + strlen(key) + 1;
            *(size_t)data_ptr = val_size;
            data_ptr += sizeof(size_t);
            memcpy(data_ptr, val, val_size);
            data_ptr += val_size;
            *(char*)data_ptr = type;
            data_ptr++;
            
            //移動到最尾部
            fseek(fp->data, 0, SEEK_END);         
         }
         else
         {

         }
      }
      else{

      }
   
   }
   else{

   }

   
   //fseek(fp->data, 0, SEEK_END);
   

   
} */

int hash_func (const char* key){
   char *str = (char*)key;
   unsigned hash = offset_basis;
   while(*str){
      hash = hash * FNV_prime;
      hash = hash ^ *str++;
   }	


   return (int)(hash % (TABLE_SIZE / NEXT_SIZE));
}
