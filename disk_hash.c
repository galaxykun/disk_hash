#include "disk_hash.h"
#include "my_malloc.c"

#ifdef _PRINTF_TIME
   struct timeval start, stop;
   struct timeval add_start, add_stop;
   struct timeval find_start, find_stop;
#endif


int main(){

   _files fp;
   _DATA data;

   #ifdef _PRINTF_TIME
      gettimeofday(&start, NULL);
   #endif

   open_table(&fp);

   #ifdef _PRINTF_TIME
      gettimeofday(&stop, NULL);
      printf("open_table time : %f\n",
      (stop.tv_sec - start.tv_sec) + (double)(stop.tv_usec - start.tv_usec)/1000000.0);
   #endif

   #ifdef _PRINTF_TIME
      gettimeofday(&add_start, NULL);
   #endif

   FILE *fpp = fopen("random_100000.txt", "r");
   char key[10];
   int val;   
   for(int i = 0; fscanf(fpp, "%s %d", key, &val) != EOF;){
      
      int return_num = add(key, &val, sizeof(int), TYPE_INT, &fp, &data);

      #ifdef _DEBUG
         printf("index : %7d\tadd return : %d\n", i++, return_num);
      #endif
   }

   #ifdef _PRINTF_TIME
      gettimeofday(&add_stop, NULL);
   #endif

   #ifdef _PRINTF_TIME
      gettimeofday(&find_start, NULL);
   #endif

   fseek(fpp, 0, SEEK_SET);
   for(int i = 0; fscanf(fpp, "%s %d", key, &val) != EOF;){
      
      int return_num = find(key, &fp, &data);

      #ifdef _DEBUG
         printf("index : %d\tfind return : %d\n", i++, return_num);
      #endif
   }

   fclose(fpp);

   #ifdef _PRINTF_TIME
      printf("add time : %f\n",
      (add_stop.tv_sec - add_start.tv_sec) + (double)(add_stop.tv_usec - add_start.tv_usec)/1000000.0);
   #endif

   #ifdef _PRINTF_TIME
      gettimeofday(&find_stop, NULL);
      printf("find time : %f\n",
      (find_stop.tv_sec - find_start.tv_sec) + (double)(find_stop.tv_usec - find_start.tv_usec)/1000000.0);
   #endif

   #ifdef _DEBUG
      printf("find return : %d\n", return_num);
   #endif

   return_num = del(key, &fp, &data);

   #ifdef _DEBUG
      printf("del return : %d\n", return_num);
   #endif

   return_num = find(key, &fp, &data);

   #ifdef _DEBUG
      printf("find2 return : %d\n", return_num);
   #endif


   close_table(&fp);


   return 0;
}

int open_table(_files *fp){
   int return_num = 0;
   struct stat info;

   //dir exist?
   if(stat("data", &info)){
      if(errno == ENOENT){
         umask(002);
         if(mkdir("data", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH)){ //775
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
   if(stat("data/table", &info) && errno != ENOENT){
      #ifdef _DEBUG
         printf("%s\n", strerror(errno));
      #endif

      return errno;
   }

   fp->table_buf = (size_t*)my_malloc(TABLE_SIZE);
   if(!fp->table_buf){
         #ifdef _DEBUG
         printf("ERROR my_malloc!\n");
      #endif

      return ERR_MY_MALLOC;
   }

   fp->block_buf = my_malloc(BLOCK_SIZE);
   if(!fp->block_buf){
      #ifdef _DEBUG
         printf("ERROR block buffer MALLOC!\n");
      #endif

      goto ERR1;
      return_num = ERR_MY_MALLOC;
   }

   if(errno == ENOENT){
      fp->table = fopen("data/table", "w+");
      if(!fp->table){
         #ifdef _DEBUG
         printf("ERROR open file!\n");
         #endif

         goto ERR2;
         return_num = ERR_OPEN_FILE;
      }

      size_t *ptr = fp->table_buf;
      for(; ptr <= fp->table_buf + (TABLE_SIZE / NEXT_SIZE); ptr++){
         *ptr = 0;
      }
   }
   else{
      if(info.st_size != TABLE_SIZE){
         #ifdef _DEBUG
            printf("ERROR TABLE SIZE!\n");
         #endif

         goto ERR2;
         return_num = ERR_TABLE_SIZE;
      }

      fp->table = fopen("data/table", "r+");
      if(!fp->table){
         #ifdef _DEBUG
         printf("ERROR open file!\n");
         #endif

         goto ERR2;
         return_num = ERR_OPEN_FILE;
      }

      size_t *ptr = fp->table_buf;
      if(TABLE_SIZE != fread(ptr, 1, TABLE_SIZE, fp->table)){
         #ifdef _DEBUG
            printf("ERROR fread!\n");
         #endif

         goto ERR3;
         return_num = ERR_READ_FILE;
      }
   }

   if(stat("data/data", &info) && errno != ENOENT){
      #ifdef _DEBUG
         printf("%s\n", strerror(errno));
      #endif

      goto ERR3;
      return_num = errno;
   }
   
   if(errno == ENOENT){
      fp->data = fopen("data/data", "w+");
   }
   else{
      fp->data = fopen("data/data", "r+");
   }
   if(!fp->data){
      #ifdef _DEBUG
         printf("ERROR open file!\n");
      #endif

      goto ERR3;
      return_num = ERR_OPEN_FILE;
   }
   
   fseek(fp->data, 0, SEEK_SET);
   if(1 != fwrite("-", 1, 1, fp->data)){
      #ifdef _DEBUG
         printf("ERROR open WRITE DATA\n");
      #endif

      goto ERR4;
      return_num = ERR_WRITE;
   }

   
   return SUCCESS;

   ERR4:
   fclose(fp->data);
   ERR3:
   fclose(fp->table);
   ERR2:
   my_free(fp->block_buf);
   ERR1:
   my_free(fp->table_buf);

   return return_num;
}

int close_table(_files *fp){
   if(!fp->table || !fp->data || !fp->table_buf || !fp->block_buf){
      return ERR_CLOSE;
   }

   fflush(fp->table);
   fseek(fp->table, 0, SEEK_SET);
   if(TABLE_SIZE != fwrite(fp->table_buf, 1, TABLE_SIZE, fp->table)){
      #ifdef _DEBUG
         printf("ERROR CLOSE WRITE TABLE\n");
      #endif

      return ERR_WRITE;
   }
   
   fclose(fp->table);
   fclose(fp->data);
   my_free(fp->table_buf);
   fp->table_buf = NULL;
   my_free(fp->block_buf);
   fp->block_buf = NULL;
   

   return SUCCESS;
}

int find(const char *key, _files *fp, _DATA *result_data){
   if(!result_data || !fp){
      #ifdef _DEBUG
         printf("ERROR find Parameter!\n");
      #endif

      return ERR_PARAMETER;
   }

   result_data->table_ptr     = hash_func(key);
   result_data->block_ptr     = *(fp->table_buf + result_data->table_ptr);
   result_data->data_ptr      = 0;
   result_data->pre_block_ptr = 0;
   result_data->total_size    = 0;
   *(result_data->key)        = 0;
   result_data->val_size      = 0;
   result_data->type          = 0;

   if(!result_data->block_ptr){
      #ifdef _DEBUG
         printf("NOT FOUND TABLE\n");
      #endif

      return NOT_FOUND;
   }
   else{
      #ifdef _DEBUG
         printf("CAN FOUND\n");
      #endif

      size_t block_ptr = result_data->block_ptr;
      size_t pre_block_ptr = 0;
 
      while(block_ptr){
         fflush(fp->data);
         fseek(fp->data, block_ptr, SEEK_SET);
         if(BLOCK_SIZE != fread(fp->block_buf, 1, BLOCK_SIZE, fp->data)){
            #ifdef _DEBUG
               printf("find read error!\n");
            #endif

            return ERR_READ_FILE;
         }

         size_t block_total = *(size_t*)fp->block_buf;
         size_t next = *(size_t*)(fp->block_buf + BLOCK_TOTAL_SIZE);
         void *data_ptr = fp->block_buf + BLOCK_TOTAL_SIZE + NEXT_SIZE;

         for(; data_ptr < fp->block_buf + block_total;){
            void *ptr = data_ptr;

            if(!strcmp(key, (char*)(ptr + (sizeof(result_data->total_size))))){
               result_data->total_size = *(size_t*)ptr;
               ptr += sizeof(result_data->total_size);

               int tmp = strlen((char*)ptr) + 1;
               strncpy(result_data->key, (char*)ptr, tmp);
               ptr += tmp;

               result_data->val_size = *(size_t*)ptr;
               ptr += sizeof(result_data->val_size);

               memcpy(result_data->val, ptr, result_data->val_size);
               ptr += result_data->val_size;

               result_data->type = *(char*)ptr;
               ptr++;

               result_data->data_ptr      = data_ptr - fp->block_buf;
               result_data->block_ptr     = block_ptr;
               result_data->pre_block_ptr = pre_block_ptr;

               return SUCCESS;
            }

            data_ptr += *(size_t*)ptr;
         }   

         pre_block_ptr = block_ptr;
         block_ptr = next;
      }

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
      fflush(fp->data);

      fseek(fp->data, result_data->block_ptr, SEEK_SET);
      if(BLOCK_SIZE != fread(fp->block_buf, 1, BLOCK_SIZE, fp->data)){
         #ifdef _DEBUG
            printf("ERROR del READ 1!\n");
         #endif

         return ERR_READ_FILE;
      }

      *(size_t*)fp->block_buf -= result_data->total_size;

      //empty block?
      if(*(size_t*)fp->block_buf == BLOCK_TOTAL_SIZE + NEXT_SIZE){
         //pre is table?
         if(result_data->pre_block_ptr){
            size_t temp = *(size_t*)(fp->block_buf + BLOCK_TOTAL_SIZE);
            fseek(fp->data, result_data->pre_block_ptr, SEEK_SET);
            if(BLOCK_SIZE != fread(fp->block_buf, 1, BLOCK_SIZE, fp->data)){
               #ifdef _DEBUG
                  printf("ERROR del READ 2!\n");
               #endif

               return ERR_READ_FILE;
            }

            *(size_t*)(fp->block_buf + BLOCK_TOTAL_SIZE) = temp;

            fseek(fp->data, result_data->pre_block_ptr, SEEK_SET);
         }
         else{
            fseek(fp->data, result_data->block_ptr, SEEK_SET); 
         }

         if(BLOCK_SIZE != fwrite(fp->block_buf, 1, BLOCK_SIZE, fp->data)){
            #ifdef _DEBUG
               printf("del ERROR add WRITE 1\n");
            #endif

            return ERR_WRITE;
         }
      }
      else{
         void *ptr = fp->block_buf;
         fseek(fp->data, result_data->block_ptr, SEEK_SET);
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

         result_data->data_ptr   = 0;
         result_data->block_ptr  = 0;
         result_data->pre_block_ptr  = 0;
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

   //total + key + val_size + val + type;
   size_t total_size = sizeof(size_t) + strlen(key) + 1 + sizeof(size_t) + val_size + sizeof(char);

   size_t table_ptr     = result_data->table_ptr;
   size_t block_ptr     = *(fp->table_buf + result_data->table_ptr);
   size_t pre_block_ptr = 0;

   fflush(fp->data);
   for(; block_ptr;){
      fseek(fp->data, block_ptr, SEEK_SET);
      if(BLOCK_SIZE != fread(fp->block_buf, 1, BLOCK_SIZE, fp->data)){
         #ifdef _DEBUG
            printf("ERROR ADD READ!\n");
         #endif

         return ERR_READ_FILE;
      }

      if(*(size_t*)fp->block_buf + total_size <= BLOCK_SIZE){
         break;
      }
      
      pre_block_ptr  = block_ptr;
      block_ptr      = *(size_t*)(fp->block_buf + BLOCK_TOTAL_SIZE);
   }

   if(!block_ptr){
      struct stat info;

      if(stat("data/data", &info)){
         #ifdef _DEBUG
            printf("%s\n", strerror(errno));
         #endif
         
         return errno;
      }

      block_ptr = info.st_size;

      if(!pre_block_ptr){
         *(fp->table_buf + result_data->table_ptr) = block_ptr;
      }
      else{
         *(size_t*)(fp->block_buf + BLOCK_TOTAL_SIZE) = block_ptr;

         fseek(fp->data, pre_block_ptr, SEEK_SET);
         if(BLOCK_SIZE != fwrite(fp->block_buf, 1, BLOCK_SIZE, fp->data)){
            #ifdef _DEBUG
               printf("ERROR add WRITE DATA\n");
            #endif

            return ERR_WRITE;
         }
      }

      *(size_t*)fp->block_buf = BLOCK_TOTAL_SIZE + NEXT_SIZE;
      *(size_t*)(fp->block_buf + BLOCK_TOTAL_SIZE) = 0;  
   }

   //add data
   void *data_ptr          = fp->block_buf + *(size_t*)fp->block_buf;
   result_data->block_ptr  =  block_ptr;
   result_data->data_ptr   = *(size_t*)fp->block_buf;
   
   *(size_t*)fp->block_buf += total_size;

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
   fflush(fp->data);
   fseek(fp->data, block_ptr, SEEK_SET);
   if(BLOCK_SIZE != fwrite(fp->block_buf, 1, BLOCK_SIZE, fp->data)){
      #ifdef _DEBUG
         printf("ERROR add WRITE TABLE\n");
      #endif

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

int reorganize(_files *fp){
   if(!fp){
      return ERR_PARAMETER;
   }

   int return_num = 0;
   return_num = close_table(fp);
   if(return_num){
      return return_num;
   }
   return_num = open_table(fp);
   if(return_num){
      return return_num;
   }

   FILE *data_fp = fopen("data/data_temp","w+");
   if(!data_fp){
      #ifdef _DEBUG
         printf("reorganize open error!\n");
      #endif

      return ERR_OPEN_FILE;
   }
   if(1 != fwrite("-", 1, 1, data_fp)){
      #ifdef _DEBUG
         printf("ERROR reorganize WRITE DATA\n");
      #endif

      goto ERROR1;
      return_num = ERR_WRITE;
   }

   /* void *block_buf = my_malloc(BLOCK_SIZE);
   if(!block_buf){
      #ifdef _DEBUG
         printf("reorganize malloc error!\n");
      #endif

      goto ERROR;
      return_num = ERR_MY_MALLOC;
   } */

   void *temp_buf = my_malloc(BLOCK_SIZE);
   if(!temp_buf){
      #ifdef _DEBUG
         printf("reorganize malloc error!\n");
      #endif

      goto ERROR1;
      return_num = ERR_MY_MALLOC;
   }

   /* struct stat info;
   fflush(data_fp);
   if(stat("data/data_temp", &info)){
      #ifdef _DEBUG
         printf("%s\n", strerror(errno));
      #endif

      goto ERROR;
      return_num = errno;
   } */

   size_t new_block_ptr = 1;

   for(int i = 0; i < TABLE_SIZE / NEXT_SIZE; i++){
      size_t old_block_ptr = fp->table_buf[i];

      if(!old_block_ptr){
         continue;
      }
      
      //find first not empty block
      for(; old_block_ptr;){
         fseek(fp->data, old_block_ptr, SEEK_SET);
         if(BLOCK_SIZE != fread(fp->block_buf, 1, BLOCK_SIZE, fp->data)){
            #ifdef _DEBUG
               printf("reorganize read error!\n");
            #endif

            goto ERROR2;
            return_num = ERR_READ_FILE;
         }

         old_block_ptr = *(size_t*)(fp->block_buf + BLOCK_TOTAL_SIZE);

         if(*(size_t*)fp->block_buf > BLOCK_TOTAL_SIZE + NEXT_SIZE){
            break;
         }
      }

      //not found
      if(*(size_t*)fp->block_buf == BLOCK_TOTAL_SIZE + NEXT_SIZE){
         fp->table_buf[i] = 0;
         continue;
      }

      //find!
      fp->table_buf[i] = new_block_ptr;
      for(; old_block_ptr;){
         fseek(fp->data, old_block_ptr, SEEK_SET);
         if(BLOCK_SIZE != fread(temp_buf, 1, BLOCK_SIZE, fp->data)){
            #ifdef _DEBUG
               printf("reorganize read error!\n");
            #endif

            goto ERROR2;
            return_num = ERR_READ_FILE;
         }

         old_block_ptr = *(size_t*)(temp_buf + BLOCK_TOTAL_SIZE);

         if(*(size_t*)temp_buf == BLOCK_TOTAL_SIZE + NEXT_SIZE){
            continue;
         }

         *(size_t*)(fp->block_buf + BLOCK_TOTAL_SIZE) = new_block_ptr + BLOCK_SIZE;

         /* fflush(data_fp);
         fseek(data_fp, new_block_ptr, SEEK_SET); */
         if(BLOCK_SIZE != fwrite(fp->block_buf, 1, BLOCK_SIZE, data_fp)){
            #ifdef _DEBUG
               printf("ERROR reorganize WRITE DATA\n");
            #endif

            goto ERROR2;
            return_num = ERR_WRITE;
         }

         new_block_ptr += BLOCK_SIZE;

         memcpy(fp->block_buf, temp_buf, BLOCK_SIZE);
      }
      
      *(size_t*)(fp->block_buf + BLOCK_TOTAL_SIZE) = 0;
      if(BLOCK_SIZE != fwrite(fp->block_buf, 1, BLOCK_SIZE, data_fp)){
         #ifdef _DEBUG
            printf("ERROR reorganize WRITE DATA\n");
         #endif

         goto ERROR2;
         return_num = ERR_WRITE;
      }

      new_block_ptr += BLOCK_SIZE;
   }

   return_num = close_table(fp);
   if(return_num){
      goto ERROR2;
   }

   if(rename("data/data_temp","data/data")){
      #ifdef _DEBUG
         printf("ERROR rename DATA\n");
      #endif

      goto ERROR2;
      return_num = ERR_RENAME;
   }

   return_num = open_table(fp);
   if(return_num){
      goto ERROR2;
   }

   ERROR2:
   my_free(temp_buf);
   ERROR1:
   fclose(data_fp);
   //my_free(block_buf);
   

   return return_num;
}