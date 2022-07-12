#include "disk_hash.h"
#include "my_malloc.c"

#ifdef _PRINTF_TIME
   struct timeval start, stop;
   struct timeval add_start, add_stop;
   struct timeval find_start, find_stop;
#endif


int main(){

   _DISK_HASH dh;
   _DATA data;
   int ret_val = 0;

   #ifdef _PRINTF_TIME
      gettimeofday(&start, NULL);
   #endif

   open_table(&dh);

   #ifdef _PRINTF_TIME
      gettimeofday(&stop, NULL);
      printf("open_table time : %f\n",
      (stop.tv_sec - start.tv_sec) + (double)(stop.tv_usec - start.tv_usec)/1000000.0);
   #endif

   #ifdef _PRINTF_TIME
      gettimeofday(&add_start, NULL);
   #endif

   FILE *fpp = fopen("random_10000.txt", "r");
   char key[10];
   int val;   
   for(int i = 0; fscanf(fpp, "%s %d", key, &val) != EOF;){
      
      ret_val = add(key, &val, sizeof(int), TYPE_INT, &dh);

      #ifdef _DEBUG
         printf("index : %7d\tadd return : %d\n", i++, ret_val);
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
      
      ret_val = find(key, &dh, &data);

      #ifdef _DEBUG
         printf("index : %d\tfind return : %d\n", i++, ret_val);
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
      printf("find return : %d\n", ret_val);
   #endif

   ret_val = del(key, &dh);

   #ifdef _DEBUG
      printf("del return : %d\n", ret_val);
   #endif

   ret_val = find(key, &dh, &data);

   #ifdef _DEBUG
      printf("find2 return : %d\n", ret_val);
   #endif


   close_table(&dh);


   return 0;
}

int open_table(_DISK_HASH *dh){
   int ret_val = SUCCESS;
   struct stat info;

   dh->buf.table     = NULL;
   dh->buf.data      = NULL;
   dh->buf.table_buf = NULL;
   dh->buf.block_buf = NULL;

   //dir exist?
   if(stat(DIR_NAME, &info)){
      if(errno == ENOENT){
         umask(002);
         if(mkdir(DIR_NAME, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH)){ //775
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

   //get table file info
   if(stat(TABLE_FILE_NAME, &info) && errno != ENOENT){
   #ifdef _DEBUG
      printf("%s\n", strerror(errno));
   #endif

   return errno;
   }

   dh->buf.table_buf = (long*)my_malloc(TABLE_SIZE);
   if(!dh->buf.table_buf){
   #ifdef _DEBUG
      printf("ERROR my_malloc!\n");
   #endif

      ret_val = ERR_MY_MALLOC;
      goto ERROR;
   }

   dh->buf.block_buf = my_malloc(BLOCK_SIZE);
   if(!dh->buf.block_buf){
   #ifdef _DEBUG
      printf("ERROR block buffer MALLOC!\n");
   #endif

      ret_val = ERR_MY_MALLOC;
      goto ERROR;
   }

   //table file is exist?
   if(errno == ENOENT){
      dh->buf.table = fopen(TABLE_FILE_NAME, "w+");
      if(!dh->buf.table){
      #ifdef _DEBUG
         printf("ERROR open file!\n");
      #endif

         ret_val = ERR_OPEN_FILE;
         goto ERROR;
      }

      //initialization table
      long *ptr = dh->buf.table_buf;
      for(int i = 0; i < TABLE_NUMBER; i++){
         *ptr++ = -1;
      }
   }
   else{
      //size error
      if(info.st_size != TABLE_SIZE){
      #ifdef _DEBUG
         printf("ERROR TABLE SIZE!\n");
      #endif

         ret_val = ERR_TABLE_SIZE;
         goto ERROR;
      }

      dh->buf.table = fopen(TABLE_FILE_NAME, "r+");
      if(!dh->buf.table){ 
      #ifdef _DEBUG
         printf("ERROR open file!\n");
      #endif

         ret_val = ERR_OPEN_FILE;
         goto ERROR;
      }

      if(TABLE_SIZE != fread(dh->buf.table_buf, 1, TABLE_SIZE, dh->buf.table)){
      #ifdef _DEBUG
         printf("ERROR fread!\n");
      #endif

         ret_val = ERR_READ_FILE;
         goto ERROR;
      }
   }

   //get data file info
   if(stat(DATA_FILE_NAME, &info) && errno != ENOENT){
      #ifdef _DEBUG
         printf("%s\n", strerror(errno));
      #endif

      ret_val = errno;
      goto ERROR;
   }
   
   //data file is exist?
   if(errno == ENOENT){
      dh->buf.data = fopen(DATA_FILE_NAME, "w+");
   }
   else{
      dh->buf.data = fopen(DATA_FILE_NAME, "r+");
   }
   if(!dh->buf.data){
   #ifdef _DEBUG
      printf("ERROR open file!\n");
   #endif

      ret_val = ERR_OPEN_FILE;
      goto ERROR;
   }

   
   return SUCCESS;

ERROR:
   if(dh->buf.data){
      fclose(dh->buf.data);
      dh->buf.data = NULL;
   }
   if(dh->buf.table){
      fclose(dh->buf.table);
      dh->buf.table = NULL;
   }
   if(dh->buf.block_buf){
      my_free(dh->buf.block_buf);
      dh->buf.block_buf = NULL;
   }
   if(dh->buf.table_buf){
      my_free(dh->buf.table_buf);
      dh->buf.table_buf = NULL;
   }


   return ret_val;
}

int close_table(_DISK_HASH *dh){
   if(!dh->buf.table || !dh->buf.data || !dh->buf.table_buf || !dh->buf.block_buf){
      return ERR_CLOSE;
   }

   fflush(dh->buf.table);
   fseek(dh->buf.table, 0, SEEK_SET);
   if(TABLE_SIZE != fwrite(dh->buf.table_buf, 1, TABLE_SIZE, dh->buf.table)){
   #ifdef _DEBUG
      printf("ERROR CLOSE WRITE TABLE\n");
   #endif

      return ERR_WRITE;
   }
   
   fclose(dh->buf.table);
   dh->buf.table = NULL;

   fclose(dh->buf.data);
   dh->buf.data = NULL;

   my_free(dh->buf.table_buf);
   dh->buf.table_buf = NULL;

   my_free(dh->buf.block_buf);
   dh->buf.block_buf = NULL;
   

   return SUCCESS;
}

int search_data(const char *key, _DISK_HASH *dh){
   if(!dh || !key){
   #ifdef _DEBUG
      printf("ERROR find Parameter!\n");
   #endif

      return ERR_PARAMETER;
   }

   //initialization
   dh->block.table_ptr     = hash_func(key);
   dh->block.block_ptr     = *(dh->buf.table_buf + dh->block.table_ptr);
   dh->block.data_ptr      = -1;
   dh->block.pre_block_ptr = -1;

   if(dh->block.block_ptr == -1){
   #ifdef _DEBUG
      printf("NOT FOUND TABLE\n");
   #endif

      return NOT_FOUND;
   }

   long block_ptr       = dh->block.block_ptr;
   long pre_block_ptr   = -1;

   //search block
   fflush(dh->buf.data);
   while(block_ptr != -1){
      fseek(dh->buf.data, block_ptr, SEEK_SET);
      if(BLOCK_SIZE != fread(dh->buf.block_buf, 1, BLOCK_SIZE, dh->buf.data)){
      #ifdef _DEBUG
         printf("find read error!\n");
      #endif

         return ERR_READ_FILE;
      }

      long next_block   = *(long*)(dh->buf.block_buf + BLOCK_TOTAL_SIZE);
      void *data_ptr    = dh->buf.block_buf + BLOCK_TOTAL_SIZE + NEXT_SIZE;

      //search data
      for(; data_ptr < dh->buf.block_buf + *(long*)dh->buf.block_buf; data_ptr += *(long*)data_ptr){
         if(!strcmp(key, (char*)(data_ptr + (sizeof(size_t))))){
            dh->block.data_ptr      = data_ptr - dh->buf.block_buf; 
            dh->block.block_ptr     = block_ptr;
            dh->block.pre_block_ptr = pre_block_ptr;

         #ifdef _DEBUG
            printf("FIND!\n");
         #endif

            return SUCCESS;
         }
      }

      pre_block_ptr = block_ptr;
      block_ptr = next_block;
   }

   #ifdef _DEBUG
      printf("NOT FOUND DATA\n");
   #endif


   return NOT_FOUND;
}

int search_enough_space_block(int space, _DISK_HASH *dh){
   if(!dh){
   #ifdef _DEBUG
      printf("ERROR find Parameter!\n");
   #endif

      return ERR_PARAMETER;
   }

   long block_ptr     = *(dh->buf.table_buf + dh->block.table_ptr);
   long pre_block_ptr = -1;

   fflush(dh->buf.data);
   for(; block_ptr != -1;){
      fseek(dh->buf.data, block_ptr, SEEK_SET);
      if(BLOCK_SIZE != fread(dh->buf.block_buf, 1, BLOCK_SIZE, dh->buf.data)){
      #ifdef _DEBUG
         printf("ERROR ADD READ!\n");
      #endif

         return ERR_READ_FILE;
      }

      if(*(long*)dh->buf.block_buf + space <= BLOCK_SIZE){
         break;
      }

      pre_block_ptr  =  block_ptr;
      block_ptr      =  *(long*)(dh->buf.block_buf + sizeof(long));
   }

   dh->block.block_ptr     = block_ptr;
   dh->block.pre_block_ptr = pre_block_ptr;

   //not find enough space block
   if(dh->block.block_ptr == -1){
      int ret_val = add_new_block_to_data(dh);
      if(ret_val){
      #ifdef _DEBUG
         printf("ERROR add call add_new_block_to_data return number : %d !\n", ret_val);
      #endif

         return ret_val;
      }
   }


   return SUCCESS;
}

int add_new_block_to_data(_DISK_HASH *dh){
   struct stat info;

   fflush(dh->buf.data);
   if(stat(DATA_FILE_NAME, &info)){
   #ifdef _DEBUG
      printf("%s\n", strerror(errno));
   #endif
      
      return errno;
   }

   dh->block.block_ptr = info.st_size;

   //pre block is table?
   if(dh->block.pre_block_ptr == -1){
      *(dh->buf.table_buf + dh->block.table_ptr) = dh->block.block_ptr;
   }
   else{
      //renew pre block
      *(long*)(dh->buf.block_buf + BLOCK_TOTAL_SIZE) = dh->block.block_ptr;

      fseek(dh->buf.data, dh->block.pre_block_ptr, SEEK_SET);
      if(BLOCK_SIZE != fwrite(dh->buf.block_buf, 1, BLOCK_SIZE, dh->buf.data)){
      #ifdef _DEBUG
         printf("ERROR add WRITE pre block DATA\n");
      #endif

         return ERR_WRITE;
      }
   }

   //initialize buffer
   *(long*)dh->buf.block_buf                       =  BLOCK_TOTAL_SIZE + NEXT_SIZE;
   *(long*)(dh->buf.block_buf + BLOCK_TOTAL_SIZE)  =  -1;


   return SUCCESS;
}

int find(const char *key, _DISK_HASH *dh, _DATA *result){
   if(!dh || !result || !key){
   #ifdef _DEBUG
      printf("ERROR find Parameter!\n");
   #endif

      return ERR_PARAMETER;
   }

   int ret_val = search_data(key, dh);

   if(!ret_val && result){
      void *ptr = dh->buf.block_buf + dh->block.data_ptr;

      ret_val = copy_to_data(ptr, result);
      if(ret_val){
      #ifdef _DEBUG
         printf("ERROR find call copy to data return number : %d !\n", ret_val);
      #endif
      }
   }


   return ret_val;
}

int del(const char *key, _DISK_HASH *dh){
   if(!dh || !key){
   #ifdef _DEBUG
      printf("ERROR del Parameter!\n");
   #endif

      return ERR_PARAMETER;
   }

   int ret_val = search_data(key, dh);
   if(ret_val){
      return ret_val;
   }

   void *s_ptr = dh->buf.block_buf + dh->block.data_ptr;
   void *f_ptr = s_ptr + *(long*)s_ptr;
   *(long*)dh->buf.block_buf -= *(long*)s_ptr;
   memcpy(s_ptr, f_ptr, *(long*)dh->buf.block_buf - (dh->block.data_ptr));
   

   fflush(dh->buf.data);

   //empty block and pre is not table?
   if(*(long*)dh->buf.block_buf == BLOCK_TOTAL_SIZE + NEXT_SIZE && dh->block.pre_block_ptr != -1){
      long temp = *(long*)(dh->buf.block_buf + BLOCK_TOTAL_SIZE);
      fseek(dh->buf.data, dh->block.pre_block_ptr, SEEK_SET);
      if(BLOCK_SIZE != fread(dh->buf.block_buf, 1, BLOCK_SIZE, dh->buf.data)){
      #ifdef _DEBUG
         printf("ERROR del READ 2!\n");
      #endif

         return ERR_READ_FILE;
      }

      *(long*)(dh->buf.block_buf + BLOCK_TOTAL_SIZE) = temp;

      fseek(dh->buf.data, dh->block.pre_block_ptr, SEEK_SET);
   }
   else{
   #ifdef _DEBUG
      if(*(long*)dh->buf.block_buf < BLOCK_TOTAL_SIZE + NEXT_SIZE){
         printf("block total error!\n");
         return -1000;
      }
   #endif

      fseek(dh->buf.data, dh->block.block_ptr, SEEK_SET); 
   }

   if(BLOCK_SIZE != fwrite(dh->buf.block_buf, 1, BLOCK_SIZE, dh->buf.data)){
      #ifdef _DEBUG
         printf("del ERROR add WRITE 1\n");
      #endif

      return ERR_WRITE;
   }

   dh->block.data_ptr      = -1;
   

   return ret_val;
}

int add(const char *key, const void *val, const size_t val_size, const char type, _DISK_HASH *dh){
   if(!dh || !key){
   #ifdef _DEBUG
      printf("ERROR del Parameter!\n");
   #endif

      return ERR_PARAMETER;
   }

   //total + key + val_size + val + type;
   size_t total_size = sizeof(size_t) + strlen(key) + 1 + sizeof(size_t) + val_size + sizeof(char);

   int ret_val = search_data(key, dh);
   if(ret_val && ret_val != NOT_FOUND){
   #ifdef _DEBUG
      printf("ERROR add return number : %d !\n", ret_val);
   #endif

      return ret_val;
   }

   //found
   if(!ret_val){
      ret_val = del(key, dh);
      if(ret_val){
      #ifdef _DEBUG
         printf("error add call del : %d\n", ret_val);
      #endif

         return ret_val;
      }
   }

   //not found and not enough space
   if(ret_val || *(long*)dh->buf.block_buf + total_size > BLOCK_SIZE){
      ret_val = search_enough_space_block(total_size, dh);
      if(ret_val){
      #ifdef _DEBUG
         printf("ERROR add call search_enough_space_block return number : %d !\n", ret_val);
      #endif

         return ret_val;
      }
   }

   void *data_ptr             =  dh->buf.block_buf + *(long*)dh->buf.block_buf;
   dh->block.data_ptr         =  *(long*)dh->buf.block_buf;
   *(long*)dh->buf.block_buf  += total_size;

   ret_val = copy_to_buffer(data_ptr, total_size, key, val, val_size, type);
   if(ret_val){
   #ifdef _DEBUG
      printf("ERROR add return number : %d !\n", ret_val);
   #endif

      return ret_val;
   }

   fflush(dh->buf.data);
   fseek(dh->buf.data, dh->block.block_ptr, SEEK_SET);
   if(BLOCK_SIZE != fwrite(dh->buf.block_buf, 1, BLOCK_SIZE, dh->buf.data)){
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


   return (int)(hash % TABLE_NUMBER);
}

int copy_to_data(void *ptr, _DATA *result){
   if(!ptr || !result){
   #ifdef _DEBUG
      printf("ERROR find Parameter!\n");
   #endif

      return ERR_PARAMETER;
   }

   result->total_size = *(size_t*)ptr;
   ptr += sizeof(size_t);

   int tmp = strlen((char*)ptr) + 1;
   strncpy(result->key, (char*)ptr, tmp);
   ptr += tmp;

   result->val_size = *(size_t*)ptr;
   ptr += sizeof(result->val_size);

   memcpy(result->val, ptr, result->val_size);
   ptr += result->val_size;

   result->type = *(char*)ptr;
   ptr++;


   return SUCCESS;
}

int copy_to_buffer( void *ptr, size_t total_size, const char *key, const void *val, const size_t val_size, const char type){
   if(!ptr){
   #ifdef _DEBUG
      printf("ERROR find Parameter!\n");
   #endif

      return ERR_PARAMETER;
   }

   *(size_t*)ptr =  total_size;
   ptr         += sizeof(size_t);

   int len  = strlen(key) + 1;
   strncpy(ptr, key, len);
   ptr      += len;

   *(size_t*)ptr =  val_size;
   ptr         += sizeof(size_t);

   memcpy(ptr, val, val_size);
   ptr         += val_size;

   *(char*)ptr++ =  type;


   return SUCCESS;
}

/* 
int reorganize(_files *fp){
   if(!fp){
      return ERR_PARAMETER;
   }

   int ret_val = 0;
   ret_val = close_table(fp);
      return ret_val;
      if(ret_val){;
   }
   ret_val = open_table(fp);
   if(ret_val){
      return ret_val;
   }

   FILE *data_fp = fopen("data/data_temp","w+");
   if(!data_fp)
         printf("reorganize open error!\n");
         #ifdef _DEBUG;
      #endif

      return ERR_OPEN_FILE;
   }
   if(1 != fwrite("-", 1, 1, data_fp)){
      #ifdef _DEBUG
         printf("ERROR reorganize WRITE DATA\n");
      #en
      goto ERROR1;
      ;
      ret_val = ERR_WRITE;
   }

   DATA_FILE_NAMEck_buf = my_malloc(BLOCK_SIZE);
   if(!block_buf){
      #ifdef _DEBUG
         printf("reorganize malloc error!\n");
      #end
      goto ERROR;
      ;
      ret_val = ERR_MY_MALLOC;
   } 

   void *tempDATA_FILE_NAMEalloc(BLOCK_SIZE);
   if(!temp_buf){
      #ifdef _DEBUG
         printf("DATA_FILE_NAMEize malloc error!\n");
      #endif

      goto ERROR1;
      ret_val = ERR_MY_MALLOC;
   }

   struct stat info;
   fflush(data_fp);
   if(stat("data/data_temp", &info)){
      #ifdef _DEBUG
         printf("%s\n", strerror(errno));
      #endif

      goto ERROR;
      ret_val = errno;
   } 

   size_t new_block_ptr = 1;

   for(int i = 0; i < TABLE_SIZE / NEXT_SIZE; i++){
      size_t old_block_ptr = dh->buf.table_buf[i];

      if(!old_block_ptr){
         continue;
      }
      
      //find first not empty block
      for(; old_block_ptr;){
         fseek(dh->buf.data, old_block_ptr, SEEK_SET);
         if(BLOCK_SIZE != fread(dh->buf.block_buf, 1, BLOCK_SIZE, dh->buf.data)){
            #ifdef _DEBUG
               printf("reorganize read error!\n");
            #endif

            goto ERROR2;
            ret_val = ERR_READ_FILE;
         }

         old_block_ptr = *(long*)(dh->buf.block_buf + BLOCK_TOTAL_SIZE);

         if(*(long*)dh->buf.block_buf > BLOCK_TOTAL_SIZE + NEXT_SIZE){
            break;
         }
      }

      //not found
      if(*(long*)dh->buf.block_buf == BLOCK_TOTAL_SIZE + NEXT_SIZE){
         dh->buf.table_buf[i] = 0;
         continue;
      }

      //find!
      dh->buf.table_buf[i] = new_block_ptr;
      for(; old_block_ptr;){
         fseek(dh->buf.data, old_block_ptr, SEEK_SET);
         if(BLOCK_SIZE != fread(temp_buf, 1, BLOCK_SIZE, dh->buf.data)){
            #ifdef _DEBUG
               printf("reorganize read error!\n");
            #endif

            goto ERROR2;
            ret_val = ERR_READ_FILE;
         }

         old_block_ptr = *(size_t*)(temp_buf + BLOCK_TOTAL_SIZE);

         if(*(size_t*)temp_buf == BLOCK_TOTAL_SIZE + NEXT_SIZE){
            continue;
         }

         *(long*)(dh->buf.block_buf + BLOCK_TOTAL_SIZE) = new_block_ptr + BLOCK_SIZE;

         fflush(data_fp);
         fseek(data_fp, new_block_ptr, SEEK_SET);
         if(BLOCK_SIZE != fwrite(dh->buf.block_buf, 1, BLOCK_SIZE, data_fp)){
            #ifdef _DEBUG
               printf("ERROR reorganize WRITE DATA\n");
            #endif

            goto ERROR2;
            ret_val = ERR_WRITE;
         }

         new_block_ptr += BLOCK_SIZE;

         memcpy(dh->buf.block_buf, temp_buf, BLOCK_SIZE);
      }
      
      *(long*)(dh->buf.block_buf + BLOCK_TOTAL_SIZE) = 0;
      if(BLOCK_SIZE != fwrite(dh->buf.block_buf, 1, BLOCK_SIZE, data_fp)){
         #ifdef _DEBUG
            printf("ERROR reorganize WRITE DATA\n");
         #endif

         goto ERROR2;
         ret_val = ERR_WRITE;
      }

      new_block_ptr += BLOCK_SIZE;
   }

   ret_val = close_table(fp);
   if(ret_val){
      goto ERROR2;
   }

   if(rename("data/data_temp","data/data")){
      #ifdef _DEBUG
         printf("ERROR rename DATA\n");
      #endif

      goto ERROR2;
      ret_val = ERR_RENAME;
   }

   ret_val = open_table(fp);
   if(ret_val){
      goto ERROR2;
   }

   ERROR2:
   my_free(temp_buf);
   ERROR1:
   fclose(data_fp);
   //my_free(block_buf);
   

   return ret_val;
} 
*/