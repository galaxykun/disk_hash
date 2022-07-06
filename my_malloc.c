#include "my_malloc.h"


//25 * 10M
static int MAX_ALLOC_TIME = 10;

static header *head;     //the head of list

//1 << 0  = 1 * 48 = 48 = 1 units;
//1 << 5  = 32 * 48 = 1530 = 1.53k;
//1 << 18 = 262,144 * 48 = 12,582,912 = 12M;
//1 << 19 = 524,288 * 48 = 25,165,824 = 25M
header *table[TABLE_NUM];

bound *limits;

int return_num;
/*
int main(){
   #ifdef _DEBUG
      MAX_ALLOC_TIME = 10000;
      srand(time(NULL));

      int **array = (int**)my_malloc(DEBUG_NUM * sizeof(int*));
      for(int i = 0; i < DEBUG_NUM; i++){
         int temp = rand() % DEBUG_SIZE;
         array[i] = (int *)my_malloc(temp * sizeof(int) + 1);
         array[i][0] = temp;
      }

      for(int i = 0; i < DEBUG_NUM; i++){
         printf("<%7d> ",array[i][0]);
      }
      printf("\n");
      for(int i = 0; i < 100; i++){
         printf("-");
      }
      printf("\n");

      print_node();

      printf("\n");
      for(int i = 0; i < 100; i++){
         printf("-");
      }
      printf("\n");

      
      for(int i = 0; i < DEBUG_NUM; i++){
         int j = rand() % DEBUG_NUM;
         int n = my_free(array[j]);
         if(n){
            printf("ERROR!\n");
         }
         else{
            printf("SUCCESS!\n");
         }
      }

      printf("\n");
      for(int i = 0; i < 100; i++){
         printf("-");
      }
      printf("\n");

      print_node();

      printf("\n");
      for(int i = 0; i < 100; i++){
         printf("-");
      }
      printf("\n");
   #endif
  
  
   return 0;
}
*/
void *my_malloc(size_t num){
   if(!MAX_ALLOC_TIME){
      return malloc(num);
   }

   size_t units = (num + sizeof(header) - 1) / sizeof(header) + 1;
   header *curr = NULL;

   //search
   int index = -1;
   for(int i = 0; i < TABLE_NUM; i++){
      if(table[i] && (1 << i) >= units){
         index = i;
         curr =  table[i];
         break;
      }
   }
   //>12M
   if(index == TABLE_NUM - 1){
      for(; curr && curr->size < units; curr = curr->tnext);
   }
   //if not enough space
   if(!curr){
      return_num = morecore(units);
      if(return_num){
         return NULL;
      }
      curr = table[TABLE_NUM - 1];
      index = TABLE_NUM - 1;
   }

   //remove form table
   remove_from_table(curr);

   //split and renew table, linked list, info
   header *result = NULL;
   
   if(curr->size == units){
      result = curr;
      result->using = TRUE;
   }
   //split
   else{
      curr->size = (curr->size - units);
      result = curr + curr->size;
      result->size = units;

      result->hpre = curr;
      result->hnext = curr->hnext;
      if(curr->hnext){
         curr->hnext->hpre = result;
      }
      curr->hnext = result;
      result->using = TRUE;

      //insert table
      insert_table(curr);
   }


   return (void*)(result + 1);
}

int my_free(void *p){
   //search bound, if exceed the limit return error
   bound *l_ptr = limits;
   for(; l_ptr; l_ptr = l_ptr->next){
      if(p >= l_ptr->start && p <= l_ptr->end){
         break;
      }
   }
   if(!l_ptr){
      free(l_ptr);
      return SUCCESS;
   }

   header *curr = (header*)p - 1;
   
   #ifdef _DEBUG
      if(!curr->using){
         return ERR_POINT;
      }
   #endif

   curr->using = FALSE;

   //新的記憶體連接，且判斷是不是第一次
   if(!curr->hpre && !curr->hnext && head != curr){
      header *ptr = head;
      for(; ptr->hnext && ptr->hnext < curr; ptr = ptr->hnext);

      header *left = ptr, *right = ptr->hnext;
      if(ptr == head && curr < head){
         left = NULL; right = ptr;
         head = curr;
      }

      if(left){
         left->hnext = curr;
      }
      if(right){
         right->hpre = curr;
      }
      
      curr->hpre = left;
      curr->hnext = right;
   }

   //merge 要有記憶體且要是連續記憶體且空的
   header *left = curr->hpre, *right = curr->hnext;
   if(right && !right->using && curr + curr->size == right){
      remove_from_table(right);

      curr->hnext = right->hnext;
      if(right->hnext){
         right->hnext->hpre = curr;
      }

      curr->size += right->size;
   }
   if(left && !left->using && left + left->size == curr){
      remove_from_table(left);

      left->hnext = curr->hnext;
      if(curr->hnext){
         curr->hnext->hpre = left;
      }

      left->size += curr->size;

      curr = left;
   }

   //insert table
   insert_table(curr);


   return SUCCESS;
}

int morecore(size_t num){
   if(num < MIN_ALLOC_SIZE){
      num = MIN_ALLOC_SIZE;
   }

   header *ptr = (header*)malloc(num * sizeof(header));
   if(!ptr){
      return ERR_OUT_OF_MEM;
   }
   ptr->hpre = NULL; ptr->hnext = NULL;
   ptr->tpre = NULL; ptr->tnext = NULL;
   ptr->size = num; ptr->using = TRUE;

   if(!head){
      head = ptr;
   }

   //renew bound
   bound *l_ptr = limits;
   if(!limits){
      limits = (bound*)malloc(sizeof(bound));
      l_ptr = limits; 
   }
   else{
      for(; l_ptr->next; l_ptr = l_ptr->next);
      l_ptr->next = (bound*)malloc(sizeof(bound));
      l_ptr = l_ptr->next;
   }
   l_ptr->start = (void*)ptr;
   l_ptr->end = (void*)(ptr + num);
   l_ptr->next = NULL;
   
   my_free(ptr + 1);

   MAX_ALLOC_TIME--;


   return SUCCESS;
}

void remove_from_table(header *ptr){
   int index = -1;
   for(int i = TABLE_NUM - 1; i > -1; i--){
      //search
      if((1 << i) <= ptr->size){
         index = i;
         break;
      }
   }
   
   if(ptr->tpre){
      ptr->tpre->tnext = ptr->tnext;
   }
   else{
      table[index] = ptr->tnext;
   }
   if(ptr->tnext){
      ptr->tnext->tpre = ptr->tpre;
   }
   ptr->tpre = NULL; ptr->tnext = NULL;


   return;
}

void insert_table(header *ptr){
   for(int i = TABLE_NUM - 1; i > -1; i--){
      if((1 << i) <= ptr->size){
         if(table[i]){
            ptr->tnext = table[i];
            table[i]->tpre = ptr;
         }
         ptr->tpre = NULL;
         table[i] = ptr;

         break;
      }
   }


   return;
}

void print_node(){
   

   header *curr = head;
   printf("Linked List : \n");
   for(int i = 0; curr; curr = curr->hnext, i++){
      printf("index : %4d\tmem : %p\tsize : %7zu\tusing : %d\n", i, curr, curr->size, curr->using);
   }

   printf("\nTable : \n");
   for(int i = 0, j = 0; i < TABLE_NUM; i++){
      printf("\ttable %2d : \n", i);
      curr = table[i];
      for(; curr; curr = curr->tnext){
         printf("index : %4d\tmem : %p\tsize : %7zu\tusing : %d\n", j++, curr, curr->size, curr->using);
      }
   }

   bound *temp = limits;
   printf("Bound : \n");
   for(int i = 0; temp; temp = temp->next, i++){
      printf("index : %4d\tstart : %p\tend : %p\tsize : %u\n", i, temp->start, temp->end, temp->end - temp->start);
   }
   printf("\n");

   return;
}