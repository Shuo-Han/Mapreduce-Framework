#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <pthread.h>
#include "mapreduce.h"

#define INIT_LIST_SIZE 256
#define NUM_NODE 10000
#define A 70123
#define B 76777
#define FIRSTH 37


#define min(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a < _b ? _a : _b; })

typedef struct Knode {
  char *key;
  char *value;
} Knode;

typedef struct Dnode {
  char *key;
  int loc;
  struct Dnode* next;
} Dnode;

typedef struct Mnode {
  char *filename;
  int size;
  struct Mnode *next;
} Mnode;

typedef struct Argpool {
  Mnode *m;
  Mapper map;
  int par;
  Reducer reduce;
} Argpool;

Knode **plist;
Dnode ***dist_key;
Dnode **cur_reduce;
char ***dist_dic;
Dnode *back;
int *dist_len;
int *len_list;
int *capacity;
Mapper glmap;
Reducer glreduce;
Partitioner userpar;
int mynumpar;


static pthread_mutex_t mutex;

void printer(){
  for (size_t j = 0; j < mynumpar; j++) {
    for (size_t i = 0; i < len_list[j]; i++) {
      printf("par: %d, key: %s, value: %s\n", (int)j, plist[j][i].key, plist[j][i].value);
    }
  }
}

void print_error(char *error_message, int code) {
    if(write(STDERR_FILENO, error_message, strlen(error_message))) {};
    exit(code);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

void MR_Emit(char *key, char *value) {
  // if (!strcmp(key, "")) return;
  unsigned long index = userpar(key, mynumpar);
  // printf("%ld, %d\n", index, len_list[index]);
  if (capacity[index] <= len_list[index]) {
    capacity[index] *= 2;
    plist[index] = realloc(plist[index], capacity[index] * sizeof(Knode));
  }
  plist[index][len_list[index]].key = strdup(key);
  plist[index][len_list[index]].value = strdup(value);
  len_list[index] += 1;
}

int get_hash_code(char * s) {
   unsigned h = FIRSTH;
   while (*s) {
     h = (h * A) ^ (s[0] * B);
     s++;
   }
   return h % NUM_NODE;
}

char* get_next(char *key, int partition_number) {
  int s = 1;
  Knode *arr = plist[partition_number];
  if (back == NULL || strcmp(back->key, key)) {
    s = 0;
    int index = get_hash_code(key);
    Dnode *cur = dist_key[partition_number][index];
    while (cur) {
      if (!strcmp(cur->key, key)) {
        back = cur;
        s = 1;
        break;
      }
      cur = cur->next;
    }
  }

  if (!s)
    print_error("get_next:should not get NULL\n", 1);
  if (back->loc >= len_list[partition_number]) return NULL;
  Knode cur2 = arr[back->loc];

  if (!strcmp(cur2.key, key)) {
    back->loc += 1;
    return cur2.value;
  }

  return NULL;
}



long int findSize(char *file_name)
{
    struct stat st; /*declare stat variable*/

    if(stat(file_name,&st)==0)
        return (st.st_size);
    else
        return -1;
}

void addtolist(Mnode **map_par, int map_parnum, Mnode *filenode) {
  Mnode *cur = map_par[map_parnum];
  if (NULL == cur) {
    map_par[map_parnum] = filenode;
    return;
  } else if (cur->size > filenode->size){
    filenode->next = cur;
    map_par[map_parnum] = filenode;
    return;
  }

  while (cur->next != NULL) {
    if (cur->next->size > filenode->size) {
      filenode->next = cur->next;
      cur->next = filenode;
      return;
    } else
      cur = cur->next;
  }

  cur->next = filenode;
}

void *map_start(void *arg) {
  Argpool *p = (Argpool*)arg;
  Mnode *file = p->m;
  while (file != NULL) {
    Mapper map = p->map;
    pthread_mutex_lock(&mutex);
    (*map)(file->filename);
    pthread_mutex_unlock(&mutex);
    file = file->next;
  }
  return NULL;
}

void insert_to_hash(Dnode **tb, Dnode *newnode) {
  int index = get_hash_code(newnode->key);
  if (NULL == tb[index]) {
    tb[index] = newnode;
  } else {
    Dnode *cur = tb[index];
    if (!strcmp(cur->key, newnode->key) != 0)
      print_error("insert_to_hash should not receive existing key!\n", 1);
    while (cur->next) {
      cur = cur->next;
      if (!strcmp(cur->key, newnode->key) != 0)
        print_error("insert_to_hash should not receive existing key!\n", 1);
    }
    cur->next = newnode;
  }
}

void get_distkey(Dnode **out, Knode *arr, int len, int par) {
  if (0 == len) return;
  Dnode *cur = malloc(sizeof(Dnode));
  cur->key = strdup(arr[0].key);
  cur->loc = 0;
  cur->next = NULL;
  dist_dic[par] = malloc(sizeof(char*) * len);
  dist_len[par] = 0;

  insert_to_hash(out, cur);
  dist_dic[par][dist_len[par]] = strdup(arr[0].key);
  dist_len[par] = 1;

  Dnode *last = cur;
  for (size_t i = 1; i < len; i++) {
    if (strcmp(last->key, arr[i].key)) {
      cur = malloc(sizeof(Dnode));
      cur->key = strdup(arr[i].key);
      cur->loc = i;
      cur->next = NULL;
      insert_to_hash(out, cur);
      dist_dic[par][dist_len[par]] = strdup(arr[i].key);
      dist_len[par] += 1;
      last = cur;
    }
  }
}

void printdist(Dnode **dist) {
  for (size_t i = 0; i < NUM_NODE; i++) {
    if (dist[i]) {
        Dnode *cur = dist[i];
        printf("%ld, %s", i, cur->key);
        while (cur->next) {
          cur = cur->next;
          printf("%s ", cur->key);
        }
        printf("\n");
    }
  }
}

void *reduce_start(void *arg) {
  Argpool *p = (Argpool*)arg;
  Knode *cur = plist[p->par];
  Reducer reduce = p->reduce;
  int len = len_list[p->par];
  dist_key[p->par] = malloc(sizeof(Dnode*) * NUM_NODE);
  memset(dist_key[p->par], 0, sizeof(Dnode*) * NUM_NODE);

  get_distkey(dist_key[p->par], cur, len, p->par);

  for (size_t i = 0; i < dist_len[p->par]; i++) {
    int s = 0;
    char *cur_key = dist_dic[p->par][i];
    int index = get_hash_code(cur_key);
    cur_reduce[p->par] = dist_key[p->par][index];
    while (cur_reduce[p->par]) {
      if (!strcmp(cur_key, cur_reduce[p->par]->key)) {
        pthread_mutex_lock(&mutex);
        (*reduce)(cur_reduce[p->par]->key, get_next, p->par);
        pthread_mutex_unlock(&mutex);
        s = 1;
        break;
      }
      cur_reduce[p->par] = cur_reduce[p->par]->next;
    }
    if (!s)
      print_error("reduce_start: should not be here.\n", 1);
  }

  return NULL;
}

void mapthinit(int num, Mnode **s) {
  pthread_t *thread_group = malloc(sizeof(pthread_t) * num);
  Argpool args[num];
  for (size_t i = 0; i < num; i++) {
    args[i].map = glmap;
    args[i].m = s[i];
    if(!pthread_create(&thread_group[i], NULL, map_start, &args[i])){}
  }
  for (size_t i = 0; i < num; i++) {
    pthread_join(thread_group[i], NULL);
  }
  free(thread_group);
}

void reducethinit(int num) {
  pthread_t *thread_group_r = malloc(sizeof(pthread_t) * num);
  cur_reduce = malloc(sizeof(Dnode*) * mynumpar);
  memset(cur_reduce, 0, sizeof(Dnode*) * mynumpar);
  Argpool args[num];
  for (size_t i = 0; i < num; i++) {
    args[i].reduce = glreduce;
    args[i].par = i;
    if(pthread_create(&thread_group_r[i], NULL, reduce_start, &args[i]) != 0){
    }
  }
  for (size_t i = 0; i < num; i++) {
    pthread_join(thread_group_r[i], NULL);
  }
  free(cur_reduce);
  cur_reduce = NULL;
  free(thread_group_r);
}

int compareto(const void *a, const void *b) {
  const Knode *left = a, *right= b;
  int rst = strcmp(left->key, right->key);
  if ( rst != 0 )
    return rst;
  return strcmp(left->value, right->value);
}

void free_mnode(Mnode** l, int len) {
  Mnode *tmp, *tmp2;
  for (size_t i = 0; i < len; i++) {
    tmp = l[i];
    while (tmp){
      tmp2 = tmp->next;
      free(tmp->filename);
      free(tmp);
      tmp = tmp2;
    }
  }
  free(l);
  l = NULL;
}

void free_all_data() {
  for (size_t i = 0; i < mynumpar; i++) {
    for (size_t j = 0; j < NUM_NODE; j++) {
      Dnode *cur = dist_key[i][j], *tmp;
      while (cur) {
        tmp = cur->next;
        free(cur->key);
        free(cur);
        cur = tmp;
      }
    }

    for (size_t k = 0; k < dist_len[i]; k++) {
      free(dist_dic[i][k]);
    }

    for (size_t m = 0; m < len_list[i]; m++) {
      free(plist[i][m].key);
      free(plist[i][m].value);
    }

    free(dist_dic[i]);
    free(plist[i]);
    free(dist_key[i]);
  }


  free(dist_dic);
  dist_dic = NULL;
  free(plist);
  plist = NULL;
  free(dist_key);
  dist_key = NULL;

  free(capacity);
  free(len_list);
  free(dist_len);
  capacity = NULL;
  len_list = NULL;
  dist_len = NULL;
  back = NULL;
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers,
	    Reducer reduce, int num_reducers, Partitioner partition) {
  if (argc < 2)
    print_error("No input specified\n", 0);

  int map_parnum;
  userpar = partition;
  mynumpar = num_reducers;
  num_mappers = min(num_mappers, argc-1);
  dist_key = malloc(sizeof(Dnode**) * mynumpar);
  memset(dist_key, 0, sizeof(Dnode**) * mynumpar);
  capacity = malloc(sizeof(int) * mynumpar);
  len_list = malloc(sizeof(int) * mynumpar);
  dist_len = malloc(sizeof(int) * mynumpar);
  plist = malloc(sizeof(Knode*) * mynumpar);
  memset(plist, 0, sizeof(Knode*) * mynumpar);
  dist_dic = malloc(sizeof(char**) * mynumpar);
  memset(dist_dic, 0, sizeof(char**) * mynumpar);
  for (size_t i = 0; i < mynumpar; i++) {
    plist[i] = malloc(sizeof(Knode) * INIT_LIST_SIZE);
    memset(plist[i], 0, sizeof(Knode) * INIT_LIST_SIZE);
    capacity[i] = INIT_LIST_SIZE;
    len_list[i] = 0;
    dist_len[i] = 0;
  }


  Mnode **map_par = malloc(sizeof(Mnode*) * num_mappers);
  memset(map_par, 0, sizeof(Mnode*) * num_mappers);
  Mnode *filenode;

  for(int i = 1; i < argc; i++) {
    map_parnum = userpar(argv[i], num_mappers);
    filenode = malloc(sizeof(Mnode));
    memset(filenode, 0, sizeof(Mnode));
    filenode->filename = strdup(argv[i]);
    filenode->size = findSize(argv[i]);
    addtolist(map_par, map_parnum, filenode);
  }

  pthread_mutex_init(&mutex, NULL);
  glmap = map;

  mapthinit(num_mappers, map_par);

  for (size_t i = 0; i < mynumpar; i++) {
    qsort(plist[i], len_list[i], sizeof(Knode), compareto);
  }

  // printer();

  glreduce = reduce;
  reducethinit(mynumpar);

  free_mnode(map_par, num_mappers);
  free_all_data();
  // clock_t start, end;
  // double cpu_time_used;
  // pthread_mutex_init(&mutex, NULL);
  // glmap = map;
  //
  // start = clock();
  // mapthinit(num_mappers, map_par);
  // end = clock();
  //
  // cpu_time_used = ((double) (end - start)) / (CLOCKS_PER_SEC/1000);
  // printf("** %.3f ms elapsed for MAPPER **\n", cpu_time_used);
  //
  // start = clock();
  // for (size_t i = 0; i < mynumpar; i++) {
  //   qsort(plist[i], len_list[i], sizeof(Knode), compareto);
  // }
  // end = clock();
  //
  // cpu_time_used = ((double) (end - start)) / (CLOCKS_PER_SEC/1000);
  // printf("** %.3f ms elapsed for SORTING **\n", cpu_time_used);
  // glreduce = reduce;
  // start = clock();
  // reducethinit(mynumpar);
  // end = clock();
  //
  // cpu_time_used = ((double) (end - start)) / (CLOCKS_PER_SEC/1000);
  // printf("** %.3f ms elapsed for Reduce **\n", cpu_time_used);
  //
  // free_plist(plist, mynumpar);
}
