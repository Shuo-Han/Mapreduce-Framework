#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <pthread.h>
#include "mapreduce.h"

#define INIT_LIST_SIZE 256

#define min(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a < _b ? _a : _b; })

// typedef struct Knode {
//   char *value;
//   struct Knode *next;
// } Knode;

typedef struct Knode {
  char *key;
  char *value;
} Knode;


typedef struct Dnode {
  char *key;
  int loc;
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
Dnode **dist_key;
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
  Knode newnode = { .key = strdup(key), .value = strdup(value)};
  unsigned long index = userpar(key, mynumpar);
  if (capacity[index] <= len_list[index]) {
    capacity[index] *= 2;
    plist[index] = realloc(plist[index], capacity[index] * sizeof(Knode));
  }
  plist[index][len_list[index]] = newnode;
  len_list[index] += 1;
}


char* get_next(char *key, int partition_number) {
  static Dnode back;
  Knode *arr = plist[partition_number];
  if (back.key == NULL || strcmp(back.key, key)) {
    for (size_t i = 0; i < dist_len[partition_number]; i++) {
      if (!strcmp(key, dist_key[partition_number][i].key)) {
        back = dist_key[partition_number][i];
      }
    }
  }

  if (back.loc >= len_list[partition_number]) return NULL;
  Knode cur = arr[back.loc];

  if (!strcmp(cur.key, key)) {
    back.loc += 1;
    return cur.value;
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

int get_distkey(Dnode *out, Knode *arr, int len) {
  if (0 == len) return 0;
  Dnode cur = {.key = strdup(arr[0].key), .loc = 0};
  out[0] = cur;
  int newlen = 1;
  for (size_t i = 1; i < len; i++) {
    if (strcmp(out[newlen-1].key, arr[i].key)) {
      Dnode cur = {.key = strdup(arr[i].key), .loc = i};
      out[newlen] = cur;
      newlen += 1;
    }
  }
  return newlen;
}

void *reduce_start(void *arg) {
  Argpool *p = (Argpool*)arg;
  Knode *cur = plist[p->par];
  Reducer reduce = p->reduce;
  int len = len_list[p->par];
  dist_key[p->par] = malloc(sizeof(Dnode) * len);
  memset(dist_key[p->par], 0, sizeof(Dnode) * len);

  dist_len[p->par] = get_distkey(dist_key[p->par], cur, len);
  for (size_t i = 0; i < dist_len[p->par]; i++) {
    pthread_mutex_lock(&mutex);
    (*reduce)(dist_key[p->par][i].key, get_next, p->par);
    pthread_mutex_unlock(&mutex);
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
  free(thread_group_r);
}

void free_mnode(Mnode** l, int len) {
  Mnode *tmp, *tmp2;
  for (size_t i = 0; i < len; i++) {
    tmp = l[i];
    while (tmp != NULL){
      // free(tmp->filename);
      tmp2 = tmp->next;
      free(tmp);
      tmp = tmp2;
    }
  }
  free(l);
}

int compareto(const void *a, const void *b) {
  const Knode *left = a, *right= b;
  int rst = strcmp(left->key, right->key);
  if ( rst != 0 )
    return rst;
  return strcmp(left->value, right->value);
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers,
	    Reducer reduce, int num_reducers, Partitioner partition) {
  if (argc < 2)
    print_error("No input specified\n", 0);

  int map_parnum;
  userpar = partition;
  mynumpar = num_reducers;
  num_mappers = min(num_mappers, argc-1);
  dist_key = malloc(sizeof(Knode*) * mynumpar);
  memset(dist_key, 0, sizeof(Knode*) * mynumpar);
  capacity = malloc(sizeof(int) * mynumpar);
  len_list = malloc(sizeof(int) * mynumpar);
  dist_len = malloc(sizeof(int) * mynumpar);
  plist = malloc(sizeof(Knode*) * mynumpar);
  memset(plist, 0, sizeof(Knode*) * mynumpar);
  for (size_t i = 0; i < mynumpar; i++) {
    plist[i] = malloc(sizeof(Knode) * INIT_LIST_SIZE);
    capacity[i] = INIT_LIST_SIZE;
    len_list[i] = 0;
    dist_len[i] = 0;
  }


  Mnode **map_par = NULL;
  map_par = malloc(sizeof(Mnode*) * num_mappers);

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

  clock_t start, end;
  double cpu_time_used;
  pthread_mutex_init(&mutex, NULL);
  glmap = map;

  start = clock();
  mapthinit(num_mappers, map_par);
  end = clock();

  cpu_time_used = ((double) (end - start)) / (CLOCKS_PER_SEC/1000);
  printf("** %.3f ms elapsed for MAPPER **\n", cpu_time_used);

  start = clock();
  for (size_t i = 0; i < mynumpar; i++) {
    qsort(plist[i], len_list[i], sizeof(Knode), compareto);
  }
  end = clock();

  cpu_time_used = ((double) (end - start)) / (CLOCKS_PER_SEC/1000);
  printf("** %.3f ms elapsed for SORTING **\n", cpu_time_used);
  glreduce = reduce;
  start = clock();
  reducethinit(mynumpar);
  end = clock();

  cpu_time_used = ((double) (end - start)) / (CLOCKS_PER_SEC/1000);
  printf("** %.3f ms elapsed for Reduce **\n", cpu_time_used);
  //
  // free_mnode(map_par, num_mappers);
  // free_plist(plist, mynumpar);
}
