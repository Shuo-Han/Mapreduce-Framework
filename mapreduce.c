#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <pthread.h>
#include "mapreduce.h"

#define min(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a < _b ? _a : _b; })

typedef struct Knode {
  char *value;
  struct Knode *next;
} Knode;

typedef struct Pnode {
  char *key;
  Knode *vl;
  Knode *cur;
  struct Pnode *next;
} Pnode;

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

struct Pnode **plist;
Mapper glmap;
Reducer glreduce;
Partitioner userpar;
int mynumpar;

static pthread_mutex_t mutex;
static int count = 0;

void printer(){
  if (plist != NULL) {
    for (size_t i = 0; i < mynumpar; i++) {
      if (plist[i] != NULL) {
        /* code */
        Pnode *loc = plist[i];
        while (loc != NULL) {
          printf("%s: ", loc->key);
          Knode *a = loc->vl;
          while(a != NULL) {
            printf("%s ", a->value);
            a = a->next;
          }
          printf("\n");
          loc = loc->next;
        }
      }
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

void insertvalue(char* value, Pnode *cur, Knode *n) {
  Knode *k = cur->vl;
  // new value smaller, insert it in head
  if (strcmp(value, k->value) < 0) {
    cur->cur = n;
    cur->vl = n;
    n->next = k;
    return;
  }
  while(NULL != k->next) {
    if (strcmp(value, k->next->value) < 0) {
      n->next = k->next;
      k->next = n;
      return;
    }
    k = k->next;
  }
  k->next = n;
}

void MR_Emit(char *key, char *value) {
  Pnode *cur;
  Knode *n = malloc(sizeof(Knode));
  n->value = strdup(value);
  Pnode *m = malloc(sizeof(Pnode));
  m->key = strdup(key);
  m->vl = n;
  unsigned long index = userpar(key, mynumpar);
  cur = plist[index];

  if (NULL == cur) {
    m->cur = n;
    plist[index] = m;
    return;
  } else if (strcmp(key, cur->key) < 0) {
    m->cur = n;
    m->next = cur;
    plist[index] = m;
    return;
  } else if (!strcmp(key, cur->key)) {
    insertvalue(value, cur, n);
    free(m->key);
    free(m);
    return;
  }
  // printf("11111111111111\n");
  while (cur->next != NULL) {
    /* search for location to put new value or create new Pnode/key */
    if (!cur->next->key)
      print_error("Error: Pnode->key should never be NULL!\n", 1);
    if (strcmp(key, cur->next->key) < 0) {
      m->next = cur->next;
      cur->next = m;
      cur->next->cur = n;
      return;
    } else if (!strcmp(key, cur->next->key)) {
      /* equal,found key*/
      insertvalue(value, cur, n);
      return;
    } else {
      cur = cur->next;
    }
  }
  cur->next = m;
  cur->next->cur = n;
}


char* get_next(char *key, int partition_number) {
  static Pnode *p;
  Pnode *p2;
  if (NULL == p || strcmp(p->key, key)) {
    /* unequal */
    p2 = plist[partition_number];
    if (NULL == p2)
      return NULL;
    while (p2 != NULL) {
      if (!strcmp(p2->key, key)) {
        p = p2;
        break;
      }
      p2 = p2->next;
    }
    if (p2 == NULL) {
      print_error("Error: no key found (in get_next)!\n", 1);
    }
  }
  // P has been set to wanting Pnode(matches key) if no err
  if (NULL == p->cur)
    return NULL;
  char* next = p->cur->value;
  p->cur = p->cur->next;
  return next;
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
  // printf("%s\n", file->filename);
  while (file != NULL) {
    Mapper map = p->map;
    pthread_mutex_lock(&mutex);
    (*map)(file->filename);
    pthread_mutex_unlock(&mutex);
    file = file->next;
  }
  return NULL;
}

void *reduce_start(void *arg) {
  Argpool *p = (Argpool*)arg;
  Pnode *cur =  plist[p->par];
  count += 1;
  // printf("%li\n", pthread_self());
  // printf("%d2\n", p->par);
  while (cur != NULL) {
    Reducer reduce = p->reduce;
    pthread_mutex_lock(&mutex);
    (*reduce)(cur->key, get_next, p->par);
    pthread_mutex_unlock(&mutex);
    cur = cur->next;
    if (cur == NULL) {

      printf("%d\n", count++);
    }
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
      // printf("Error： thread creation failed.\n");
    }
  }
  // printf("%d!!!!!!!!!!!!!!!!!!!\n", count);
  for (size_t i = 0; i < num; i++) {
    pthread_join(thread_group_r[i], NULL);
  }
  free(thread_group_r);
}

void free_mnode(Mnode** l, int len) {
  for (size_t i = 0; i < len; i++) {
    if (l[i] != NULL)
      free(l[i]);
  }
  free(l);
}

void free_plist(Pnode **p, int len) {
  Pnode *cur, *tmp;
  Knode *k, *temp;
  for (size_t i = 0; i < len; i++) {
    cur = p[i];
    while (cur != NULL) {
      k = cur->vl;
      while (k != NULL) {
        temp = k->next;
        free(k->value);
        free(k);
        k = temp;
      }
      tmp = cur->next;
      free(cur->key);
      free(cur);
      cur = tmp;
    }
  }
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers,
	    Reducer reduce, int num_reducers, Partitioner partition) {
  // printf("11111111111111\n");
  if (argc < 2)
    print_error("No input specified\n", 0);

  int map_parnum;
  userpar = partition;
  mynumpar = num_reducers;
  num_mappers = min(num_mappers, argc-1);
  Mnode **map_par = malloc(sizeof(Mnode*) * num_mappers);
  plist = malloc(sizeof(Pnode*) * mynumpar);

  Mnode *filenode;

  for(int i = 1; i < argc; i++) {
    map_parnum = userpar(argv[i], num_mappers);
    filenode = malloc(sizeof(Mnode));
    filenode->filename = argv[i];
    filenode->size = findSize(argv[i]);
    addtolist(map_par, map_parnum, filenode);
  }

  pthread_mutex_init(&mutex, NULL);
  glmap = map;
  mapthinit(num_mappers, map_par);

  // printer();
  glreduce = reduce;
  reducethinit(mynumpar);

  free_mnode(map_par, num_mappers);
  free_plist(plist, mynumpar);
}
