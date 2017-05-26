#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdint.h>
#include <pthread.h>

typedef struct node_t {
  char *pagedata;
  struct node_t *next;
  struct node_t *prev;
  char* fromlink;
  char* pagelink;
} node_t;

typedef struct queue_t {
  node_t *front;
  node_t *end;
  int count;
  int max;
  pthread_mutex_t mutex;
  pthread_cond_t condp;
  pthread_cond_t condd;
} queue_t;

queue_t links;
queue_t pages;

typedef struct work_t {
  int count;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
} work_t;

work_t work;


//hash function is from:
//http://www.ks.uiuc.edu/Research/vmd/doxygen/hash_8c-source.html

#undef get16bits
#if (defined(__GNUC__) && defined(__i386__)) || defined(__WATCOMC__) || defined(_MSC_VER) || defined (__BORLANDC__) || defined (__TURBOC__)
#define get16bits(d) (*((const unsigned short *) (d)))
#endif

#if !defined (get16bits)
#define get16bits(d) ((((unsigned int)(((const uint8_t *)(d))[1])) << 8)+(unsigned int)(((const uint8_t *)(d))[0]) )
#endif

int totalLinks = 0;
unsigned int* visitedLinks = NULL;

unsigned int SuperFastHash (const char * data, int len) {
  unsigned int hash = len, tmp;
  int rem;

  if (len <= 0 || data == NULL) return 0;

  rem = len & 3;
  len >>= 2;

  /* Main loop */
  for (;len > 0; len--) {
    hash  += get16bits (data);
    tmp    = (get16bits (data+2) << 11) ^ hash;
    hash   = (hash << 16) ^ tmp;
    data  += 2*sizeof (unsigned short);
    hash  += hash >> 11;
  }

  /* Handle end cases */
  switch (rem) {
  case 3: hash += get16bits (data);
    hash ^= hash << 16;
    hash ^= ((signed char)data[sizeof (unsigned short)]) << 18;
    hash += hash >> 11;
    break;
  case 2: hash += get16bits (data);
    hash ^= hash << 11;
    hash += hash >> 17;
    break;
  case 1: hash += (signed char)*data;
    hash ^= hash << 10;
    hash += hash >> 1;
  }

  /* Force "avalanching" of final 127 bits */
  hash ^= hash << 3;
  hash += hash >> 5;
  hash ^= hash << 4;
  hash += hash >> 17;
  hash ^= hash << 25;
  hash += hash >> 6;

  return hash;
}

void initialize(unsigned int **linkArray){

  *linkArray=malloc(sizeof(unsigned int));

  if(*linkArray==NULL)
    {
      printf("Malloc error\n");
      return;
    }

  (*linkArray)[0]=0;
}

void add(unsigned int **linkArray, unsigned int newLink){
  
  static int sizeCount = 0;
  totalLinks = sizeCount;
  sizeCount++;

  unsigned int *temp;

  temp=realloc(*linkArray, (sizeCount+1) * sizeof(unsigned int));

  if(temp==NULL)
    {
     printf("Realloc error!");
      return;
    }

  *linkArray = temp;

  (*linkArray)[sizeCount] = newLink;
}



// Callbakc function pointers
char *(*fetch_ptr)(char *url);
void (*edge_ptr)(char *from, char *to);


// Appends nodes to queue
void enqueue(node_t *new_node, queue_t *queue) {

  if (queue->count == 0){
    new_node->next = NULL;
    queue->front = new_node;
    queue->end = new_node;
  } else {
    queue->end->next = new_node;
    queue->end = new_node;
  }
  (queue->count)++;
}

// Removes node from front of queue
node_t *dequeue(queue_t *queue) {

  struct node_t *rem_node = malloc(sizeof(node_t));

  rem_node = queue->front;
  rem_node->pagedata = queue->front->pagedata;
  rem_node->pagelink = queue->front->pagelink;

  if (queue->count == 0){
    return NULL;
  } else  if (queue->count == 1){
    queue->front = NULL;
    queue->end = NULL;
    queue->count = 0;
  } else {
    queue->front = queue->front->next;
    queue->count--;
  }
  return rem_node;
}

void increment_work(){

  pthread_mutex_lock(&work.mutex);
  work.count++;
  pthread_cond_signal(&work.cond);
  pthread_mutex_unlock(&work.mutex);
}

void decrement_work(){

  pthread_mutex_lock(&work.mutex);
 
  while(work.count < 0){
    pthread_cond_wait(&work.cond, &work.mutex);
  }
  
  work.count--;
  pthread_mutex_unlock(&work.mutex);
}

// Adds to page queue
void download_pages ( node_t * ptr){

  pthread_mutex_lock(&pages.mutex);
  enqueue(ptr,&pages);
  pthread_cond_signal(&pages.condp);
  pthread_mutex_unlock(&pages.mutex);
}

// Transfer from link queue to page queue
void *download_links (void *ptr){

  do{

    pthread_mutex_lock(&links.mutex);
  
    while (links.count == 0){
      pthread_cond_wait(&links.condd, &links.mutex);
    }

    node_t *tmp = dequeue(&links);
    pthread_cond_signal(&links.condp);
    pthread_mutex_unlock(&links.mutex);

    char* page= fetch_ptr(tmp->pagelink);
    tmp->pagedata = page;
    download_pages(tmp);
   
  }while(work.count > 0);
 
  exit(0);
}

// Adds to link queue
void parse_links(node_t *ptr){

  pthread_mutex_lock(&links.mutex);

  while (links.count == links.max){
    pthread_cond_wait(&links.condp, &links.mutex);
  }

  enqueue(ptr,&links);
  pthread_cond_signal(&links.condd);
  pthread_mutex_unlock(&links.mutex);

}

char ** parsePage(char* page, char** returnArray, int* linkCount) {

  char * saveptr;
  char * saveptr2;

  char s[2] = " ";
  char n[2] = "\n";
  int i = 0;
  int linkLength = 0;

  char *token = strtok_r(page,n,&saveptr);
  char *token2 = strdup(token);

  token2 = strtok_r(token, s, &saveptr2);

  while (token != NULL) {
    while (token2 != NULL) {
      if ((token2[0] == 'l') && (token2[1] == 'i') && 
	  (token2[2] == 'n') && (token2[3] == 'k') && (token2[4] == ':')) {
	if (strlen(token2)>5){
	  linkLength = strlen(token2) - 5 + 1;
	  returnArray[i] = malloc(linkLength * sizeof(char));

	  int j = 0;
	  for (j = 0; j <= (linkLength - 1); j++) {
	    returnArray[i][j] = token2[5+j];
	  }
	
	  returnArray[i][j++] = '\0';
	  i++;
	}
      }
      token2 = strtok_r(NULL, s, &saveptr2);
    }

    if (token != NULL) {
      token = strtok_r(NULL, n, &saveptr);
    }

    if ((token == NULL) && (token2 == NULL)) {
      break;
    }
    else {
      token2 = strdup(token);
    }
  }
  *linkCount = i;

  return returnArray;
}

// Parses page and removes from page queue 
void parse(node_t *node ){
  
  char *page = node->pagedata;
  char *destLink;
  char** linkArray = malloc(sizeof(char) * 100);
  int* linkCount = malloc(sizeof(int));
  int alreadyDone = 0;
  int testLength = strlen(node->pagelink);
  unsigned int testHash = SuperFastHash(node->pagelink, testLength);
   
  int j=0;
  for (j = 0; j <= totalLinks; j++) {
     if (testHash == visitedLinks[j]) {
      alreadyDone = 1;
    }
  }

  if (alreadyDone != 1){
    add(&visitedLinks, testHash);
  }

  linkArray = (char**) parsePage(page, linkArray, linkCount);
  visitedLinks[totalLinks] = testHash;

  int i=0;
  for (i = 0; i <= *linkCount; i++) {
    if (linkArray[i] != NULL) {

      node_t *newnode = malloc(sizeof(node_t));
      newnode->fromlink = node->pagelink;
      destLink = strdup(linkArray[i]);
      newnode->pagelink = destLink;

      edge_ptr(node->pagelink, destLink);

      if (alreadyDone != 1){
	increment_work();
	parse_links(newnode);
      }
    }
  }
  decrement_work();
}

// removes from page queue, parses page, adds to link queue
void *parse_pages(void* ptr){

   do{

    pthread_mutex_lock(&pages.mutex);

    while (pages.count == 0){
      pthread_cond_wait(&pages.condp, &pages.mutex);
    }

    node_t *tmp = dequeue(&pages);
    pthread_mutex_unlock(&pages.mutex);
    parse(tmp);

  }while (work.count > 0);

   exit(0);
}

int crawl(char *start_url,
	  int download_workers,
	  int parse_workers,
	  int queue_size,
	  char * (*_fetch_fn)(char *url),
	  void (*_edge_fn)(char *from, char *to)) {
  
  fetch_ptr = _fetch_fn;
  edge_ptr = _edge_fn;

  char *page = fetch_ptr(start_url);
  node_t *newnode = malloc(sizeof(node_t));
  newnode->pagedata = page;
  newnode->pagelink = start_url;
  newnode->fromlink = malloc(sizeof(char));
  
  assert(page != NULL);

  //link queue
  links.front = NULL;
  links.end = NULL;
  links.count = 0;
  links.max = queue_size;
  pthread_mutex_init(&links.mutex, NULL);
  pthread_cond_init(&links.condp, NULL);
  pthread_cond_init(&links.condd, NULL);

  //page queue
  pages.front = NULL;
  pages.end = NULL;
  pages.count = 0;
  pages.max = -1;
  pthread_mutex_init(&pages.mutex, NULL);
  pthread_cond_init(&pages.condp, NULL);

  work.count = 0;
  pthread_mutex_init(&work.mutex, NULL);
  pthread_cond_init(&work.cond, NULL);

  initialize(&visitedLinks);
  
  enqueue(newnode,&pages);

  pthread_t downloader[download_workers], parser[parse_workers];
  int i,j;

  for (j=0; j < parse_workers; j++){
    if (pthread_create(&parser[j], NULL, parse_pages, NULL)!=0){
      printf("issue creating parser thread\n");
    } 
  }
  
   for (i=0; i < download_workers; i++){
    if ( pthread_create(&downloader[i], NULL, download_links, NULL)!=0){
      printf("issue creating downloader thread\n");
    } 
  }
  
  for (j=0; j < parse_workers; j++){
    pthread_join(parser[j], NULL );
  }
  
  for (i=0; i < download_workers; i++){
    pthread_join(downloader[i], NULL);
  }
 
  free(page);

  return 0;
}
