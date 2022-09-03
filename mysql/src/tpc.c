#include <stdlib.h>
#include <stddef.h>
#include <stdio.h>
#include "tpc.h"

int max_items = 100000;
int cust_per_dist = 3000;
int dist_per_ware = 10;
int ord_per_dist = 3000;

int max_num_items = 15;
int max_item_len = 24;

void ReadEnvironmentVariable() {
  char *c = getenv("MAXITEMS");
  if (c != NULL) {
    MAXITEMS = atoi(c);
  }
  c = getenv("CUST_PER_DIST");
  if (c != NULL) {
    CUST_PER_DIST = atoi(c);
  }
  c = getenv("DIST_PER_WARE");
  if (c != NULL) {
    DIST_PER_WARE = atoi(c);
  }
  c = getenv("ORD_PER_DIST");
  if (c != NULL) {
    ORD_PER_DIST = atoi(c);
  }
  c = getenv("MAX_NUM_ITEMS");
  if (c != NULL) {
    MAX_NUM_ITEMS = atoi(c);
  }
  c = getenv("MAX_ITEM_LEN");
  if (c != NULL) {
    MAX_ITEM_LEN = atoi(c);
  }
  printf("MAXITEMS = %d\n", MAXITEMS);
  printf("CUST_PER_DIST = %d\n", CUST_PER_DIST);
  printf("DIST_PER_WARE = %d\n", DIST_PER_WARE);
  printf("ORD_PER_DIST = %d\n", ORD_PER_DIST);
  printf("MAX_NUM_ITEMS = %d\n", MAX_NUM_ITEMS);
  printf("MAX_ITEM_LEN = %d\n", MAX_ITEM_LEN);
}
