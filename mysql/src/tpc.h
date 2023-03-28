/*
 * tpc.h
 * definitions for tpcc loading program && transactions
 */

#ifdef __cplusplus
extern "C" {
#endif

/*
 * correct values
 */
#define MAXITEMS max_items
#define CUST_PER_DIST cust_per_dist
#define DIST_PER_WARE dist_per_ware
#define ORD_PER_DIST ord_per_dist

/*
 */

/*
 * small values

#define MAXITEMS 	1000
#define CUST_PER_DIST 	30
#define DIST_PER_WARE	3
#define ORD_PER_DIST	30

 */

/* definitions for new order transaction */
#define MAX_NUM_ITEMS max_num_items
#define MAX_ITEM_LEN max_item_len

#define swap_int(a, b) {int tmp; tmp=a; a=b; b=tmp;}

/*
 * hack MakeAddress() into a macro so that we can pass Oracle
 * VARCHARs instead of char *s
 */
#define MakeAddressMacro(str1, str2, city, state, zip)                         \
{int tmp; \
    tmp = MakeAlphaString(10, 20, str1.arr);                                   \
    str1.len = tmp;                                                            \
    tmp = MakeAlphaString(10, 20, str2.arr);                                   \
    str2.len = tmp;                                                            \
    tmp = MakeAlphaString(10, 20, city.arr);                                   \
    city.len = tmp;                                                            \
    tmp = MakeAlphaString(2, 2, state.arr);                                    \
    state.len = tmp;                                                           \
    tmp = MakeNumberString(9, 9, zip.arr);                                     \
 zip.len = tmp;}

/*
 * while we're at it, wrap MakeAlphaString() and MakeNumberString()
 * in a similar way
 */
#define MakeAlphaStringMacro(x, y, str)                                        \
{int tmp; tmp = MakeAlphaString(x,y,str.arr); str.len = tmp;}
#define MakeNumberStringMacro(x, y, str)                                       \
{int tmp; tmp = MakeNumberString(x,y,str.arr); str.len = tmp;}

/*
 * likewise, for Lastname()
 * counts on Lastname() producing null-terminated strings
 */
#define LastnameMacro(num, str)                                                \
{Lastname(num, str.arr); str.len = strlen(str.arr);}

extern long count_ware;

extern int max_items;
extern int cust_per_dist;
extern int dist_per_ware;
extern int ord_per_dist;

extern int max_num_items;
extern int max_item_len;

/* Functions */

void LoadItems();
void LoadWare();
void LoadCust();
void LoadOrd();
void LoadNewOrd();
int Stock();
int District();
void Customer();
void Orders();
void New_Orders();
void MakeAddress();
void Error();

#ifdef __STDC__
void SetSeed(int seed);
int RandomNumber(int min, int max);
int NURand(unsigned A, unsigned x, unsigned y);
int MakeAlphaString(int x, int y, char str[]);
int MakeNumberString(int x, int y, char str[]);
void gettimestamp(char str[], char *format, size_t n);
void InitPermutation(void);
void DestroyPermutation(void);
int GetPermutation(void);
void Lastname(int num, char *name);

void ReadEnvironmentVariable();

#endif /* __STDC__ */

#ifdef __cplusplus
}
#endif
