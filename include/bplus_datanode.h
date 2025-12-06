#ifndef BP_DATANODE_H
#define BP_DATANODE_H
/* Στο αντίστοιχο αρχείο .h μπορείτε να δηλώσετε τις συναρτήσεις
 * και τις δομές δεδομένων που σχετίζονται με τους Κόμβους Δεδομένων.*/
#include "record.h"
#include "bf.h"
#include "bplus_file_structs.h"

typedef struct bplus_datanode{

    int number_of_records;                            // number of records contained inside of the dataNode
    int next_data_block;                              // the pointer towards the next dataNode at the bottom of the Bplus tree
    int foobar;                                       // a dummy variable to make the dataNode 512 bytes big 
    Record rec_array[BF_BLOCK_SIZE/sizeof(Record)];   // the dataNode array which can contain a maximum of 5 records

} dataNode;

// dataNode helper functions

void print_datanode(const TableSchema *schema , dataNode * node);
void insert_in_data_block(dataNode *node, const Record *record, int target);
int first_insert_in_tree(int file_desc, BPlusMeta *metadata, const Record *record);
int insert_in_full_data_block(const int file_desc, BPlusMeta *metadata, const Record *record , int* traceroute , BF_Block *block , int target);
// int split_data_block(int file_desc, BF_Block *block, int *count, const Record *record, int target, BPlusMeta *metada);

#endif
