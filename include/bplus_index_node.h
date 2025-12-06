#ifndef BP_INDEX_NODE_H
#define BP_INDEX_NODE_H
#include "bf.h"
#include "bplus_file_structs.h"
#define SIZE (BF_BLOCK_SIZE - sizeof(int)) / sizeof(int)

/* Στο αντίστοιχο αρχείο .h μπορείτε να δηλώσετε τις συναρτήσεις
 * και τις δομές δεδομένων που σχετίζονται με τους Κόμβους Δεδομένων.*/


typedef struct bplus_index_node{
    int pointer_counter;            // number of pointers contained in the indexNode
    int pointer_key_array[SIZE];    // the array containing the key-index pairs of the Bplus tree
    // in our example the SIZE is 127 positions which fits 63 pairs of pointer-key and one final pointer in the end
}indexNode;


// indexNode helper functions

void print_index_node(indexNode * node);
void insert_in_index_block(indexNode *node, int key , int pointer);
void insert_in_full_index_block(indexNode *node, int key, int pointer, int *new_block_pos);
int split_index_block(int file_desc, BPlusMeta *metadata, indexNode *parent_node, int key, int new_block_position);
int make_first_root(int file_desc, BPlusMeta * metadata, const Record* record , BF_Block* block , int record_count);



#endif