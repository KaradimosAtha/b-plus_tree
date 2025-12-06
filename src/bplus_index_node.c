// Μπορείτε να προσθέσετε εδώ βοηθητικές συναρτήσεις για την επεξεργασία Κόμβων Δεδομένων.
#include "bplus_index_node.h"
#include "bplus_file_structs.h"
#include "bplus_datanode.h"
#include <stdio.h>

void print_index_node(indexNode * node)
{
    printf("Pointer counter is %d\n" , node->pointer_counter);
    // CAUTION for case where in the root we have one pointer and one data !!!!!!!!!!!!!!!!!!!!!!!!!!!
    for(int i = 0 ; i < 2 * node->pointer_counter - 1 ; i++)
    {
        if(i%2 == 0)
        {
            printf("Pointer is ");
        }
        else
        {
            printf("Data is ");
        }
        printf("%d\n" , node->pointer_key_array[i]);
    }
}

void insert_in_index_block(indexNode *node, int key , int pointer)
{

    
    // target is the array position where the key-pointer couple will be put
    int target = 2*node->pointer_counter - 1; // in case it is not meant to be put somewhere between the existing items of the array it should be put at the end of the data of the array


    // we scan the array from left to right to find the right index to place our couple
    // we take advantage of the fact that the array is sorted and after each insertion we keep it sorted
    for(int i = 1; i < 2*(node->pointer_counter-1); i += 2)
    {
        if (key < node->pointer_key_array[i])
        {
            // we found the target position of our new key-pointer couple
            target = i;
            break;
        }
    }
    

    // we shift all the elements from the target position onwards 2 positions to the right 
    // in order to fit at the target position the new key-pointer couple

    for(int i = 2*(node->pointer_counter - 1) ; i>=target ; i--)
    {
        node->pointer_key_array[i+2] = node->pointer_key_array[i];
    }


    node->pointer_key_array[target] = key;
    node->pointer_key_array[target + 1] = pointer;
    node->pointer_counter++;

}

// this function helps splitting an index block and returns the key 
// that will be inserted to the higher level index block
int split_index_block(int file_desc, BPlusMeta *metadata, indexNode *parent_node, int key, int new_block_position)
{

    // the key that will be inserted in the higher level index block is in the middle of the current
    int new_key_to_above = parent_node->pointer_key_array[parent_node->pointer_counter - 1]; 

    BF_Block* new_index_block;

    BF_Block_Init(&new_index_block);
    CALL_BF(BF_AllocateBlock(file_desc, new_index_block));
    indexNode* new_index_block_node = (indexNode *)BF_Block_GetData(new_index_block);

    // pointers to lower level are equally distributed in half
    parent_node->pointer_counter /= 2;
    new_index_block_node->pointer_counter = parent_node->pointer_counter ;

    // take the right half, after the middle key and copy it to the new index block
    // since index block is full is has metadata->pointers_per_block = 64 pointers and 
    // metadata->keys_per_block = 63 keys, which are the middle positions in the block
    for( int i = 0; i < metadata->keys_per_block; i++)
    {
        new_index_block_node->pointer_key_array[i] = parent_node->pointer_key_array[ i + metadata->pointers_per_block ];
    }

    // now insert the key to the old or the new index block, according to its value
    if (key < new_key_to_above)
    {
        insert_in_index_block(parent_node, key, new_block_position);
    }
    else
    {
        insert_in_index_block(new_index_block_node, key, new_block_position);
    }

    block_routine(new_index_block, 1, 1, 1);

    return new_key_to_above;

}

int make_first_root(int file_desc, BPlusMeta * metadata, const Record* record , BF_Block* block , int target)
{

    dataNode *node = (dataNode *)BF_Block_GetData(block);

    BF_Block *new_block ;
    BF_Block_Init(&new_block);
    CALL_BF(BF_AllocateBlock(file_desc, new_block));
    dataNode* data = (dataNode *)BF_Block_GetData(new_block);

    // new block will point to the one the old block pointed to
    data->next_data_block = -1;
    // old block will point to the new one
    node->next_data_block = 2 ;

    // temporary array to help with record splitting
    Record record_array[ metadata->record_capacity_per_block + 1 ];

    for(int i = 0 ; i < target; i++)
    {
        record_array[i] = node->rec_array[i];
    }
    record_array[target] = *record;
    for(int i = target + 1 ; i < 6 ; i++)
    {
        record_array[i] = node->rec_array[i-1];
    }

    node->number_of_records = data->number_of_records = 3 ;

    for(int i = 0 ; i < 3 ; i++)
    {
        node->rec_array[i] = record_array[i];
        data->rec_array[i] = record_array[i+3];
    }

    block_routine(block, 1, 1, 1);
    block_routine(new_block, 1, 1, 1);

    BF_Block *new_root ;
    BF_Block_Init(&new_root);
    CALL_BF(BF_AllocateBlock(file_desc, new_root));
    indexNode* root_data = (indexNode *)BF_Block_GetData(new_root);
    

    root_data->pointer_counter = 2;
    root_data->pointer_key_array[0] = 1;
    root_data->pointer_key_array[1] = record_get_key(&metadata->schema, &record_array[3]);
    root_data->pointer_key_array[2] = 2;

    metadata->root_id = 3;
    metadata->depth = 1;

    // middle key (first of new data block) of the full array 
    // will be pushed to higher level according to the algorithm
    // printf("Record inserted succesfully!\n");
    return record_get_key(&metadata->schema, &record_array[3]);
}
