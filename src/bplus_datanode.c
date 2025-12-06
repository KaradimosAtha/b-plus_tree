#include "bplus_datanode.h"
#include "bplus_index_node.h"
#include "record.h"
#include "bplus_file_structs.h"
#include <stdio.h>

void print_datanode(const TableSchema *schema , dataNode * node)
{
    printf("Number of records is %d\n" , node->number_of_records);
    printf("The next data block is %d\n" , node->next_data_block);
    // iterate through all the records of the dataNode
    for(int i = 0; i < node->number_of_records; i++)
    {
        record_print(schema , &(node->rec_array[i]));
        printf("\n");
    }
}

void insert_in_data_block(dataNode *node, const Record *record, int target)
{
    // we shift all the elements of the record array one position to the right
    // in order to fit the incoming block in correct 'target' position
    for(int i = node->number_of_records - 1 ; i >= target ; i--){
        node->rec_array[i+1] = node->rec_array[i];
    }

    node->rec_array[target] = *record;
    node->number_of_records++;
}

// this function initialises the B+ tree and inserts the first record in it
// initialises the root (index level) and two data blocks(data level)
int first_insert_in_tree(int file_desc, BPlusMeta *metadata, const Record *record)
{
    // insert record on the left data block, initialise its values
    BF_Block *block;
    BF_Block_Init(&block);
    CALL_BF(BF_AllocateBlock(file_desc, block));
    dataNode* data_left = (dataNode *)BF_Block_GetData(block);

    data_left->number_of_records = 1;
    data_left->next_data_block = 2;
    data_left->rec_array[0] = *record;

    block_routine(block, 1, 1, 0);


    // initialise an empty data block on the right of the already existing one
    CALL_BF(BF_AllocateBlock(file_desc, block));
    dataNode* data_right = (dataNode *)BF_Block_GetData(block);

    data_right->number_of_records = 0;
    data_right->next_data_block = -1;

    block_routine(block, 1, 1, 0);

    // also initialise the first root of the tree pointing to those two blocks
    // and having as first key the first rec_id + 5 of the first record (check README for more)
    CALL_BF(BF_AllocateBlock(file_desc, block));
    indexNode* root = (indexNode *)BF_Block_GetData(block);

    root->pointer_counter = 2;
    root->pointer_key_array[0] = 1;
    root->pointer_key_array[1] = record_get_key(&(metadata->schema), record) + metadata->record_capacity_per_block;
    root->pointer_key_array[2] = 2;
    block_routine(block, 1, 1, 1);

    metadata->depth = 1;
    metadata->root_id = 3;

    return 1;

}

// this helper function splits the (full) data block given, inserts the record and 
// returns the key that will be inserted to the parent index block 
int split_data_block(int file_desc, BF_Block *block, int *count, const Record *record, int target, BPlusMeta *metadata)
{
    dataNode *node = (dataNode *)BF_Block_GetData(block);

    BF_Block *new_block ;
    BF_Block_Init(&new_block);
    CALL_BF(BF_AllocateBlock(file_desc, new_block));
    dataNode* data = (dataNode *)BF_Block_GetData(new_block);

    CALL_BF(BF_GetBlockCounter(file_desc ,count));

    // new block will point to the one the old block pointed to
    data->next_data_block = node->next_data_block;
    // old block will point to the new one
    node->next_data_block = --(*count);

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

    // middle key (first of new data block) of the full array 
    // will be pushed to higher level according to the algorithm
    return record_get_key(&metadata->schema, &record_array[3]);

}

// this function inserts a record to a full data block and fixes the index level above
// more detailed explanation in README :)
int insert_in_full_data_block(const int file_desc, BPlusMeta *metadata, const Record *record , int* traceroute , BF_Block * block , int target)
{
    int count;
    int key_to_above = split_data_block(file_desc, block, &count, record, target, metadata);
    int new_block_position = count; // keep a copy of count (the new data block id), to return in the end


    // now its the time to insert the right key from data level to index level and fix the tree
    BF_Block * parent_block ;
    BF_Block_Init(&parent_block);

    int parent_index;
    indexNode* parent;

    // this loop with the help of traceroute will guid us from the last index level
    // to the level that an index parent will be found that fits the key given to them
    // or split the root and make the tree deeper
    for(int i = metadata->depth ; i > 0; i--)  
    {
        // start from the end where the parent of the data block we reached is stored
        parent_index = traceroute[i];
        CALL_BF(BF_GetBlock(file_desc, parent_index, parent_block));
        parent = (indexNode *)BF_Block_GetData(parent_block);

        if(parent->pointer_counter == metadata->pointers_per_block ) // if parent index block is full
        {

            // if we haven't reached insertion to the root
            if(i > 1){

                key_to_above = split_index_block(file_desc, metadata, parent, key_to_above, new_block_position);
                // keep the new value that has to be inserted to the parent index block and loop again
                new_block_position++;

                block_routine(parent_block, 1, 1, 0);

            }
            else // if we have to insert in full root
            {
                // similar procidure as before, with the difference that we make one extra new index 
                // block, the root, and we increase the depth of the tree 

                int new_key_for_new_root = split_index_block(file_desc, metadata, parent, key_to_above, new_block_position);

                // make a new root and initialise it with on key and two pointers
                // one to the old root and one to the new index block next to old root
                CALL_BF(BF_AllocateBlock(file_desc, parent_block));
                indexNode* new_root = (indexNode *)BF_Block_GetData(parent_block);
                new_root->pointer_counter = 2;
                new_root->pointer_key_array[0] = metadata->root_id;
                new_root->pointer_key_array[1] = new_key_for_new_root;
                new_root->pointer_key_array[2] = ++new_block_position; // blocks are alocated linearly so we have the block id
                metadata->root_id = ++new_block_position;
                metadata->depth++;

                block_routine(parent_block, 1, 1, 1);

            }

        }
        else
        {
            insert_in_index_block(parent, key_to_above, new_block_position);
            block_routine(parent_block, 1, 1, 1);
            break;
        }


    }
    return count;

}
