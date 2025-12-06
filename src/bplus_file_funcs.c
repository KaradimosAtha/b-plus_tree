#include "bplus_file_funcs.h"
#include "bplus_index_node.h"
#include "bplus_datanode.h"
#include <stdio.h>

// this function creates a new B+ tree file according to a data base schema 
// provided and a specific name, returns an integer file identifier
int bplus_create_file(const TableSchema *schema, const char *fileName)
{
  CALL_BF(BF_CreateFile(fileName));
  int file_handle;
  CALL_BF(BF_OpenFile(fileName , &file_handle));

  // initialise first block in file with the metadata of the struct
  BF_Block* block;
  BPlusMeta* header;
  BF_Block_Init(&block);
  CALL_BF(BF_AllocateBlock(file_handle , block));
  CALL_BF(BF_GetBlock(file_handle , 0 , block));
  header = (BPlusMeta*)BF_Block_GetData(block);

  header->schema = *schema;
  header->depth = -1;
  header->root_id = -1;
  header->record_size = sizeof(Record);
  header->record_capacity_per_block = BF_BLOCK_SIZE/header->record_size; // 512-byte data block can fit 5 100-byte records
  header->pointers_per_block = (BF_BLOCK_SIZE-sizeof(int))/(2 * sizeof(int)) + 1; // index blocks fit 64 pointers(int)
  header->keys_per_block = header->pointers_per_block -1; // index blocks fit pointers - 1 keys (63)

  block_routine(block, 1, 1, 1);
  CALL_BF(BF_CloseFile(file_handle));

  return 0;
}


// opens B+ tree file and brings the metadata block (first in file)
// and pins it until the file is closed
int bplus_open_file(const char *fileName, int *file_desc, BPlusMeta **metadata)
{
  CALL_BF(BF_OpenFile(fileName , file_desc));

  BF_Block* block;
  BPlusMeta* header;
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(*file_desc , 0 , block));
  header = (BPlusMeta*)BF_Block_GetData(block);
  *metadata = header;
  block_routine(block, 0, 0, 1);
  return 0;
}


// closes B+ tree file, updates metadata block and sets it dirty 
// so it can be stored in secondary storage unit
int bplus_close_file(const int file_desc, BPlusMeta* metadata)
{
  BF_Block* block;
  BPlusMeta* header;
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(file_desc, 0 ,block));
  header = (BPlusMeta*)BF_Block_GetData(block);
  *header = *metadata;
  block_routine(block, 1, 1, 1);
  CALL_BF(BF_CloseFile(file_desc));

  return 0;
}


// this function implements how the insertion works in the B+ tree
// it returns the data block identifier (position in file) that the record was inserted to
int bplus_record_insert(const int file_desc, BPlusMeta *metadata, const Record *record){

  BF_Block *block;
  BF_Block_Init(&block);

  // we handle the first insertion in the tree differently than the others
  if (metadata->depth == -1)
  {
    return first_insert_in_tree(file_desc, metadata, record);
  }
  else
  {

    // we keep a path from the root to the data block, in which we will insert the record to
    // if it does not exist, in the traceroute table. every cell, is a tree level
    // and the value in it is the parent index block from the previous level. last cell is the parent 
    // of the data block we reached at etc...
    int traceroute[metadata->depth + 1];
    int root_index ,prev_index;

    prev_index = root_index = metadata->root_id;

    // find the data block that should take the record and store the path to it
    // similar to bplus_record_find(). nothing new to explain :)
    for (int depth = 0; depth < metadata->depth; depth++)
    {

      traceroute[depth] = prev_index;
      prev_index = root_index;

      CALL_BF(BF_GetBlock(file_desc, root_index, block));
  
      indexNode* node = (indexNode *)BF_Block_GetData(block);

      int pointer_count = node->pointer_counter;
      for (int block_index = 1; block_index < 2*pointer_count - 1; block_index += 2)
      {

        int curr_key = node->pointer_key_array[block_index];

        if (record_get_key(&(metadata->schema), record) < curr_key){

          root_index = node->pointer_key_array[block_index - 1];
          break;
                  // if we reach last block
        }else if( (block_index == 2*pointer_count - 3) ||
                  (record_get_key(&metadata->schema, record) < node->pointer_key_array[block_index + 2]) ){ 
          root_index = node->pointer_key_array[block_index + 1];
          break;  // check for segfault <-------------------
        }

      }

      block_routine(block, 0, 1, 0);

    }
    // parent of root does not exist
    traceroute[0] = -1;
    traceroute[metadata->depth] = prev_index; // <-----------------------------------

    // thats the only block that could contain the record
    // that has to be inserted (so it wont be inserted)
    // or the block in which we will insert the record.

    CALL_BF(BF_GetBlock(file_desc, root_index, block));
    dataNode* node = (dataNode *)BF_Block_GetData(block);
    int pos_to_insert = -1;
    int record_count = node->number_of_records;

    // search the data block to check if the record already exists or 
    // to find a place in the already existing records to put it (to stay in order) 
    for (int i = 0; i < record_count; i++)
    {

      // if record already exists, insertion failed return -1
      if (record_get_key(&(metadata->schema), record) == record_get_key(&(metadata->schema), &(node->rec_array[i])))
      {
        printf("Already exists! Was not inserted.\n");
        block_routine(block , 0 , 1 , 1);
        return -1;
      } 
      else if (record_get_key(&(metadata->schema), record) < record_get_key(&(metadata->schema), &(node->rec_array[i])))
      {
        // the record has to take the place of the next one.
        pos_to_insert = i;
        break;
      }
    }

    if (pos_to_insert == -1) // there was no place found to insert between records, so insert in the end
    {

      // if the data block is full we have to split it and fix the tree
      if(record_count == metadata->record_capacity_per_block)
      {
        return insert_in_full_data_block(file_desc, metadata, record , traceroute , block , record_count);
      }

      // insert and return :)
      insert_in_data_block(node, record, record_count);
      printf("Record inserted succesfully!\n");

      block_routine(block, 1, 1, 1);
      return root_index;
    }
    else // insert between records in given position
    {
      if(record_count == metadata->record_capacity_per_block)
      {
        return insert_in_full_data_block(file_desc, metadata, record , traceroute , block , pos_to_insert);
      }

      insert_in_data_block(node , record, pos_to_insert);
      printf("Record inserted succesfully!\n");

      block_routine(block, 1, 1, 1);
      return root_index;
      
    }
    
  }

  return -1;
}


// in this function we implement how the search in the B+ tree is done
// it returns the record in case it exists otherwise it prints an error message and returns error code -1
int bplus_record_find(const int file_desc, const BPlusMeta *metadata, const int key, Record** out_record)
{  

  BF_Block *block;
  BF_Block_Init(&block);

  if( metadata->depth == -1 )
  {
    CALL_BF(BF_GetBlock(file_desc, 1, block));
    dataNode* node = (dataNode *)BF_Block_GetData(block);

    int record_count = node->number_of_records;

    for(int i = 0 ; i < record_count; i++)
    {
      if(record_get_key(&(metadata)->schema, &(node->rec_array[i])) == key)
      {
        **out_record = node->rec_array[i];
        block_routine(block, 0, 1, 1);
        return 0;
      }
    }

    block_routine(block, 0, 1, 1);

    *out_record=NULL; // search failed
    return -1;

  }

  // the iteration in the tree begins from the root and continues
  // to its children, roots of smaller subtrees.
  int root_index = metadata->root_id;
  for (int depth = 0; depth < metadata->depth; depth++){

    CALL_BF(BF_GetBlock(file_desc, root_index, block));
    indexNode* node = (indexNode *)BF_Block_GetData(block);

    // we search the stored keys in the block so they can guide as deeper towards the data level
    int pointer_count = node->pointer_counter;
    for (int block_index = 1; block_index < 2*pointer_count - 1; block_index += 2){

      int curr_key = node->pointer_key_array[block_index];

      if (key < curr_key){

        root_index = node->pointer_key_array[block_index - 1];
        break;
                
      } // if we reach last block
      else if( (block_index == 2* pointer_count - 3) ||
                (key < node->pointer_key_array[block_index + 2]) )
      { 
        root_index = node->pointer_key_array[block_index + 1];
        break;
      }

    }
    block_routine(block, 0, 1, 0);
    // after examining the index tree at a certain depth we unpin 
    // the index block at that depth since it is no longer need it

  }

  // after the iteration the root_index will point to the data block 
  // that might contain the record that is being searched
  
  CALL_BF(BF_GetBlock(file_desc, root_index, block));
  dataNode* node = (dataNode *)BF_Block_GetData(block);

  int record_count = node->number_of_records;

  for(int i = 0 ; i < record_count; i++)
  {
    if(record_get_key(&(metadata)->schema, &(node->rec_array[i])) == key)
    {
      **out_record = node->rec_array[i];
      block_routine(block, 0, 1, 1);
      return 0;
    }
  }

  block_routine(block, 0, 1, 1);

  *out_record=NULL; // search failed
  return -1;
}