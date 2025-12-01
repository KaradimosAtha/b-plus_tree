#include "bplus_file_funcs.h"
#include "bplus_index_node.h"
#include "bplus_datanode.h"
#include <stdio.h>


#define bplus_ERROR -1
#define CALL_BF(call)         \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK)        \
    {                         \
      BF_PrintError(code);    \
      return bplus_ERROR;     \
    }                         \
  }

#define block_routine(block , dirty , unpin ,  destroy)    \    
  {                                    \
    if(dirty)                          \
    {                                  \
      BF_Block_SetDirty(block);        \
    }                                  \
    if(unpin)                          \
    {                                  \
      CALL_BF(BF_UnpinBlock(block));   \
    }                                  \
    if(destroy)                        \
    {                                  \
      BF_Block_Destroy(&block);        \
    }                                  \
  }


int bplus_create_file(const TableSchema *schema, const char *fileName)
{
  CALL_BF(BF_CreateFile(fileName));
  int file_handle;
  CALL_BF(BF_OpenFile(fileName , &file_handle));

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
  header->record_capacity_per_block = BF_BLOCK_SIZE/header->record_size;
  header->pointers_per_block = (BF_BLOCK_SIZE-sizeof(int))/(2 * sizeof(int)) + 1;
  header->keys_per_block = header->pointers_per_block -1;


  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));

  BF_Block_Destroy(&block);
  CALL_BF(BF_CloseFile(file_handle));

  return 0;
}


int bplus_open_file(const char *fileName, int *file_desc, BPlusMeta **metadata)
{
  int file_handle;
  CALL_BF(BF_OpenFile(fileName , &file_handle));

  BF_Block* block;
  BPlusMeta* header;
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(file_handle , 0 , block));
  header = (BPlusMeta*)BF_Block_GetData(block);
  *metadata = header;
  BF_Block_Destroy(&block);
  return 0;
}

int bplus_close_file(const int file_desc, BPlusMeta* metadata)
{
  BF_Block* block;
  BPlusMeta* header;
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(file_desc , 0 , block));
  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
  CALL_BF(BF_CloseFile(file_desc));

  return 0;
}

int insert_in_full_block(const int file_desc, BPlusMeta *metadata, const Record *record , int* traceroute , BF_Block * block , int target);



int bplus_record_insert(const int file_desc, BPlusMeta *metadata, const Record *record){

  BF_Block *block;
  BF_Block_Init(&block);
  
  if (metadata->depth == -1)
  {
      BF_Block* block;
      BF_Block_Init(&block);
      CALL_BF(BF_AllocateBlock( file_desc, block));
      dataNode* data = BF_Block_GetData(block);


      data->number_of_records = 1;
      data->next_data_block = -1;
      data->rec_array[0] = *record;

      block_routine(block , 1 , 1, 0);


      CALL_BF(BF_AllocateBlock( file_desc, block));
      indexNode* root = BF_Block_GetData(block);

      root->pointer_counter =1;
      root->pointer_key_array[0] = 1;
      root->pointer_key_array[1] = record_get_key(&(metadata->schema), record);

      metadata->depth = 0;
      metadata->root_id = 2;

      block_routine(block  , 1 , 0 , 1);

  }
  else
  {


    int traceroute[metadata->depth + 1];
    int root_index ,prev_index;

    prev_index = root_index = metadata->root_id;


    for (int depth = 0; depth < metadata->depth; depth++)
    {

      traceroute[depth] = prev_index;
      prev_index = root_index;

      CALL_BF(BF_GetBlock(file_desc, root_index, block));
  
      indexNode* node = BF_Block_GetData(block);

      // the block contains an int in the 4 first bytes which is the 
      // count of pointers to other blocks, in the block
      int pointer_count = node->pointer_counter;
      for (int block_index = 2; block_index < 2*pointer_count; block_index += 2)
      {

        int curr_key = node->pointer_key_array[block_index];

        if (record_get_key(&(metadata->schema), record) < curr_key){

          root_index = node->pointer_key_array[block_index - 1];
          break;
                  // if we reach last block
        }else if( (block_index == 2*(pointer_count - 1)) ||
                  (record_get_key(&metadata->schema, record) < node->pointer_key_array[block_index + 2]) ){ 
          root_index = node->pointer_key_array[block_index + 1];
          break;
        }

      }

      CALL_BF(BF_UnpinBlock(block));
    }
    traceroute[0] = -1;

    traceroute[metadata->depth] = prev_index; // <-----------------------------------


    // thats the only block that could contain the record
    // that has to be inserted (so it wont be inserted)
    // or the block in which we will insert the record.

    CALL_BF(BF_GetBlock(file_desc, root_index, block));
    dataNode* node = BF_Block_GetData(block);
    int found = 0;
    int record_count = node->number_of_records;

    for (int i = 0; i < record_count; i++)
    {
      if (record_get_key(&(metadata->schema), record) == record_get_key(&(metadata->schema), &(node->rec_array[i])))
      {
        record_print(&(metadata->schema), record);
        printf("Already exists! Was not inserted.\n");
        block_routine(block , 0 , 1 , 1);
        return -1;
      } 
      else if (record_get_key(&(metadata->schema), record) < record_get_key(&(metadata->schema), &(node->rec_array[i])))
      {
        found = 1;
        if(record_count == BF_BLOCK_SIZE/sizeof(Record))
        {
          if(!insert_in_full_block(file_desc, metadata, record , traceroute , block , i))
          {
            return 0;
          }
          return -1;
        }
        else
        {
          insert_in_data_block(node,record,i);
          printf("Record inserted succesfully!\n");
          block_routine(block , 1 , 1 , 1);
          return 0;
        }
      }
    }

    if ( !found )
    {
      insert_in_data_block(node , record, record_count);
      block_routine(block , 1, 1 ,1);
    }
    
  }

  return -1;
}

int bplus_record_find(const int file_desc, const BPlusMeta *metadata, const int key, Record** out_record)
{  

  BF_Block *block;
  BF_Block_Init(&block);

  int root_index = metadata->root_id;
  for (int depth = 0; depth < metadata->depth; depth++){

    CALL_BF(BF_GetBlock(file_desc, root_index, block));
    indexNode* node = BF_Block_GetData(block);

    // the block contains an int in the 4 first bytes which is the 
    // count of pointers to other blocks, in the block
    int pointer_count = node->pointer_counter;
    for (int block_index = 2; block_index < 2*pointer_count; block_index += 2){

      int curr_key = node->pointer_key_array[block_index];

      if (key < curr_key){

        root_index = node->pointer_key_array[block_index - 1];
        break;
                // if we reach last block
      }else if( (block_index == 2*(pointer_count - 1)) ||
                (key < node->pointer_key_array[block_index + 2]) ){ 
        root_index = node->pointer_key_array[block_index + 1];
        break;
      }

    }
    CALL_BF(BF_UnpinBlock(block));
  }

  // so after reaching the metadata_depth number we
  // have reached the last level of index nodes.

  CALL_BF(BF_GetBlock(file_desc, root_index, block));
  dataNode* node = BF_Block_GetData(block);

  int record_count = node->number_of_records;

  for(int i = 0 ; i < record_count; i++)
  {
    if(record_get_key(&(metadata)->schema, &(node->rec_array[i])) == key)
    {
      **out_record = node->rec_array[i];
      CALL_BF(BF_UnpinBlock(block));
      BF_Block_Destroy(&block);
      return 0;
    }
  }

  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);

  *out_record=NULL;
  return -1;
}



int insert_in_full_block(const int file_desc, BPlusMeta *metadata, const Record *record, int* traceroute , BF_Block * block, int target)
{
  dataNode* node = BF_Block_GetData(block);

  BF_Block * new_block ; 
  BF_Block_Init(&new_block);
  CALL_BF(BF_AllocateBlock(file_desc, new_block));
  dataNode* data = BF_Block_GetData(new_block);

  int count;
  CALL_BF(BF_GetBlockCounter(file_desc ,&count));

  int new_block_position = --count;
  data->next_data_block = node->next_data_block;
  node->next_data_block = new_block_position;

  Record record_array[6];

  for(int i= 0 ; i < target; i++){
    record_array[i] = node->rec_array[i];
  }
  record_array[target] = *record;
  for(int i = target + 1 ; i <6 ; i++){
    record_array[i] = node->rec_array[i-1];
  }

  node->number_of_records = data->number_of_records = 3 ; 

  for(int i = 0 ; i<3 ; i++){
    node->rec_array[i] = record_array[i];
    data->rec_array[i] = record_array[i+3];
  }

  int key_to_above = record_get_key(&metadata->schema, &record_array[3]);

  block_routine(block , 1,1,1);
  block_routine( new_block ,1,1,1);

  int parent_index = traceroute[metadata->depth];

  BF_Block * parent_block ; 
  BF_Block_Init(&parent_block);
  CALL_BF(BF_GetBlock(file_desc, parent_index, parent_block));
  indexNode* parent = BF_Block_GetData(parent_block);
  // for(){

  if(parent->pointer_counter == 64 ) // <---------------- change this 
  {
    int new_key_to_above = parent->pointer_counter - 1;

    insert_in_full_index_block( parent , key_to_above , new_block_position, );  //<----------------add argument to return new index block
  }
  else
  {
    insert_in_index_block(parent, key_to_above, new_block_position);
    block_routine(parent_block, 1, 1, 1);
    // break;
  }
  // }


  return -1;
}

