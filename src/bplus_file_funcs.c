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
  header->depth = 0;
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

void insert_in_block(dataNode *node, const Record *record, int target);
void insert_in_full_block();

int bplus_record_insert(const int file_desc, BPlusMeta *metadata, const Record *record){

  BF_Block *block;
  BF_Block_Init(&block);

  if (metadata->depth == 0){

  }else{

    int root_index = metadata->root_id;
    for (int depth = 1; depth <= metadata->depth; depth++){

      CALL_BF(BF_GetBlock(file_desc, root_index, block));
      indexNode* node = BF_Block_GetData(block);

      // the block contains an int in the 4 first bytes which is the 
      // count of pointers to other blocks, in the block
      int pointer_count = node->pointer_counter;
      for (int block_index = 2; block_index < 2*pointer_count; block_index += 2){

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

    // thats the only block that could contain the record
    // that has to be inserted (so it wont be inserted)
    // or the block in which we will insert the record.

    CALL_BF(BF_GetBlock(file_desc, root_index, block));
    dataNode* node = BF_Block_GetData(block);

    int record_count = node->number_of_records;
    
    for (int i = 0; i < record_count; i++){
      if (record_get_key(&(metadata->schema), record) == record_get_key(&(metadata->schema), &(node->rec_array[i]))){
        record_print(&(metadata->schema), record);
        printf("Already exists! Was not inserted.\n");
        return -1;
      } else if (record_get_key(&(metadata->schema), record) < record_get_key(&(metadata->schema), &(node->rec_array[i]))){
        
        if(record_count == BF_BLOCK_SIZE/sizeof(Record)){
          // insert_in_full_block();
        }else{
          // insert_in_block();
        }

      }
    }
    


  }

  return -1;
}

int bplus_record_find(const int file_desc, const BPlusMeta *metadata, const int key, Record** out_record)
{  

  BF_Block *block;
  BF_Block_Init(&block);

  int root_index = metadata->root_id;
  for (int depth = 1; depth <= metadata->depth; depth++){

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

