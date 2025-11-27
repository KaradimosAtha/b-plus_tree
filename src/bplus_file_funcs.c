#include "bplus_file_funcs.h"
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
  header->record_size = MAX_ATTRIBUTES * MAX_STRING_LENGTH;
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

int bplus_record_insert(const int file_desc, BPlusMeta *metadata, const Record *record)
{
  return -1;
}

int bplus_record_find(const int file_desc, const BPlusMeta *metadata, const int key, Record** out_record)
{  
  *out_record=NULL;
  return -1;
}

