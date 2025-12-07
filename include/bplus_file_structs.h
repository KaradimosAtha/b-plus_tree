//
// Created by theofilos on 11/4/25.
//

#ifndef BPLUS_BPLUS_FILE_STRUCTS_H
#define BPLUS_BPLUS_FILE_STRUCTS_H

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

// similar to CALL_BF, its responsible for what has 
// to be done to a given block according to the flags
#define block_routine(block , dirty , unpin ,  destroy)    \
  {                                                        \
    if(dirty)                                              \
    {                                                      \
      BF_Block_SetDirty(block);                            \
    }                                                      \
    if(unpin)                                              \
    {                                                      \
      CALL_BF(BF_UnpinBlock(block));                       \
    }                                                      \
    if(destroy)                                            \
    {                                                      \
      BF_Block_Destroy(&block);                            \
    }                                                      \
  }

#include "record.h"

typedef struct {
    TableSchema schema;
    int depth;                           // depth of tree !without! data blocks
    int root_id;                         // the block_id of the root of the Bplus tree or the only data block if tree has no root
    int record_size;                     // the size of each record in bytes
    int record_capacity_per_block;       // the number of records that fit in a datablock
    int keys_per_block;                  // the number of keys in an indexblock
    int pointers_per_block;              // the number of pointers in an indexblock
} BPlusMeta;

#endif //BPLUS_BPLUS_FILE_STRUCTS_H

