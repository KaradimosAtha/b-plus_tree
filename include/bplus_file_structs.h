//
// Created by theofilos on 11/4/25.
//

#ifndef BPLUS_BPLUS_FILE_STRUCTS_H
#define BPLUS_BPLUS_FILE_STRUCTS_H
#include "bf.h"
#include "bplus_datanode.h"
#include "bplus_index_node.h"
#include "record.h"
#include "bplus_file_structs.h"

typedef struct {
    TableSchema schema;
    int depth;
    int root_id;
    int record_size;    
    int record_capacity_per_block;
    int keys_per_block;
    int pointers_per_block;
} BPlusMeta;

#endif //BPLUS_BPLUS_FILE_STRUCTS_H

