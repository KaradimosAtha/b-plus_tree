# BPlus Tree - Assignment 2 - YSBD


This is a short explanation of the functions and code for our second assignment for the course Database Implementation and Design (5th semester , offered by *Ioannis Ioannidis* and *Theofilos Mailis*)

- Dimitrios Gkritzalis (sdi2300033)
- Athanasios Karadimos (sdi2300062)
- Dimosthenis Theodosiou (sdi2300051)

## Structs Created

For the readability and easiness of the code we created 2 data structs, each responsible for managing 2 types of blocks: DataNodes and IndexNodes.

- DataNodes: This is where a block that contains data is stored and manipulated throughout our code

```c
typedef struct bplus_datanode{

    int number_of_records; // number of records contained inside of the dataNode
    int next_data_block;   // the pointer towards the next dataNode at the bottom of the Bplus tree
    int foobar;            // a dummy variable to make the dataNode 512 bytes big 
    Record rec_array[BF_BLOCK_SIZE/sizeof(Record)];   // the dataNode array which can contain a maximum of 5 records

} dataNode;
```

In our implementation a block (data or index) is 512 bytes wide, so we added an extra variable foobar so the struct has the size of an ordinary block.


- IndexNodes: This is where a block that contains the key-pointer couples for traversing the BPlus tree is stored

```c
typedef struct bplus_index_node{
    int pointer_counter;            // number of pointers contained in the indexNode
    int pointer_key_array[SIZE];     // the array containing the key-index pairs of the Bplus tree
    // in our example the SIZE is 127 positions which fits 63 pairs of pointer-key and one final pointer in the end
}indexNode;
```

## Helper Functions / Macros

We have 2 helper functions each for printing the contents of data blocks and index blocks, here are the definitions of the functions:

```c
void print_index_node(indexNode * node);
void print_datanode(const TableSchema *schema , dataNode * node)
```

We also added the macro block_routine to enhance readaility and decrease reoccuring lines of code throughout the program

```c
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
```

What we basically do here is we give the block we are working with and set 3 flags each with a specific purpose: setDirty , unpin or destroy

## BPlus Tree Manipulation Functions

### bplus_create_file
In the create_file function we create the first block of our bplus file and set the parameters of the metadata. Since the metadata block is the first to be created, it's block id will be 0.
```c
header->schema = *schema;
header->depth = -1;
header->root_id = -1;
header->record_size = sizeof(Record);
header->record_capacity_per_block = BF_BLOCK_SIZE/header->record_size;
header->pointers_per_block = (BF_BLOCK_SIZE-sizeof(int))/(2 * sizeof(int)) + 1;
header->keys_per_block = header->pointers_per_block -1;
```
By convention , when our tree is empty we set the depth and root_id of our tree to -1. Please do remember that the depth accounts only for the depth of the tree as per it's index blocks, it does not account for the data blocks.

As per the last three parameters, they are block size dependent, but the above calculations fit out 512 byte block best. A different block size could require an alteration of the above values.
### bplus_open_file

The actions performed in open file are pretty straightforward, but here are some key takeaways:

```c
int bplus_open_file(const char *fileName, int *file_desc, BPlusMeta **metadata)
{
  CALL_BF(BF_OpenFile(fileName , file_desc));

  BF_Block* block;
  BPlusMeta* header;
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(*file_desc , 0 , block));
  header = (BPlusMeta*)BF_Block_GetData(block);
  *metadata = header;
  BF_Block_Destroy(&block);
  return 0;
}
```

In the above code every time we open a bplus file we retrieve it's metadata from the block with block index 0. This is a convention that is kept throughout the code. Also notice that 
the newly brought to memory block is kept pinned and will stay that way when it's time to close the bplus file.

### bplus_close_file

In the next funcion close_file, things aren't much different. 

``` c
int bplus_close_file(const int file_desc, BPlusMeta* metadata)
{
  BF_Block* block;
  BPlusMeta* header;
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(file_desc , 0 , block));
  header = (BPlusMeta*)BF_Block_GetData(block);
  *header = *metadata;
  block_routine(block, 1, 1, 1);
  CALL_BF(BF_CloseFile(file_desc));

  return 0;
}
```

We retrieve the address of the metadata block that is pinned in memory and we insert into it the latest version of the metadata struct that has possibly been altered during the execution of other functions. After that, we finally handle the block using our block routine. 

### bplus_record_find

The process for finding the desired record splits into 2 main procedures
  - Navigating through the indexNode tree towards the dataNode
  - Selecting the correct Record from the dataNode
  
Let's go through the navigation of the index tree first

```c
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
```

For each index block, starting from our root ofcourse, we traverse the key-pointer pairs to determine the next index block we will need to parse in the lower level. The id of the index block we are investigating each time is ```root_index ```.

After getting the data of our current indexNode we parse its ```key_pointer_array``` starting from position ```1``` until ```2*pointer_count -1``` which is the end of the array with a step of ```2```. 
- If the key of the array at the current index is larger than the key of the record we are searching for we should follow the left pointer so we update ```root_index = node->pointer_key_array[block_index - 1];```.
- If the previous condition is not fullfilled we check if the record key is smaller than the next key in ```pointer_key_array[index+2]``` , if so we follow the right pointer and update ```root_index = node->pointer_key_array[block_index + 1];```. Please do notice the edge case condition we added in the second if which ensures that when we reach the last key of the ```pointer_key_array```, we always follow the right pointer and prevent a segfault by perfoming an invalid read on ```pointer_key_array```. 


At the termination of that process ```root_index``` wil finally contain the block id of the dataNode we want.


```c
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
```

Now we just simply traverse the dataNode based on the number of records it is already containing and return the record if it actualyl exists inside the block.
### bplus_record_insert



