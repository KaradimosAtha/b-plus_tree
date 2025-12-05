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

### bplus_record_insert

### bplus_record_find

