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

Also for basic functionality we added a struct for the metadata stored in block #0 called ```BPlusMeta```

```c
typedef struct {
    TableSchema schema;
    int depth;                       // depth of tree !without! data blocks
    int root_id;                     // the block_id of the root of the Bplus tree
    int record_size;                 // the size of each record in bytes
    int record_capacity_per_block;   // the number of records that fit in a datablock
    int keys_per_block;              // the number of keys in an indexblock
    int pointers_per_block;          // the number of pointers in an indexblock
} BPlusMeta;
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

Before we create the basic record insert functionality we need to take care of the insertion of a record on an empty BPlus tree. If the depth of the tree is ```-1```, which is when we have just created the tree, we need to call a special function to handle the first insertion calling ```first_insert_in_tree```.

```c
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
```
In our implementation we treat the first insertion in a special way
- We create 2 record blocks and 1 index block, which is the root of course
- We insert the first record in the left record block, while we leave the right record block empty
- We set the only key of the root to be the ```record_get_key(&(metadata->schema), record) + metadata->record_capacity_per_block;```, in order to achieve equal distribution of the records amongst the left data block

With that idea in our mind, we set all the remaining attributes of the block accordinly.


Having that edge case sorted out, its' time to do normal insertions in our Bplus Tree. In order to find where we will place the incoming record inside the tree, we first need to "find" it. So we need to traverse the BPlus Tree again to determine the data block it should be placed into. During the traversal of the tree we keep track of the index nodes we traversed in case we need to edit the index nodes due to an overflow.

To achieve this we create the array ```int traceroute[metadata->depth + 1]```. 
```traceroute[i]``` gives us the block id that led us to depth i. As a convention we set ```traceroute[0]``` to -1. 

What we do now is traverse the Bplus tree just they way we described in record_find, but we now care to keep a record of traceroute throughout the traversal.

Having found the appropriate data block we now search our incoming record key inside the block, if it already exists we discard the insertion. If it doesn't we find the position it should be inserted by taking advantage of the sorted keys of the record block. After that process there are now 4 cases we should take care of:

- Record should be placed at the end (```pos_to_insert == -1```) and data block is full -> ```insert_in_full_data_block(file_desc, metadata, record , traceroute , block , record_count);```
- Record should be placed at the end (```pos_to_insert == -1```) and data block is not full->```insert_in_data_block(node, record, record_count);```
- Record should be placed at ```pos_to_insert``` and data block is full -> ```insert_in_full_data_block(file_desc, metadata, record , traceroute , block , pos_to_insert);```
- Record should be placed at ```pos_to_insert``` and data block is not full ->```insert_in_data_block(node , record, pos_to_insert);```

Let's now analyze the functionality of these 2 new functions used above:

- ```void insert_in_data_block(dataNode *node, const Record *record, int target)```

```c
for(int i = node->number_of_records - 1 ; i >= target ; i--){
    node->rec_array[i+1] = node->rec_array[i];
}

node->rec_array[target] = *record;
node->number_of_records++;
```
Here we want to insert the incoming record at the ```target``` position, so we just shift all the elements from ```target``` onwards one position to the right and place the new record at it's new position.

- ```insert_in_full_data_block(const int file_desc, BPlusMeta *metadata, const Record *record , int* traceroute , BF_Block * block , int target)```


When we want to insert into a full data block, it is certain that the original data block will be spllited into 2 and the new Record will be inserted into one of two. That action is perfomed with ```split_data_block```. which is pretty straighforward and any explanation if needed is in the comments.

Now for the hard part we need to determine if we need to split any of the index nodes. In order to determine that we need to iterate the index nodes like this from the ground up: ```for(int i = metadata->depth ; i > 0; i--) ``` , in each iteration we find the parent index block that is pointing to an overflowed record/index block.

```c
// start from the end where the parent of the data block we reached is stored
parent_index = traceroute[i];
CALL_BF(BF_GetBlock(file_desc, parent_index, parent_block));
parent = (indexNode *)BF_Block_GetData(parent_block);
```

Once we have gotten there we just got to insert a key-pointer pair inside of the parent block. Again we have 2 cases here:

- Parent block is not full so we just insert the key-pointer pair in it with the call of ```insert_in_index_block(parent, key_to_above, new_block_position)```
- ```parent->pointer_counter == metadata->pointers_per_block ``` which means the parent block is full, which again leads us to 2 cases:
  1.  ```i==0``` which means we reached root-level and need to perform special actions  like splitting the root with ```split_index_block``` and creating a new one which points to the now splitted-ex root
  2.  ```i!=0``` which means we split the index block with ```split_index_block``` and  update the key-pointer pair that needs to be sent one level above
   
Finally, lets analyze a few things about the function ```split_index_block```. which is crucial for the bubble-up of our tree.

- We select the key element placed at the middle of the ```pointer_key_array```
- We split the key-pointer pairs between the existing and the new index block equally
- We insert the desired key-pair in the correct of the 2 index blocks based on it's comparison with the median key we chose earlier

