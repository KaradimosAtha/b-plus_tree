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

## Helper Functions

We have 2 helper functions each for printing the contents of data blocks and index blocks, here are the definitions of the functions:

```c
void print_index_node(indexNode * node);
void print_datanode(const TableSchema *schema , dataNode * node)
```

## BPlus Tree Manipulation Functions

### bplus_create_file

### bplus_open_file

### bplus_close_file

### bplus_record_insert

### bplus_record_find

