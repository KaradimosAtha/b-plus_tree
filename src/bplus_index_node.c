// Μπορείτε να προσθέσετε εδώ βοηθητικές συναρτήσεις για την επεξεργασία Κόμβων Δεδομένων.
#include "bplus_index_node.h"
#include <stdio.h>

void print_index_node(indexNode * node)
{
    printf("Pointer counter is %d\n" , node->pointer_counter);
    // CAUTION for case where in the root we have one pointer and one data !!!!!!!!!!!!!!!!!!!!!!!!!!!
    for(int i = 0 ; i < 2 * node->pointer_counter - 1 ; i++)
    {
        if(i%2 == 0)
        {
            printf("Pointer is ");
        }
        else
        {
            printf("Data is ");
        }
        printf("%d\n" , node->pointer_key_array[i]);
    }
}

void insert_in_index_block(indexNode *node, int key , int pointer){

    
    // target is the array position where the key-pointer couple will be put
    int target = 2*node->pointer_counter - 1; // in case it is not meant to be put somewhere between the existing items of the array it should be put at the end of the data of the array


    // we scan the array from left to right to find the right index to place our couple
    // we take advantage of the fact that the array is sorted and after each insertion we keep it sorted
    for(int i = 1; i < 2*(node->pointer_counter-1); i += 2)
    {
        if (key < node->pointer_key_array[i])
        {
            // we found the target position of our new key-pointer couple
            target = i;
            break;
        }
    }
    

    // we shift all the elements from the target position onwards 2 positions to the right 
    // in order to fit at the target position the new key-pointer couple

    for(int i = 2*(node->pointer_counter - 1) ; i>=target ; i--)
    {
        node->pointer_key_array[i+2] = node->pointer_key_array[i];
    }


    node->pointer_key_array[target] = key;
    node->pointer_key_array[target + 1] = pointer;
    node->pointer_counter++;

}
