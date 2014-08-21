#include <stdio.h>
#include <stdlib.h>

// node of a list (bucket)
struct node_t{
    int value;
    int hash_code;
    struct node_t * next;
};


struct HashSet{
    //int length;
    struct node_t ** table;
    int capacity;
    int setSize;
    int * atomic_locks;
    int locks_length;
    unsigned long long owner;
};

int NULL_VALUE = 5139239;
unsigned long long get_count(unsigned long long a){

    unsigned long long b = a >>48;
    return b;
}

unsigned long long get_pointer(unsigned long long a){
    unsigned long long b = a << 16;
    b= b >>16;
    return b;
}

unsigned long long set_count(unsigned long long  a, unsigned long long count){
    unsigned long long count_temp =  count << 48;
    unsigned long long b = get_pointer(a);
    b = b | count_temp;
    return b;
}

unsigned long long set_pointer(unsigned long long a, unsigned long long ptr){
    unsigned long long b = 0;
    unsigned long long c = get_count(a);
    b = set_count(b,c);
    ptr = get_pointer(ptr);
    b= b | ptr;
    return b;
}

unsigned long long set_both(unsigned long long a, unsigned long long ptr, unsigned long long count){
    a=set_pointer(a,ptr);
    a=set_count(a,count);
    return a;
}


void lock_set (int * locks, int hash_code){

    int indx=hash_code;
    //int indx=hash_code % H->locks_length;
    while (1){
        if (!locks[indx]){
            if(!__sync_lock_test_and_set(&(locks[indx]),1)) break;
        }
    }

 
}

void unlock_set(int *,int);

// operations call acquire to lock
void acquire(struct HashSet *H,int hash_code){
    int me = omp_get_thread_num();
    int who,cpy_owner,mark;
    while (1){
        cpy_owner=H->owner;
        who=get_pointer(cpy_owner);
        mark=get_count(cpy_owner);
        while((mark==1)&&(who!=me)){
            cpy_owner=H->owner;
            who=get_pointer(cpy_owner);
            mark=get_count(cpy_owner);
        }
        //int old_locks_length=H->locks_length;
        //int * old_locks=H->locks;
        int *cpy_locks=H->atomic_locks;
        int *old_locks=get_pointer(cpy_locks);
        int old_locks_length =get_count(cpy_locks);
        lock_set(old_locks,hash_code % old_locks_length);
        cpy_owner=H->owner;
        who=get_pointer(cpy_owner);
        mark=get_count(cpy_owner);
        
        if(((!mark) || (who==me))&&(H->atomic_locks==cpy_locks)){
            return;
        }
        else{
            unlock_set(old_locks,hash_code % old_locks_length);
        }
    }

}
void unlock_set(int * locks, int hash_code){

    int indx=hash_code;
    //int indx=hash_code % H->locks_length;
    locks[indx] = 0;
}

void release(struct HashSet * H,int hash_code){

    unlock_set((int *)get_pointer(H->atomic_locks),hash_code % ((int)get_count(H->atomic_locks)));
}



//search value in bucket;
int list_search(struct node_t * Head,int val){
    
    struct node_t * curr;
    
    curr=Head;
    while(curr){
        if(curr->value==val) return 1;
        curr=curr->next;
    }
    return 0;
}


//add value in bucket;
//NOTE: duplicate values are allowed...
void list_add(struct HashSet * H, int key,int val,int hash_code){
    
    struct node_t * curr;
    struct node_t * next;
    struct node_t * node=(struct node_t *)malloc(sizeof(struct node_t));
    /*node->value=val;
    node->next=NULL;
    curr=H->table[key];
    if(curr==NULL){
        H->table[key]=node;
        return ;
    }
    while(curr->next){
        curr=curr->next;
        next=curr->next;
    }
    curr->next=node;
    */
    node->value=val;
    node->hash_code=hash_code;
    if(H->table[key]==NULL) node->next=NULL;
    else node->next=H->table[key];
    H->table[key]=node;
}


// delete from bucket. The fist value equal to val will be deleted
int list_delete(struct HashSet *H,int key,int val){
    
    struct node_t * curr;
    struct node_t * next;
    struct node_t * prev;

    curr=H->table[key];
    prev=curr;
    if((curr!=NULL)&&(curr->value==val)) {
        H->table[key]=curr->next;
        free(curr);
        return 1;
    }
    while(curr){
        if( curr->value==val){
            prev->next=curr->next;
            free(curr);
            return 1;
        }
        prev=curr;
        curr=curr->next;
    }
    return 0;
}





void initialize(struct HashSet * H, int capacity){
    
    int i;
    H->setSize=0;
    H->capacity=capacity;
    H->table = (struct node_t **)malloc(sizeof(struct node_t *)*capacity);
    for(i=0;i<capacity;i++){
        H->table[i]=NULL;
    }
    int new_locks_length=capacity;
    int * new_locks=(int *)malloc(sizeof(int) * capacity);
    for(i=0;i<capacity;i++) new_locks[i]=0;
    H->atomic_locks = set_both(H->atomic_locks,new_locks,new_locks_length);
    H->owner = set_both(H->owner,NULL_VALUE,0);

}


int policy(struct HashSet *H){
    return ((H->setSize/H->capacity) >4);
}

void resize(struct HashSet *);

int contains(struct HashSet *H,int hash_code, int val){
    
    acquire(H,hash_code);
    int bucket_index = hash_code % H->capacity;
    int res=list_search(H->table[bucket_index],val);
    release(H,hash_code);
    return res;
}

//reentrant ==1 means we must not lock( we are calling from resize so we have already locked the data structure)
void add(struct HashSet *H,int hash_code, int val, int reentrant){
    
    if(!reentrant) acquire(H,hash_code);
    int bucket_index = hash_code % H->capacity;
    list_add(H,bucket_index,val,hash_code);
    //H->setSize++;
    __sync_fetch_and_add(&(H->setSize),1);
    if(!reentrant) release(H,hash_code);
    if(!reentrant) {if (policy(H)) resize(H);}
}

int delete(struct HashSet *H,int hash_code, int val){
    
    acquire(H,hash_code);
    int bucket_index =  hash_code % H->capacity;
    int res=list_delete(H,bucket_index,val);
    //H->setSize--;
    __sync_fetch_and_sub(&(H->setSize),1);
    release(H,hash_code);
    return res;
}

void quiesce(struct HashSet *H){
    int i;
    int *locks=get_pointer(H->atomic_locks);
    for(i=0;i<H->capacity;i++){
        while(locks[i]==1); //TODO: is it a race?
    }
}

void resize(struct HashSet *H){
    
    int i,mark,me;
    struct node_t * curr;
    int old_capacity = H->capacity;
    int new_capacity =  old_capacity * 2;

    me = omp_get_thread_num();
    int expected_value = set_both(expected_value,NULL_VALUE,0);
    int new_owner=set_both(new_owner,me,1);
    if(__sync_bool_compare_and_swap(&(H->owner),expected_value,new_owner)){
        
    //for(i=0;i<H->locks_length;i++) lock_set(H,i);
        if(old_capacity!=H->capacity) {
            //for(i=0;i<H->locks_length;i++) //unlock_set(H,i);
                H->owner=set_both(H->owner,NULL_VALUE,0);
                return; //somebody beat us to it
        }
        quiesce(H);  
        H->capacity =  new_capacity;
        //H->locks_length = new_capacity; //in this implementetion 
                                        //locks_length == capacity
                                        //edit!!
        int new_locks_length=new_capacity;
        struct node_t ** old_table = H->table;
        H->setSize=0;
        H->table = (struct node_t **)malloc(sizeof(struct node_t *)*new_capacity);
        for(i=0;i<new_capacity;i++){
            H->table[i]=NULL;
        }
        //re hash everything from the old table to the new one
        for(i=0;i<old_capacity;i++){
        
            curr=old_table[i];
            while(curr){
                int val = curr->value;
                int hash_code = curr->hash_code;
                //int bucket_index= hash_code % new_capacity;
                add(H,hash_code,val,1);
                curr=curr->next;
            }
        }
        //free(old_table);
        //all locks should be free now (quiesce ensures that)
        //so we might as well delete the old ones and make new ones
        int * old_locks = get_pointer(H->atomic_locks);
        //for(i=0;i<old_capacity;i++) if( H->locks[i]!=0) printf("HEY!\n");
        int * new_locks = (int *)malloc(sizeof(int) * new_capacity);//edit!
        for(i=0;i<new_locks_length;i++) new_locks[i]=0;//edit
        H->atomic_locks=(int *)set_both(H->atomic_locks,new_locks,new_locks_length);
        expected_value = new_owner;
        new_owner = set_both(new_owner,NULL_VALUE,0);
        if(!__sync_bool_compare_and_swap(&(H->owner),expected_value,new_owner))
            printf("This should not have happened\n");

        //free(old_locks);
    }

    

}

void print_set(struct HashSet * H){
    
    int i;
    for(i=0;i<H->capacity;i++){
        
        struct node_t * curr=H->table[i];
        while(curr){
            printf("(%d) ",curr->value);
            curr=curr->next;
        }
        printf("--\n");
    }
}

void main(int argc,char * argv[]){

    struct HashSet * H=(struct HashSet *) malloc(sizeof(struct HashSet));
    initialize(H,10);
    srand(time(NULL));
    int i,j;
    #pragma omp parallel for num_threads(8) shared(H) private(i,j)
    for(j=0;j<8;j++){
        for(i=0;i<700;i++){
            add(H,i+j*700,i+j*700,0);
            //add(H,rand(),i,0);
        }
    }
    
    /*
    for(i=0;i<55;i++){
        add(H,i,i,0);
    }
    */
    print_set(H);
    printf("%d \n",H->setSize);
    printf("%d \n",H->capacity);
    return;

    
}
