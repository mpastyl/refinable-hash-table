#include <stdio.h>
#include <stdlib.h>
#include "timers_lib.h"
// node of a list (bucket)
struct node_t{
    int value;
    int hash_code;
    struct node_t * next;
};

int MAX_LOCKS=64;

struct HashSet{
    //int length;
    struct node_t ** table;
    int capacity;
    int setSize;
    __int128 atomic_locks;
    int locks_length;
    __int128 owner;
};

int NULL_VALUE = 5139239;

__int128 get_count(__int128 a){

    __int128 b = a >>64;
    return b;
}

__int128 get_pointer(__int128 a){
    __int128 b = a << 64;
    b= b >>64;
    return b;
}

__int128 set_count(__int128  a, __int128 count){
    __int128 count_temp =  count << 64;
    __int128 b = get_pointer(a);
    b = b | count_temp;
    return b;
}

__int128 set_pointer(__int128 a, __int128 ptr){
    __int128 b = 0;
    __int128 c = get_count(a);
    b = set_count(b,c);
    ptr = get_pointer(ptr);
    b= b | ptr;
    return b;
}

__int128 set_both(__int128 a, __int128 ptr, __int128 count){
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
    __int128 me = (__int128) omp_get_thread_num();
    __int128 who,cpy_owner,mark;
    while (1){
        cpy_owner=H->owner;
        who=get_pointer(cpy_owner);
        mark=get_count(cpy_owner);
        while((mark==(__int128)1)&&(who!=me)){
            cpy_owner=H->owner;
            who=get_pointer(cpy_owner);
            mark=get_count(cpy_owner);
        }
        //int old_locks_length=H->locks_length;
        //int * old_locks=H->locks;
        __int128 cpy_locks=H->atomic_locks;
        __int128 old_locks=get_pointer(cpy_locks);
        int  old_locks_length =(int)get_count(cpy_locks);
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
    H->atomic_locks = set_both(H->atomic_locks,(__int128)new_locks,(__int128)new_locks_length);
    printf("length count %lld \n",get_count(H->atomic_locks));
    H->owner = set_both(H->owner,(__int128)NULL_VALUE,0);

}


int policy(struct HashSet *H){
    return ((H->setSize/H->capacity) >4);
}

void resize(struct HashSet *);

int contains(struct HashSet *H,int hash_code, int val){
    
    

        int bucket_index = hash_code % H->capacity;
        int res=list_search(H->table[bucket_index],val);
        acquire(H,hash_code);
        bucket_index = hash_code % H->capacity;
        res=list_search(H->table[bucket_index],val);
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
    int *locks=(int *)get_pointer(H->atomic_locks);
    int locks_length=(int*) get_count(H->atomic_locks);
    for(i=0;i<locks_length;i++){
        while(locks[i]==1); //TODO: is it a race?
    }
}

void resize(struct HashSet *H){
    
    int i;
    __int128 mark,me;
    struct node_t * curr;
    int old_capacity = H->capacity;
    int new_capacity =  old_capacity * 2;

    me = (__int128)omp_get_thread_num();
    __int128 expected_value = set_both(expected_value,(__int128)NULL_VALUE,0);
    __int128 new_owner=set_both(new_owner,me,(__int128)1);
    if(__sync_bool_compare_and_swap(&(H->owner),expected_value,new_owner)){
        
    //for(i=0;i<H->locks_length;i++) lock_set(H,i);
        if(old_capacity!=H->capacity) {
            //for(i=0;i<H->locks_length;i++) //unlock_set(H,i);
                H->owner=set_both(H->owner,(__int128)NULL_VALUE,0);
                return; //somebody beat us to it
        }
        quiesce(H);  
        //H->locks_length = new_capacity; //in this implementetion 
                                        //locks_length == capacity
                                        //edit!!
        int new_locks_length;
        if(new_capacity<=64) new_locks_length=new_capacity;
        else new_locks_length=64;
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
        int * old_locks = (int *)get_pointer(H->atomic_locks);
        //for(i=0;i<old_capacity;i++) if( H->locks[i]!=0) printf("HEY!\n");
        int * new_locks = (int *)malloc(sizeof(int) * new_locks_length);//edit!
        for(i=0;i<new_locks_length;i++) new_locks[i]=0;//edit
        __int128 temp_atomic_locks=set_both(temp_atomic_locks,(__int128)new_locks,(__int128)new_locks_length);
        H->atomic_locks=temp_atomic_locks;
        H->capacity =  new_capacity;
        expected_value = new_owner;
        new_owner = set_both(new_owner,(__int128)NULL_VALUE,0);
        if(!__sync_bool_compare_and_swap(&(H->owner),expected_value,new_owner))
            printf("This should not have happened\n");

        //free(old_locks);
    }

    

}

/* Arrange the N elements of ARRAY in random order.
   Only effective if N is much smaller than RAND_MAX;
   if this may not be the case, use a better random
   number generator. */
void shuffle(int *array, size_t n)
{
    if (n > 1) 
    {
        size_t i;
        for (i = 0; i < n - 1; i++) 
        {
          size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
          int t = array[j];
          array[j] = array[i];
          array[i] = t;
        }
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

    int num_threads=atoi(argv[1]);
    int inp_1=atoi(argv[2]);
    int inp_2=atoi(argv[3]);
    int inp_3=atoi(argv[4]);
    struct HashSet * H=(struct HashSet *) malloc(sizeof(struct HashSet));
    initialize(H,16);
    srand(time(NULL));
    int c,k,i,j;
    int op_count=1000000;
    int finds=inp_1;
    int deletes=inp_2;
    int inserts=inp_3;
    timer_tt * timer;
    //int MAX_COUNT=100000;//NOTE!!!! we assume that count=10*MAX_SIZE
    int value_table[op_count];
    int op_table[op_count];

    for(i=0;i<op_count;i++) value_table[i]=rand()%100000;

    for(i=0;i<finds;i++) op_table[i]=1;
    for(i=0;i<deletes;i++) op_table[finds+i]=2;
    for(i=0;i<inserts;i++) op_table[finds+deletes+i]=3;
    shuffle(op_table,op_count);

    int segm=op_count/num_threads;
    int indx,res;
    double total_time=0;

    #pragma omp parallel for num_threads(num_threads) shared(H,value_table,op_table) private(c,j,timer,res,k,indx) reduction(+:total_time)
    for(i=0;i<num_threads;i++){
        c=50;
        timer = timer_init(timer);
        timer_start(timer);
        for(j=0;j<(1000000/num_threads);j++){
            for(k=0;k<c;k++);
            indx=(omp_get_thread_num()*segm+j)%op_count;
            if(op_table[indx]==1) res=contains(H,value_table[indx],value_table[indx]);
            else if(op_table[indx]==2) res=delete(H,value_table[indx],value_table[indx]);
            else add(H,value_table[indx],value_table[indx],0);

        }
        timer_stop(timer);
        total_time = timer_report_sec(timer);
        printf("%thread %d timer %lf\n",omp_get_thread_num(),timer_report_sec(timer));
    }

    double avg_total_time=total_time/(double)num_threads;
    printf("avg total time %lf\n",avg_total_time);
    printf("%d \n",H->setSize);
    printf("%d \n",H->capacity);
    return;

    
}
