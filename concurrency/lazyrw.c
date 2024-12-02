#include<stdlib.h>
#include<stdio.h>
#include<unistd.h>
#include<pthread.h>
#include<string.h>
#include<bits/time.h>
#include<errno.h>


#define YELLOW "\x1b[33m"
#define PINK "\x1b[35m"
#define GREEN "\x1b[32m"
#define RED "\x1b[31m"
#define RESET "\x1b[0m"

typedef struct request
{
    int userid;
    int fileno;
    int oper;
    int time;
    struct request *next;
    int completed;
    int canceled;
    pthread_t thread;
}request;

typedef struct file
{
    int exist;
    int no_of_users;
    int is_writting;
    pthread_mutex_t mutex;
    pthread_cond_t reader_cond;
    pthread_cond_t writer_cond;
    pthread_cond_t delete_cond;
}file;

pthread_mutex_t time_mutex=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t time_cond=PTHREAD_COND_INITIALIZER;
int read_time,write_time,delete_time;
file *files;
int max_users,time_out;
int curr_time=0;

request* create_request()
{
    request *new=(request*)malloc(sizeof(request));
    new->userid=-1;
    new->fileno=-1;
    new->oper=-1;
    new->time=-1;
    new->next=NULL;
    new->completed=0;
    new->canceled=0;
    return new;
}

int get_curr_time()
{
    pthread_mutex_lock(&time_mutex);
    int time=curr_time;
    pthread_mutex_unlock(&time_mutex);
    return time;
}

void* process_request(void* arg)
{
    request *req=(request*)arg;

    struct timespec ts;
    while(get_curr_time()<req->time)
    {
        pthread_mutex_lock(&time_mutex);
        pthread_cond_wait(&time_cond,&time_mutex);
        pthread_mutex_unlock(&time_mutex);
    }
    printf(YELLOW "\nUser %d has made request for perfoming %s on file %d at %d seconds\n" RESET,req->userid,req->oper==0 ? "READ": (req->oper==1 ? "WRITE" : "DELETE"),req->fileno+1,req->time);   

    file *f=&files[req->fileno];
    pthread_mutex_lock(&f->mutex);
    if(!f->exist)
    {
        printf("\nLAZY has declined the request of User %d at %d seconds because an invalid/deleted file was requested\n" RESET,req->userid,get_curr_time());
        req->canceled=1;
        pthread_mutex_unlock(&f->mutex);
        return NULL;
    }
    pthread_mutex_lock(&time_mutex);
    pthread_cond_wait(&time_cond,&time_mutex);
    pthread_mutex_unlock(&time_mutex);
    switch(req->oper)
    {
        case 0:
        struct timespec ts1;
        clock_gettime(0,&ts1);
        time_t start_time=ts1.tv_sec;
        while(f->no_of_users>=max_users)
        {
            time_t elapsed_time=time(NULL)-start_time;
            if(elapsed_time>=time_out)
            {
                printf(RED "\nUser %d canceled the request due to no response at %d seconds\n" RESET,req->userid,req->time+time_out+1);
                req->canceled=1;
                pthread_mutex_unlock(&f->mutex);
                return NULL;
            }
            clock_gettime(0,&ts1);
            elapsed_time=time(NULL)-start_time;
            ts1.tv_sec+=(time_out-elapsed_time);
            int ret0=pthread_cond_timedwait(&f->reader_cond,&f->mutex,&ts1);
            if(ret0==ETIMEDOUT || time(NULL)-start_time>=time_out)
            {
                printf(RED "\nUser %d canceled the request due to no response at %d seconds\n" RESET,req->userid,req->time+time_out+1);
                req->canceled=1;
                pthread_mutex_unlock(&f->mutex);
                return NULL;
            }
        }
        if(!f->exist)
        {
            printf("\nLAZY has declined the request of User %d at %d seconds because an invalid/deleted file was requested\n" RESET,req->userid,get_curr_time());
            req->canceled=1;
            pthread_mutex_unlock(&f->mutex);
            return NULL;
        }
        f->no_of_users++;
        printf(PINK"\nLAZY has taken up the request of User %d at %d seconds\n" RESET,req->userid,get_curr_time());
        pthread_mutex_unlock(&f->mutex);
        for(int i=0;i<read_time;i++)
        {
            pthread_mutex_lock(&time_mutex);
            pthread_cond_wait(&time_cond,&time_mutex);
            pthread_mutex_unlock(&time_mutex);
        }
        pthread_mutex_lock(&f->mutex);
        printf(GREEN "\nThe request of User %d was completed at %d seconds\n" RESET,req->userid,get_curr_time());
        f->no_of_users--;
        pthread_cond_broadcast(&f->reader_cond);
        if(f->is_writting==0 && f->no_of_users<max_users)
            pthread_cond_broadcast(&f->writer_cond);
        if(f->no_of_users==0)
            pthread_cond_broadcast(&f->delete_cond);   
        pthread_mutex_unlock(&f->mutex);
        req->completed=1;
        return NULL; 
        break;
        
        case 1:
        struct timespec ts2;
        clock_gettime(0,&ts2);
        time_t start_time1=ts2.tv_sec;
        while(f->no_of_users>=max_users || f->is_writting>0)
        {
            time_t elapsed_time1=time(NULL)-start_time1;
            if(elapsed_time1>=time_out)
            {
                printf(RED "\nUser %d canceled the request due to no response at %d seconds\n" RESET,req->userid,req->time+time_out+1);
                req->canceled=1;
                pthread_mutex_unlock(&f->mutex);
                return NULL;
            }
            clock_gettime(0,&ts2);
            elapsed_time1=time(NULL)-start_time1;
            ts2.tv_sec+=(time_out-elapsed_time1);
            int ret1=pthread_cond_timedwait(&f->writer_cond,&f->mutex,&ts2);
            if(ret1==ETIMEDOUT || time(NULL)-start_time1>=time_out)
            {
                printf(RED "\nUser %d canceled the request due to no response at %d seconds\n" RESET,req->userid,req->time+time_out+1);
                req->canceled=1;
                pthread_mutex_unlock(&f->mutex);
                return NULL;
            }
        }
        if(!f->exist)
        {
            printf("\nLAZY has declined the request of User %d at %d seconds because an invalid/deleted file was requested\n" RESET,req->userid,get_curr_time());
            req->canceled=1;
            pthread_mutex_unlock(&f->mutex);
            return NULL;
        }
        f->no_of_users++;
        f->is_writting=1;
        printf(PINK"\nLAZY has taken up the request of User %d at %d seconds\n" RESET,req->userid,get_curr_time());
        pthread_mutex_unlock(&f->mutex);
        for(int i=0;i<write_time;i++)
        {
            pthread_mutex_lock(&time_mutex);
            pthread_cond_wait(&time_cond,&time_mutex);
            pthread_mutex_unlock(&time_mutex);
        }
        pthread_mutex_lock(&f->mutex);
        printf(GREEN "\nThe request of User %d was completed at %d seconds\n" RESET,req->userid,get_curr_time());
        f->no_of_users--;
        f->is_writting=0;
        pthread_cond_broadcast(&f->writer_cond);
        if(f->no_of_users<max_users)
            pthread_cond_broadcast(&f->reader_cond);
        if(f->no_of_users==0)
            pthread_cond_broadcast(&f->delete_cond);
        pthread_mutex_unlock(&f->mutex);
        req->completed=1;
        return NULL;
        break;

        case 2:
        struct timespec ts3;
        clock_gettime(0,&ts3);
        time_t start_time2=ts3.tv_sec;
        while(f->no_of_users>0)
        { 
            time_t elapsed_time2=time(NULL)-start_time2;
            if(elapsed_time2>=time_out)
            {
                printf(RED "\nUser %d canceled the request due to no response at %d seconds\n" RESET,req->userid,req->time+time_out+1);
                req->canceled=1;
                pthread_mutex_unlock(&f->mutex);
                return NULL;
            }
            clock_gettime(0,&ts3);
            elapsed_time2=time(NULL)-start_time2;
            ts3.tv_sec+=(time_out-elapsed_time2);
            int ret2=pthread_cond_timedwait(&f->delete_cond,&f->mutex,&ts3); 
            if(ret2==ETIMEDOUT || time(NULL)-start_time2>=time_out)
            {
                printf(RED "\nUser %d canceled the request due to no response at %d seconds\n" RESET,req->userid,req->time+time_out+1);
                req->canceled=1;
                pthread_mutex_unlock(&f->mutex);
                return NULL;
            }
        }
        if(!f->exist)
        {
            printf("\nLAZY has declined the request of User %d at %d seconds because an invalid/deleted file was requested\n" RESET,req->userid,get_curr_time());
            req->canceled=1;
            pthread_mutex_unlock(&f->mutex);
            return NULL;
        }
        f->exist=0;
        printf(PINK"\nLAZY has taken up the request of User %d at %d seconds\n" RESET,req->userid,get_curr_time());
        for(int i=0;i<delete_time;i++)
        {
            pthread_mutex_lock(&time_mutex);
            pthread_cond_wait(&time_cond,&time_mutex);
            pthread_mutex_unlock(&time_mutex);
        }
        printf(GREEN "\nThe request of User %d was completed at %d seconds\n" RESET,req->userid,get_curr_time());
        f->no_of_users=0;
        f->is_writting=0;
        pthread_mutex_unlock(&f->mutex);
        req->completed=1;
        return NULL;
        break;
    }
}
int main()
{
    int r,w,d;
    int n,c,T;

    scanf("%d %d %d",&r,&w,&d);
    scanf("%d %d %d",&n,&c,&T);
    read_time=r;
    write_time=w;
    delete_time=d;
    max_users=c;
    time_out=T-1;
    files=(file*)malloc((n)*sizeof(file));
    for(int i=0;i<n;i++)
    {
        files[i].exist=1;
        files[i].no_of_users=0;
        files[i].is_writting=0;
        pthread_mutex_init(&files[i].mutex,NULL);
        pthread_cond_init(&files[i].reader_cond,NULL);
        pthread_cond_init(&files[i].writer_cond,NULL);
        pthread_cond_init(&files[i].delete_cond,NULL);
    }
    request *prev=NULL;
    request *head=NULL;
    while(1)
    {
        char user[10];
        scanf("%s",user);
        if(strcmp(user,"STOP")==0)
            break;
        char oper[10];    
        int fileno,time;    
        scanf("%d %s %d",&fileno,oper,&time);  
        request* curr=create_request();
        curr->userid=atoi(user);
        curr->fileno=fileno-1;
        if(strcmp(oper,"READ")==0)
            curr->oper=0;
        else if(strcmp(oper,"WRITE")==0)
            curr->oper=1;
        else if(strcmp(oper,"DELETE")==0)    
            curr->oper=2;
        curr->time=time;
        if(curr->oper!=-1 && curr->userid!=-1 && curr->fileno<n && curr->fileno>=0 && curr->time!=-1)
        {
            if(prev==NULL)
            {
                prev=curr;
                head=curr;
            }
            else
            {
                prev->next=curr;
                prev=curr;
            }
        } 
        else 
            free(curr);
    }
    printf("LAZY has woken up!\n");
    request* curr=head;
    while(curr!=NULL)
    {
        pthread_create(&curr->thread,NULL,process_request,(void*)curr);
        curr=curr->next;
    }
    while(1)
    {
        usleep(1000000);
        pthread_mutex_lock(&time_mutex);
        curr_time++;
        pthread_cond_broadcast(&time_cond);
        pthread_mutex_unlock(&time_mutex);
        int all_done=1;
        curr=head;
        while(curr!=NULL)
        {
            if(!curr->completed && !curr->canceled)
            {
                all_done=0;
                break;
            }
            curr=curr->next;
        }
        if(all_done==1)
            break;
    }

    curr=head;
    while(curr!=NULL)
    {
        pthread_join(curr->thread,NULL);
        curr=curr->next;
    }
    printf("\nLAZY has no more pending requests and is going to sleep!\n");
    for(int i=0;i<n;i++)
    {
        pthread_mutex_destroy(&files[i].mutex);
        pthread_cond_destroy(&files[i].reader_cond);
        pthread_cond_destroy(&files[i].writer_cond);
        pthread_cond_destroy(&files[i].delete_cond);
    }
    free(files);

    curr=head;
    while(curr!=NULL)
    {
        request* temp=curr;
        curr=curr->next;
        free(temp);
    }
    return 0;
}