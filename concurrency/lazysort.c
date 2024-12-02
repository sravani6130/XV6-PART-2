#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>

#define MAX_FILES 1000000
#define NAME_LENGTH 5
#define THRESHOLD 42
#define FILES_PER_THREAD 10

int THREAD_COUNT;
int N; // number of files
char sorting_criteria[20];
typedef struct
{
    char name[NAME_LENGTH];
    int id;
    char timestamp[20];
} file;

typedef struct
{
    int start;
    int end;
    int key;
} thread_args;

file files[MAX_FILES];
int sorted_segments = 0; // number of sorted segments
pthread_mutex_t merge_mutex;
pthread_cond_t merge_cond;

unsigned long long hash(char *str);
void count_sort_by_id(int start, int end);
void count_sort_by_name(int start, int end);
void count_sort_by_timestamp(int start, int end);
void merge(file arr[], int start, int mid, int end);
void merge_sort_by_id(int start, int end);
void merge_by_name(file arr[], int start, int mid, int end);
void merge_sort_by_name(int start, int end);
void merge_by_timestamp(file arr[], int start, int mid, int end);
void merge_sort_by_timestamp(int start, int end);
void count_sort(thread_args *args);
void merge_sort(thread_args *args);
void *sort_worker(void *arg);


unsigned long long hash(char *str)
{
    unsigned long long hash = 0;
    int len=0;
    int c;
    while ((c = *str++))
        if(c>='A' && c<='Z')
        {
            hash = (hash*31) + c-'A'+1;
            len++;
        }
        else if(c>='a' && c<='z')
        {
            hash = (hash*31) + c-'a'+1;
            len++;
        }
    for(int i=0;i<NAME_LENGTH-len;i++)    
        hash = (hash*31);
    return hash;
}

unsigned long long timestamp_hash(char *str)
{
    int year,month,day,hour,min,sec;
    sscanf(str, "%4d-%2d-%2dT%2d:%2d:%2d", &year, &month, &day, &hour, &min, &sec);
    unsigned long long total_seconds=((unsigned long long)(year-1960)*365*24*60*60)+
                                ((unsigned long long)month*30*24*60*60)+
                                ((unsigned long long)day*24*60*60)+
                                ((unsigned long long)hour*60*60)+
                                ((unsigned long long)min*60)+
                                (unsigned long long)sec;
    return total_seconds;
}
void count_sort_by_name(int start, int end)
{
    int n = end - start + 1;
    file *output=(file *)malloc(n*sizeof(file));

    unsigned long long MinHash=~0;
    unsigned long long MaxHash=0;
    unsigned long long *hashes=(unsigned long long *)malloc(n*sizeof(unsigned long long));

    for(int i=0;i<n;i++)
    {
        hashes[i]=hash(files[start+i].name);
        if(hashes[i]>MaxHash) MaxHash=hashes[i];
        if(hashes[i]<MinHash) MinHash=hashes[i];
    }
    unsigned long long HASH_SIZE=MaxHash-MinHash+1;
    unsigned long long *count=(unsigned long long *)calloc(HASH_SIZE,sizeof(unsigned long long));

    for(int i=0;i<n;i++)
    {
        count[hashes[i]-MinHash]++;
    }    

    for(unsigned long long i=1;i<HASH_SIZE;i++)
    {
        count[i]+=count[i-1];
    }

    for(int i=n-1;i>=0;i--)
    {
        output[count[hashes[i]-MinHash]-1]=files[start+i];
        count[hashes[i]-MinHash]--;
    }

    for(int i=0;i<n;i++)
    {
        files[start+i]=output[i];
    }

    free(count);
    free(hashes);
    free(output);
}

void count_sort_by_id(int start, int end)
{
    int max = 0;
    for (int i = start; i <= end; i++)
    {
        if (files[i].id > max)
        {
            max = files[i].id;
        }
    }
    int *count = (int *)calloc(max + 1, sizeof(int));
    file *result = (file *)malloc((end - start + 1) * sizeof(file));

    for (int i = start; i <= end; i++)
    {
        count[files[i].id]++;
    }
    for (int i = 1; i <= max; i++)
    {
        count[i] += count[i - 1];
    }
    for (int i = end; i >= start; i--)
    {
        result[count[files[i].id] - 1] = files[i];
        count[files[i].id]--;
    }
    for (int i = 0; i <= end - start; i++)
    {
        files[start + i] = result[i];
    }
    free(count);
    free(result);
}
void count_sort_by_timestamp(int start, int end)
{
    int n = end - start + 1;
    file output[n];

    unsigned long long *hashes=(unsigned long long *)malloc(n*sizeof(unsigned long long));
    int minHash=~0;
    int maxHash=0;
    for(int i=0;i<n;i++)
    {
        hashes[i]=timestamp_hash(files[start+i].timestamp);
        if(hashes[i]>maxHash) maxHash=hashes[i];
        if(hashes[i]<minHash) minHash=hashes[i];
    }
    unsigned long long HASH_SIZE=maxHash-minHash+1;
    unsigned long long *count=(unsigned long long *)calloc(HASH_SIZE,sizeof(unsigned long long));
    for(int i=0;i<n;i++)
    {
        count[hashes[i]-minHash]++;
    }

    for(unsigned long long i=1;i<HASH_SIZE;i++)
    {
        count[i]+=count[i-1];  
    }

    for(int i=n-1;i>=0;i--)
    {
        output[count[hashes[i]-minHash]-1]=files[start+i];
        count[hashes[i]-minHash]--;
    }

    for(int i=0;i<n;i++)
    {
        files[start+i]=output[i];
    }

    free(count);
    free(hashes);
}

void merge_by_name(file arr[], int start, int mid, int end)
{
    int i, j, k;
    int n1 = mid - start + 1;
    int n2 = end - mid;

    file *L = (file *)malloc(n1 * sizeof(file));
    file *R = (file *)malloc(n2 * sizeof(file));

    for (i = 0; i < n1; i++)
        L[i] = arr[start + i];
    for (j = 0; j < n2; j++)
        R[j] = arr[mid + 1 + j];

    i = 0;
    j = 0;
    k = start;

    while (i < n1 && j < n2)
    {
        if (strcmp(L[i].name, R[j].name) <= 0)
        {
            arr[k++] = L[i++];
        }
        else
        {
            arr[k++] = R[j++];
        }
    }

    while (i < n1)
        arr[k++] = L[i++];
    while (j < n2)
        arr[k++] = R[j++];

    free(L);
    free(R);
}

void merge_sort_by_name(int start, int end)
{
    if (start < end)
    {
        int mid = start + (end - start) / 2;
        merge_sort_by_name(start, mid);
        merge_sort_by_name(mid + 1, end);
        merge_by_name(files, start, mid, end);
    }
}

void merge(file arr[], int start, int mid, int end)
{
    int i, j, k;
    int n1 = mid - start + 1;
    int n2 = end - mid;

    file *L = (file *)malloc(n1 * sizeof(file));
    file *R = (file *)malloc(n2 * sizeof(file));

    for (i = 0; i < n1; i++)
        L[i] = arr[start + i];
    for (j = 0; j < n2; j++)
        R[j] = arr[mid + 1 + j];

    i = 0;
    j = 0;
    k = start;
    while (i < n1 && j < n2)
    {
        if (L[i].id <= R[j].id)
        {
            arr[k++] = L[i++];
        }
        else
        {
            arr[k++] = R[j++];
        }
    }
    while (i < n1)
        arr[k++] = L[i++];
    while (j < n2)
        arr[k++] = R[j++];

    free(L);
    free(R);
}

void merge_sort_by_id(int start, int end)
{
    if (start < end)
    {
        int mid = start + (end - start) / 2;
        merge_sort_by_id(start, mid);
        merge_sort_by_id(mid + 1, end);
        merge(files, start, mid, end);
    }
}

void merge_by_timestamp(file arr[], int start, int mid, int end)
{
    int i, j, k;
    int n1 = mid - start + 1;
    int n2 = end - mid;

    file *L = (file *)malloc(n1 * sizeof(file));
    file *R = (file *)malloc(n2 * sizeof(file));

    for (i = 0; i < n1; i++)
        L[i] = arr[start + i];
    for (j = 0; j < n2; j++)
        R[j] = arr[mid + 1 + j];

    i = 0;
    j = 0;
    k = start;

    while (i < n1 && j < n2)
    {
        if (strcmp(L[i].timestamp, R[j].timestamp) <= 0)
        {
            arr[k++] = L[i++];
        }
        else
        {
            arr[k++] = R[j++];
        }
    }

    while (i < n1)
        arr[k++] = L[i++];
    while (j < n2)
        arr[k++] = R[j++];

    free(L);
    free(R);
}

void merge_sort_by_timestamp(int start, int end)
{
    if (start < end)
    {
        int mid = start + (end - start) / 2;
        merge_sort_by_timestamp(start, mid);
        merge_sort_by_timestamp(mid + 1, end);
        merge_by_timestamp(files, start, mid, end);
    }
}

void count_sort(thread_args *args)
{
    if (args->key == 0)
    {
        // Sort based on name
        count_sort_by_name(args->start, args->end);
    }
    else if (args->key == 1)
    {
        // Sort based on id
        count_sort_by_id(args->start, args->end);
    }
    else if (args->key == 2)
    {
        // Sort based on timestamp
        count_sort_by_timestamp(args->start, args->end);
    }
}

void merge_sort(thread_args *args)
{
    if (args->key == 0)
    {
        // Sort based on name
        merge_sort_by_name(args->start, args->end);
    }
    else if (args->key == 1)
    {
        // Sort based on id
        merge_sort_by_id(args->start, args->end);
    }
    else if (args->key == 2)
    {
        // Sort based on timestamp
        merge_sort_by_timestamp(args->start, args->end);
    }
}

void *sort_worker(void *arg)
{
    thread_args *args = (thread_args *)arg;
    if (N < THRESHOLD)
    {
        printf("count_sort is being called\n");
        // count_sort_by_id(args->start, args->end);
        count_sort(args);
    }
    else
    {
        printf("merge_sort is being called\n");
        // merge_sort_by_id(args->start, args->end);
        merge_sort(args);
    }

    pthread_mutex_lock(&merge_mutex);
    sorted_segments++;
    if (sorted_segments == THREAD_COUNT)
    {
        pthread_cond_signal(&merge_cond);
    }
    pthread_mutex_unlock(&merge_mutex);
    return NULL;
}

int main()
{
    scanf("%d", &N);
    for (int i = 0; i < N; i++)
    {
        scanf("%s %d %s", files[i].name, &files[i].id, files[i].timestamp);
    }
    scanf("%s", sorting_criteria);

    int key;
    if (strcmp(sorting_criteria, "Name") == 0)
    {
        key = 0;
    }
    else if (strcmp(sorting_criteria, "ID") == 0)
    {
        key = 1;
    }
    else if (strcmp(sorting_criteria, "Timestamp") == 0)
    {
        key = 2;
    }
    else
    {
        printf("Invalid sorting criteria\n");
        return -1;
    }
    THREAD_COUNT = (N + FILES_PER_THREAD - 1) / FILES_PER_THREAD;
    pthread_t threads[THREAD_COUNT];
    thread_args args[THREAD_COUNT];
    int segment_size = N / THREAD_COUNT;
    pthread_mutex_init(&merge_mutex, NULL);
    pthread_cond_init(&merge_cond, NULL);

    // Creating threads for sorting segments
    for (int i = 0; i < THREAD_COUNT; i++)
    {
        args[i].start = i * segment_size;
        args[i].end = (i == THREAD_COUNT - 1) ? N - 1 : (i + 1) * segment_size - 1;
        args[i].key = key;
        pthread_create(&threads[i], NULL, sort_worker, &args[i]);
    }

    // Wait for sorting to complete
    pthread_mutex_lock(&merge_mutex);
    while (sorted_segments < THREAD_COUNT)
    {
        pthread_cond_wait(&merge_cond, &merge_mutex);
    }
    pthread_mutex_unlock(&merge_mutex);

    // Merge sorted segments
    // for (int i = 1; i < THREAD_COUNT; i++)
    // {
    //     merge(files, 0, args[i - 1].end, args[i].end);
    // }
    if (N)
    {
        for (int size = FILES_PER_THREAD; size < N; size *= 2)
        {
            for (int start = 0; start < N; start += 2 * size)
            {
                int mid = start + size - 1;
                int end = (start + 2 * size - 1 < N) ? start + 2 * size - 1 : N - 1;
                if (mid < end)
                {
                    if (key == 0)
                    {
                        merge_by_name(files, start, mid, end);
                    }
                    else if (key == 1)
                    {
                        merge(files, start, mid, end);
                    }
                    else if (key == 2)
                    {
                        merge_by_timestamp(files, start, mid, end);
                    }
                }
            }
        }
    }

    printf("Sorted files based on %s:\n", sorting_criteria);
    for (int i = 0; i < N; i++)
    {
        printf("%s %d %s\n", files[i].name, files[i].id, files[i].timestamp);
    }

    pthread_mutex_destroy(&merge_mutex);
    pthread_cond_destroy(&merge_cond);
    return 0;
}