#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <ctype.h>
#include "mapreduce.h"
#include "common.h"
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include "usr_functions.h"

// add your code here ...
void handler(int value) {

}

void mapreduce(MAPREDUCE_SPEC * spec, MAPREDUCE_RESULT * result)
{
    // add you code here ...
    int n = spec -> split_num;
    //register signal
    //signal(SIGUSR1,handler);
    //used to store processes' fds
    int* p_fd_in = (int *)malloc(n * sizeof(int));
    off_t posarr[n + 1];
    int i;
    for (i = 0; i < n; ++i)
        posarr[i] = 0;
    int fd = open(spec -> input_data_filepath,O_RDONLY);
    off_t filesize = lseek(fd,0,SEEK_END);
    posarr[n] = filesize;
    off_t splitsize;
    if (n == 1)
        splitsize = filesize;
    else
        splitsize = filesize / n + 1;
    //work for signal
    sigset_t initMask, prevMask, waitMask;
    struct sigaction sa;
    sigemptyset(&initMask);
    sigaddset(&initMask, SIGUSR1);
    sa.sa_flags = 0;
    sa.sa_handler = handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGUSR1, &sa, NULL);
    sigfillset(&waitMask);
    sigdelset(&waitMask, SIGUSR1);
    sigprocmask(SIG_BLOCK, &initMask, &prevMask);

    char c;
    char* buffer = (char *)malloc(10 * sizeof(char));
    memset(buffer,'\0',10);
    lseek(fd,0,SEEK_SET);
    //split the large data file
    for (i = 1; i < n; ++i) {
        lseek(fd,(posarr[i - 1] + splitsize) * sizeof(char),SEEK_SET);
        do {
            read(fd, &c, sizeof(char));
        } while (c != '\n');
        posarr[i] = lseek(fd,0,SEEK_CUR);
    }
    
    struct timeval start, end;

    if (NULL == spec || NULL == result)
    {
        EXIT_ERROR(ERROR, "NULL pointer!\n");
    }   

    for (i = 0; i < n; ++i) {
        sprintf(buffer,"mr-%d.itm",i);
        p_fd_in[i] = open(buffer,O_CREAT|O_RDWR|O_TRUNC,S_IRUSR|S_IWUSR);
    }
    free(buffer);
    gettimeofday(&start, NULL);

    for (i=0;i<n;++i) {
        
    }
    
    // add your code here ...
    //fork four children processes
    for (i = 0; i < n; ++i) {
        int pid = fork();
        if(pid == 0) {
            //here is the child process
            //fd_c is original file
            int fd_c = open(spec -> input_data_filepath,O_RDONLY);
            lseek(fd_c,(size_t)posarr[i],SEEK_SET);
            DATA_SPLIT *split_c = (DATA_SPLIT *)malloc(sizeof(DATA_SPLIT));
            split_c -> fd = fd_c;
            split_c -> size = posarr[i + 1] - posarr[i];
            split_c -> usr_data = spec -> usr_data;
            //sleep(i*3);
            spec -> map_func(split_c,p_fd_in[i]);
            //printf("process %d finish map\n",i);
            if(i == 0) {
                //result -> reduce_worker_pid = getpid();
                int fdout_result = open(result -> filepath, O_CREAT|O_RDWR|O_TRUNC,S_IRUSR|S_IWUSR);
                if (fdout_result <= 0) {
                    printf("fdout error\n");
                }
                sigsuspend(&waitMask);
                spec -> reduce_func(p_fd_in, n, fdout_result);
                close(fdout_result);
            }
            close(fd_c);
            close(fd);
            for (i = 0; i < n; ++i) {
                close(p_fd_in[i]);
            }
            free(split_c);
            free(p_fd_in);
            free(result -> map_worker_pid);
            exit(0);
        } else {
            result -> map_worker_pid[i] = pid;
            if (i == 0)
                result -> reduce_worker_pid = pid;
        }
    }
    //wait all the children except the first child process finish
    for (i = 0; i < n - 1; ++i) {
        wait(NULL);
    }
    kill(result->map_worker_pid[0],SIGUSR1);
    wait(NULL);
    sigprocmask(SIG_SETMASK, &prevMask, NULL);

    gettimeofday(&end, NULL);   
    for (i = 0; i < n; ++i) {
        close(p_fd_in[i]);
    }
    free(p_fd_in);
    close(fd);
    result->processing_time = (end.tv_sec - start.tv_sec) * US_PER_SEC + (end.tv_usec - start.tv_usec);
}
