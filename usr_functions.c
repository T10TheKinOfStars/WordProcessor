#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <ctype.h>
#include "common.h"
#include "usr_functions.h"

//SIZE is for the number of character of each line in mediate file
#define SIZE 100

/* User-defined map function for the "Letter counter" task.  
   This map function is called in a map worker process.
   @param split: The data split that the map function is going to work on.
                 Note that the file offset of the file descripter split->fd should be set to the properly
                 position when this map function is called.
   @param fd_out: The file descriptor of the itermediate data file output by the map function.
   @ret: 0 on success, -1 on error.
 */
int letter_counter_map(DATA_SPLIT * split, int fd_out)
{
    // add your implementation here ...
    int fd = split -> fd;
    if (fd < 0 || fd_out < 0) {
        printf("invalid fd\n");
        return -1;
    }
    char c;
    char *buffer = (char*)malloc(SIZE * sizeof(char));
    memset(buffer,'\0',sizeof(buffer));
    int letters[26] = {0};
    int i,k,readsize = 0;

    while (readsize < split->size) {
        read(fd, &c, 1);
        readsize++;
        if (isalpha(c))
            letters[toupper(c) - 'A']++;
    }
    
    for (i = 0; i < 26; ++i) {
        k = sprintf(buffer,"%c %d\n",i + 'A', letters[i]);
        write(fd_out,buffer,k);
        memset(buffer,'\0',sizeof(buffer));
    }

    free(buffer);
    return 0;
}

/* User-defined reduce function for the "Letter counter" task.  
   This reduce function is called in a reduce worker process.
   @param p_fd_in: The address of the buffer holding the intermediate data files' file descriptors.
                   The imtermeidate data files are output by the map worker processes, and they
                   are the input for the reduce worker process.
   @param fd_in_num: The number of the intermediate files.
   @param fd_out: The file descriptor of the final result file.
   @ret: 0 on success, -1 on error.
   @example: if fd_in_num == 3, then there are 3 intermediate files, whose file descriptor is 
             identified by p_fd_in[0], p_fd_in[1], and p_fd_in[2] respectively.

*/
int letter_counter_reduce(int * p_fd_in, int fd_in_num, int fd_out)
{
    // add your implementation here ...
    if (fd_in_num <= 0 || fd_out <= 0) {
        printf("error fdinnum is %d,fd out is %d\n",fd_in_num, fd_out);
        return -1;
    }

    int letters[26] = {0};
    int i, j, value, start, end, len;
    char c, k;
    char *buffer = (char*)malloc(SIZE * sizeof(char));
    memset(buffer,'\0',sizeof(buffer));

    for (i = 0; i < fd_in_num; ++i) {
        lseek(p_fd_in[i],0,SEEK_SET);
        for(j = 0;j < 26; ++j) {
            do {
                read(p_fd_in[i], &c, sizeof(char));
            } while (!isdigit(c));
            start = lseek(p_fd_in[i],(-1) * sizeof(char), SEEK_CUR);
            do {
                read(p_fd_in[i], &c, sizeof(char));
            } while (isdigit(c));

            end = lseek(p_fd_in[i], (-1) * sizeof(char), SEEK_CUR);
            len = end - start;
            char* valstr = (char *)malloc(sizeof(char) * (len + 1));
            memset(valstr,'\0',len + 1);
            lseek(p_fd_in[i], start * sizeof(char), SEEK_SET);
            read(p_fd_in[i],valstr,len);
            value = atoi(valstr);
            letters[j] += value;

            free(valstr);
        }
    }

    for (i = 0; i < 26; ++i) {
        k = sprintf(buffer,"%c %d\n",i + 'A', letters[i]);
        write(fd_out,buffer,k);
        memset(buffer,'\0',sizeof(buffer));
    }

    free(buffer);
    return 0;
}

/* User-defined map function for the "Word finder" task.  
   This map function is called in a map worker process.
   @param split: The data split that the map function is going to work on.
                 Note that the file offset of the file descripter split->fd should be set to the properly
                 position when this map function is called.
   @param fd_out: The file descriptor of the itermediate data file output by the map function.
   @ret: 0 on success, -1 on error.
 */
int word_finder_map(DATA_SPLIT * split, int fd_out)
{
    // add your implementation here ...
    int fd = split -> fd;
    if(fd < 0) {
        printf("open input file error\n");
        return(-1);
    }
    if(fd_out < 0) {
        printf("open output file error\n");
        return(-1);
    }
    int readsize = 0, flag = 0;
    off_t start = 0, end = 0;
    char c;
    while (readsize < split -> size) {
        start = lseek(fd,0,SEEK_CUR);
        do {
            flag = read(fd,&c,sizeof(char));
            readsize++;
        } while (c != '\n' && flag);
        end = lseek(fd,0,SEEK_CUR);
        lseek(fd,start,SEEK_SET);
        char* buffer = (char*)malloc((end - start + 1)*sizeof(char));
        memset(buffer,'\0',(end - start + 1));
        read(fd,buffer,end - start);
        if (strstr(buffer,split -> usr_data)) {
            write(fd_out,buffer,strlen(buffer));
        }
        free(buffer); 
    }
    return 0;
}

/* User-defined reduce function for the "Word finder" task.  
   This reduce function is called in a reduce worker process.
   @param p_fd_in: The address of the buffer holding the intermediate data files' file descriptors.
                   The imtermeidate data files are output by the map worker processes, and they
                   are the input for the reduce worker process.
   @param fd_in_num: The number of the intermediate files.
   @param fd_out: The file descriptor of the final result file.
   @ret: 0 on success, -1 on error.
   @example: if fd_in_num == 3, then there are 3 intermediate files, whose file descriptor is 
             identified by p_fd_in[0], p_fd_in[1], and p_fd_in[2] respectively.

*/
int word_finder_reduce(int * p_fd_in, int fd_in_num, int fd_out)
{
    // add your implementation here ...
    lseek(fd_out,0,SEEK_SET);

    if (fd_out <= 0) {
        printf("open output file error.\n");
        return -1;
    }

    int i;
    off_t size = 0;
    for (i = 0; i < fd_in_num; ++i) {
        lseek(p_fd_in[i],0,SEEK_SET);
        size = lseek(p_fd_in[i],0,SEEK_END);
        char* buffer = (char *)malloc((size)*sizeof(char));
        memset(buffer,'\0',size);
        lseek(p_fd_in[i],0,SEEK_SET);
        read(p_fd_in[i],buffer,size);
        write(fd_out,buffer,size);
        free(buffer);
    }

    return 0;
}


