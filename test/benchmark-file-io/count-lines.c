#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>

#define BUFFER_SIZE (16 * 1024)
//#define BUFFER_SIZE 16777216
//char buffer[BUFFER_SIZE];

long count_lines(int fd) {
  ssize_t read_size;
  long count;
  char buffer[BUFFER_SIZE];
  //unsigned char buffer[BUFFER_SIZE];

  count = 0;
  while ((read_size = read(fd, buffer, BUFFER_SIZE)) > 0) {
  //if ((read_size = read(fd, buffer, BUFFER_SIZE)) > 0) {
    for (int i = 0; i < read_size; ++i) {

      count += (((unsigned char)(buffer[i])) - 10);

      //count += (buffer[i] - 10);

      /*if (buffer[i] == '\n') {*/
        /*count += 1;*/
      /*}*/

    }
  }
  if (read_size == -1) {
    return -1;
  }
  return count;
}

int main() {
  int fd = open("rtx-kg2-s3/rtx-kg2_nodes_2.8.1.tsv", O_RDONLY);
  //int fd = open("rtx-kg2-s3/rtx-kg2_edges_2.8.1.tsv", O_RDONLY);
  printf("%ld\n", count_lines(fd));
  close(fd);
}
