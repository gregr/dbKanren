// gcc -O2 -o share-prefix-with-previous.out share-prefix-with-previous.c && ./share-prefix-with-previous.out && rm share-prefix-with-previous.out
#include <sys/time.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

struct timeval timespec_start, timespec_end;
void time_start() {
  if (gettimeofday(&timespec_start, 0) != 0) {
    printf("time_start failed\n");
    exit(1);
  }
}
void time_end() {
  if (gettimeofday(&timespec_end, 0) != 0) {
    printf("time_end failed\n");
    exit(1);
  }
  time_t delta_sec = timespec_end.tv_sec - timespec_start.tv_sec;
  suseconds_t delta_usec = timespec_end.tv_usec - timespec_start.tv_usec;
  delta_usec += delta_sec * 1000000;
  delta_sec = delta_usec / 1000000;
  delta_usec = delta_usec % 1000000;
  printf("%ld.%06d elapsed seconds\n", delta_sec, delta_usec);
}

typedef unsigned long long u64;
typedef unsigned int u32;
typedef unsigned char u8;

void show(u8* vec, u64 start, u64 end) {
  for (u64 i = start; i < end; ++i) {
    u8 ch = vec[i];
    if (31 < ch && ch < 128) {
      printf("%c", ch);
    } else {
      printf("\\%x", ch);
    }
  }
  printf("\n");
}

#define COUNT 10000000
#define DIGIT_COUNT 7
#define LEN_PREFIX 6
#define LEN (DIGIT_COUNT + LEN_PREFIX)
u8* prefix = (u8*)"SHARE:";
u64 count = COUNT;
u64 digit_count = DIGIT_COUNT;
u64 len_prefix = LEN_PREFIX;
u64 len = LEN;
u64 size = LEN * COUNT;
u8 input[LEN * COUNT], output[LEN * COUNT];
u64 lengths[COUNT], shared_prefix_lengths[COUNT];
void init() {
  for (u64 i = 0; i < count; ++i) { lengths[i] = len; }
}

static inline void copy(u8* out, u8* in, u64 size) {
  for (u64 i = 0; i < size; ++i) { out[i] = in[i]; }
}

void generate_input() {
  time_start();
  u64 pos = 0;
  u8 buf1[LEN], buf2[LEN];
  u8 *previous = buf1, *next = buf2;
  for (u64 i = 0; i < len; ++i) { previous[i] = 0; }
  for (u64 i = 0; i < count; ++i) {
    for (u64 i = 0; i < len; ++i) { next[i] = '0'; }
    copy(next, prefix, len_prefix);
    for (u64 n = i, j = len - 1; 0 < n; n /= 10, --j) {
      next[j] = '0' + (n % 10);
    }
    u64 shared_prefix_length = 0;
    {
      u64 j = 0;
      for (; (j < len) && (previous[j] == next[j]); ++j) { }
      shared_prefix_length = j;
    }
    shared_prefix_lengths[i] = shared_prefix_length;
    u64 diff_len = len-shared_prefix_length;
    copy(input+pos, next+shared_prefix_length, diff_len);
    pos += diff_len;
    u8 *temp = previous;
    previous = next;
    next = temp;
  }
  time_end();
}

void decode_input() {
  time_start();
  for (u64 i = 0, start_in = 0, start_out = 0, prev_out = 0; i < count; ++i, start_out += len) {
    u64 shared_prefix_length = shared_prefix_lengths[i];
    u64 diff_len = len - shared_prefix_length;
    copy(output+start_out, output+prev_out, shared_prefix_length);
    copy(output+start_out+shared_prefix_length, input+start_in, diff_len);
    start_in += diff_len;
    prev_out = start_out;
  }
  time_end();
}

void pretend_decode_input() {
  time_start();
  for (u64 i = 0, start_in = 0, start_out = 0, prev_out = 0; i < count; ++i, start_out += len) {
    u64 shared_prefix_length = shared_prefix_lengths[i];
    u64 diff_len = len - shared_prefix_length;
    /*copy(output+start_out, output+prev_out, shared_prefix_length);*/
    /*copy(output+start_out+shared_prefix_length, input+start_in, diff_len);*/
    start_in += diff_len;
    prev_out = start_out;
  }
  time_end();
}

int main() {
  assert(sizeof(u64) == 8);
  assert(sizeof(u32) == 4);
  assert(sizeof(u8) == 1);
  init();

  // 0.376858 elapsed seconds
  generate_input();

  // 0.152725 elapsed seconds
  time_start();
  memcpy(output, input, size);  // slower than any of the loops with O2 optimization
  time_end();
  // 0.020945 elapsed seconds
  time_start();
  copy(output, input, size);
  time_end();
  // 0.021991 elapsed seconds
  time_start();
  for (u64 i = 0; i < size; ++i) { output[i] = input[i]; }
  time_end();
  // For some reason, unrolling any of the following loops by 2x, 4x, or 8x is
  // slower than not unrolling.
  /*time_start();*/
  /*for (u64 i = 0; i < size; i+=2) {*/
    /*output[i]   = input[i];*/
    /*output[i+1] = input[i+1];*/
  /*}*/
  /*time_end();*/
  // 0.019748 elapsed seconds
  {
    time_start();
    u32* in32 = (u32*)input;
    u32* out32 = (u32*)output;
    u64 size32 = size/sizeof(u32);
    for (u64 i = 0; i < size32; ++i) { out32[i] = in32[i]; }
    time_end();
  }
  // 0.028728 elapsed seconds
  {
    time_start();
    u64* in64 = (u64*)input;
    u64* out64 = (u64*)output;
    u64 size64 = size/sizeof(u64);
    for (u64 i = 0; i < size64; ++i) { out64[i] = in64[i]; }
    time_end();
  }

  // 0.000000 elapsed seconds
  pretend_decode_input();
  // 0.137580 elapsed seconds
  decode_input();

  show(input, 0, 100);
  show(input, 10000, 10100);
  show(output, 0, 100);
  show(output, size-100, size);
  return 0;
}
