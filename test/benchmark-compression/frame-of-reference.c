// gcc -O2 -o frame-of-reference.out frame-of-reference.c && ./frame-of-reference.out && rm frame-of-reference.out
#include <sys/time.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
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
typedef long long s64;
typedef unsigned char u8;

void show_u8(u8* vec, u64 start, u64 end) {
  for (u64 i = start; i < end; ++i) {
    printf("%hhu ", vec[i]);
  }
  printf("\n");
}
void show_s64(s64* vec, u64 start, u64 end) {
  for (u64 i = start; i < end; ++i) {
    printf("%lld ", vec[i]);
  }
  printf("\n");
}

u64 min_bits(u64 n) {
  u64 bits = 0;
  while (0 < n) {
    bits += 1;
    n >>= 1;
  }
  return bits;
}
u64 min_bytes(u64 n) {
  u64 bits = min_bits(n);
  return (bits / 8) + (((bits % 8) == 0) ? 0 : 1);
}
static inline u64 max(u64 a, u64 b) { return (a < b) ? b : a; }
u64 nat_min_byte_width(u64 n_max) { return max(min_bytes(n_max), 1); }

s64 v_min, v_max;
u64 expected_byte_width, count, byte_width, size;
u8 *input;
s64* output;

void init() {
  // Fit into 4 bytes
/*#define nat_set nat_set4*/
/*#define nat_ref nat_ref4*/
  /*expected_byte_width = 4;*/
  /*v_min = -16250000;*/
  /*v_max =  16250000;*/

#define nat_set nat_set4
#define nat_ref nat_ref4
  expected_byte_width = 4;
  v_min = -10000000;
  v_max =  10000000;

  // Fit into 3 bytes
/*#define nat_set nat_set3*/
/*#define nat_ref nat_ref3*/
  /*expected_byte_width = 3;*/
  /*v_min = -8000000;*/
  /*v_max =  8000000;*/

  count = v_max - v_min;
  byte_width = nat_min_byte_width(count);
  size = count * byte_width;

  input = malloc(size);
  output = malloc(count * sizeof(s64));
}

static inline u64 encode(s64 n) { return n - v_min; }
static inline s64 decode(u64 n) { return n + v_min; }

static inline void nat_set3(u8* bvec, u64 i, u64 n) {
  bvec[i]   = ((n >> 16) & 255);
  bvec[i+1] = ((n >> 8)  & 255);
  bvec[i+2] = (n & 255);
}

static inline u64 nat_ref3(u8* bvec, u64 i) {
  return
    ((bvec[i]   << 16) +
     (bvec[i+1] <<  8) +
     bvec[i+2]);
}

static inline void nat_set4(u8* bvec, u64 i, u64 n) {
  bvec[i]   = ((n >> 24) & 255);
  bvec[i+1] = ((n >> 16) & 255);
  bvec[i+2] = ((n >> 8)  & 255);
  bvec[i+3] = (n & 255);
}

static inline u64 nat_ref4(u8* bvec, u64 i) {
  return
    ((bvec[i]   << 24) +
     (bvec[i+1] << 16) +
     (bvec[i+2] <<  8) +
     bvec[i+3]);
}

void generate_input() {
  time_start();
  u64 start = 0;
  for (s64 i = v_min; i < v_max; ++i, start += byte_width) {
    nat_set(input, start, encode(i));
  }
  time_end();
}

void decode_input() {
  time_start();
  for (u64 i = 0, start = 0; i < count; ++i, start += byte_width) {
    output[i] = decode(nat_ref(input, start));
  }
  time_end();
}

u64 throwaway = 0;

void pretend_decode_input() {
  time_start();
  for (u64 i = 0, start = 0; i < count; ++i, start += byte_width) {
    ++throwaway;
    //output[i] = decode(nat_ref(input, start));
  }
  time_end();
}

void pretend_decode_input_more() {
  time_start();
  for (u64 i = 0, start = 0; i < count; ++i, start += byte_width) {
    throwaway += decode(nat_ref(input, start));
    //output[i] = ;
  }
  time_end();
}

int main() {
  assert(sizeof(u64) == 8);
  assert(sizeof(s64) == 8);
  assert(sizeof(u8) == 1);
  assert(byte_width == expected_byte_width);

  init();
  printf("count: %llu byte-width: %llu\n", count, byte_width);

  // count: 16000000 byte-width: 3
  // 0.047027 elapsed seconds
  // 0.000001 elapsed seconds
  // 0.021228 elapsed seconds
  // 0.099596 elapsed seconds
  // count: 20000000 byte-width: 4
  // 0.069665 elapsed seconds
  // 0.000000 elapsed seconds
  // 0.037357 elapsed seconds
  // 0.135385 elapsed seconds
  // count: 32500000 byte-width: 4
  // 0.120382 elapsed seconds
  // 0.000000 elapsed seconds
  // 0.050082 elapsed seconds
  // 0.206945 elapsed seconds
  generate_input();
  pretend_decode_input();
  pretend_decode_input_more();
  decode_input();

  show_u8(input, 0, 100);
  show_u8(input, 10000, 10100);
  show_s64(output, 0, 10);
  show_s64(output, count-10, count);
  return 0;
}
