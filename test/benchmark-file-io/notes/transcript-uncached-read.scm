14:20:16 (master) greg@greg-mac.local:~/private/repositories/projects/dbKanren/test/medikanren-tests/semmed.db
> ll
total 0
drwxr-xr-x  46 greg  staff   1.5K Nov  7  2022 block/
drwxr-xr-x   3 greg  staff   102B Nov  7  2022 metadata/
drwxr-xr-x   2 greg  staff    68B Nov  7  2022 trash/
14:20:16 (master) greg@greg-mac.local:~/private/repositories/projects/dbKanren/test/medikanren-tests/semmed.db
> cd trash/^C
14:20:20 (master) greg@greg-mac.local:~/private/repositories/projects/dbKanren/test/medikanren-tests/semmed.db
> t
.
├── [1.5K Nov  7  2022]  block/
│   ├── [316M Nov  7  2022]  41
│   ├── [ 33M Nov  7  2022]  42
│   ├── [321M Nov  7  2022]  50
│   ├── [  24 Nov  7  2022]  51
│   ├── [107M Nov  7  2022]  52
│   ├── [321M Nov  7  2022]  53
│   ├── [2.8M Nov  7  2022]  54
│   ├── [  12 Nov  7  2022]  55
│   ├── [971K Nov  7  2022]  56
│   ├── [2.8M Nov  7  2022]  57
│   ├── [ 40M Nov  7  2022]  58
│   ├── [ 40M Nov  7  2022]  59
│   ├── [  24 Nov  7  2022]  60
│   ├── [107M Nov  7  2022]  61
│   ├── [321M Nov  7  2022]  62
│   ├── [  24 Nov  7  2022]  63
│   ├── [ 23M Nov  7  2022]  64
│   ├── [  27 Nov  7  2022]  65
│   ├── [321M Nov  7  2022]  66
│   ├── [ 31M Nov  7  2022]  67
│   ├── [432K Nov  7  2022]  68
│   ├── [ 30M Nov  7  2022]  69
│   ├── [432K Nov  7  2022]  70
│   ├── [ 40M Nov  7  2022]  71
│   ├── [ 30M Nov  7  2022]  72
│   ├── [390K Nov  7  2022]  73
│   ├── [ 30M Nov  7  2022]  74
│   ├── [390K Nov  7  2022]  75
│   ├── [ 40M Nov  7  2022]  76
│   ├── [ 30M Nov  7  2022]  77
│   ├── [ 40M Nov  7  2022]  78
│   ├── [432K Nov  7  2022]  79
│   ├── [ 40M Nov  7  2022]  80
│   ├── [ 40M Nov  7  2022]  81
│   ├── [390K Nov  7  2022]  82
│   ├── [ 40M Nov  7  2022]  83
│   ├── [  12 Nov  7  2022]  84
│   ├── [971K Nov  7  2022]  85
│   ├── [2.8M Nov  7  2022]  86
│   ├── [  12 Nov  7  2022]  87
│   ├── [1.4M Nov  7  2022]  88
│   ├── [  21 Nov  7  2022]  89
│   ├── [2.8M Nov  7  2022]  90
│   └── [1.4M Nov  7  2022]  91
├── [ 102 Nov  7  2022]  metadata/
│   └── [ 32K Nov  7  2022]  current.scm
└── [  68 Nov  7  2022]  trash/

3 directories, 45 files
14:20:21 (master) greg@greg-mac.local:~/private/repositories/projects/dbKanren/test/medikanren-tests/semmed.db
> cd block/
14:20:26 (master) greg@greg-mac.local:~/private/repositories/projects/dbKanren/test/medikanren-tests/semmed.db/block
> racket
Welcome to Racket v8.4 [cs].
> (define (time-read fname size)
    (call-with-input-file fname
      (lambda (in)
        (file-stream-buffer-mode in 'none)
        (let ((bv.target (make-bytes size)))
          (time (read-bytes! bv.target in 0 size))))))
> (define 1kb 1024)
> (define 1mb (* 1kb 1024))
> (define 64kb (* 1kb 64))
> (define 4kb (* 1kb 4))
> (define 16kb (* 1kb 16))
> (define 16mb (* 1mb 16))
> (define 4mb (* 1mb 4))
> (time-read "41" 4mb)
cpu time: 2 real time: 4 gc time: 0
4194304
> (time-read "42" 1mb)
cpu time: 0 real time: 2 gc time: 0
1048576
> (time-read "80" 1mb)
cpu time: 0 real time: 2 gc time: 0
1048576
> (define 2mb (* 1mb 2))
> (time-read "91" 2mb)
cpu time: 0 real time: 2 gc time: 0
1475616
> (time-read "50" 64kb)
cpu time: 0 real time: 1 gc time: 0
65536
> (time-read "51" 16kb)
cpu time: 0 real time: 0 gc time: 0
24
> (time-read "90" 2mb)
cpu time: 1 real time: 3 gc time: 0
2097152
> (time-read "52" 16kb)
cpu time: 0 real time: 1 gc time: 0
16384
>
14:31:06 (master) greg@greg-mac.local:~/private/repositories/projects/dbKanren/test/medikanren-tests/semmed.db/block
> scheme
Chez Scheme Version 9.4.1
Copyright 1984-2016 Cisco Systems, Inc.

> (define 1kb 1024)
> (define 4kb (* 1kb 4))
> (define 16kb (* 1kb 16))
> (define 64kb (* 1kb 64))
> (define 1mb (* 1kb 1024))
> (define 2mb (* 1mb 2))
> (define 4mb (* 1mb 4))
> (define 16mb (* 1mb 16))
> (define (time-read fname size)
    (call-with-input-file fname
      (lambda (in)
        (let ((bv.target (make-bytevector size)))
          (time (get-bytevector-some! in bv.target 0 size))))
      'unbuffered))
> (time-read "51" 16kb)
Exception in get-bytevector-some!: #<input port 51> is not a binary input port
Type (debug) to enter the debugger.
> (define (time-read fname size)
    (let ((in (open-file-input-port name (file-options) 'none))
          (bv.target (make-bytevector size)))
      (let ((result (time (get-bytevector-some! in bv.target 0 size))))
        (close-port in)
        result)))
> (time-read "51" 16kb)
Exception: variable name is not bound
Type (debug) to enter the debugger.
> (define (time-read fname size)
    (let ((in (open-file-input-port fname (file-options) 'none))
          (bv.target (make-bytevector size)))
      (let ((result (time (get-bytevector-some! in bv.target 0 size))))
        (close-port in)
        result)))
> (time-read "51" 16kb)
(time (get-bytevector-some! in ...))
    no collections
    0.000010000s elapsed cpu time
    0.000010000s elapsed real time
    112 bytes allocated
24
> (time-read "41" 16kb)
(time (get-bytevector-some! in ...))
    no collections
    0.000044000s elapsed cpu time
    0.000041000s elapsed real time
    112 bytes allocated
16384
> (time-read "42" 16kb)
(time (get-bytevector-some! in ...))
    no collections
    0.000042000s elapsed cpu time
    0.000042000s elapsed real time
    112 bytes allocated
16384
> (time-read "42" 1mb)
(time (get-bytevector-some! in ...))
    no collections
    0.001215000s elapsed cpu time
    0.001274000s elapsed real time
    112 bytes allocated
1048576
> (time-read "42" 4kb)
(time (get-bytevector-some! in ...))
    no collections
    0.000017000s elapsed cpu time
    0.000013000s elapsed real time
    112 bytes allocated
4096
> (time-read "60" 4kb)
(time (get-bytevector-some! in ...))
    no collections
    0.000111000s elapsed cpu time
    0.000919000s elapsed real time
    112 bytes allocated
24
> (time-read "61" 16kb)
(time (get-bytevector-some! in ...))
    no collections
    0.000157000s elapsed cpu time
    0.001002000s elapsed real time
    112 bytes allocated
16384
> (time-read "62" 64kb)
(time (get-bytevector-some! in ...))
    no collections
    0.000193000s elapsed cpu time
    0.001161000s elapsed real time
    112 bytes allocated
65536
> (define 256kb (* 1kb 256))
> (time-read "63" 256kb)
(time (get-bytevector-some! in ...))
    no collections
    0.000113000s elapsed cpu time
    0.001004000s elapsed real time
    112 bytes allocated
24
> (time-read "64" 256kb)
(time (get-bytevector-some! in ...))
    no collections
    0.000333000s elapsed cpu time
    0.001556000s elapsed real time
    112 bytes allocated
262144
> (time-read "65" 4kb)
(time (get-bytevector-some! in ...))
    no collections
    0.000120000s elapsed cpu time
    0.000976000s elapsed real time
    112 bytes allocated
27
> (time-read "65" 1mb)
(time (get-bytevector-some! in ...))
    no collections
    0.000011000s elapsed cpu time
    0.000009000s elapsed real time
    112 bytes allocated
27
> (time-read "66" 1mb)
(time (get-bytevector-some! in ...))
    no collections
    0.001409000s elapsed cpu time
    0.003386000s elapsed real time
    112 bytes allocated
1048576
> (define 512kb (* 1kb 512))
> (time-read "66" 512kb)
(time (get-bytevector-some! in ...))
    no collections
    0.000153000s elapsed cpu time
    0.000154000s elapsed real time
    112 bytes allocated
524288
> (time-read "67" 512kb)
(time (get-bytevector-some! in ...))
    no collections
    0.000499000s elapsed cpu time
    0.001823000s elapsed real time
    112 bytes allocated
524288
> (define 128kb (* 1kb 128))
> (time-read "68" 128kb)
(time (get-bytevector-some! in ...))
    no collections
    0.000215000s elapsed cpu time
    0.001263000s elapsed real time
    112 bytes allocated
131072
> (time-read "69" 4kb)
(time (get-bytevector-some! in ...))
    no collections
    0.000114000s elapsed cpu time
    0.000962000s elapsed real time
    112 bytes allocated
4096
> (time-read "70" 4mb)
(time (get-bytevector-some! in ...))
    no collections
    0.000492000s elapsed cpu time
    0.001836000s elapsed real time
    112 bytes allocated
442773
> (time-read "71" 4mb)
(time (get-bytevector-some! in ...))
    no collections
    0.001858000s elapsed cpu time
    0.005316000s elapsed real time
    112 bytes allocated
4194304
> (time-read "72" 2mb)
(time (get-bytevector-some! in ...))
    no collections
    0.002822000s elapsed cpu time
    0.003951000s elapsed real time
    112 bytes allocated
2097152
> (time-read "74" 4mb)
(time (get-bytevector-some! in ...))
    no collections
    0.004336000s elapsed cpu time
    0.006845000s elapsed real time
    112 bytes allocated
4194304
> (time-read "88" 1mb)
(time (get-bytevector-some! in ...))
    no collections
    0.001257000s elapsed cpu time
    0.002910000s elapsed real time
    112 bytes allocated
1048576
>
