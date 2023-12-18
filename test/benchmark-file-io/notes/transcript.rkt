Welcome to Racket v8.4 [cs].
> (define out (open-output-file "test.txt"))
> (define in  (open-input-file "test.txt"))
> (read-byte in)
#<eof>
> (read-byte in)
#<eof>
> (write-bytes (bytes 1 2 3 4 5) out)
5
> (read-byte in)
#<eof>
> (read-byte in)
#<eof>
> (read-byte in)
#<eof>
> (read-byte in)
#<eof>
> (read-byte in)
#<eof>
> (flush-output out)
> (read-byte in)
1
> (read-byte in)
2
> (file-position out 3)
> (write-bytes (bytes 6 7 8 9 10) out)
5
> (flush-output out)
> (read-byte in)
3
> (read-byte in)
4
> (read-byte in)
5
> (read-byte in)
8
> (file-position out 0)
> (write-bytes (bytes 1 2 3 4 5) out)
5
> (flush-output out)
> (file-position in 0)
> (read-byte in)
1
> (read-byte in)
2
> (read-byte in)
3
> (file-position out 0)
> (write-bytes (bytes 6 7 8 9 10) out)
5
> (flush-output out)
> (file-position in 0)
> (read-byte in)
6
> (read-byte in)
7
> (read-byte in)
8
> (file-position out 0)
> (write-bytes (bytes 1 2 3 4 5) out)
5
> (flush-output out)
> (file-position in)
3
> (file-position in 3)
> (read-byte in)
4
> (file-position out 0)
> (file-position in 0)
> (write-bytes (bytes 11 12 13 14 15 16 17 18 19 20) out)
10
> (flush-output out)
> (file-position in)
0
> (read-byte in)
11
> (write-bytes (bytes 21 22 23 24 25 26 27 28 29 30) out)
10
> (flush-output out)
> (read-byte in)
12
> (file-position in (file-position in))
> (read-byte in)
13
> (read-byte in)
14
> (file-position in)
4
> (file-position in 4)
> (read-byte in)
15
> (file-position in)
5
> (file-position in 4)
> (file-position in 5)
> (read-byte in)
16
> (file-stream-buffer-mode in)
block
> (file-stream-buffer-mode in 'none)
> (read-byte in)
17
> (file-position in 4)
> (file-position in 5)
> (read-byte in)
16
> (read-byte in)
17
> (define in2  (open-input-file "test.txt"))
> (file-position in)
7
> (file-position in2 7)
> (read-byte in2)
18
> (read-byte in)
18
> hexdump -d test.txt
hexdump: undefined;
 cannot reference an identifier before its definition
  in module: top-level
 [,bt for context]
-d: undefined;
 cannot reference an identifier before its definition
  in module: top-level
 [,bt for context]
test.txt: undefined;
 cannot reference an identifier before its definition
  in module: top-level
 [,bt for context]
> (file->bytes "test.txt")
#"\v\f\r\16\17\20\21\22\23\24\25\26\27\30\31\32\e\34\35\36"
> (bytes->list (file->bytes "test.txt"))
(11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30)
>

Welcome to Racket v8.4 [cs].
> (define out  (open-output-file "test.txt"))
> (define in   (open-input-file "test.txt"))
> (define in2  (open-input-file "test.txt"))
> (read-byte in)
#<eof>
> (write-bytes (bytes 11 12 13 14 15 16 17 18 19 20) out)
10
> (flush-output out)
> (read-byte in)
11
> (write-bytes (bytes 21 22 23 24 25 26 27 28 29 30) out)
10
> (flush-output out)
> (read-byte in2)
11
>

Welcome to Racket v8.4 [cs].
> (define out  (open-output-file "test.txt"))
> (define in   (open-input-file "test.txt"))
> (define in2  (open-input-file "test.txt"))
> (file-position out 0)
> (write-bytes (bytes 11 12 13 14 15 16 17 18 19 20) out)
10
> (flush-output out)
> (read-byte in)
11
> (file-position out 0)
> (write-bytes (bytes 21 22 23 24 25 26 27 28 29 30) out)
10
> (flush-output out)
> (read-byte in2)
21
> (read-byte in)
12
> (read-byte in2)
22
> (read-byte in)
13
> (read-byte in)
14
> (file-position in (file-position in))
> (read-byte in)
25
> (read-byte in2)
23
> (read-byte in2)
24
> (read-byte in2)
25
> (file-position out 500)
> (flush-output out)
> (file-position out)
500
> (write-byte 111 out)
> (flush-output out)
>
