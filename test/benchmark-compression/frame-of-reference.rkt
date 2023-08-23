#lang racket/base
(require ffi/unsafe/vm racket/fixnum racket/list racket/pretty)
(require
  ;"../../dbk/safe-unsafe.rkt"
  racket/unsafe/ops
  )

;;; Frame-of-reference compression for integers

(define (min-bits n)
  (let loop ((n n))
    (if (< 0 n) (+ 1 (loop (fxrshift n 1))) 0)))
(define (min-bytes n)
  (let ((bits (min-bits n)))
    (+ (quotient bits 8) (if (= 0 (remainder bits 8)) 0 1))))
(define (nat-min-byte-width nat.max) (max (min-bytes nat.max) 1))

(define (unsafe-bytes-nat-array-ref width len bs offset)
  (let ((go (lambda (n-ref)
              (let ((n* (make-fxvector len 0)))
                (let loop ((i 0) (pos offset))
                  (when (unsafe-fx< i len)
                    (unsafe-fxvector-set! n* i (n-ref bs pos))
                    (loop (unsafe-fx+ i 1) (unsafe-fx+ pos width))))
                n*))))
    (case width
      ((0) (make-fxvector len 0))
      ((1) (go 1-unrolled-unsafe-bytes-nat-ref))
      ((2) (go 2-unrolled-unsafe-bytes-nat-ref))
      ((3) (go 3-unrolled-unsafe-bytes-nat-ref))
      ((4) (go 4-unrolled-unsafe-bytes-nat-ref))
      ((5) (go 5-unrolled-unsafe-bytes-nat-ref))
      ((6) (go 6-unrolled-unsafe-bytes-nat-ref)))))
(define (unsafe-bytes-nat-array-set! width len bs offset n*)
  (let ((go (lambda (n-set!)
              (let ((n* (make-fxvector len 0)))
                (let loop ((i 0) (pos offset))
                  (when (unsafe-fx< i len)
                    (n-set! bs pos (unsafe-fxvector-ref n* i))
                    (loop (unsafe-fx+ i 1) (unsafe-fx+ pos width))))))))
    (case width
      ((0) (void))
      ((1) (go 1-unrolled-unsafe-bytes-nat-set!))
      ((2) (go 2-unrolled-unsafe-bytes-nat-set!))
      ((3) (go 3-unrolled-unsafe-bytes-nat-set!))
      ((4) (go 4-unrolled-unsafe-bytes-nat-set!))
      ((5) (go 5-unrolled-unsafe-bytes-nat-set!))
      ((6) (go 6-unrolled-unsafe-bytes-nat-set!)))))

(define (unsafe-bytes-nat-set! width bs offset n)
  (case width
    ((1)  (1-unrolled-unsafe-bytes-nat-set! bs offset n))
    ((2)  (2-unrolled-unsafe-bytes-nat-set! bs offset n))
    ((3)  (3-unrolled-unsafe-bytes-nat-set! bs offset n))
    ((4)  (4-unrolled-unsafe-bytes-nat-set! bs offset n))
    ((5)  (5-unrolled-unsafe-bytes-nat-set! bs offset n))
    ((6)  (6-unrolled-unsafe-bytes-nat-set! bs offset n))
    (else (rolled-unsafe-bytes-nat-set! width bs offset n))))
(define (rolled-unsafe-bytes-nat-set! width bs offset n)
  (let loop ((i     offset)
             (shift (unsafe-fxlshift (unsafe-fx- width 1) 3)))
    (when (unsafe-fx<= 0 shift)
      (unsafe-bytes-set! bs i (unsafe-fxand 255 (unsafe-fxrshift n shift)))
      (loop (unsafe-fx+ i     1)
            (unsafe-fx- shift 8)))))
(define (1-unrolled-unsafe-bytes-nat-set! bs i n) (unsafe-bytes-set! bs i n))
(define (2-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 n)))
(define (3-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 2) (unsafe-fxand 255 n)))
(define (4-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 24)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 2) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 3) (unsafe-fxand 255 n)))
(define (5-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 32)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 24)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 2) (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 3) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 4) (unsafe-fxand 255 n)))
(define (6-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 40)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 32)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 2) (unsafe-fxand 255 (unsafe-fxrshift n 24)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 3) (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 4) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 5) (unsafe-fxand 255 n)))
(define (unsafe-bytes-nat-ref width bs offset)
  (case width
    ((1)  (1-unrolled-unsafe-bytes-nat-ref bs offset))
    ((2)  (2-unrolled-unsafe-bytes-nat-ref bs offset))
    ((3)  (3-unrolled-unsafe-bytes-nat-ref bs offset))
    ((4)  (4-unrolled-unsafe-bytes-nat-ref bs offset))
    ((5)  (5-unrolled-unsafe-bytes-nat-ref bs offset))
    ((6)  (6-unrolled-unsafe-bytes-nat-ref bs offset))
    (else (rolled-unsafe-bytes-nat-ref width bs offset))))
(define (rolled-unsafe-bytes-nat-ref width bs offset)
  (let ((end (unsafe-fx+ offset width)))
    (let loop ((i offset) (n 0))
      (cond ((unsafe-fx< i end) (loop (unsafe-fx+ i 1)
                                      (unsafe-fx+ (unsafe-fxlshift n 8)
                                                  (unsafe-bytes-ref bs i))))
            (else               n)))))
(define (1-unrolled-unsafe-bytes-nat-ref bs i) (unsafe-bytes-ref bs i))
(define (2-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fx+ (unsafe-fxlshift (unsafe-bytes-ref bs             i)     8)
              (unsafe-bytes-ref                  bs (unsafe-fx+ i 1))))
(define (3-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fx+ (unsafe-fxlshift (unsafe-bytes-ref bs             i)    16)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 1))  8)
              (unsafe-bytes-ref                  bs (unsafe-fx+ i 2))))
(define (4-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fx+ (unsafe-fxlshift (unsafe-bytes-ref bs             i)    24)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 1)) 16)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 2))  8)
              (unsafe-bytes-ref                  bs (unsafe-fx+ i 3))))
(define (5-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fx+ (unsafe-fxlshift (unsafe-bytes-ref bs             i)    32)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 1)) 24)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 2)) 16)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 3))  8)
              (unsafe-bytes-ref                  bs (unsafe-fx+ i 4))))
(define (6-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fx+ (unsafe-fxlshift (unsafe-bytes-ref bs             i)    40)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 1)) 32)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 2)) 24)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 3)) 16)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 4))  8)
              (unsafe-bytes-ref                  bs (unsafe-fx+ i 5))))

(define enable-interrupts  (vm-primitive 'enable-interrupts))
(define disable-interrupts (vm-primitive 'disable-interrupts))

(define bytevector-u32-native-ref  (vm-primitive 'bytevector-u32-native-ref))
(define bytevector-u32-native-set! (vm-primitive 'bytevector-u32-native-set!))

;; Fit into 4 bytes
;(define v.min -16250000)
;(define v.max  16250000)

(define v.min -10000000)
(define v.max  10000000)

;; Fit into 3 bytes
;(define v.min -8000000)
;(define v.max  8000000)

(define count (- v.max v.min))

(define byte-width (nat-min-byte-width count))
(pretty-write `(count: ,count byte-width: ,byte-width))

(define size   (* count byte-width))
(define input  (make-bytes size))
(define output (make-fxvector count))

(define (encode n) (unsafe-fx- n v.min))
(define (decode n) (unsafe-fx+ n v.min))

(define (generate-input)
  (let ((n-set! (case byte-width
                  ((1) 1-unrolled-unsafe-bytes-nat-set!)
                  ((2) 2-unrolled-unsafe-bytes-nat-set!)
                  ((3) 3-unrolled-unsafe-bytes-nat-set!)
                  ((4) 4-unrolled-unsafe-bytes-nat-set!)
                  ((5) 5-unrolled-unsafe-bytes-nat-set!)
                  ((6) 6-unrolled-unsafe-bytes-nat-set!)
                  (else (error "invalid byte width" byte-width)))))
    (time (let loop ((i v.min) (start 0))
            (when (unsafe-fx< i v.max)
              (n-set! input start (encode i))
              (loop (unsafe-fx+ i 1) (unsafe-fx+ start byte-width)))))))

(define (decode-input)
  (let ((n-ref (case byte-width
                 ((1) 1-unrolled-unsafe-bytes-nat-ref)
                 ((2) 2-unrolled-unsafe-bytes-nat-ref)
                 ((3) 3-unrolled-unsafe-bytes-nat-ref)
                 ((4) 4-unrolled-unsafe-bytes-nat-ref)
                 ((5) 5-unrolled-unsafe-bytes-nat-ref)
                 ((6) 6-unrolled-unsafe-bytes-nat-ref)
                 (else (error "invalid byte width" byte-width)))))
    (time (let loop ((i 0) (start 0))
            (when (unsafe-fx< i count)
              (unsafe-fxvector-set! output i (decode (n-ref input start)))
              (loop (unsafe-fx+ i 1) (unsafe-fx+ start byte-width)))))))

(define (pretend-decode-input)
  (let ((n-ref (case byte-width
                 ((1) 1-unrolled-unsafe-bytes-nat-ref)
                 ((2) 2-unrolled-unsafe-bytes-nat-ref)
                 ((3) 3-unrolled-unsafe-bytes-nat-ref)
                 ((4) 4-unrolled-unsafe-bytes-nat-ref)
                 ((5) 5-unrolled-unsafe-bytes-nat-ref)
                 ((6) 6-unrolled-unsafe-bytes-nat-ref)
                 (else (error "invalid byte width" byte-width)))))
    (time (let loop ((i 0) (start 0))
            (when (unsafe-fx< i count)
              ;(unsafe-fxvector-set! output i (decode (n-ref input start)))
              (loop (unsafe-fx+ i 1) (unsafe-fx+ start byte-width)))))))

(define (pretend-decode-input-more)
  (let ((n-ref (case byte-width
                 ((1) 1-unrolled-unsafe-bytes-nat-ref)
                 ((2) 2-unrolled-unsafe-bytes-nat-ref)
                 ((3) 3-unrolled-unsafe-bytes-nat-ref)
                 ((4) 4-unrolled-unsafe-bytes-nat-ref)
                 ((5) 5-unrolled-unsafe-bytes-nat-ref)
                 ((6) 6-unrolled-unsafe-bytes-nat-ref)
                 (else (error "invalid byte width" byte-width)))))
    (time (let loop ((i 0) (start 0))
            (when (unsafe-fx< i count)
              (decode (n-ref input start))
              ;(unsafe-fxvector-set! output i (decode (n-ref input start)))
              (loop (unsafe-fx+ i 1) (unsafe-fx+ start byte-width)))))))

(define (generate-input4)
  (let (;(n-set! (lambda (bs i n) (integer->integer-bytes n 4 #f #f bs i)))
        (n-set! bytevector-u32-native-set!)
        )
    (time (let loop ((i v.min) (start 0))
            (when (unsafe-fx< i v.max)
              (n-set! input start (encode i))
              (loop (unsafe-fx+ i 1) (unsafe-fx+ start byte-width)))))))

(define (decode-input4)
  (let (;(n-ref (lambda (bs i) (integer-bytes->integer bs #f #f i (unsafe-fx+ i 4))))
        (n-ref bytevector-u32-native-ref)
        )
    (time (let loop ((i 0) (start 0))
            (when (unsafe-fx< i count)
              (unsafe-fxvector-set! output i (decode (n-ref input start)))
              (loop (unsafe-fx+ i 1) (unsafe-fx+ start byte-width)))))))

;; (count: 16000000 byte-width: 3)
;; cpu time: 200 real time: 201 gc time: 132
;; cpu time: 32 real time: 32 gc time: 0
;; cpu time: 61 real time: 63 gc time: 0
;; cpu time: 74 real time: 76 gc time: 0
;; (count: 20000000 byte-width: 4)
;; cpu time: 301 real time: 303 gc time: 184
;; cpu time: 40 real time: 40 gc time: 0
;; cpu time: 93 real time: 97 gc time: 0
;; cpu time: 105 real time: 109 gc time: 0
;; (count: 32500000 byte-width: 4)
;; cpu time: 525 real time: 528 gc time: 330
;; cpu time: 66 real time: 66 gc time: 0
;; cpu time: 152 real time: 156 gc time: 0
;; cpu time: 167 real time: 170 gc time: 0
(generate-input)
(pretend-decode-input)
(pretend-decode-input-more)
(decode-input)

;; with integer->integer-bytes and integer-bytes->integer:
;;   (count: 32500000 byte-width: 4)
;;   cpu time: 1016 real time: 1019 gc time: 312
;;   cpu time: 638 real time: 641 gc time: 0
;; with bytevector-u32-native-ref and bytevector-u32-native-set!:
;;   (count: 32500000 byte-width: 4)
;;   cpu time: 453 real time: 454 gc time: 294
;;   cpu time: 156 real time: 158 gc time: 0
;(generate-input4)
;(decode-input4)

(pretty-write (subbytes input 0 100))
(pretty-write (subbytes input 10000 10100))
(pretty-write (map (lambda (i) (fxvector-ref output i)) (range 10)))
(pretty-write (map (lambda (i) (fxvector-ref output (- (fxvector-length output) (+ 1 i)))) (reverse (range 10))))
