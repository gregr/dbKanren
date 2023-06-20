#lang racket/base
(provide
  (struct-out storage-read-write-methods)
  storage:file-read-only
  storage:file-read-write
  storage:port-read-only
  storage:port-read-write
  storage:bytevector-read-only
  storage:bytevector-read-write
  )
(require
  "../dbk/safe-unsafe.rkt"
  ;racket/unsafe/ops
  racket/fixnum racket/splicing)

;;;;;;;;;;;;;;;
;;; Storage ;;;
;;;;;;;;;;;;;;;

(struct storage-read-write-methods (read write resize))
(define (storage:file-read-only  path)
  (unless (file-exists? path) (error "storage does not exist" path))
  (storage:port-read-only  (open-input-file path)))
(define (storage:file-read-write path)
  (unless (file-exists? path) (error "storage does not exist" path))
  (storage:port-read-write (open-input-file path) (open-output-file path)))
(splicing-local
  ((define (port-read-method in)
     (file-stream-buffer-mode in 'none)
     (lambda (count pos bv.target start.target)
       (file-position in pos)
       ;; NOTE: in must have count bytes available at pos
       (read-bytes! bv.target in start.target (unsafe-fx+ start.target count)))))
  (define (storage:port-read-only in) (port-read-method in))
  (define (storage:port-read-write in out)
    (file-stream-buffer-mode out 'none)
    (storage-read-write-methods
      (port-read-method in)
      (lambda (count pos bv.source start.source)
        (file-position out pos)
        (write-bytes bv.source out start.source (unsafe-fx+ start.source count)))
      (lambda (final-size) (file-truncate out final-size)))))
(define ((storage:bytevector-read-only bv.source start.source) count pos bv.target start.target)
  (let ((start.source (unsafe-fx+ pos start.source)))
    (unsafe-bytes-copy! bv.target start.target bv.source start.source
                        (unsafe-fx+ start.source count))))
(define (storage:bytevector-read-write bv.rw start.rw)
  (storage-read-write-methods
    (storage:bytevector-read-only bv.rw start.rw)
    (lambda (count pos bv.source start.source)
      (unsafe-bytes-copy! bv.rw (unsafe-fx+ pos start.rw) bv.source start.source
                          (unsafe-fx+ start.source count)))
    (lambda (final-size) (void))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Codec for text and nat values ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (unsafe-bytes-text-ref count bv.source start.source)
  (let ((text (make-bytes count)))
    (unsafe-bytes-copy! text 0 bv.source start.source (unsafe-fx+ start.source count))
    text))
(define (unsafe-bytes-text-set! count bv.target start.target text start.text)
  (unsafe-bytes-copy! bv.target start.target text start.text (unsafe-fx+ start.text count)))

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

(define (min-bits n)
  (let loop ((n n))
    (if (< 0 n) (+ 1 (loop (fxrshift n 1))) 0)))
(define (min-bytes n)
  (let ((bits (min-bits n)))
    (+ (quotient bits 8) (if (= 0 (remainder bits 8)) 0 1))))
(define (nat-min-byte-width nat.max) (max (min-bytes nat.max) 1))
