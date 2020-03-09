#lang racket/base
(provide bisect bisect-next
         table/vector table/bytes table/port
         table/bytes/offsets table/port/offsets)
(require "codec.rkt" "method.rkt"
         racket/function)

;; TODO: see if common parts can be consolidated without a performance penalty
(define (table/port/offsets table:offsets type in)
  (define (ref i) (file-position in (table:offsets 'ref i)) (decode in type))
  (define (i< <?) (lambda (i) (<? (ref i))))
  (method-lambda
    ((length)         (table:offsets 'length))
    ((ref i)          (ref i))
    ((ref* start end) (let loop ((i start))
                        (if (<= end i) '()
                          (thunk (cons (ref i) (loop (+ i 1)))))))
    ((find      start end <?) (bisect      start end (i< <?)))
    ((find-next start end <?) (bisect-next start end (i< <?)))))

(define (table/bytes/offsets table:offsets type bs)
  (define in (open-input-bytes bs))
  (table/port/offsets table:offsets type in))

(define (table/port type len in)
  (define width (sizeof type (void)))
  (define (ref/pos i) (file-position in i) (decode in type))
  (define (ref     i) (ref/pos (* i width)))
  (define (i< <?) (lambda (i) (<? (ref i))))
  (method-lambda
    ((length)         len)
    ((ref i)          (ref i))
    ((ref* start end) (define j (* end width))
                      (let loop ((i (* start width)))
                        (if (<= j i) '()
                          (thunk (cons (ref/pos i) (loop (+ i width)))))))
    ((find      start end <?) (bisect      start end (i< <?)))
    ((find-next start end <?) (bisect-next start end (i< <?)))))

(define (table/bytes type bs)
  (define in (open-input-bytes bs))
  (table/port type (quotient (bytes-length bs) (sizeof type (void))) in))

(define (table/vector v)
  (define (i< <?) (lambda (i) (<? (vector-ref v i))))
  (method-lambda
    ((length)         (vector-length v))
    ((ref i)          (vector-ref v i))
    ((ref* start end) (let loop ((i start))
                        (if (<= end i) '()
                          (thunk (cons (vector-ref v i) (loop (+ i 1)))))))
    ((find      start end <?) (bisect      start end (i< <?)))
    ((find-next start end <?) (bisect-next start end (i< <?)))))

(define (bisect start end i<)
  (let loop ((start start) (end end))
    (if (<= end start) end
      (let ((i (+ start (quotient (- end start) 2))))
        (if (i< i) (loop (+ 1 i) end) (loop start i))))))

(define (bisect-next start end i<)
  (let loop ((i (- start 1)) (offset 1))
    (define next (+ i offset))
    (cond ((and (< next end) (i< next))
           (loop i (arithmetic-shift offset 1)))
          (else (let loop ((i i) (o offset))
                  (let* ((offset (arithmetic-shift o -1)) (next (+ i offset)))
                    (cond ((= offset 0) (+ i 1))
                          ((i< next)    (loop next offset))
                          (else         (loop i    offset)))))))))
