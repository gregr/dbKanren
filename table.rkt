#lang racket/base
(provide bisect bisect-next
         table/vector table/bytes table/port
         table/bytes/offsets table/port/offsets tabulate)
(require "codec.rkt" "method.rkt" "stream.rkt"
         racket/function racket/match racket/vector)

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

(define (tabulate file-name offset-file-name? zmax type v< s)
  (define fname-multi        (string-append file-name ".multi"))
  (define fname-multi-offset (string-append file-name ".multi.offset"))
  (define (main out out-offset)
    (match-define (vector item-count chunk-count v?)
      (multi-sort fname-multi fname-multi-offset zmax type v< s))
    (define omax (if v? (sizeof `#(array ,item-count ,type) v?)
                   (file-size fname-multi)))
    (define otype (and out-offset `#(nat ,(- (sizeof 'nat omax) 1))))
    (cond (v? (for ((_ (in-range item-count)) (x (in-vector v?)))
                   (when out-offset (encode out-offset otype
                                            (file-position out)))
                   (encode out type x)))
          (else (call-with-input-file
                  fname-multi
                  (lambda (in)
                    (call-with-input-file
                      fname-multi-offset
                      (lambda (in-offset)
                        (multi-merge out out-offset type otype v< chunk-count
                                     in in-offset)))))
                (delete-file fname-multi)
                (delete-file fname-multi-offset)))
    item-count)
  (call-with-output-file
    file-name (lambda (out)
                (if offset-file-name?
                  (call-with-output-file
                    offset-file-name?
                    (lambda (out-offset) (main out out-offset)))
                  (main out #f)))))

;; TODO: for performance, pass a fill! procedure instead of stream?
(define (multi-sort chunk-file-name offset-file-name zmax type v< s0)
  (define v (make-vector zmax))
  (match-define (cons n s1) (s-prefix! v zmax s0))
  (vector-sort! v v< 0 n)
  (define s2 (s-next s1))
  (if (null? s2) (vector n 0 v)
    (call-with-output-file
      chunk-file-name
      (lambda (out-chunk)
        (call-with-output-file
          offset-file-name
          (lambda (out-offset)
            (let loop ((n n) (s s2) (item-count n) (chunk-count 1))
              (for ((_ (in-range n)) (x (in-vector v)))
                   (encode out-chunk type x))
              (encode out-offset 'nat (file-position out-chunk))
              (define s1 (s-next s))
              (cond ((null? s1) (vector item-count chunk-count #f))
                    (else (match-define (cons n s2) (s-prefix! v zmax s1))
                          (vector-sort! v v< 0 n)
                          (loop n s2 (+ item-count n)
                                (+ chunk-count 1)))))))))))

;; TODO: separate chunk streaming from merging
(define (multi-merge out out-offset type otype v< chunk-count in in-offset)
  (define (s< sa sb) (v< (car sa) (car sb)))
  (define (s-chunk pos end)
    (cond ((<= end pos) '())
          (else (file-position in pos)
                (cons (decode in type) (let ((pos (file-position in)))
                                         (thunk (s-chunk pos end)))))))
  (define heap (make-vector chunk-count))
  (let loop ((hi 0) (start 0)) (when (< hi chunk-count)
                                 (define end (decode in-offset 'nat))
                                 (vector-set! heap hi (s-chunk start end))
                                 (loop (+ hi 1) end)))
  (heap! s< heap chunk-count)
  (let loop ((hend chunk-count))
    (unless (= hend 0)
      (let* ((top (heap-top heap))
             (x (car top)) (top (s-next (cdr top))))
        (when out-offset (encode out-offset otype (file-position out)))
        (encode out type x)
        (cond ((null? top) (heap-remove!  s< heap hend)
                           (loop (- hend 1)))
              (else        (heap-replace! s< heap hend top)
                           (loop    hend)))))))

(define (heap-top h) (vector-ref h 0))
(define (heap! ? h end)
  (let loop ((i (- (quotient end 2) 1)))
    (when (<= 0 i) (heap-sink! ? h end i) (loop (- i 1)))))
(define (heap-remove! ? h end)
  (vector-set! h 0 (vector-ref h (- end 1))) (heap-sink! ? h (- end 1) 0))
(define (heap-replace! ? h end top)
  (vector-set! h 0 top)                      (heap-sink! ? h    end    0))
(define (heap-sink! ? h end i)
  (let loop ((i i))
    (let ((ileft (+ i i 1)) (iright (+ i i 2)))
      (cond ((<= end ileft))  ;; done
            ((<= end iright)
             (let ((p (vector-ref h i)) (l (vector-ref h ileft)))
               (when (? l p) (vector-set! h i l) (vector-set! h ileft p))))
            (else (let ((p (vector-ref h i))
                        (l (vector-ref h ileft)) (r (vector-ref h iright)))
                    (cond ((? l p) (cond ((? r l) (vector-set! h i r)
                                                  (vector-set! h iright p)
                                                  (loop iright))
                                         (else (vector-set! h i l)
                                               (vector-set! h ileft p)
                                               (loop ileft))))
                          ((? r p) (vector-set! h i r)
                                   (vector-set! h iright p)
                                   (loop iright)))))))))
