#lang racket/base
(require ffi/unsafe/atomic ffi/unsafe/vm
         ;"../../dbk/safe-unsafe.rkt"
         racket/unsafe/ops
         racket/fixnum racket/pretty racket/vector)

(define enable-interrupts  (vm-primitive 'enable-interrupts))
(define disable-interrupts (vm-primitive 'disable-interrupts))

;; Normal interrupts
;(define (poll-interrupts) (void))
;(define-syntax-rule (explicit-interrupts body ...)
;  (begin body ...))

;; Explicit interrupts
(define (poll-interrupts)
  (time (enable-interrupts))
  (disable-interrupts))
(define-syntax-rule (explicit-interrupts body ...)
  (begin
    (disable-interrupts)
    body ...
    (time (enable-interrupts))))

(define output-file-name "sorted-and-deduped-tuples.bin")

;; 4.8GB
(define input-file-name "rtx-kg2-s3/rtx-kg2_nodes_2.8.1.tsv")
(define field-count 16)
;; 37GB
;(define input-file-name "rtx-kg2-s3/rtx-kg2_edges_2.8.1.tsv")
;(define field-count 18)
;; 40GB
;(define input-file-name "rtx_kg2_20210204.edgeprop.tsv")
;(define field-count 3)

(define-syntax-rule (pause e) (lambda () e))
(define (s->list s)
  (let loop ((s s))
    (cond
      ((null?      s) '())
      ((procedure? s) (loop (s)))
      (else           (cons (car s) (loop (cdr s)))))))

(define (s-take n s)
  (if n
      (let loop ((n n) (s s))
        (cond ((= n 0)        '())
              ((null?      s) '())
              ((procedure? s) (loop n (s)))
              (else           (cons (car s) (loop (- n 1) (cdr s))))))
      (s->list s)))

(define (s-map f s)
  (let loop ((s s))
    (cond ((null? s)      '())
          ((procedure? s) (lambda () (loop (s))))
          (else           (cons (f (car s)) (loop (cdr s)))))))

;; stream:line-block*-spare-buffer from parse-tsv.rkt
(define (stream:line-block* in)
  (define block-length 1024)
  (define buffer-size 16384)
  (define (loop.single start end buffer j.block block buffer.spare)
    (let loop.inner ((i start))
      (cond
        ((unsafe-fx<= end i) (if (unsafe-fx= start end)
                                 (let ((end (read-bytes! buffer in 0 buffer-size)))
                                   (cond
                                     ((not (eof-object? end)) (loop.single 0 end buffer j.block block buffer.spare))
                                     ((unsafe-fx< 0 j.block)  (list (vector-copy block 0 j.block)))
                                     (else                    '())))
                                 (loop.multi buffer.spare 0 '() '() buffer start end j.block block)))
        ((unsafe-fx= (unsafe-bytes-ref buffer i) 10)
         (unsafe-vector*-set! block j.block (subbytes buffer start i))
         (let ((j.block (unsafe-fx+ j.block 1)))
           (if (unsafe-fx= j.block block-length)
               (cons block (pause (loop.single (unsafe-fx+ i 1) end buffer 0 (make-vector block-length) buffer.spare)))
               (loop.single (unsafe-fx+ i 1) end buffer j.block block buffer.spare))))
        (else (loop.inner (unsafe-fx+ i 1))))))
  (define (loop.multi buffer.current len.middle end*.middle buffer*.middle buffer.first start.first end.first j.block block)
    (let ((end (read-bytes! buffer.current in 0 buffer-size)))
      (cond
        ((not (eof-object? end))
         (let loop.first ((i 0))
           (cond
             ((unsafe-fx<= end i) (loop.multi (make-bytes buffer-size)
                                              (unsafe-fx+ len.middle end)
                                              (cons end            end*.middle)
                                              (cons buffer.current buffer*.middle)
                                              buffer.first start.first end.first j.block block))
             ((unsafe-fx= (unsafe-bytes-ref buffer.current i) 10)
              (let* ((len.prev (unsafe-fx+ len.middle (unsafe-fx- end.first start.first)))
                     (line     (make-bytes (unsafe-fx+ len.prev i))))
                (unsafe-vector*-set! block j.block line)
                (unsafe-bytes-copy! line len.prev buffer.current 0 i)
                (let loop ((pos len.prev) (end* end*.middle) (buffer* buffer*.middle))
                  (when (pair? end*)
                    (let* ((end (unsafe-car end*))
                           (pos (unsafe-fx- pos end)))
                      (bytes-copy! line pos (unsafe-car buffer*) 0 end)
                      (loop pos (unsafe-cdr end*) (unsafe-cdr buffer*)))))
                (unsafe-bytes-copy! line 0 buffer.first start.first end.first))
              (let ((j.block (unsafe-fx+ j.block 1)))
                (if (unsafe-fx= j.block block-length)
                    (cons block (pause (loop.single (unsafe-fx+ i 1) end buffer.current 0 (make-vector block-length) buffer.first)))
                    (loop.single (unsafe-fx+ i 1) end buffer.current j.block block buffer.first))))
             (else (loop.first (unsafe-fx+ i 1))))))
        (else
          (let* ((len  (unsafe-fx+ len.middle (unsafe-fx- end.first start.first)))
                 (line (make-bytes len)))
            (unsafe-vector*-set! block j.block line)
            (let loop ((pos len) (end* end*.middle) (buffer* buffer*.middle))
              (when (pair? end*)
                (let* ((end (unsafe-car end*))
                       (pos (unsafe-fx- pos end)))
                  (unsafe-bytes-copy! line pos (unsafe-car buffer*) 0 end)
                  (loop pos (unsafe-cdr end*) (unsafe-cdr buffer*)))))
            (unsafe-bytes-copy! line 0 buffer.first start.first end.first))
          (list (let ((j.block (unsafe-fx+ j.block 1)))
                  (if (unsafe-fx= j.block block-length)
                      block
                      (vector-copy block 0 j.block))))))))
  (pause
    (let ((buffer       (make-bytes buffer-size))
          (buffer.spare (make-bytes buffer-size)))
      (let ((end (read-bytes! buffer in 0 buffer-size)))
        (if (eof-object? end)
            '()
            (loop.single 0 end buffer 0 (make-vector block-length) buffer.spare))))))

;; Based on stream:tsv-vector-block*-via-map-spare-buffer from parse-tsv.rkt
(define ((stream:tsv-vector-block*/field-count field-count) in)
  (s-map
    (lambda (block)
      (vector-map
        (lambda (line)
          (let ((len.line (bytes-length line))
                (tuple    (make-vector field-count)))
            (let loop ((i 0) (j.field 0) (start.field 0))
              (cond
                ((unsafe-fx= i len.line)
                 (unsafe-vector*-set! tuple j.field (subbytes line start.field i))
                 (unless (unsafe-fx= (unsafe-fx+ j.field 1) field-count) (error "too few fields" j.field line))
                 tuple)
                ((unsafe-fx= (unsafe-bytes-ref line i) 9)
                 (unsafe-vector*-set! tuple j.field (subbytes line start.field i))
                 (let ((i (unsafe-fx+ i 1)) (j.field (unsafe-fx+ j.field 1)))
                   (when (unsafe-fx= j.field field-count) (error "too many fields" line))
                   (loop i j.field i)))
                (else (loop (unsafe-fx+ i 1) j.field start.field))))))
        block))
    (stream:line-block* in)))

(define (vector-tuple-sort! tuple* start end)
  (define (vector-tuple<? a b)
    (define (text-compare/k a b k.< k.= k.>)
      (let ((len.a (unsafe-bytes-length a))
            (len.b (unsafe-bytes-length b)))
        (cond ((unsafe-fx< len.a len.b) (k.<))
              ((unsafe-fx< len.b len.a) (k.>))
              (else (let loop ((i 0))
                      (if (unsafe-fx= i len.a)
                          (k.=)
                          (let ((x.a (unsafe-bytes-ref a i)) (x.b (unsafe-bytes-ref b i)))
                            (cond ((unsafe-fx< x.a x.b) (k.<))
                                  ((unsafe-fx< x.b x.a) (k.>))
                                  (else (loop (unsafe-fx+ i 1)))))))))))
    (let ((len (unsafe-vector*-length a)))
      (let loop ((i 0))
        (and (unsafe-fx< i len)
             (text-compare/k (unsafe-vector*-ref a i) (unsafe-vector*-ref b i)
                             (lambda () #t)
                             (lambda () (loop (unsafe-fx+ i 1)))
                             (lambda () #f))))))
  (pretty-write `(sorting: ,start ,end))
  (time (vector-sort! tuple* vector-tuple<? start end)))

(define (vector-tuple-sort-and-dedup! tuple* start end)
  (vector-tuple-sort! tuple* start end)
  ;(let loop ((i 0))
    ;)
  (error "TODO: dedup"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Fill the staging area and count flushes only ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;; normal interrupts
;; 4.8GB
;;  (flush: 11)
;;  cpu time: 34465 real time: 35774 gc time: 13568
;; 37GB
;;  (flush: 55)
;;  cpu time: 243132 real time: 253263 gc time: 93007
;; 40GB
;;  (flush: 573)
;;  cpu time: 356848 real time: 368531 gc time: 149675
;;; explicit interrupts
;; 4.8GB
;;  cpu time: 20664 real time: 20666 gc time: 742
;; 37GB
;;  cpu time: 157554 real time: 157680 gc time: 5876
;; 40GB
;;  cpu time: 219454 real time: 219494 gc time: 11211
;(explicit-interrupts
;  (let ()
;    (pretty-write `(stage-only: ,input-file-name))
;    (define len.tuple*.stage 1048576)
;    (define tuple*.stage     (make-vector len.tuple*.stage))
;    (define in (open-input-file input-file-name))
;    (file-stream-buffer-mode in 'none)
;    (define flush-count 0)
;    (define (flush!)
;      (let loop ((i (unsafe-fx- len.tuple*.stage 1)))
;        (when (unsafe-fx<= 0 i)
;          (unsafe-vector*-set! tuple*.stage i 0)
;          (loop (unsafe-fx- i 1))))
;      (poll-interrupts)
;      (set! flush-count (unsafe-fx+ flush-count 1))
;      (pretty-write `(flush: ,flush-count)))
;    (define (loop.all i.tuple*.stage b*)
;      (cond ((null?      b*) (flush!))
;            ((procedure? b*) (loop.all i.tuple*.stage (b*)))
;            (else
;              (let* ((tuple*     (unsafe-car b*))
;                     (len.tuple* (unsafe-vector*-length tuple*)))
;                (if (unsafe-fx= len.tuple* 0)
;                    (loop.all i.tuple*.stage (unsafe-cdr b*))
;                    (let loop.one ((i.tuple*.stage i.tuple*.stage) (i.tuple* 0))
;                      (let ((end (unsafe-fxmin (unsafe-fx- len.tuple*       i.tuple*)
;                                               (unsafe-fx- len.tuple*.stage i.tuple*.stage))))
;                        (let loop ((i 0))
;                          (cond ((unsafe-fx< i end)
;                                 (unsafe-vector*-set!
;                                   tuple*.stage
;                                   (unsafe-fx+ i.tuple*.stage i)
;                                   (unsafe-vector*-ref tuple* (unsafe-fx+ i.tuple* i)))
;                                 (loop (unsafe-fx+ i 1)))
;                                (else
;                                  (let* ((i.tuple        (unsafe-fx+ i.tuple*       end))
;                                         (i.tuple*.stage (unsafe-fx+ i.tuple*.stage end))
;                                         (i.tuple*.stage
;                                           (if (unsafe-fx= i.tuple*.stage len.tuple*.stage)
;                                               (begin (flush!) 0)
;                                               i.tuple*.stage)))
;                                    (if (unsafe-fx= i.tuple len.tuple*)
;                                        (loop.all i.tuple*.stage (unsafe-cdr b*))
;                                        (loop.one i.tuple*.stage i.tuple)))))))))))))
;    (time (loop.all 0 ((stream:tsv-vector-block*/field-count field-count) in)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Fill the staging area, sort ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;; normal interrupts
;; 4.8GB
;;  cpu time: 71034 real time: 74430 gc time: 12569
;; 37GB
;;  cpu time: 365050 real time: 379895 gc time: 83537
;; 40GB
;;  cpu time: 458670 real time: 476403 gc time: 124260
;;; explicit interrupts
;; 4.8GB
;;  cpu time: 56244 real time: 56254 gc time: 814
;; 37GB
;;  cpu time: 280639 real time: 280817 gc time: 7144
;; 40GB
;;  cpu time: 368726 real time: 368885 gc time: 15055
(explicit-interrupts
  (let ()
    (pretty-write `(sort: ,input-file-name))
    (define len.tuple*.stage 1048576)
    (define tuple*.stage     (make-vector len.tuple*.stage))
    (define in (open-input-file input-file-name))
    (file-stream-buffer-mode in 'none)
    (define flush-count 0)
    (define (flush! i.tuple*.stage)
      (vector-tuple-sort! tuple*.stage 0 i.tuple*.stage)
      (let loop ((i (unsafe-fx- i.tuple*.stage 1)))
        (when (unsafe-fx<= 0 i)
          (unsafe-vector*-set! tuple*.stage i 0)
          (loop (unsafe-fx- i 1))))
      (poll-interrupts)
      (set! flush-count (unsafe-fx+ flush-count 1))
      (pretty-write `(flush: ,flush-count)))
    (define (loop.all i.tuple*.stage b*)
      (cond ((null?      b*) (flush! i.tuple*.stage))
            ((procedure? b*) (loop.all i.tuple*.stage (b*)))
            (else
              (let* ((tuple*     (unsafe-car b*))
                     (len.tuple* (unsafe-vector*-length tuple*)))
                (if (unsafe-fx= len.tuple* 0)
                    (loop.all i.tuple*.stage (unsafe-cdr b*))
                    (let loop.one ((i.tuple*.stage i.tuple*.stage) (i.tuple* 0))
                      (let ((end (unsafe-fxmin (unsafe-fx- len.tuple*       i.tuple*)
                                               (unsafe-fx- len.tuple*.stage i.tuple*.stage))))
                        (let loop ((i 0))
                          (cond ((unsafe-fx< i end)
                                 (unsafe-vector*-set!
                                   tuple*.stage
                                   (unsafe-fx+ i.tuple*.stage i)
                                   (unsafe-vector*-ref tuple* (unsafe-fx+ i.tuple* i)))
                                 (loop (unsafe-fx+ i 1)))
                                (else
                                  (let* ((i.tuple        (unsafe-fx+ i.tuple*       end))
                                         (i.tuple*.stage (unsafe-fx+ i.tuple*.stage end))
                                         (i.tuple*.stage
                                           (if (unsafe-fx= i.tuple*.stage len.tuple*.stage)
                                               (begin (flush! i.tuple*.stage) 0)
                                               i.tuple*.stage)))
                                    (if (unsafe-fx= i.tuple len.tuple*)
                                        (loop.all i.tuple*.stage (unsafe-cdr b*))
                                        (loop.one i.tuple*.stage i.tuple)))))))))))))
    (time (loop.all 0 ((stream:tsv-vector-block*/field-count field-count) in)))))

;;; TODO:

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Fill the staging area, sort, dedup ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Fill the staging area, sort, dedup, write mergeable chunks to disk ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Fill the staging area, sort, dedup, write mergeable chunks to disk, merge chunks ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Fill the staging area, sort, dedup, write mergeable chunks to disk, merge chunks, write to disk again ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
