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
  (let () (disable-interrupts) body ... (time (enable-interrupts))))

(define output-file-name "sorted-and-deduped-tuples.bin")

;; 4.8GB
;(define input-file-name "rtx-kg2-s3/rtx-kg2_nodes_2.8.1.tsv")
;(define field-count 16)
;; 37GB
;(define input-file-name "rtx-kg2-s3/rtx-kg2_edges_2.8.1.tsv")
;(define field-count 18)
;; 40GB
(define input-file-name "rtx_kg2_20210204.edgeprop.tsv")
(define field-count 3)

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

;; Based on stream:line-block*/map from parse-tsv.rkt
;(define (stream:line-block* in block-length)
  ;;(define block-length 65536)
  ;;(define block-length 1024)
  ;(define buffer-size 16384)
  ;(define (loop.single start end buffer j.block block buffer.spare)
    ;(let loop.inner ((i start))
      ;(cond
        ;((unsafe-fx<= end i) (if (unsafe-fx= start end)
                                 ;(let ((end (read-bytes! buffer in 0 buffer-size)))
                                   ;(cond
                                     ;((not (eof-object? end)) (loop.single 0 end buffer j.block block buffer.spare))
                                     ;((unsafe-fx< 0 j.block)  (list (vector-copy block 0 j.block)))
                                     ;(else                    '())))
                                 ;(loop.multi buffer.spare 0 '() '() buffer start end j.block block)))
        ;((unsafe-fx= (unsafe-bytes-ref buffer i) 10)
         ;(unsafe-vector*-set! block j.block (subbytes buffer start i))
         ;(let ((j.block (unsafe-fx+ j.block 1)))
           ;(if (unsafe-fx= j.block block-length)
               ;(cons block (pause (loop.single (unsafe-fx+ i 1) end buffer 0 (make-vector block-length) buffer.spare)))
               ;(loop.single (unsafe-fx+ i 1) end buffer j.block block buffer.spare))))
        ;(else (loop.inner (unsafe-fx+ i 1))))))
  ;(define (loop.multi buffer.current len.middle end*.middle buffer*.middle buffer.first start.first end.first j.block block)
    ;(let ((end (read-bytes! buffer.current in 0 buffer-size)))
      ;(cond
        ;((not (eof-object? end))
         ;(let loop.first ((i 0))
           ;(cond
             ;((unsafe-fx<= end i) (loop.multi (make-bytes buffer-size)
                                              ;(unsafe-fx+ len.middle end)
                                              ;(cons end            end*.middle)
                                              ;(cons buffer.current buffer*.middle)
                                              ;buffer.first start.first end.first j.block block))
             ;((unsafe-fx= (unsafe-bytes-ref buffer.current i) 10)
              ;(let* ((len.prev (unsafe-fx+ len.middle (unsafe-fx- end.first start.first)))
                     ;(line     (make-bytes (unsafe-fx+ len.prev i))))
                ;(unsafe-vector*-set! block j.block line)
                ;(unsafe-bytes-copy! line len.prev buffer.current 0 i)
                ;(let loop ((pos len.prev) (end* end*.middle) (buffer* buffer*.middle))
                  ;(when (pair? end*)
                    ;(let* ((end (unsafe-car end*))
                           ;(pos (unsafe-fx- pos end)))
                      ;(bytes-copy! line pos (unsafe-car buffer*) 0 end)
                      ;(loop pos (unsafe-cdr end*) (unsafe-cdr buffer*)))))
                ;(unsafe-bytes-copy! line 0 buffer.first start.first end.first))
              ;(let ((j.block (unsafe-fx+ j.block 1)))
                ;(if (unsafe-fx= j.block block-length)
                    ;(cons block (pause (loop.single (unsafe-fx+ i 1) end buffer.current 0 (make-vector block-length) buffer.first)))
                    ;(loop.single (unsafe-fx+ i 1) end buffer.current j.block block buffer.first))))
             ;(else (loop.first (unsafe-fx+ i 1))))))
        ;(else
          ;(let* ((len  (unsafe-fx+ len.middle (unsafe-fx- end.first start.first)))
                 ;(line (make-bytes len)))
            ;(unsafe-vector*-set! block j.block line)
            ;(let loop ((pos len) (end* end*.middle) (buffer* buffer*.middle))
              ;(when (pair? end*)
                ;(let* ((end (unsafe-car end*))
                       ;(pos (unsafe-fx- pos end)))
                  ;(unsafe-bytes-copy! line pos (unsafe-car buffer*) 0 end)
                  ;(loop pos (unsafe-cdr end*) (unsafe-cdr buffer*)))))
            ;(unsafe-bytes-copy! line 0 buffer.first start.first end.first))
          ;(list (let ((j.block (unsafe-fx+ j.block 1)))
                  ;(if (unsafe-fx= j.block block-length)
                      ;block
                      ;(vector-copy block 0 j.block))))))))
  ;(pause
    ;(let ((buffer       (make-bytes buffer-size))
          ;(buffer.spare (make-bytes buffer-size)))
      ;(let ((end (read-bytes! buffer in 0 buffer-size)))
        ;(if (eof-object? end)
            ;'()
            ;(loop.single 0 end buffer 0 (make-vector block-length) buffer.spare))))))
;; Based on stream:tsv-vector-block*-via-map-spare-buffer from parse-tsv.rkt
;(define (((stream:tsv-vector-block*/field-count field-count) in) block-length)
  ;(s-map
    ;(lambda (block)
      ;(vector-map
        ;(lambda (line)
          ;(let ((len.line (bytes-length line))
                ;(tuple    (make-vector field-count)))
            ;(let loop ((i 0) (j.field 0) (start.field 0))
              ;(cond
                ;((unsafe-fx= i len.line)
                 ;(unsafe-vector*-set! tuple j.field (subbytes line start.field i))
                 ;(unless (unsafe-fx= (unsafe-fx+ j.field 1) field-count) (error "too few fields" j.field line))
                 ;tuple)
                ;((unsafe-fx= (unsafe-bytes-ref line i) 9)
                 ;(unsafe-vector*-set! tuple j.field (subbytes line start.field i))
                 ;(let ((i (unsafe-fx+ i 1)) (j.field (unsafe-fx+ j.field 1)))
                   ;(when (unsafe-fx= j.field field-count) (error "too many fields" line))
                   ;(loop i j.field i)))
                ;(else (loop (unsafe-fx+ i 1) j.field start.field))))))
        ;block))
    ;(stream:line-block* in block-length)))

;; stream:line-block*/map from parse-tsv.rkt
(define (((stream:line-block*/map f) in) block-length)
  ;(define block-length 1048576)
  ;(define block-length 524288)
  ;(define block-length 262144)
  ;(define block-length 131072)
  ;(define block-length 65536)
  ;(define block-length 1024)
  (define buffer-size 16384)
  ;(define buffer-size 4096)
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
         (unsafe-vector*-set! block j.block (f (subbytes buffer start i)))
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
                     (len      (unsafe-fx+ len.prev i))
                     (line     (make-bytes len)))
                (unsafe-bytes-copy! line len.prev buffer.current 0 i)
                (let loop ((pos len.prev) (end* end*.middle) (buffer* buffer*.middle))
                  (when (pair? end*)
                    (let* ((end (unsafe-car end*))
                           (pos (unsafe-fx- pos end)))
                      (bytes-copy! line pos (unsafe-car buffer*) 0 end)
                      (loop pos (unsafe-cdr end*) (unsafe-cdr buffer*)))))
                (unsafe-bytes-copy! line 0 buffer.first start.first end.first)
                (unsafe-vector*-set! block j.block (f line)))
              (let ((j.block (unsafe-fx+ j.block 1)))
                (if (unsafe-fx= j.block block-length)
                    (cons block (pause (loop.single (unsafe-fx+ i 1) end buffer.current 0 (make-vector block-length) buffer.first)))
                    (loop.single (unsafe-fx+ i 1) end buffer.current j.block block buffer.first))))
             (else (loop.first (unsafe-fx+ i 1))))))
        (else
          (let* ((len  (unsafe-fx+ len.middle (unsafe-fx- end.first start.first)))
                 (line (make-bytes len)))
            (let loop ((pos len) (end* end*.middle) (buffer* buffer*.middle))
              (when (pair? end*)
                (let* ((end (unsafe-car end*))
                       (pos (unsafe-fx- pos end)))
                  (unsafe-bytes-copy! line pos (unsafe-car buffer*) 0 end)
                  (loop pos (unsafe-cdr end*) (unsafe-cdr buffer*)))))
            (unsafe-bytes-copy! line 0 buffer.first start.first end.first)
            (unsafe-vector*-set! block j.block (f line)))
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
;; Based on stream:tsv-vector-block*-via-immediate-map from parse-tsv.rkt
(define (((stream:tsv-vector-block*/field-count field-count) in) block-length)
  (((stream:line-block*/map
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
              (else (loop (unsafe-fx+ i 1) j.field start.field)))))))
    in)
   block-length))

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
  (vector-sort! tuple* vector-tuple<? start end))

;; TODO: adaptive merge sort that recognizes sorted runs as in database.rkt
(define (vector-tuple-sort! tuple* start end)
  )

(define (vector-tuple-dedup-adjacent tuple* start end)
  (define tuple=? equal?)
  ;; TODO: compare with this
  ;(define (tuple=? a b)
  ;  (let ((len (unsafe-vector*-length a)))
  ;    (let loop ((i 0))
  ;      (or (unsafe-fx= i len)
  ;          (and (let* ((a   (unsafe-vector*-ref a i))
  ;                      (b   (unsafe-vector*-ref b i))
  ;                      (len (unsafe-bytes-length a)))
  ;                 (and (unsafe-fx= (unsafe-bytes-length b) len)
  ;                      (let loop ((i 0))
  ;                        (or (unsafe-fx= i len)
  ;                            (and (unsafe-fx= (unsafe-bytes-ref a i) (unsafe-bytes-ref b i))
  ;                                 (loop (unsafe-fx+ i 1)))))))
  ;               (loop (unsafe-fx+ i 1)))))))
  (if (unsafe-fx< start end)
      (let loop ((tuple.current (unsafe-vector*-ref tuple* start))
                 (i             (unsafe-fx+ start 1)))
        (if (unsafe-fx= i end)
            end
            (let ((tuple.next (unsafe-vector*-ref tuple* i)))
              (if (tuple=? tuple.current tuple.next)
                  (let loop ((tuple.current tuple.current)
                             (target        i)
                             (i             (unsafe-fx+ i 1)))
                    (if (unsafe-fx= i end)
                        target
                        (let ((tuple.next (unsafe-vector*-ref tuple* i)))
                          (if (tuple=? tuple.current tuple.next)
                              (loop tuple.current target (unsafe-fx+ i 1))
                              (begin (unsafe-vector*-set! tuple* target tuple.next)
                                     (loop tuple.next (unsafe-fx+ target 1) (unsafe-fx+ i 1)))))))
                  (loop tuple.next (unsafe-fx+ i 1))))))
      end))

(define (vector-tuple-sort-and-dedup tuple* start end)
  (pretty-write `(sorting: ,start ,end))
  (time (vector-tuple-sort!          tuple* start end))
  (pretty-write `(deduping: ,start ,end))
  (time (vector-tuple-dedup-adjacent tuple* start end)))

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
;    (time (loop.all 0 (((stream:tsv-vector-block*/field-count field-count) in) block-length)))))

;;;;;;;;;;;;;;;;;;;;;;;;
;;; No staging, sort ;;;
;;;;;;;;;;;;;;;;;;;;;;;;

;; TODO: pass desired block length explicitly

;; TODO: block stream transformer that consolidates or splits blocks to obtain a new desired block length
;; - define a combinator that converts a block stream into a value-by-value block population process

;; TODO: macro that abstracts, but inlines, an arbitrary value-by-value block population process
;; - can try to use a higher order producer/consumer coroutine first, and see if that inlines well enough on its own
;; - give the producer these: (k.yield value k.resume) (k.done)
;;   - where (define (k.resume k.yield k.done) etc.) resumes the producer

;;; explicit interrupts
;; 4.8GB
;;  cpu time: 57304 real time: 57344 gc time: 7759
;; 37GB
;;

;; 40GB
;;

(explicit-interrupts
  (let ()
    (define block-length 1048576)
    ;(define block-length 524288)
    ;(define block-length 262144)
    ;(define block-length 131072)
    ;(define block-length 65536)
    (pretty-write `(sort-directly: ,input-file-name))
    (define in (open-input-file input-file-name))
    (file-stream-buffer-mode in 'none)
    (define flush-count 0)
    (define (flush! tuple*)
      (vector-tuple-sort! tuple* 0 (unsafe-vector*-length tuple*))
      (set! tuple* #f)
      (poll-interrupts)
      (set! flush-count (unsafe-fx+ flush-count 1))
      (pretty-write `(flush: ,flush-count)))
    (define (loop.all b*)
      (cond ((null?      b*) (void))
            ((procedure? b*) (loop.all (b*)))
            (else            (flush! (unsafe-car b*)) (loop.all (unsafe-cdr b*)))))
    (time (loop.all (((stream:tsv-vector-block*/field-count field-count) in) block-length)))))

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
;;  cpu time: 49916 real time: 49949 gc time: 658
;; 37GB
;;  cpu time: 280639 real time: 280817 gc time: 7144
;; 40GB
;;  cpu time: 368726 real time: 368885 gc time: 15055
;(explicit-interrupts
;  (let ()
;    (pretty-write `(sort: ,input-file-name))
;    (define len.tuple*.stage 1048576)
;    (define tuple*.stage     (make-vector len.tuple*.stage))
;    (define in (open-input-file input-file-name))
;    (file-stream-buffer-mode in 'none)
;    (define flush-count 0)
;    (define (flush! i.tuple*.stage)
;      (vector-tuple-sort! tuple*.stage 0 i.tuple*.stage)
;      (let loop ((i (unsafe-fx- i.tuple*.stage 1)))
;        (when (unsafe-fx<= 0 i)
;          (unsafe-vector*-set! tuple*.stage i 0)
;          (loop (unsafe-fx- i 1))))
;      (poll-interrupts)
;      (set! flush-count (unsafe-fx+ flush-count 1))
;      (pretty-write `(flush: ,flush-count)))
;    (define (loop.all i.tuple*.stage b*)
;      (cond ((null?      b*) (flush! i.tuple*.stage))
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
;                                               (begin (flush! i.tuple*.stage) 0)
;                                               i.tuple*.stage)))
;                                    (if (unsafe-fx= i.tuple len.tuple*)
;                                        (loop.all i.tuple*.stage (unsafe-cdr b*))
;                                        (loop.one i.tuple*.stage i.tuple)))))))))))))
;    (time (loop.all 0 (((stream:tsv-vector-block*/field-count field-count) in) block-length)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Fill the staging area, sort, dedup ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;; explicit interrupts
;; 4.8GB
;;

;; 37GB
;;

;; 40GB
;;

;(explicit-interrupts
;  (let ()
;    (pretty-write `(sort: ,input-file-name))
;    (define len.tuple*.stage 1048576)
;    (define tuple*.stage     (make-vector len.tuple*.stage))
;    (define in (open-input-file input-file-name))
;    (file-stream-buffer-mode in 'none)
;    (define flush-count 0)
;    (define (flush! i.tuple*.stage)
;      (vector-tuple-sort-and-dedup! tuple*.stage 0 i.tuple*.stage)
;      (let loop ((i (unsafe-fx- i.tuple*.stage 1)))
;        (when (unsafe-fx<= 0 i)
;          (unsafe-vector*-set! tuple*.stage i 0)
;          (loop (unsafe-fx- i 1))))
;      (poll-interrupts)
;      (set! flush-count (unsafe-fx+ flush-count 1))
;      (pretty-write `(flush: ,flush-count)))
;    (define (loop.all i.tuple*.stage b*)
;      (cond ((null?      b*) (flush! i.tuple*.stage))
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
;                                               (begin (flush! i.tuple*.stage) 0)
;                                               i.tuple*.stage)))
;                                    (if (unsafe-fx= i.tuple len.tuple*)
;                                        (loop.all i.tuple*.stage (unsafe-cdr b*))
;                                        (loop.one i.tuple*.stage i.tuple)))))))))))))
;    (time (loop.all 0 ((stream:tsv-vector-block*/field-count field-count) in)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Fill the staging area, sort, dedup, write mergeable chunks to disk ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; 4.8GB
;;

;; 37GB
;;

;; 40GB
;;


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Fill the staging area, sort, dedup, write mergeable chunks to disk, merge chunks ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; 4.8GB
;;

;; 37GB
;;

;; 40GB
;;


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Fill the staging area, sort, dedup, write mergeable chunks to disk, merge chunks, write to disk again ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; 4.8GB
;;

;; 37GB
;;

;; 40GB
;;


;; TODO: test each factor independently
;; - first test a version that fills the staging area and flushes, but does not sort or dedup
;; - then include sorting and deduping, but don't write to disk
;; - then include writing to disk, but no merging
;; - then include merging
;; Writing to disk will be naive for now, but we should also test compression, decompression, and building indexes.
;(let ()
;  (pretty-write `(sorting-and-deduping: ,input-file-name writing-to: ,output-file-name))
;  (define len.tuple*.stage   1048576)
;  (define tuple*.stage       (make-vector len.tuple*.stage))
;  ;; TODO: these should be shorter, just the number of chunks is all we need
;  ;(define pos*.materialized (make-fxvector 1048576))
;  ;(define len*.materialized (make-fxvector 1048576))
;  (define in (open-input-file input-file-name))
;  (file-stream-buffer-mode in 'none)
;  (define out (open-output-file output-file-name))
;  (define flush-count 0)
;  (define (loop.stage i.tuple*.stage b*)
;    (cond ((null?      b*) (vector-tuple-sort-and-dedup! tuple*.stage 0 i.tuple*.stage)
;                           (set! flush-count (unsafe-fx+ flush-count 1))
;                           flush-count)
;          ((procedure? b*) (loop.stage i.tuple*.stage (b*)))
;          (else            (let* ((tuple*     (unsafe-car b*))
;                                  (len.tuple* (unsafe-vector*-length tuple*)))
;                             (let loop.flush ((i.tuple*.stage i.tuple*.stage) (i.tuple* 0))
;                               (if (unsafe-fx< i.tuple* len.tuple*)
;                                   (let ((end (unsafe-fxmin (unsafe-fx- len.tuple*       i.tuple*)
;                                                            (unsafe-fx- len.tuple*.stage i.tuple*.stage))))
;                                     (let loop ((i 0))
;                                       (cond ((unsafe-fx< i end)
;                                              (unsafe-vector*-set! tuple*.stage (unsafe-fx+ i.tuple*.stage i)
;                                                                   (unsafe-vector*-ref tuple* (unsafe-fx+ i.tuple* i)))
;                                              (loop (unsafe-fx+ i 1)))
;                                             (else
;                                               (vector-tuple-sort-and-dedup! tuple*.stage 0 i.tuple*.stage)
;                                               (set! flush-count (unsafe-fx+ flush-count 1))
;                                               (loop.flush 0 (unsafe-fx+ i.tuple* end))))))
;                                   (loop.stage i.tuple*.stage (unsafe-cdr b*))))))))
;  (time (loop.stage 0 ((stream:tsv-vector-block*/field-count field-count) in))))


;(
; (let loop.materialize ((i.materialize 0))
;   (define (sort-and-dedup! end) (vector-tuple-sort! staging-block 0 end))
;   ;; TODO: should we materialize in chunks instead, so that we know the file boundaries
;   ;; of smaller tuple blocks that we can page in during heap sort?
;   ;; what (typical) chunk length?
;   (define (materialize end)
;     (let loop ((i 0))
;       (cond ((unsafe-fx< i end)
;              (write-tuple i)
;              (loop (unsafe-fx+ i 1)))
;             (else
;               (flush-output out)
;               (unsafe-fxvector-set! len*.materialized i.materialize end)
;               (unsafe-fxvector-set! pos*.materialized i.materialize (file-position out))))))
;   (let loop.stage ((i.staging 0) (b* ((stream:tsv-vector-block*/field-count field-count) in)))
;     (cond ((null?      b*)
;            )
;           ((procedure? b*) (loop i.staging (b*)))
;           (else            (loop (unsafe-fx+ count
;                                              (let ((tuple* (unsafe-car b*)))
;                                                (vector-tuple-sort! tuple*)
;                                                (unsafe-vector*-length tuple*)))
;                                  (unsafe-cdr b*)))
;           ))
;   )
; )
