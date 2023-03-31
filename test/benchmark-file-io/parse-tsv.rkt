#lang racket/base
(require ffi/unsafe/atomic ffi/unsafe/vm
         ;"../../dbk/safe-unsafe.rkt"
         racket/unsafe/ops
         racket/fixnum racket/pretty racket/vector)

(define enable-interrupts  (vm-primitive 'enable-interrupts))
(define disable-interrupts (vm-primitive 'disable-interrupts))

;; 4.8GB
(define in (open-input-file "rtx-kg2-s3/rtx-kg2_nodes_2.8.1.tsv"))
(define field-count 16)
;; 37GB
;(define in (open-input-file "rtx-kg2-s3/rtx-kg2_edges_2.8.1.tsv"))
;(define field-count 18)
;; 40GB
;(define in (open-input-file "rtx_kg2_20210204.edgeprop.tsv"))
;(define field-count 3)

(define-syntax-rule (pause e) (lambda () e))
(define (s->list s)
  (let loop ((s s))
    (cond
      ((null?      s) '())
      ((procedure? s) (loop (s)))
      (else           (cons (car s) (loop (cdr s)))))))

;(define-syntax-rule (pause e) e)
;(define (s->list s) s)

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; streaming line blocks with multiple buffers ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; NOTE: to measure something simpler first, this version doesn't parse tabs, just entire lines.
(define (stream:line-block* in)
  (define block-length 1024)
  (define buffer-size 16384)
  ;(define block-length 5)
  ;(define buffer-size 16)
  (define (loop.single start end buffer j.block block)
    ;(pretty-write `(loop.single ,start ,end ,buffer ,j.block ,block))
    (let loop.inner ((i start))
      (cond
        ((unsafe-fx<= end i) (if (unsafe-fx= start end)
                        (let ((end (read-bytes! buffer in 0 buffer-size)))
                          (cond
                            ((not (eof-object? end)) (loop.single 0 end buffer j.block block))
                            ((unsafe-fx< 0 j.block)  (list (vector-copy block 0 j.block)))
                            (else                    '())))
                        (loop.multi 0 '() '() buffer start end j.block block)))
        ((unsafe-fx= (unsafe-bytes-ref buffer i) 10)
         (unsafe-vector*-set! block j.block (subbytes buffer start i))
         (let ((j.block (unsafe-fx+ j.block 1)))
           (if (unsafe-fx= j.block block-length)
               (cons block (pause (loop.single (unsafe-fx+ i 1) end buffer 0 (make-vector block-length))))
               (loop.single (unsafe-fx+ i 1) end buffer j.block block))))
        (else (loop.inner (unsafe-fx+ i 1))))))
  (define (loop.multi len.middle end*.middle buffer*.middle buffer.first start.first end.first j.block block)
    ;(pretty-write `(loop.multi ,len.middle ,end*.middle ,buffer*.middle ,buffer.first ,start.first ,end.first ,j.block ,block))
    (let* ((buffer.current (make-bytes buffer-size))
           (end            (read-bytes! buffer.current in 0 buffer-size)))
      (cond
        ((not (eof-object? end))
         (let loop.first ((i 0))
           (cond
             ((unsafe-fx<= end i) (loop.multi (unsafe-fx+ len.middle end)
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
                    (cons block (pause (loop.single (unsafe-fx+ i 1) end buffer.current 0 (make-vector block-length))))
                    (loop.single (unsafe-fx+ i 1) end buffer.current j.block block))))
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
    (let ((buffer (make-bytes buffer-size)))
      (let ((end (read-bytes! buffer in 0 buffer-size)))
        (if (eof-object? end)
            '()
            (loop.single 0 end buffer 0 (make-vector block-length)))))))

;; 4.8GB (turning off interrupts is worse)
;;  cpu time: 9029 real time: 9646 gc time: 202
;; ==> 11342763
;; 37GB
;;  cpu time: 80502 real time: 85579 gc time: 3795
;; ==> 56965146
;; 40GB
;;  cpu time: 86906 real time: 92369 gc time: 790
;; ==> 600183480
;(file-stream-buffer-mode in 'none)
;(pretty-write
;  (time
;    (let loop ((count 0) (b* (stream:line-block* in)))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count (unsafe-vector*-length (unsafe-car b*))) (unsafe-cdr b*)))))))

;; Alternative test:
;; 4.8GB
;;  cpu time: 15048 real time: 15669 gc time: 5486
;; 4.8GB no interrupts
;;  cpu time: 10871 real time: 10871 gc time: 0
;;  cpu time: 2990 real time: 2990 gc time: 2990
;; 4.8GB no streams
;;  cpu time: 13656 real time: 14276 gc time: 5210
;; 4.8GB no streams, no interrupts
;;  cpu time: 11175 real time: 11175 gc time: 0
;;  cpu time: 3015 real time: 3016 gc time: 3015
;(file-stream-buffer-mode in 'none)
;;(disable-interrupts)
;(define result (time (s->list (stream:line-block* in))))
;;(time (enable-interrupts))
;(pretty-write (vector-ref (car result) 0))
;(pretty-write
;  (let loop ((count 0) (b* result))
;    (cond ((null? b*) count)
;          (else (loop (+ count (vector-length (car b*))) (cdr b*))))))
;(let ((v (car (reverse result))))
;  (pretty-write (vector-ref v (- (vector-length v) 1))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; streaming vector-tuple blocks ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (stream:tsv-vector-block* in field-count)
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

;; 4.8GB
;;  cpu time: 21355 real time: 22647 gc time: 567
;; ==> 11342763
;; 4.8GB no interrupts (not as good)
;;  cpu time: 23707 real time: 23712 gc time: 0
;;  cpu time: 1342 real time: 1342 gc time: 1342
;; 37GB
;;  cpu time: 155789 real time: 165649 gc time: 5917
;; ==> 56965146
;; 40GB
;;  cpu time: 206260 real time: 217554 gc time: 2132
;; ==> 600183480
(file-stream-buffer-mode in 'none)
;(disable-interrupts)
(pretty-write
  (time
    (let loop ((count 0) (b* (stream:tsv-vector-block* in field-count)))
      (cond ((null?      b*) count)
            ((procedure? b*) (loop count (b*)))
            (else            (loop (unsafe-fx+ count (unsafe-vector*-length (unsafe-car b*))) (unsafe-cdr b*)))))))
;(time (enable-interrupts))

;; Alternative test:
;; 4.8GB
;;  cpu time: 35571 real time: 36858 gc time: 15091
;; 4.8GB no interrupts
;;  cpu time: 23832 real time: 23832 gc time: 0
;;  cpu time: 7173 real time: 7174 gc time: 7173
;; 4.8GB no streams (slightly slower for some reason)
;;  cpu time: 37629 real time: 38910 gc time: 17940
;; 4.8GB no streams, no interrupts
;;  cpu time: 23231 real time: 23232 gc time: 0
;;  cpu time: 7453 real time: 7453 gc time: 7453
;(file-stream-buffer-mode in 'none)
;;(disable-interrupts)
;(define result (time (s->list (stream:tsv-vector-block* in field-count))))
;;(time (enable-interrupts))
;(pretty-write (vector-ref (car result) 0))
;(pretty-write
;  (let loop ((count 0) (b* result))
;    (cond ((null? b*) count)
;          (else (loop (+ count (vector-length (car b*))) (cdr b*))))))
;(let ((v (car (reverse result))))
;  (pretty-write (vector-ref v (- (vector-length v) 1))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; streaming bytevector-tuple blocks ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; TODO: should we just s-map over stream:line-block* for simplicity, or will the extra allocation hurt too much?

;; Each tuple will be represented as a single bytevector with packed, length-encoded fields.
;; This means that sorting tuples with bytes<? is not lexicographical relative to the field text.
;; But this is fine since we only care about equality, not order.

;(define (stream:tsv-bytevector-block* field-count in)
;  ;; TODO: Pre-allocate a fxvector buffer for storing intermediate lengths and offsets for the current line/tuple
;  ;; - field-count tells us how large this buffer needs to be
;  (s-map
;    (let ((end* (make-fxvector (- field-count 1))))
;      (lambda (block)
;        (vector-map
;          (lambda (line)
;            (let ((len.line (bytes-length line)))
;              (let loop ((i 0) (j.field 0) (start.field 0) (size.field-length* 0))
;                (cond
;                  ((= i len.line)
;                   (unless (= (+ j.field 1) field-count)
;                     (error "too few fields" j.field line))
;                   (let* ((len.last-field         (- i start.field))
;                          (size.last-field-length
;
;                            ))
;
;                     (make-bytes (- (+ size.field-length* size.last-field-length len.line 1) field-count))
;
;                     ))
;                  ((= (bytes-ref line i) 9)
;                   (fxvector-set! end* j.field i)
;                   (let ((len.field (- i start.field))
;                         (i         (+ i 1)))
;                     (loop i (+ j.field 1) i
;
;                           )))
;                  (else (loop (+ i 1) j.field start.field size.field-length*))))))
;          block)))
;    (stream:line-block* in)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; naive tuple construction unbuffered ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; ~42 seconds (~25.4 seconds gc) for 4.8GB (~17.2 seconds with disable-interrupts w/ 8.8 seconds gc follow-up)
;(file-stream-buffer-mode in 'none)
;(define result #f)
;(disable-interrupts)
;;(start-atomic)  ; for some reason, this is actively harmful if we also disable interrupts
;(pretty-write
;  (let ()
;    ;(define part-buffer-size 65536)
;    ;(define part-buffer-size 32768)
;    (define part-buffer-size 16384)
;    ;(define part-buffer-size 8192)
;    ;(define part-buffer-size 4096)
;    ;; Because the largest field is 3089399 bytes, with a part-buffer-size of 16384 we need at
;    ;; least 189 parts to fit it.
;    ;> (/ 3089399.0 (expt 2 14))
;    ;188.56195068359375
;    (define part-buffer-count 189)
;    ;(define part-buffer-count 4)
;    (define full-buffer-size (* part-buffer-size part-buffer-count))
;    (time
;      (let ((buffer (make-bytes full-buffer-size)))
;        (let loop.outer ((prev.buffer 0) (start.buffer 0) (record* '()) (field* '()))
;          (let ((size (read-bytes! buffer in start.buffer (unsafe-fx+ start.buffer part-buffer-size))))
;            (if (eof-object? size)
;                (begin (set! result
;                         (let ((field* (if (unsafe-fx= prev.buffer start.buffer) field* (cons (subbytes buffer prev.buffer start.buffer) field*))))
;                           (if (null? field*) record* (cons field* record*))))
;                       (length result))
;                (let ((end.buffer (unsafe-fx+ start.buffer size)))
;                  (let loop.inner ((i start.buffer) (start.field prev.buffer) (record* record*) (field* '()))
;                    (if (unsafe-fx<= end.buffer i)
;                        (if (unsafe-fx<= full-buffer-size (unsafe-fx+ end.buffer part-buffer-size))
;                            (begin
;                              (bytes-copy! buffer 0 buffer start.field end.buffer)
;                              (let* ((start.buffer (unsafe-fx- end.buffer start.field))
;                                     (end.buffer   (unsafe-fx+ start.buffer part-buffer-size)))
;                                (when (unsafe-fx<= full-buffer-size end.buffer)
;                                  (error "buffer requirements are too large" end.buffer))
;                                (loop.outer 0 start.buffer record* field*)))
;                            (loop.outer start.field end.buffer record* field*))
;                        (let ((b (bytes-ref buffer i)))
;                          (cond
;                            ((unsafe-fx= b 9)  (loop.inner (unsafe-fx+ i 1) (unsafe-fx+ i 1) record* (cons (subbytes buffer start.field i) field*)))
;                            ((unsafe-fx= b 10) (loop.inner (unsafe-fx+ i 1) (unsafe-fx+ i 1) (cons (cons (subbytes buffer start.field i) field*) record*) '()))
;                            (else              (loop.inner (unsafe-fx+ i 1) start.field      record* field*))))))))))))))
;;(end-atomic)
;(time (enable-interrupts))  ; ~8.8 seconds of gc
;(pretty-write (car result))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; naive tuple construction with line-reading ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; ~57 seconds (~23.3 seconds gc) for 4.8GB (~35.4 seconds with disable-interrupts)
;(define result #f)
;(disable-interrupts)
;(pretty-write
;  (let ()
;    (time
;      (let loop.outer ((record* '()) (full-size 0) (min-field-count #f) (max-field-count 0))
;        (let ((line (read-bytes-line in)))
;          (if (eof-object? line)
;              (begin (set! result record*) (list full-size min-field-count max-field-count))
;              (let ((len (unsafe-bytes-length line)))
;                (let loop.inner ((i 0) (field-start 0) (field* '()))
;                  (if (unsafe-fx<= len i)
;                      (let ((field-count (length field*)))
;                        (loop.outer (cons field* record*)
;                                    (unsafe-fx+ full-size (unsafe-fx- len field-count) 1)
;                                    (if min-field-count (unsafe-fxmin min-field-count field-count) field-count)
;                                    (unsafe-fxmax max-field-count field-count)))
;                      (let ((b (unsafe-bytes-ref line i)))
;                        (if (unsafe-fx= b 9)
;                            (loop.inner (unsafe-fx+ i 1) (unsafe-fx+ i 1) (cons (subbytes line field-start i) field*))
;                            (loop.inner (unsafe-fx+ i 1) field-start      field*))))))))))))
;(time (enable-interrupts))  ; ~11.4 seconds of gc
;(pretty-write (car result))
;(pretty-write (length result))
;;; ~15 seconds
;(pretty-write
;  (time
;    (let loop ((max-field-size (bytes-length (caar result))) (field** result))
;      (if (null? field**)
;          max-field-size
;          (loop (max max-field-size (apply max (map (lambda (field) (bytes-length field)) (car field**))))
;                (cdr field**))))))
;; ==> 3089399

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; old naive tuple construction with line-reading ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; 4.8GB
;;  cpu time: 39451 real time: 41282 gc time: 119
;; ==> (11342763 181484208)
;; 37GB
;;  cpu time: 331689 real time: 346845 gc time: 501
;; ==> (56965146 1025372628)
;; 40GB
;;  cpu time: 347328 real time: 362632 gc time: 563
;; ==> (600183480 1800550440)
;(pretty-write
;  (time
;    (let loop ((line-count 0) (field-count 0))
;      (define l (read-bytes-line in 'any))
;      (if (eof-object? l)
;          (list line-count field-count)
;          (let field-loop ((end    (unsafe-bytes-length l))
;                           (i      (unsafe-fx- (unsafe-bytes-length l) 1))
;                           (fields '()))
;            (cond ((unsafe-fx< i 0)                      (loop (unsafe-fx+ line-count 1)
;                                                               (unsafe-fx+ field-count
;                                                                           (length (cons (subbytes l 0 end)
;                                                                                         fields)))))
;                  ((unsafe-fx= (unsafe-bytes-ref l i) 9) (field-loop i
;                                                                     (unsafe-fx- i 1)
;                                                                     (cons (subbytes l i end) fields)))
;                  (else                                  (field-loop end
;                                                                     (unsafe-fx- i 1)
;                                                                     fields))))))))

(define (stream:tsv-list* in)
  (let loop ()
    (lambda ()
      (define l (read-bytes-line in 'any))
      (if (eof-object? l)
          '()
          (let loop.field ((end    (unsafe-bytes-length l))
                           (i      (unsafe-fx- (unsafe-bytes-length l) 1))
                           (fields '()))
            (cond ((unsafe-fx< i 0)                      (cons (cons (subbytes l 0 end) fields) (loop)))
                  ((unsafe-fx= (unsafe-bytes-ref l i) 9) (loop.field i
                                                                     (unsafe-fx- i 1)
                                                                     (cons (subbytes l i end) fields)))
                  (else                                  (loop.field end
                                                                     (unsafe-fx- i 1)
                                                                     fields))))))))

;; 4.8GB
;;  cpu time: 35759 real time: 37508 gc time: 96
;; ==> 181484208
;; 37GB
;;  cpu time: 320390 real time: 335324 gc time: 488
;; ==> 1025372628
;; 40GB
;;  cpu time: 344196 real time: 359847 gc time: 589
;; ==> 1800550440
;(pretty-write
;  (time
;    (let loop ((count 0) (b* (stream:tsv-list* in)))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count (length (unsafe-car b*))) (unsafe-cdr b*)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; old naive line-reading ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; 4.8GB
;;  cpu time: 34447 real time: 36184 gc time: 69
;; ==> (11342763 11342763)
;; 37GB
;;  cpu time: 313759 real time: 328743 gc time: 324
;; ==> (56965146 56965146)
;; 40GB
;;  cpu time: 312599 real time: 327515 gc time: 318
;; ==> (600183480 600183480)
;(pretty-write
;  (time
;    (let loop ((line-count 0) (field-count 0))
;      (define l (read-bytes-line in 'any))
;      (if (eof-object? l)
;          (list line-count field-count)
;          (let field-loop ((end (unsafe-bytes-length l))
;                           (i   (unsafe-fx- (unsafe-bytes-length l) 1)))
;            (cond ((unsafe-fx< i 0)                      (loop (unsafe-fx+ line-count 1)
;                                                               (unsafe-fx+ field-count 1)))
;                  ((unsafe-fx= (unsafe-bytes-ref l i) 9) (field-loop i   (unsafe-fx- i 1)))
;                  (else                                  (field-loop end (unsafe-fx- i 1)))))))))

(define (stream:line-list* in)
  (let loop ()
    (lambda ()
      (define l (read-bytes-line in 'any))
      ;; 4.8GB
      ;;  cpu time: 34405 real time: 36123 gc time: 67
      ;; ==> 5170574573
      ;; 37GB
      ;;  cpu time: 307719 real time: 322435 gc time: 314
      ;; ==> 39823101310
      ;; 40GB
      ;; killed
      ;(define l (read-bytes-line in 'return-linefeed))
      ;; 4.8GB
      ;;  cpu time: 32863 real time: 34613 gc time: 71
      ;; ==> 5181917336
      ;; 37GB
      ;;  cpu time: 303419 real time: 318348 gc time: 330
      ;; ==> 39880066456
      ;; 40GB
      ;;  cpu time: 301032 real time: 315945 gc time: 346
      ;; ==> 42879384804
      ;(define l (read-bytes-line in 'linefeed))
      (if (eof-object? l)
          '()
          (let loop.field ((end (unsafe-bytes-length l))
                           (i   (unsafe-fx- (unsafe-bytes-length l) 1)))
            (cond ((unsafe-fx< i 0)                      (cons l (loop)))
                  ((unsafe-fx= (unsafe-bytes-ref l i) 9) (loop.field i   (unsafe-fx- i 1)))
                  (else                                  (loop.field end (unsafe-fx- i 1)))))))))

;; 4.8GB
;;  cpu time: 34268 real time: 36001 gc time: 66
;; ==> 5170574573
;; 37GB
;;  cpu time: 312282 real time: 327077 gc time: 309
;; ==> 39823101310
;; 40GB
;;  cpu time: 327785 real time: 343182 gc time: 367
;; ==> 42879384804
;(pretty-write
;  (time
;    (let loop ((count 0) (b* (stream:line-list* in)))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count (unsafe-bytes-length (unsafe-car b*))) (unsafe-cdr b*)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; old naive tuple construction as a map over line-reading ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; 4.8GB
;;  cpu time: 53002 real time: 55449 gc time: 140
;; ==> 181484208
;; 37GB
;;  cpu time: 439936 real time: 460281 gc time: 730
;; ==> 1025372628
;; 40GB
;;  cpu time: 438471 real time: 459773 gc time: 818
;; ==> 1800550440
;(pretty-write
;  (time
;    (let loop ((count 0) (b* (s-map
;                               (lambda (l)
;                                 (let loop.field ((end    (unsafe-bytes-length l))
;                                                  (i      (unsafe-fx- (unsafe-bytes-length l) 1))
;                                                  (fields '()))
;                                   (cond ((unsafe-fx< i 0)                      (cons (subbytes l 0 end) fields))
;                                         ((unsafe-fx= (unsafe-bytes-ref l i) 9) (loop.field i
;                                                                                            (unsafe-fx- i 1)
;                                                                                            (cons (subbytes l i end) fields)))
;                                         (else                                  (loop.field end
;                                                                                            (unsafe-fx- i 1)
;                                                                                            fields)))))
;                               (stream:line-list* in))))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count (length (unsafe-car b*))) (unsafe-cdr b*)))))))

;; 4.8GB
;;  cpu time: 41178 real time: 43030 gc time: 119
;; ==> 181484208
;; 37GB
;;  cpu time: 343095 real time: 358246 gc time: 553
;; ==> 1025372628
;; 40GB
;;  cpu time: 373815 real time: 389834 gc time: 688
;; ==> 1800550440
;(pretty-write
;  (time
;    (let loop ((count 0) (b* (s-map
;                               (lambda (l)
;                                 (let loop.field ((end    (unsafe-bytes-length l))
;                                                  (i      (unsafe-fx- (unsafe-bytes-length l) 1))
;                                                  (fields '()))
;                                   (cond ((unsafe-fx< i 0)                      (cons (subbytes l 0 end) fields))
;                                         ((unsafe-fx= (unsafe-bytes-ref l i) 9) (loop.field i
;                                                                                            (unsafe-fx- i 1)
;                                                                                            (cons (subbytes l i end) fields)))
;                                         (else                                  (loop.field end
;                                                                                            (unsafe-fx- i 1)
;                                                                                            fields)))))
;                               (let loop ()
;                                 (lambda ()
;                                   (define l (read-bytes-line in 'any))
;                                   (if (eof-object? l)
;                                       '()
;                                       (cons l (loop))))))))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count (length (unsafe-car b*))) (unsafe-cdr b*)))))))
