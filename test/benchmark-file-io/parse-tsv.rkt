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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; streams with irregular suspension ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (s-take n s)
  (if n
      (let loop ((n n) (s s))
        (cond ((= n 0)        '())
              ((null?      s) '())
              ((procedure? s) (loop n (s)))
              (else           (cons (car s) (loop (- n 1) (cdr s))))))
      (s->list s)))

(define s-map
  (case-lambda
    ((f s)
     (let loop ((s s))
       (cond ((null?      s) '())
             ((procedure? s) (lambda () (loop (s))))
             (else           (cons (f (car s)) (loop (cdr s)))))))
    ((f s . s*)
     (let loop.outer ((s s) (s* s*))
       (cond ((null?      s) '())
             ((procedure? s) (lambda () (loop.outer (s) s*)))
             (else (let loop ((s*-pending s*) (rs* '()))
                     (if (null? s*-pending)
                         (let ((s* (reverse rs*)))
                           (cons (apply f (car s) (map car s*))
                                 (loop.outer (cdr s) (map cdr s*))))
                         (let next ((s*0 (car s*-pending)))
                           (cond ((procedure? s*0) (lambda () (next (s*0))))
                                 (else (loop (cdr s*-pending) (cons s*0 rs*)))))))))))))

(define (s-chunk s len.chunk)
  (cond
    ((<= len.chunk 0) (error "chunk length must be positive" len.chunk))
    ((=  len.chunk 1) (s-map vector s))
    (else (let new ((s s))
            (cond ((null?      s) '())
                  ((procedure? s) (lambda () (new (s))))
                  (else (let ((chunk (make-vector len.chunk)))
                          (unsafe-vector*-set! chunk 0 (car s))
                          (let loop ((s (cdr s)) (i 1))
                            (cond
                              ((null?      s) (list (vector-copy chunk 0 i)))
                              ((procedure? s) (lambda () (loop (s) i)))
                              (else (unsafe-vector*-set! chunk i (car s))
                                    (let ((i (unsafe-fx+ i 1)))
                                      (if (unsafe-fx< i len.chunk)
                                          (loop (cdr s) i)
                                          (cons chunk (new (cdr s)))))))))))))))

(define (s-unchunk s)
  (let next ((s s))
    (cond ((null?      s) '())
          ((procedure? s) (lambda () (next (s))))
          (else (let* ((x* (car s)) (len.chunk (vector-length x*)))
                  (let loop ((i 0))
                    (if (unsafe-fx< i len.chunk)
                        (cons (unsafe-vector*-ref x* i) (loop (unsafe-fx+ i 1)))
                        (next (cdr s)))))))))

;;;;;;;;;;;;;;;;;;;
;;; combinators ;;;
;;;;;;;;;;;;;;;;;;;

(define (source-map f src)
  (define ((loop resume) yield.0 done)
    (define (yield.f x resume) (yield.0 (f x) (loop resume)))
    (resume yield.f done))
  (loop src))

(define (source->s src)
  (define ((loop resume))
    (define (yield x resume) (cons x (loop resume)))
    (define (done) '())
    (resume yield done))
  (loop src))

(define (s->source x*)
  (let resume/x* ((x* x*))
    (lambda (yield done)
      (let loop ((x* x*))
        (cond
          ((null?      x*) (done))
          ((procedure? x*) (loop (x*)))
          (else            (yield (car x*) (resume/x* (cdr x*)))))))))

(define (source-chunk len.chunk src)
  (cond
    ((<= len.chunk 0) (error "chunk length must be positive" len.chunk))
    ((=  len.chunk 1) (source-map vector src))
    (else
      (define ((new resume) yield done)
        (resume
          (lambda (x resume)
            (let ((chunk (make-vector len.chunk)))
              (unsafe-vector*-set! chunk 0 x)
              (let loop ((resume resume) (i 1))
                (resume
                  (lambda (x resume)
                    (unsafe-vector*-set! chunk i x)
                    (let ((i (unsafe-fx+ i 1)))
                      (if (unsafe-fx< i len.chunk)
                          (loop resume i)
                          (yield chunk (new resume)))))
                  (lambda () (yield (vector-copy chunk 0 i) (lambda (yield done) (done))))))))
          done))
      (new src))))

(define ((source-unchunk src) yield done)
  (let next ((resume src) (yield yield) (done done))
    (resume
      (lambda (x* resume)
        (let ((len.chunk (vector-length x*)))
          (let loop ((yield yield) (done done) (i 0))
            (if (unsafe-fx< i len.chunk)
                (yield (unsafe-vector*-ref x* i)
                       (lambda (yield done) (loop yield done (unsafe-fx+ i 1))))
                (next resume yield done)))))
      done)))

;; benchmark helpers
(define ((source->block*/length len.block) src) (source->s (source-chunk len.block src)))
(define (block*->source b*)                     (source-unchunk (s->source b*)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; streaming line blocks with multiple buffers ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; NOTE: to measure something simpler first, this version doesn't parse tabs, just entire lines.
(define (stream:line-block* in)
  ;(define block-length 8192)
  ;(define block-length 4096)
  ;(define block-length 2048)
  (define block-length 1024)
  ;(define block-length 512)
  ;(define block-length 256)
  ;(define block-length 128)
  ;(define block-length 64)
  (define buffer-size 16384)
  ;(define buffer-size 8192)
  ;(define buffer-size 4096)
  ;(define buffer-size 1024)
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; streaming vector-tuple blocks using s-map ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (stream:tsv-vector-block*-via-map in field-count)
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
;;  cpu time: 19542 real time: 20806 gc time: 537
;; ==> 11342763
;; 4.8GB no interrupts (not as good)
;;  cpu time: 23145 real time: 23148 gc time: 0
;;  cpu time: 1081 real time: 1081 gc time: 1081
;; 37GB
;;  cpu time: 155706 real time: 165540 gc time: 6000
;; ==> 56965146
;; 40GB
;;  cpu time: 195573 real time: 206854 gc time: 2116
;; ==> 600183480
;(file-stream-buffer-mode in 'none)
;;(disable-interrupts)
;(pretty-write
;  (time
;    (let loop ((count 0) (b* (stream:tsv-vector-block*-via-map in field-count)))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count (unsafe-vector*-length (unsafe-car b*))) (unsafe-cdr b*)))))))
;;(time (enable-interrupts))

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
;(define result (time (s->list (stream:tsv-vector-block*-via-map in field-count))))
;;(time (enable-interrupts))
;(pretty-write (vector-ref (car result) 0))
;(pretty-write
;  (let loop ((count 0) (b* result))
;    (cond ((null? b*) count)
;          (else (loop (+ count (vector-length (car b*))) (cdr b*))))))
;(let ((v (car (reverse result))))
;  (pretty-write (vector-ref v (- (vector-length v) 1))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; streaming vector-tuple blocks without mapping ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (stream:tsv-vector-block*-no-map in field-count)
  (define block-length 1024)
  (define buffer-size 16384)
  (define (tuple:span buffer start end)
    (let ((tuple (make-vector field-count)))
      (let loop ((i (unsafe-fx- end 1)) (end end) (j.field (unsafe-fx- field-count 1)))
        (cond
          ((unsafe-fx< i start)
           (unsafe-vector*-set! tuple j.field (subbytes buffer start end))
           (unless (fx= j.field 0)
             (error "too few fields" (- field-count (+ j.field 1)) tuple)))
          ((unsafe-fx= (unsafe-bytes-ref buffer i) 9)
           (unsafe-vector*-set! tuple j.field (subbytes buffer (unsafe-fx+ i 1) end))
           (when (unsafe-fx= j.field 0) (error "too many fields" tuple))
           (loop (unsafe-fx- i 1) i (unsafe-fx- j.field 1)))
          (else (loop (unsafe-fx- i 1) end j.field))))
      tuple))
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
         (unsafe-vector*-set! block j.block (tuple:span buffer start i))
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
                (unsafe-vector*-set! block j.block (tuple:span line 0 len)))
              (let ((j.block (unsafe-fx+ j.block 1)))
                (if (unsafe-fx= j.block block-length)
                    (cons block (pause (loop.single (unsafe-fx+ i 1) end buffer.current 0 (make-vector block-length))))
                    (loop.single (unsafe-fx+ i 1) end buffer.current j.block block))))
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
            (unsafe-vector*-set! block j.block (tuple:span line 0 len)))
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

;; 4.8GB
;;  cpu time: 18605 real time: 19855 gc time: 483
;; ==> 11342763
;; 37GB
;;  cpu time: 150435 real time: 160222 gc time: 6042
;; ==> 56965146
;; 40GB
;;  cpu time: 165683 real time: 176561 gc time: 1666
;; ==> 600183480
;(file-stream-buffer-mode in 'none)
;(pretty-write
;  (time
;    (let loop ((count 0) (b* (stream:tsv-vector-block*-no-map in field-count)))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count (unsafe-vector*-length (unsafe-car b*))) (unsafe-cdr b*)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; streaming vector-tuple blocks without mapping, plus spare buffer ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (stream:tsv-vector-block*-spare-buffer in field-count)
  (define block-length 1024)
  (define buffer-size 16384)
  (define (tuple:span buffer start end)
    (let ((tuple (make-vector field-count)))
      (let loop ((i (unsafe-fx- end 1)) (end end) (j.field (unsafe-fx- field-count 1)))
        (cond
          ((unsafe-fx< i start)
           (unsafe-vector*-set! tuple j.field (subbytes buffer start end))
           (unless (fx= j.field 0)
             (error "too few fields" (- field-count (+ j.field 1)) tuple)))
          ((unsafe-fx= (unsafe-bytes-ref buffer i) 9)
           (unsafe-vector*-set! tuple j.field (subbytes buffer (unsafe-fx+ i 1) end))
           (when (unsafe-fx= j.field 0) (error "too many fields" tuple))
           (loop (unsafe-fx- i 1) i (unsafe-fx- j.field 1)))
          (else (loop (unsafe-fx- i 1) end j.field))))
      tuple))
  (define (loop.single start end buffer j.block block buffer.spare)
    ;(pretty-write `(loop.single ,start ,end ,buffer ,j.block ,block))
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
         (unsafe-vector*-set! block j.block (tuple:span buffer start i))
         (let ((j.block (unsafe-fx+ j.block 1)))
           (if (unsafe-fx= j.block block-length)
               (cons block (pause (loop.single (unsafe-fx+ i 1) end buffer 0 (make-vector block-length) buffer.spare)))
               (loop.single (unsafe-fx+ i 1) end buffer j.block block buffer.spare))))
        (else (loop.inner (unsafe-fx+ i 1))))))
  (define (loop.multi buffer.current len.middle end*.middle buffer*.middle buffer.first start.first end.first j.block block)
    ;(pretty-write `(loop.multi ,len.middle ,end*.middle ,buffer*.middle ,buffer.first ,start.first ,end.first ,j.block ,block))
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
                (unsafe-vector*-set! block j.block (tuple:span line 0 len)))
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
            (unsafe-vector*-set! block j.block (tuple:span line 0 len)))
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

;; 4.8GB
;;  cpu time: 17428 real time: 18669 gc time: 301
;; ==> 11342763
;; 37GB
;;  cpu time: 141501 real time: 151152 gc time: 4717
;; ==> 56965146
;; 40GB
;;  cpu time: 167181 real time: 178047 gc time: 1197
;; ==> 600183480
;(file-stream-buffer-mode in 'none)
;(pretty-write
;  (time
;    (let loop ((count 0) (b* (stream:tsv-vector-block*-spare-buffer in field-count)))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count (unsafe-vector*-length (unsafe-car b*))) (unsafe-cdr b*)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; streaming vector-tuple blocks with immediate mapping, plus spare buffer ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define ((stream:line-block*/map f) in)
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

(define (stream:tsv-vector-block*-via-immediate-map in field-count)
  ((stream:line-block*/map
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
   in))

;; 4.8GB
;;  cpu time: 19631 real time: 20888 gc time: 475
;; ==> 11342763
;; 37GB
;;  cpu time: 152438 real time: 162311 gc time: 5617
;; ==> 56965146
;; 40GB
;;  cpu time: 173673 real time: 184644 gc time: 1653
;; ==> 600183480
;(file-stream-buffer-mode in 'none)
;(pretty-write
;  (time
;    (let loop ((count 0) (b* (stream:tsv-vector-block*-via-immediate-map in field-count)))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count (unsafe-vector*-length (unsafe-car b*))) (unsafe-cdr b*)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; streaming vector-tuple blocks with mapping, plus spare buffer ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (stream:line-block*-spare-buffer in)
  ;(define block-length 8192)
  ;(define block-length 4096)
  ;(define block-length 2048)
  (define block-length 1024)
  ;(define block-length 512)
  ;(define block-length 256)
  ;(define block-length 128)
  ;(define block-length 64)
  (define buffer-size 16384)
  ;(define buffer-size 8192)
  ;(define buffer-size 4096)
  ;(define buffer-size 1024)
  ;(define block-length 5)
  ;(define buffer-size 16)
  (define (loop.single start end buffer j.block block buffer.spare)
    ;(pretty-write `(loop.single ,start ,end ,buffer ,j.block ,block))
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
    ;(pretty-write `(loop.multi ,len.middle ,end*.middle ,buffer*.middle ,buffer.first ,start.first ,end.first ,j.block ,block))
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

(define (stream:tsv-vector-block*-via-map-spare-buffer in field-count)
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
    (stream:line-block*-spare-buffer in)))

;; 4.8GB
;;  cpu time: 8107 real time: 8751 gc time: 136
;; ==> 11342763
;; 37GB
;;  cpu time: 67623 real time: 72737 gc time: 3075
;; ==> 56965146
;; 40GB
;;  cpu time: 72545 real time: 77743 gc time: 484
;; ==> 600183480
;(file-stream-buffer-mode in 'none)
;(pretty-write
;  (time
;    (let loop ((count 0) (b* (stream:line-block*-spare-buffer in)))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count (unsafe-vector*-length (unsafe-car b*))) (unsafe-cdr b*)))))))

;; 4.8GB
;;  cpu time: 18005 real time: 19264 gc time: 484
;; ==> 11342763
;; 37GB
;;  cpu time: 151652 real time: 161298 gc time: 7374
;; ==> 56965146
;; 40GB
;;  cpu time: 187051 real time: 198424 gc time: 1962
;; ==> 600183480
;(file-stream-buffer-mode in 'none)
;(pretty-write
;  (time
;    (let loop ((count 0) (b* (stream:tsv-vector-block*-via-map-spare-buffer in field-count)))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count (unsafe-vector*-length (unsafe-car b*))) (unsafe-cdr b*)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; streaming bytevector-tuple blocks ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Each tuple will be represented as a single bytevector with packed, length-encoded fields.
;; This means that sorting tuples with bytes<? is not lexicographical relative to the field text.
;; But this is fine since we only care about equality, not order.

(define (nat-size n)
  (cond
    ((unsafe-fx< n 128)               1)
    ((unsafe-fx< n 16384)             2)
    ((unsafe-fx< n 2097152)           3)
    ((unsafe-fx< n 268435456)         4)
    ((unsafe-fx< n 34359738368)       5)
    ((unsafe-fx< n 4398046511104)     6)
    ((unsafe-fx< n 562949953421312)   7)
    ((unsafe-fx< n 72057594037927936) 8)
    (else                             9)))

(define (write-nat! buffer start n)
  (cond
    ((unsafe-fx< n 128)
     (unsafe-bytes-set! buffer start                n))
    ((unsafe-fx< n 16384)
     (unsafe-bytes-set! buffer (unsafe-fx+ start 1) (unsafe-fxand n                                    127))
     (unsafe-bytes-set! buffer start                (unsafe-fxior (unsafe-fxrshift n 7)                     128)))
    ((unsafe-fx< n 2097152)
     (unsafe-bytes-set! buffer (unsafe-fx+ start 2) (unsafe-fxand n                                    127))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 1) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 7)  127) 128))
     (unsafe-bytes-set! buffer start                (unsafe-fxior (unsafe-fxrshift n 14)                    128)))
    ((unsafe-fx< n 268435456)
     (unsafe-bytes-set! buffer (unsafe-fx+ start 3) (unsafe-fxand n                                    127))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 2) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 7)  127) 128))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 1) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 14) 127) 128))
     (unsafe-bytes-set! buffer start                (unsafe-fxior (unsafe-fxrshift n 21)                    128)))
    ((unsafe-fx< n 34359738368)
     (unsafe-bytes-set! buffer (unsafe-fx+ start 4) (unsafe-fxand n                                    127))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 3) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 7)  127) 128))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 2) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 14) 127) 128))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 1) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 21) 127) 128))
     (unsafe-bytes-set! buffer start                (unsafe-fxior (unsafe-fxrshift n 28)                    128)))
    ((unsafe-fx< n 4398046511104)
     (unsafe-bytes-set! buffer (unsafe-fx+ start 5) (unsafe-fxand n                                    127))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 4) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 7)  127) 128))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 3) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 14) 127) 128))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 2) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 21) 127) 128))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 1) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 28) 127) 128))
     (unsafe-bytes-set! buffer start                (unsafe-fxior (unsafe-fxrshift n 35)                    128)))
    ((unsafe-fx< n 562949953421312)
     (unsafe-bytes-set! buffer (unsafe-fx+ start 6) (unsafe-fxand n                                    127))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 5) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 7)  127) 128))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 4) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 14) 127) 128))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 3) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 21) 127) 128))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 2) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 28) 127) 128))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 1) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 35) 127) 128))
     (unsafe-bytes-set! buffer start                (unsafe-fxior (unsafe-fxrshift n 42)                    128)))
    ((unsafe-fx< n 72057594037927936)
     (unsafe-bytes-set! buffer (unsafe-fx+ start 7) (unsafe-fxand n                                    127))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 6) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 7)  127) 128))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 5) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 14) 127) 128))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 4) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 21) 127) 128))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 3) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 28) 127) 128))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 2) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 35) 127) 128))
     (unsafe-bytes-set! buffer (unsafe-fx+ start 1) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 42) 127) 128))
     (unsafe-bytes-set! buffer start                (unsafe-fxior (unsafe-fxrshift n 49)                    128)))
    (else
      (unsafe-bytes-set! buffer (unsafe-fx+ start 8) (unsafe-fxand n                                    255))
      (unsafe-bytes-set! buffer (unsafe-fx+ start 7) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 8)  127) 128))
      (unsafe-bytes-set! buffer (unsafe-fx+ start 6) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 15) 127) 128))
      (unsafe-bytes-set! buffer (unsafe-fx+ start 5) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 22) 127) 128))
      (unsafe-bytes-set! buffer (unsafe-fx+ start 4) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 29) 127) 128))
      (unsafe-bytes-set! buffer (unsafe-fx+ start 3) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 36) 127) 128))
      (unsafe-bytes-set! buffer (unsafe-fx+ start 2) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 43) 127) 128))
      (unsafe-bytes-set! buffer (unsafe-fx+ start 1) (unsafe-fxior (unsafe-fxand (unsafe-fxrshift n 50) 127) 128))
      (unsafe-bytes-set! buffer start                (unsafe-fxior (unsafe-fxrshift n 57)                    128)))))

;; TODO:
;(define (read-nat! buffer start)
  ;)

(define (stream:tsv-bytevector-block*-via-map-spare-buffer in field-count)
  (s-map
    (lambda (block)
      (vector-map
        (lambda (line)
          (let* ((len.line   (unsafe-bytes-length line))
                 (len-count  (unsafe-fx- field-count 1))
                 (len*.delim (make-fxvector len-count)))
            (let loop ((i 0) (j.field 0) (start.field 0) (size.len* 0))
              (cond
                ((unsafe-fx= i len.line)
                 (unless (unsafe-fx= j.field len-count) (error "too few fields" j.field line))
                 (let* ((len.field      (unsafe-fx- i start.field))
                        (size.len.field (nat-size len.field))
                        (size.tuple     (unsafe-fx+ (unsafe-fx- len.line len-count) (unsafe-fx+ size.len* size.len.field)))
                        (tuple          (make-bytes size.tuple)))
                   (let loop ((j.field        j.field)
                              (len.field      len.field)
                              (size.len.field size.len.field)
                              (start.tuple    (unsafe-fx- size.tuple len.field))
                              (start          (unsafe-fx- len.line   len.field))
                              (end            len.line))
                     (cond ((unsafe-fx= j.field 0) tuple)
                           (else
                             (unsafe-bytes-copy! tuple start.tuple line start end)
                             (let ((start.tuple (unsafe-fx- start.tuple size.len.field)))
                               (write-nat! tuple start.tuple len.field)
                               (let* ((j.field        (unsafe-fx- j.field 1))
                                      (len.field      (unsafe-fxvector-ref len*.delim j.field))
                                      (size.len.field (nat-size len.field))
                                      (end            (unsafe-fx- start 1)))
                                 (loop j.field
                                       len.field
                                       size.len.field
                                       (unsafe-fx- start.tuple len.field)
                                       (unsafe-fx- end len.field)
                                       end))))))))
                ((unsafe-fx= (unsafe-bytes-ref line i) 9)
                 (when (unsafe-fx= j.field len-count) (error "too many fields" line))
                 (let ((len.field (unsafe-fx- i start.field))
                       (i         (unsafe-fx+ i 1)))
                   (unsafe-fxvector-set! len*.delim j.field len.field)
                   (loop i (unsafe-fx+ j.field 1) i (unsafe-fx+ size.len* (nat-size len.field)))))
                (else (loop (unsafe-fx+ i 1) j.field start.field size.len*))))))
        block))
    (stream:line-block*-spare-buffer in)))

;; 4.8GB
;;  cpu time: 18892 real time: 20149 gc time: 305
;; ==> 11342763
;; 37GB
;;  cpu time: 140360 real time: 150143 gc time: 4855
;; ==> 56965146
;; 40GB
;;  cpu time: 184234 real time: 195508 gc time: 1405
;; ==> 600183480
;(file-stream-buffer-mode in 'none)
;(pretty-write
;  (time
;    (let loop ((count 0) (b* (stream:tsv-bytevector-block*-via-map-spare-buffer in field-count)))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count (unsafe-vector*-length (unsafe-car b*))) (unsafe-cdr b*)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; streaming lines and vector-tuple blocks with source combinators ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (source:port-split delim in)
  (define size.buffer 16384)
  (file-stream-buffer-mode in 'none)
  (lambda (yield done)
    (define (loop.single yield done start end buffer buffer.spare)
      (let loop.inner ((i start))
        (cond
          ((unsafe-fx<= end i)
           (if (unsafe-fx= start end)
               (let ((end (read-bytes! buffer in 0 size.buffer)))
                 (if (eof-object? end)
                     (done)
                     (loop.single yield done 0 end buffer buffer.spare)))
               (loop.multi yield done buffer.spare 0 '() '() start end buffer)))
          ((unsafe-fx= (unsafe-bytes-ref buffer i) delim)
           (yield (subbytes buffer start i)
                  (lambda (yield done)
                    (loop.single yield done (unsafe-fx+ i 1) end buffer buffer.spare))))
          (else (loop.inner (unsafe-fx+ i 1))))))
    (define (loop.multi yield done buffer.current len.middle end*.middle buffer*.middle
                        start.first end.first buffer.first)
      (let ((end (read-bytes! buffer.current in 0 size.buffer)))
        (if (not (eof-object? end))
            (let loop.inner ((i 0))
              (cond
                ((unsafe-fx<= end i) (loop.multi yield done
                                                 (make-bytes size.buffer)
                                                 (unsafe-fx+ len.middle end)
                                                 (cons end            end*.middle)
                                                 (cons buffer.current buffer*.middle)
                                                 start.first end.first buffer.first))
                ((unsafe-fx= (unsafe-bytes-ref buffer.current i) delim)
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
                   (yield line (lambda (yield done)
                                 (loop.single yield done (unsafe-fx+ i 1) end
                                              buffer.current buffer.first)))))
                (else (loop.inner (unsafe-fx+ i 1)))))
            (let* ((len  (unsafe-fx+ len.middle (unsafe-fx- end.first start.first)))
                   (line (make-bytes len)))
              (let loop ((pos len) (end* end*.middle) (buffer* buffer*.middle))
                (when (pair? end*)
                  (let* ((end (unsafe-car end*))
                         (pos (unsafe-fx- pos end)))
                    (unsafe-bytes-copy! line pos (unsafe-car buffer*) 0 end)
                    (loop pos (unsafe-cdr end*) (unsafe-cdr buffer*)))))
              (unsafe-bytes-copy! line 0 buffer.first start.first end.first)
              (yield line (lambda (yield done) (done)))))))
    (let ((buffer (make-bytes size.buffer)))
      (let ((end (read-bytes! buffer in 0 size.buffer)))
        (if (eof-object? end)
            (done)
            (loop.single yield done 0 end buffer (make-bytes size.buffer)))))))

(define (source:port-line* in) (source:port-split 10 in))

(define (source:port-tsv* field-count in)
  (source-map
    (lambda (line)
      (let ((len.line (bytes-length line))
            (tuple    (make-vector field-count)))
        (let loop ((i 0) (j.field 0) (start.field 0))
          (cond
            ((unsafe-fx= i len.line)
             (unsafe-vector*-set! tuple j.field (subbytes line start.field i))
             (unless (unsafe-fx= (unsafe-fx+ j.field 1) field-count)
               (error "too few fields" j.field line))
             tuple)
            ((unsafe-fx= (unsafe-bytes-ref line i) 9)
             (unsafe-vector*-set! tuple j.field (subbytes line start.field i))
             (let ((i (unsafe-fx+ i 1)) (j.field (unsafe-fx+ j.field 1)))
               (when (unsafe-fx= j.field field-count) (error "too many fields" line))
               (loop i j.field i)))
            (else (loop (unsafe-fx+ i 1) j.field start.field))))))
    (source:port-line* in)))

;;; line*

;; 4.8GB
;;  cpu time: 8488 real time: 9101 gc time: 143
;; ==> 11342763
;; 37GB
;;  cpu time: 69718 real time: 74451 gc time: 3062
;; ==> 56965146
;; 40GB
;;  cpu time: 84399 real time: 89836 gc time: 893
;; ==> 600183480
;(pretty-write
;  (time
;    (let loop ((count 0) (b* ((source->block*/length 1024) (source:port-line* in))))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count (unsafe-vector*-length (unsafe-car b*))) (unsafe-cdr b*)))))))

;; 4.8GB
;;   cpu time: 8538 real time: 9155 gc time: 32
;; ==> 11342763
;; 37GB
;;  cpu time: 65771 real time: 70531 gc time: 175
;; ==> 56965146
;; 40GB
;;  cpu time: 80721 real time: 86104 gc time: 325
;; ==> 600183480
;(pretty-write
;  (time
;    (let loop ((count 0) (resume (source:port-line* in)))
;      (resume
;        (lambda (x resume) (loop (unsafe-fx+ count 1) resume))
;        (lambda () count)))))

;; 4.8GB
;;  cpu time: 8951 real time: 9570 gc time: 159
;; ==> 11342763
;; 37GB
;;  cpu time: 69778 real time: 74495 gc time: 2825
;; ==> 56965146
;; 40GB
;;  cpu time: 85219 real time: 90636 gc time: 995
;; ==> 600183480
;(pretty-write
;  (time
;    (let loop ((count 0) (resume (source-chunk 1024 (source:port-line* in))))
;      (resume
;        (lambda (x resume) (loop (unsafe-fx+ count (unsafe-vector*-length x)) resume))
;        (lambda () count)))))

;;; tsv*

;; 4.8GB
;;  cpu time: 19234 real time: 20497 gc time: 498
;; ==> 11342763
;; 37GB
;;  cpu time: 145026 real time: 154654 gc time: 5333
;; ==> 56965146
;; 40GB
;;  cpu time: 179781 real time: 190858 gc time: 2626
;; ==> 600183480
;(pretty-write
;  (time
;    (let loop ((count 0) (b* ((source->block*/length 1024) (source:port-tsv* field-count in))))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count (unsafe-vector*-length (unsafe-car b*))) (unsafe-cdr b*)))))))

;; 4.8GB
;;  cpu time: 19331 real time: 20588 gc time: 58
;; ==> 11342763
;; 37GB
;;  cpu time: 136394 real time: 145911 gc time: 374
;; ==> 56965146
;; 40GB
;;  cpu time: 183406 real time: 194453 gc time: 669
;; ==> 600183480
;(pretty-write
;  (time
;    (let loop ((count 0) (resume (source:port-tsv* field-count in)))
;      (resume
;        (lambda (x resume) (loop (unsafe-fx+ count 1) resume))
;        (lambda () count)))))

;; 4.8GB
;;  cpu time: 20781 real time: 22041 gc time: 590
;; ==> 11342763
;; 37GB
;;  cpu time: 155325 real time: 164917 gc time: 6294
;; ==> 56965146
;; 40GB
;;  cpu time: 188667 real time: 199784 gc time: 2957
;; ==> 600183480
;(pretty-write
;  (time
;    (let loop ((count 0) (resume (source-chunk 1024 (source:port-tsv* field-count in))))
;      (resume
;        (lambda (x resume) (loop (unsafe-fx+ count (unsafe-vector*-length x)) resume))
;        (lambda () count)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; streaming lines and vector-tuples individually ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; TODO: port-split-RLF

(define (port-split in delim)
  (define size.buffer 16384)
  (file-stream-buffer-mode in 'none)
  (define (loop.single start end buffer buffer.spare)
    (let loop.inner ((i start))
      (cond
        ((unsafe-fx<= end i)
         (if (unsafe-fx= start end)
             (let ((end (read-bytes! buffer in 0 size.buffer)))
               (if (eof-object? end)
                   '()
                   (loop.single 0 end buffer buffer.spare)))
             (loop.multi buffer.spare 0 '() '() start end buffer)))
        ((unsafe-fx= (unsafe-bytes-ref buffer i) delim)
         (cons (subbytes buffer start i)
               (lambda () (loop.single (unsafe-fx+ i 1) end buffer buffer.spare))))
        (else (loop.inner (unsafe-fx+ i 1))))))
  (define (loop.multi buffer.current len.middle end*.middle buffer*.middle
                      start.first end.first buffer.first)
    (let ((end (read-bytes! buffer.current in 0 size.buffer)))
      (if (not (eof-object? end))
          (let loop.inner ((i 0))
            (cond
              ((unsafe-fx<= end i) (loop.multi (make-bytes size.buffer)
                                               (unsafe-fx+ len.middle end)
                                               (cons end            end*.middle)
                                               (cons buffer.current buffer*.middle)
                                               start.first end.first buffer.first))
              ((unsafe-fx= (unsafe-bytes-ref buffer.current i) delim)
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
                 (cons line (lambda ()
                              (loop.single (unsafe-fx+ i 1) end buffer.current buffer.first)))))
              (else (loop.inner (unsafe-fx+ i 1)))))
          (let* ((len  (unsafe-fx+ len.middle (unsafe-fx- end.first start.first)))
                 (line (make-bytes len)))
            (let loop ((pos len) (end* end*.middle) (buffer* buffer*.middle))
              (when (pair? end*)
                (let* ((end (unsafe-car end*))
                       (pos (unsafe-fx- pos end)))
                  (unsafe-bytes-copy! line pos (unsafe-car buffer*) 0 end)
                  (loop pos (unsafe-cdr end*) (unsafe-cdr buffer*)))))
            (unsafe-bytes-copy! line 0 buffer.first start.first end.first)
            (cons line (lambda () '()))))))
  (lambda ()
    (let ((buffer (make-bytes size.buffer)))
      (let ((end (read-bytes! buffer in 0 size.buffer)))
        (if (eof-object? end)
            '()
            (loop.single 0 end buffer (make-bytes size.buffer)))))))

(define (port-line* in) (port-split in 10))

(define ((line->tsv/field-count field-count) line)
  (let ((len.line (bytes-length line))
        (tuple    (make-vector field-count)))
    (let loop ((i 0) (j.field 0) (start.field 0))
      (cond
        ((unsafe-fx= i len.line)
         (unsafe-vector*-set! tuple j.field (subbytes line start.field i))
         (unless (unsafe-fx= (unsafe-fx+ j.field 1) field-count)
           (error "too few fields" j.field line))
         tuple)
        ((unsafe-fx= (unsafe-bytes-ref line i) 9)
         (unsafe-vector*-set! tuple j.field (subbytes line start.field i))
         (let ((i (unsafe-fx+ i 1)) (j.field (unsafe-fx+ j.field 1)))
           (when (unsafe-fx= j.field field-count) (error "too many fields" line))
           (loop i j.field i)))
        (else (loop (unsafe-fx+ i 1) j.field start.field))))))

(define (port-tsv* in field-count)
  (s-map (line->tsv/field-count field-count) (port-line* in)))

;;; line*
;; 4.8GB
;;  cpu time: 7708 real time: 8315 gc time: 31
;; ==> 11342763
;; 37GB
;;  cpu time: 61245 real time: 65918 gc time: 138
;; ==> 56965146
;; 40GB
;;  cpu time: 74276 real time: 79608 gc time: 236
;; ==> 600183480
(pretty-write
  (time
    (let loop ((count 0) (line* (port-line* in)))
      (cond ((null?      line*) count)
            ((procedure? line*) (loop count (line*)))
            (else               (loop (unsafe-fx+ count 1) (unsafe-cdr line*)))))))

;;; line**
;; 4.8GB
;;  cpu time: 8378 real time: 8997 gc time: 143
;; ==> 11342763
;; 37GB
;;  cpu time: 65511 real time: 70208 gc time: 3199
;; ==> 56965146
;; 40GB
;;  cpu time: 79205 real time: 84693 gc time: 735
;; ==> 600183480
;(pretty-write
;  (time
;    (let loop ((count 0) (line** (s-chunk (port-line* in) 1024)))
;      (cond ((null?      line**) count)
;            ((procedure? line**) (loop count (line**)))
;            (else                (loop (unsafe-fx+ count (unsafe-vector*-length (unsafe-car line**))) (unsafe-cdr line**)))))))

;;; tsv*
;; 4.8GB
;;  cpu time: 18174 real time: 19428 gc time: 58
;; ==> 11342763
;; 37GB
;;  cpu time: 128575 real time: 138002 gc time: 288
;; ==> 56965146
;; 40GB
;;  cpu time: 167210 real time: 178156 gc time: 497
;; ==> 600183480
;(pretty-write
;  (time
;    (let loop ((count 0) (tsv* (port-tsv* in field-count)))
;      (cond ((null?      tsv*) count)
;            ((procedure? tsv*) (loop count (tsv*)))
;            (else              (loop (unsafe-fx+ count 1) ((unsafe-cdr tsv*))))))))

;;; tsv**
;; 4.8GB
;;  cpu time: 18731 real time: 19987 gc time: 461
;; ==> 11342763
;; 37GB
;;  cpu time: 130234 real time: 139721 gc time: 5035
;; ==> 56965146
;; 40GB
;;  cpu time: 175915 real time: 187106 gc time: 2174
;; ==> 600183480
;(pretty-write
;  (time
;    (let loop ((count 0) (tsv** (s-chunk (port-tsv* in field-count) 1024)))
;      (cond ((null?      tsv**) count)
;            ((procedure? tsv**) (loop count (tsv**)))
;            (else               (loop (unsafe-fx+ count (unsafe-vector*-length (unsafe-car tsv**))) (unsafe-cdr tsv**)))))))

;;; tsv** with resizing
;; 4.8GB
;;  cpu time: 25701 real time: 26978 gc time: 7636
;; ==> 11342763
;; 37GB
;;  cpu time: 199729 real time: 209406 gc time: 59567
;; ==> 56965146
;; 40GB
;;  cpu time: 263930 real time: 275982 gc time: 69542
;; ==> 600183480
;(pretty-write
;  (time
;    (let loop ((count 0) (tsv** (s-chunk (s-unchunk (s-chunk (port-tsv* in field-count) 65536)) 1024)))
;      (cond ((null?      tsv**) count)
;            ((procedure? tsv**) (loop count (tsv**)))
;            (else               (loop (unsafe-fx+ count (unsafe-vector*-length (unsafe-car tsv**))) (unsafe-cdr tsv**)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; sorting vector-tuple blocks ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (vector-tuple-sort! tuple*)
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
  (vector-sort! tuple* vector-tuple<?))

;;; s-map
;; 4.8GB
;;  cpu time: 29876 real time: 31893 gc time: 498
;; ==> 11342763
;; 37GB
;;  cpu time: 176952 real time: 188737 gc time: 5503
;; ==> 56965146
;; 40GB
;;  cpu time: 275484 real time: 291020 gc time: 2008
;; ==> 600183480
;(file-stream-buffer-mode in 'none)
;(pretty-write
;  (time
;    (let loop ((count 0) (b* (stream:tsv-vector-block*-via-map-spare-buffer in field-count)))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count
;                                               (let ((tuple* (unsafe-car b*)))
;                                                 (vector-tuple-sort! tuple*)
;                                                 (unsafe-vector*-length tuple*)))
;                                   (unsafe-cdr b*)))))))

;;; immediate-map
;; 4.8GB
;;  cpu time: 33395 real time: 35520 gc time: 517
;; ==> 11342763
;; 37GB
;;  cpu time: 186301 real time: 198042 gc time: 5681
;; ==> 56965146
;; 40GB
;;  cpu time: 258402 real time: 273479 gc time: 1782
;; ==> 600183480
;(file-stream-buffer-mode in 'none)
;(pretty-write
;  (time
;    (let loop ((count 0) (b* (stream:tsv-vector-block*-via-immediate-map in field-count)))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count
;                                               (let ((tuple* (unsafe-car b*)))
;                                                 (vector-tuple-sort! tuple*)
;                                                 (unsafe-vector*-length tuple*)))
;                                   (unsafe-cdr b*)))))))

;;; combinator
;; 4.8GB
;;  cpu time: 31225 real time: 33352 gc time: 554
;; ==> 11342763
;; 37GB
;;  cpu time: 174235 real time: 185874 gc time: 5772
;; ==> 56965146
;; 40GB
;;  cpu time: 256142 real time: 271380 gc time: 2875
;; ==> 600183480
;(pretty-write
;  (time
;    (let loop ((count 0) (b* ((source->block*/length 1024) (source:port-tsv* field-count in))))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count
;                                               (let ((tuple* (unsafe-car b*)))
;                                                 (vector-tuple-sort! tuple*)
;                                                 (unsafe-vector*-length tuple*)))
;                                   (unsafe-cdr b*)))))))

;;; combinator with resizing
;; 4.8GB
;;  cpu time: 40072 real time: 42183 gc time: 8813
;; ==> 11342763
;; 37GB
;;  cpu time: 243806 real time: 255555 gc time: 62417
;; ==> 56965146
;; 40GB
;;  cpu time: 353606 real time: 369411 gc time: 80791
;; ==> 600183480
;(pretty-write
;  (time
;    (let loop ((count 0) (b* ((source->block*/length 1024)
;                              (source-unchunk (source-chunk 65536 (source:port-tsv* field-count in))))))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count
;                                               (let ((tuple* (unsafe-car b*)))
;                                                 (vector-tuple-sort! tuple*)
;                                                 (unsafe-vector*-length tuple*)))
;                                   (unsafe-cdr b*)))))))

;;; source-only

;; 4.8GB
;;  cpu time: 33786 real time: 35862 gc time: 620
;; ==> 11342763
;; 37GB
;;  cpu time: 183669 real time: 195223 gc time: 6222
;; ==> 56965146
;; 40GB
;;  cpu time: 279083 real time: 294317 gc time: 3171
;; ==> 600183480
;(pretty-write
;  (time
;    (let loop ((count 0) (resume (source-chunk 1024 (source:port-tsv* field-count in))))
;      (resume
;        (lambda (tuple* resume)
;          (vector-tuple-sort! tuple*)
;          (loop (unsafe-fx+ count (unsafe-vector*-length tuple*)) resume))
;        (lambda () count)))))

;;; source-only with resizing

;; 4.8GB
;;  cpu time: 43416 real time: 45526 gc time: 9003
;; ==> 11342763
;; 37GB
;;  cpu time: 251643 real time: 263321 gc time: 66461
;; ==> 56965146
;; 40GB
;;  cpu time: 367554 real time: 383242 gc time: 84051
;; ==> 600183480
;(pretty-write
;  (time
;    (let loop ((count 0) (resume (source-chunk 1024 (source-unchunk (source-chunk 65536 (source:port-tsv* field-count in))))))
;      (resume
;        (lambda (tuple* resume)
;          (vector-tuple-sort! tuple*)
;          (loop (unsafe-fx+ count (unsafe-vector*-length tuple*)) resume))
;        (lambda () count)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; sorting bytevector-tuple blocks ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; NOTE: using bytes<? like this is surprisingly slow.
;; 4.8GB
;;  cpu time: 45799 real time: 48009 gc time: 329
;; ==> 11342763
;; 37GB
;;  cpu time: 219707 real time: 231937 gc time: 4847
;; ==> 56965146
;; 40GB
;;  cpu time: 658636 real time: 685149 gc time: 1599
;; ==> 600183480
;(define (bytevector-tuple-sort! tuple*) (vector-sort! tuple* bytes<?))

(define (bytevector-tuple-sort! tuple*)
  (define (bytevector-tuple<? a b)
    (let ((len (unsafe-fxmin (unsafe-bytes-length a) (unsafe-bytes-length b))))
      (let loop ((i 0))
        (and (unsafe-fx< i len)
             (let ((x.a (unsafe-bytes-ref a i)) (x.b (unsafe-bytes-ref b i)))
               (or (unsafe-fx< x.a x.b)
                   (and (unsafe-fx= x.a x.b)
                        (loop (unsafe-fx+ i 1)))))))))
  (vector-sort! tuple* bytevector-tuple<?))

;; 4.8GB
;;  cpu time: 28726 real time: 30827 gc time: 359
;; ==> 11342763
;; 37GB
;;  cpu time: 186084 real time: 198105 gc time: 5519
;; ==> 56965146
;; 40GB
;;  cpu time: 394967 real time: 419776 gc time: 1616
;; ==> 600183480
;(file-stream-buffer-mode in 'none)
;(pretty-write
;  (time
;    (let loop ((count 0) (b* (stream:tsv-bytevector-block*-via-map-spare-buffer in field-count)))
;      (cond ((null?      b*) count)
;            ((procedure? b*) (loop count (b*)))
;            (else            (loop (unsafe-fx+ count
;                                               (let ((tuple* (unsafe-car b*)))
;                                                 (bytevector-tuple-sort! tuple*)
;                                                 (unsafe-vector*-length tuple*)))
;                                   (unsafe-cdr b*)))))))

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

(define (port-line*-naive in)
  (let loop ()
    (lambda ()
      (let ((l (read-bytes-line in
                                ;'any
                                ;'return-linefeed
                                'linefeed
                                )))
        (if (eof-object? l) '() (cons l (loop)))))))

;; 4.8GB
;;  cpu time: 24938 real time: 26044 gc time: 60
;; ==> 11342763
;; 37GB
;;  cpu time: 247322 real time: 257375 gc time: 338
;; ==> 56965146
;; 40GB
;;  cpu time: 247828 real time: 258028 gc time: 382
;; 600183480
;(pretty-write
;  (time
;    (let loop ((count 0) (line* (port-line*-naive in)))
;      (cond ((null?      line*) count)
;            ((procedure? line*) (loop count (line*)))
;            (else               (loop (unsafe-fx+ count 1) (unsafe-cdr line*)))))))

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
