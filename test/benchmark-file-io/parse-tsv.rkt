#lang racket/base
(require ffi/unsafe/atomic ffi/unsafe/vm racket/fixnum racket/pretty racket/unsafe/ops racket/vector)

(define enable-interrupts  (vm-primitive 'enable-interrupts))
(define disable-interrupts (vm-primitive 'disable-interrupts))

;; 37GB
;(define in (open-input-file "rtx-kg2-s3/rtx-kg2_edges_2.8.1.tsv"))
;; 4.8GB
(define in (open-input-file "rtx-kg2-s3/rtx-kg2_nodes_2.8.1.tsv"))
;; 40GB
;(define in (open-input-file "rtx_kg2_20210204.edgeprop.tsv"))

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; streaming tuple blocks with multiple buffers ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; TODO: should we just s-map over stream:line-block* for simplicity, or will the extra allocation hurt too much?

;; Each tuple will be represented as a single bytevector with packed, length-encoded fields.
;; This means that sorting tuples with bytes<? is not lexicographical relative to the field text.
;; But this is fine since we only care about equality, not order.

;(define (stream:tsv-tuple-block* field-count in)
;  ;; TODO: Pre-allocate a fxvector buffer for storing intermediate lengths and offsets for the current line/tuple
;  ;; - field-count tells us how large this buffer needs to be
;  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; streaming line blocks with multiple buffers ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; ~20 seconds without streams (~22 seconds with streams) (~5.3 seconds gc) for 4.8GB (~17 seconds with disable-interrupts w/ ~3.1 seconds gc follow-up)
(file-stream-buffer-mode in 'none)
(define result #f)
;(disable-interrupts)
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
        ((<= end i) (if (= start end)
                        (let ((end (read-bytes! buffer in 0 buffer-size)))
                          (cond
                            ((not (eof-object? end)) (loop.single 0 end buffer j.block block))
                            ((< 0 j.block)           (list (vector-copy block 0 j.block)))
                            (else                    '())))
                        (loop.multi 0 '() '() buffer start end j.block block)))
        ((= (bytes-ref buffer i) 10)
         (vector-set! block j.block (subbytes buffer start i))
         (let ((j.block (+ j.block 1)))
           (if (= j.block block-length)
               (cons block (pause (loop.single (+ i 1) end buffer 0 (make-vector block-length))))
               (loop.single (+ i 1) end buffer j.block block))))
        (else (loop.inner (+ i 1))))))
  (define (loop.multi len.middle end*.middle buffer*.middle buffer.first start.first end.first j.block block)
    ;(pretty-write `(loop.multi ,len.middle ,end*.middle ,buffer*.middle ,buffer.first ,start.first ,end.first ,j.block ,block))
    (let* ((buffer.current (make-bytes buffer-size))
           (end            (read-bytes! buffer.current in 0 buffer-size)))
      (cond
        ((not (eof-object? end))
         (let loop.first ((i 0))
           (cond
             ((<= end i) (loop.multi (+ len.middle end)
                                     (cons end            end*.middle)
                                     (cons buffer.current buffer*.middle)
                                     buffer.first start.first end.first j.block block))
             ((= (bytes-ref buffer.current i) 10)
              (let* ((len.prev (+ len.middle (- end.first start.first)))
                     (line     (make-bytes (+ len.prev i))))
                (vector-set! block j.block line)
                (bytes-copy! line len.prev buffer.current 0 i)
                (let loop ((pos len.prev) (end* end*.middle) (buffer* buffer*.middle))
                  (when (pair? end*)
                    (let* ((end (car end*))
                           (pos (- pos end)))
                      (bytes-copy! line pos (car buffer*) 0 end)
                      (loop pos (cdr end*) (cdr buffer*)))))
                (bytes-copy! line 0 buffer.first start.first end.first))
              (let ((j.block (+ j.block 1)))
                (if (= j.block block-length)
                    (cons block (pause (loop.single (+ i 1) end buffer.current 0 (make-vector block-length))))
                    (loop.single (+ i 1) end buffer.current j.block block))))
             (else (loop.first (+ i 1))))))
        (else
          (let* ((len  (+ len.middle (- end.first start.first)))
                 (line (make-bytes len)))
            (vector-set! block j.block line)
            (let loop ((pos len) (end* end*.middle) (buffer* buffer*.middle))
              (when (pair? end*)
                (let* ((end (car end*))
                       (pos (- pos end)))
                  (bytes-copy! line pos (car buffer*) 0 end)
                  (loop pos (cdr end*) (cdr buffer*)))))
            (bytes-copy! line 0 buffer.first start.first end.first))
          (list (let ((j.block (+ j.block 1)))
                  (if (= j.block block-length)
                      block
                      (vector-copy block 0 j.block))))))))
  (pause
    (let ((buffer (make-bytes buffer-size)))
      (let ((end (read-bytes! buffer in 0 buffer-size)))
        (if (eof-object? end)
            '()
            (loop.single 0 end buffer 0 (make-vector block-length)))))))

(pretty-write
  (time
    (let loop ((count 0) (b* (stream:line-block* in)))
      (cond ((null?      b*) count)
            ((procedure? b*) (loop count (b*)))
            (else            (loop (+ count (vector-length (car b*))) (cdr b*)))))))

;(set! result (time (s->list (stream:line-block* in))))
;;(time (enable-interrupts))
;(pretty-write (vector-ref (car result) 0))
;(pretty-write
;  (let loop ((count 0) (b* result))
;    (cond ((null? b*) count)
;          (else (loop (+ count (vector-length (car b*))) (cdr b*))))))
;(let ((v (car (reverse result))))
;  (pretty-write (vector-ref v (- (vector-length v) 1))))

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
