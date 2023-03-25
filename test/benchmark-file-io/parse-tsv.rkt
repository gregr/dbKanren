#lang racket/base
(require ffi/unsafe/atomic ffi/unsafe/vm racket/fixnum racket/pretty racket/unsafe/ops)

(define enable-interrupts  (vm-primitive 'enable-interrupts))
(define disable-interrupts (vm-primitive 'disable-interrupts))

;; 37GB
;(define in (open-input-file "rtx-kg2-s3/rtx-kg2_edges_2.8.1.tsv"))
;; 4.8GB
(define in (open-input-file "rtx-kg2-s3/rtx-kg2_nodes_2.8.1.tsv"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; block-encoded tuples ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;(define block-size 1073741824)
;(define block      (time (make-bytes block-size)))

;; TODO

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; naive tuple construction unbuffered ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; ~42 seconds (~25.4 seconds gc) for 4.8GB (~17.2 seconds with disable-interrupts)
(file-stream-buffer-mode in 'none)
(define result #f)
(disable-interrupts)
;(start-atomic)  ; for some reason, this is actively harmful if we also disable interrupts
(pretty-write
  (let ()
    ;(define part-buffer-size 65536)
    ;(define part-buffer-size 32768)
    (define part-buffer-size 16384)
    ;(define part-buffer-size 8192)
    ;(define part-buffer-size 4096)
    ;; Because the largest field is 3089399 bytes, with a part-buffer-size of 16384 we need at
    ;; least 189 parts to fit it.
    ;> (/ 3089399.0 (expt 2 14))
    ;188.56195068359375
    (define part-buffer-count 189)
    ;(define part-buffer-count 4)
    (define full-buffer-size (* part-buffer-size part-buffer-count))
    (time
      (let ((buffer (make-bytes full-buffer-size)))
        (let loop.outer ((prev.buffer 0) (start.buffer 0) (record* '()) (field* '()))
          (let ((size (read-bytes! buffer in start.buffer (unsafe-fx+ start.buffer part-buffer-size))))
            (if (eof-object? size)
                (begin (set! result
                         (let ((field* (if (unsafe-fx= prev.buffer start.buffer) field* (cons (subbytes buffer prev.buffer start.buffer) field*))))
                           (if (null? field*) record* (cons field* record*))))
                       (length result))
                (let ((end.buffer (unsafe-fx+ start.buffer size)))
                  (let loop.inner ((i start.buffer) (start.field prev.buffer) (record* record*) (field* '()))
                    (if (unsafe-fx<= end.buffer i)
                        (if (unsafe-fx<= full-buffer-size (unsafe-fx+ end.buffer part-buffer-size))
                            (begin
                              (bytes-copy! buffer 0 buffer start.field end.buffer)
                              (let* ((start.buffer (unsafe-fx- end.buffer start.field))
                                     (end.buffer   (unsafe-fx+ start.buffer part-buffer-size)))
                                (when (unsafe-fx<= full-buffer-size end.buffer)
                                  (error "buffer requirements are too large" end.buffer))
                                (loop.outer 0 start.buffer record* field*)))
                            (loop.outer start.field end.buffer record* field*))
                        (let ((b (bytes-ref buffer i)))
                          (cond
                            ((unsafe-fx= b 9)  (loop.inner (unsafe-fx+ i 1) (unsafe-fx+ i 1) record* (cons (subbytes buffer start.field i) field*)))
                            ((unsafe-fx= b 10) (loop.inner (unsafe-fx+ i 1) (unsafe-fx+ i 1) (cons (cons (subbytes buffer start.field i) field*) record*) '()))
                            (else              (loop.inner (unsafe-fx+ i 1) start.field      record* field*))))))))))))))
;(end-atomic)
(time (enable-interrupts))  ; ~8.8 seconds of gc
(pretty-write (car result))

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
