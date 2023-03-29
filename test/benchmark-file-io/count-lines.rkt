#lang racket/base
(require ffi/unsafe/atomic racket/fixnum racket/unsafe/ops)

;; 37GB for 56965146 lines
;(define in (open-input-file "rtx-kg2-s3/rtx-kg2_edges_2.8.1.tsv"))
;; 4.8GB for 11342763 lines
;(define in (open-input-file "rtx-kg2-s3/rtx-kg2_nodes_2.8.1.tsv"))
;; 40GB for 600183480 lines
(define in (open-input-file "rtx_kg2_20210204.edgeprop.tsv"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; 4x unrolled read-bytes! unbuffered futures ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; ~5.5 seconds for 4.8GB (and overcounts at the end of the file)
;(require racket/future)
;(file-stream-buffer-mode in 'none)
;;(start-atomic)  ; does not seem to help
;(let ()
;  ;(define buffer-size 1048576)
;  (define buffer-size 524288)  ; ~5.5 seconds for 4.8GB
;  ;(define buffer-size 262144)
;  ;(define buffer-size 131072)
;  ;(define buffer-size 65536)
;  ;(define buffer-size 32768)
;  ;(define buffer-size 16384)
;  ;(define buffer-size 8192)
;  ;(define buffer-size 4096)
;  (define buffer-size.1/4 (/ buffer-size 4))
;  (define buffer-size.2/4 (+ buffer-size.1/4 buffer-size.1/4))
;  (define buffer-size.3/4 (+ buffer-size.2/4 buffer-size.1/4))
;  (time (let ((buffer (make-bytes buffer-size)))
;          (let loop.outer ((count 0))
;            (let ((size (read-bytes! buffer in 0 buffer-size)))
;              (if (eof-object? size)
;                  count
;                  (let ((go (lambda (start end)
;                              (touch
;                                (future
;                                  (lambda ()
;                                    (let loop.inner ((i start) (count 0))
;                                      (if (unsafe-fx<= end i)
;                                          count
;                                          (loop.inner
;                                            (unsafe-fx+ i 4)
;                                            ;(unsafe-fx+ i 8)
;                                            (unsafe-fx+
;                                              count
;                                              (if (unsafe-fx= 10 (unsafe-bytes-ref buffer i))                1 0)
;                                              (if (unsafe-fx= 10 (unsafe-bytes-ref buffer (unsafe-fx+ i 1))) 1 0)
;                                              (if (unsafe-fx= 10 (unsafe-bytes-ref buffer (unsafe-fx+ i 2))) 1 0)
;                                              (if (unsafe-fx= 10 (unsafe-bytes-ref buffer (unsafe-fx+ i 3))) 1 0)
;                                              ;(if (unsafe-fx= 10 (unsafe-bytes-ref buffer (unsafe-fx+ i 4))) 1 0)
;                                              ;(if (unsafe-fx= 10 (unsafe-bytes-ref buffer (unsafe-fx+ i 5))) 1 0)
;                                              ;(if (unsafe-fx= 10 (unsafe-bytes-ref buffer (unsafe-fx+ i 6))) 1 0)
;                                              ;(if (unsafe-fx= 10 (unsafe-bytes-ref buffer (unsafe-fx+ i 7))) 1 0)
;                                              ))))))))))
;                    (loop.outer
;                      (unsafe-fx+ count
;                                  (go 0               buffer-size.1/4)
;                                  (go buffer-size.1/4 buffer-size.2/4)
;                                  (go buffer-size.2/4 buffer-size.3/4)
;                                  (go buffer-size.3/4 buffer-size))))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; read-bytes! unbuffered futures ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; ~6.3 seconds for 4.8GB (and overcounts at the end of the file)
;(require racket/future)
;(file-stream-buffer-mode in 'none)
;(let ()
;  ;(define buffer-size 1048576)  ; ~6.4 seconds for 4.8GB
;  (define buffer-size 524288)  ; ~6.3 seconds for 4.8GB
;  ;(define buffer-size 262144)  ; ~6.7 seconds for 4.8GB
;  ;(define buffer-size 131072)  ; ~6.7 seconds for 4.8GB
;  ;(define buffer-size 65536)  ; ~6.9 seconds for 4.8GB
;  ;(define buffer-size 32768)  ; ~9 seconds for 4.8GB
;  ;(define buffer-size 16384)  ; ~11 seconds for 4.8GB
;  ;(define buffer-size 8192)  ; ~15.6 seconds for 4.8GB
;  ;(define buffer-size 4096)  ; ~50 seconds for 4.8GB !
;  (define buffer-size.1/4 (/ buffer-size 4))
;  (define buffer-size.2/4 (+ buffer-size.1/4 buffer-size.1/4))
;  (define buffer-size.3/4 (+ buffer-size.2/4 buffer-size.1/4))
;  (time (let ((buffer (make-bytes buffer-size)))
;          (let loop.outer ((count 0))
;            (let ((size (read-bytes! buffer in 0 buffer-size)))
;              (if (eof-object? size)
;                  count
;                  (let ((go (lambda (start end)
;                              (touch
;                                (future
;                                  (lambda ()
;                                    (let loop.inner ((i start) (count 0))
;                                      (if (unsafe-fx= i end)
;                                          count
;                                          (let ((b (unsafe-bytes-ref buffer i)))
;                                            (loop.inner (unsafe-fx+ i 1) (if (unsafe-fx= b 10) (unsafe-fx+ count 1) count)))))))))))
;                    (loop.outer
;                      (unsafe-fx+ count
;                                  (go 0               buffer-size.1/4)
;                                  (go buffer-size.1/4 buffer-size.2/4)
;                                  (go buffer-size.2/4 buffer-size.3/4)
;                                  (go buffer-size.3/4 buffer-size))))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; 4x unrolled read-bytes! unbuffered ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; ~3.2 seconds for 4.8GB (~3 seconds in atomic mode)
;; NOTE: using a byte vector for the dispatch table is ~33% slower
(file-stream-buffer-mode in 'none)
;(start-atomic)
(let ()
  ;(define buffer-size 65536)
  ;(define buffer-size 32768)
  (define buffer-size 16384)
  ;(define buffer-size 8192)
  ;(define buffer-size 4096)
  (time (let ((dispatch (make-fxvector 256 0))
              (buffer (make-bytes buffer-size)))
          (fxvector-set! dispatch 10 1)
          (let loop.outer ((count 0))
            (let ((size (read-bytes! buffer in 0 buffer-size)))
              (if (eof-object? size)
                  count
                  (let loop.inner ((i 0) (count count))
                    (if (unsafe-fx<= size i)
                        (loop.outer count)
                        (loop.inner
                          (unsafe-fx+ i 4)
                          (unsafe-fx+ count
                                      (unsafe-fxvector-ref dispatch (unsafe-bytes-ref buffer i))
                                      (unsafe-fxvector-ref dispatch (unsafe-bytes-ref buffer (unsafe-fx+ i 1)))
                                      (unsafe-fxvector-ref dispatch (unsafe-bytes-ref buffer (unsafe-fx+ i 2)))
                                      (unsafe-fxvector-ref dispatch (unsafe-bytes-ref buffer (unsafe-fx+ i 3)))))))))))))
;(end-atomic)

;; ~6.3 seconds for 4.8GB (~5 seconds in atomic mode)
;(file-stream-buffer-mode in 'none)
;(start-atomic)
;(let ()
;  ;(define buffer-size 65536)
;  ;(define buffer-size 32768)
;  (define buffer-size 16384)
;  ;(define buffer-size 8192)
;  ;(define buffer-size 4096)
;  (time (let ((buffer (make-bytes buffer-size)))
;          (let loop.outer ((count 0))
;            (let ((size (read-bytes! buffer in 0 buffer-size)))
;              (if (eof-object? size)
;                  count
;                  (let loop.inner ((i 0) (count count))
;                    (if (unsafe-fx<= size i)
;                        (loop.outer count)
;                        (loop.inner
;                          (unsafe-fx+ i 4)
;                          ;(unsafe-fx+ i 8)
;                          (unsafe-fx+
;                            count
;                            (if (unsafe-fx= 10 (unsafe-bytes-ref buffer i))                1 0)
;                            (if (unsafe-fx= 10 (unsafe-bytes-ref buffer (unsafe-fx+ i 1))) 1 0)
;                            (if (unsafe-fx= 10 (unsafe-bytes-ref buffer (unsafe-fx+ i 2))) 1 0)
;                            (if (unsafe-fx= 10 (unsafe-bytes-ref buffer (unsafe-fx+ i 3))) 1 0)
;                            ;(if (unsafe-fx= 10 (unsafe-bytes-ref buffer (unsafe-fx+ i 4))) 1 0)
;                            ;(if (unsafe-fx= 10 (unsafe-bytes-ref buffer (unsafe-fx+ i 5))) 1 0)
;                            ;(if (unsafe-fx= 10 (unsafe-bytes-ref buffer (unsafe-fx+ i 6))) 1 0)
;                            ;(if (unsafe-fx= 10 (unsafe-bytes-ref buffer (unsafe-fx+ i 7))) 1 0)
;                            ))))))))))
;;(end-atomic)

; ~2.9 seconds for 4.8GB
;(file-stream-buffer-mode in 'none)
;(start-atomic)
;(let ()
;  ;(define buffer-size 65536)
;  ;(define buffer-size 32768)
;  (define buffer-size 16384)
;  ;(define buffer-size 8192)
;  ;(define buffer-size 4096)
;  (time (let ((buffer (make-bytes buffer-size)))
;          (let loop.outer ((count 0))
;            (let ((size (read-bytes! buffer in 0 buffer-size)))
;              (if (eof-object? size)
;                  count
;                  (let loop.inner ((i 0) (count count))
;                    (if (unsafe-fx<= size i)
;                        (loop.outer count)
;                        (loop.inner
;                          (unsafe-fx+ i 4)
;                          (unsafe-fx+
;                            count
;                            (unsafe-fx- (unsafe-bytes-ref buffer i)                10)
;                            (unsafe-fx- (unsafe-bytes-ref buffer (unsafe-fx+ i 1)) 10)
;                            (unsafe-fx- (unsafe-bytes-ref buffer (unsafe-fx+ i 2)) 10)
;                            (unsafe-fx- (unsafe-bytes-ref buffer (unsafe-fx+ i 3)) 10)
;                            ))))))))))
;;(end-atomic)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; read-bytes! unbuffered ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; ~7.2 seconds for 4.8GB (~6 seconds in atomic mode)
;(file-stream-buffer-mode in 'none)
;;(start-atomic)
;(let ()
;  ;(define buffer-size 65536)  ; ~8.3 seconds for 4.8GB
;  ;(define buffer-size 32768)  ; ~8.3 seconds for 4.8GB
;  (define buffer-size 16384)  ; ~7.6 seconds for 4.8GB
;  ;(define buffer-size 8192)  ; ~7.6 seconds for 4.8GB
;  ;(define buffer-size 4096)  ; ~8 seconds for 4.8GB
;  (time (let ((dispatch (make-fxvector 256 0))
;              (buffer (make-bytes buffer-size)))
;          (fxvector-set! dispatch 10 1)
;          (let loop.outer ((count 0))
;            (let ((size (read-bytes! buffer in 0 buffer-size)))
;              (if (eof-object? size)
;                  count
;                  (let loop.inner ((i 0) (count count))
;                    (if (unsafe-fx= i size)
;                        (loop.outer count)
;                        (loop.inner (unsafe-fx+ i 1)
;                                    (unsafe-fx+ count (unsafe-fxvector-ref dispatch (unsafe-bytes-ref buffer i))))))))))))
;;(end-atomic)

;; ~7.6 seconds for 4.8GB (~6.5 seconds in atomic mode)
;(file-stream-buffer-mode in 'none)
;(start-atomic)
;(let ()
;  ;(define buffer-size 65536)  ; ~8.3 seconds for 4.8GB
;  ;(define buffer-size 32768)  ; ~8.3 seconds for 4.8GB
;  (define buffer-size 16384)  ; ~7.6 seconds for 4.8GB
;  ;(define buffer-size 8192)  ; ~7.6 seconds for 4.8GB
;  ;(define buffer-size 4096)  ; ~8 seconds for 4.8GB
;  (time (let ((buffer (make-bytes buffer-size)))
;          (let loop.outer ((count 0))
;            (let ((size (read-bytes! buffer in 0 buffer-size)))
;              (if (eof-object? size)
;                  count
;                  (let loop.inner ((i 0) (count count))
;                    (if (unsafe-fx= i size)
;                        (loop.outer count)
;                        (let ((b (unsafe-bytes-ref buffer i)))
;                          (loop.inner (unsafe-fx+ i 1) (if (unsafe-fx= b 10) (unsafe-fx+ count 1) count)))))))))))
;;(end-atomic)

;(file-stream-buffer-mode in 'none)
;(start-atomic)
;(let ()
;  ;(define buffer-size 65536)  ; ~8.3 seconds for 4.8GB
;  ;(define buffer-size 32768)  ; ~8.3 seconds for 4.8GB
;  (define buffer-size 16384)  ; ~7.6 seconds for 4.8GB
;  ;(define buffer-size 8192)  ; ~7.6 seconds for 4.8GB
;  ;(define buffer-size 4096)  ; ~8 seconds for 4.8GB
;  (time (let ((buffer (make-bytes buffer-size)))
;          (let loop.outer ((count 0))
;            (let ((size (read-bytes! buffer in 0 buffer-size)))
;              (if (eof-object? size)
;                  count
;                  (let loop.inner ((i 0) (count count))
;                    (if (unsafe-fx= i size)
;                        (loop.outer count)
;                        (let ((b (unsafe-bytes-ref buffer i)))
;                          (loop.inner (unsafe-fx+ i 1) (unsafe-fx+ count (unsafe-fx- b 10))))))))))))
;;(end-atomic)

;(file-stream-buffer-mode in 'none)
;(let ()
;  ;(define buffer-size 65536)  ; ~8.3 seconds for 4.8GB
;  ;(define buffer-size 32768)  ; ~8.3 seconds for 4.8GB
;  (define buffer-size 16384)  ; ~7.6 seconds for 4.8GB
;  ;(define buffer-size 8192)  ; ~7.6 seconds for 4.8GB
;  ;(define buffer-size 4096)  ; ~8 seconds for 4.8GB
;  (time (let ((buffer (make-bytes buffer-size)))
;          (let loop.outer ((count 0))
;            (let ((size (read-bytes! buffer in 0 buffer-size)))
;              (if (eof-object? size)
;                  count
;                  (let loop.inner ((i 0) (count count))
;                    (if (eq? i size)
;                        (loop.outer count)
;                        (let ((b (bytes-ref buffer i)))
;                          (loop.inner (+ i 1) (if (eq? b 10) (+ count 1) count))))
;                    )))))))

;(file-stream-buffer-mode in 'none)
;(let ()
;  ;(define buffer-size 65536)  ; ~8.3 seconds for 4.8GB
;  ;(define buffer-size 32768)  ; ~8.3 seconds for 4.8GB
;  (define buffer-size 16384)  ; ~7.6 seconds for 4.8GB
;  ;(define buffer-size 8192)  ; ~7.6 seconds for 4.8GB
;  ;(define buffer-size 4096)  ; ~8 seconds for 4.8GB
;  (time (let ((buffer (make-bytes buffer-size)))
;          (let loop.outer ((count.outer 0))
;            (let ((size (read-bytes! buffer in 0 buffer-size)))
;              (if (eof-object? size)
;                  count.outer
;                  (let loop.inner ((i 0) (count 0))
;                    (if (eq? i size)
;                        (loop.outer (+ count count.outer))
;                        (let ((b (bytes-ref buffer i)))
;                          (loop.inner (+ i 1) (if (eq? b 10) (+ count 1) count))))
;                    )))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; single pass read-bytes! unbuffered ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; ~7.6 seconds for 4.8GB (~6.5 seconds in atomic mode)
;(file-stream-buffer-mode in 'none)
;;(start-atomic)
;(let ()
;  (define buffer-size 16777216)  ; ~8.3 seconds for 4.8GB
;  ;(define buffer-size 65536)  ; ~8.3 seconds for 4.8GB
;  ;(define buffer-size 32768)  ; ~8.3 seconds for 4.8GB
;  ;(define buffer-size 16384)  ; ~7.6 seconds for 4.8GB
;  ;(define buffer-size 8192)  ; ~7.6 seconds for 4.8GB
;  ;(define buffer-size 4096)  ; ~8 seconds for 4.8GB
;  (let ((buffer (make-bytes buffer-size)))
;      (let ((size (read-bytes! buffer in 0 buffer-size)))
;        (if (eof-object? size)
;            0
;            (time (let loop.inner ((i 0) (count 0))
;              (if (unsafe-fx= i size)
;                  count
;                  (let ((b (unsafe-bytes-ref buffer i)))
;                    (loop.inner (unsafe-fx+ i 1) (if (unsafe-fx= b 10) (unsafe-fx+ count 1) count))))))))))
;;(end-atomic)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; read-bytes unbuffered ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; 7.6 seconds for 4.8GB
;(file-stream-buffer-mode in 'none)
;(let ()
;  ;(define buffer-size 65536)  ; 7.6 seconds for 4.8GB
;  ;(define buffer-size 32768)  ; 8 seconds for 4.8GB
;  (define buffer-size 16384)  ; ~7.6 seconds for 4.8GB
;  ;(define buffer-size 8192)  ; ~8 seconds for 4.8GB
;  ;(define buffer-size 4096)  ; ~8.5 seconds for 4.8GB
;  ;(define buffer-size 2048)  ; ~9.7 seconds for 4.8GB
;  ;(define buffer-size 1024)  ; ~10.8 seconds for 4.8GB
;  ;(define buffer-size 512)  ; ~14 seconds for 4.8GB
;  (time (let loop.outer ((count 0))
;          (let ((buffer (read-bytes buffer-size in)))
;            (if (eof-object? buffer)
;                count
;                (let ((size   (bytes-length buffer)))
;                  (let loop.inner ((i 0) (count count))
;                    (if (unsafe-fx= i size)
;                        (loop.outer count)
;                        (let ((b (unsafe-bytes-ref buffer i)))
;                          (loop.inner (unsafe-fx+ i 1) (if (unsafe-fx= b 10) (unsafe-fx+ count 1) count)))))))))))

;;;;;;;;;;;;;;;;;
;;; read-line ;;;
;;;;;;;;;;;;;;;;;

;; ~44 seconds for 4.8GB
;(time (let loop ((i 0))
;        ;(let ((x (read-line in 'any)))
;        (let ((x (read-line in)))
;          (if (eof-object? x)
;              i
;              (loop (unsafe-fx+ i 1))))))
