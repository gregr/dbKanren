;; 37GB
;(define in (open-file-input-port "rtx-kg2-s3/rtx-kg2_edges_2.8.1.tsv" (file-options) 'none))
;; 4.8GB
(define in (open-file-input-port "rtx-kg2-s3/rtx-kg2_nodes_2.8.1.tsv" (file-options) 'none))

;(#%$assembly-output #t)
(optimize-level 3)

(let ()
  ;(define buffer-size 65536)
  ;(define buffer-size 32768)
  (define buffer-size 16384)
  ;(define buffer-size 8192)
  ;(define buffer-size 4096)
  (write
    (time (let ((buffer (make-bytevector buffer-size)))
            (let loop.outer ((count 0))
              (let ((size (get-bytevector-some! in buffer 0 buffer-size)))
                (if (eof-object? size)
                    count
                    (let loop.inner ((i 0) (count count))
                      (if (fx= i size)
                          (loop.outer count)
                          (let ((b (bytevector-u8-ref buffer i)))
                            (loop.inner (fx+ i 1) (fx+ count (fx- b 10)))))
                      ))))))))

;(let ()
;  ;(define buffer-size 65536)
;  ;(define buffer-size 32768)
;  (define buffer-size 16384)
;  ;(define buffer-size 8192)
;  ;(define buffer-size 4096)
;  (write
;    (time (let ((buffer (make-bytevector buffer-size)))
;            (let loop.outer ((count 0))
;              (let ((size (get-bytevector-some! in buffer 0 buffer-size)))
;                (if (eof-object? size)
;                    count
;                    (let loop.inner ((i 0) (count count))
;                      (if (fx= i size)
;                          (loop.outer count)
;                          (let ((b (bytevector-u8-ref buffer i)))
;                            (loop.inner (fx+ i 1) (if (fx= b 10) (fx+ count 1) count))))
;                      ))))))))

;(let ()
;  ;(define buffer-size 65536)
;  ;(define buffer-size 32768)
;  (define buffer-size 16384)
;  ;(define buffer-size 8192)
;  ;(define buffer-size 4096)
;  (write
;    (time (let ((buffer (make-bytevector buffer-size)))
;            (let loop.outer ((count.outer 0))
;              (let ((size (get-bytevector-some! in buffer 0 buffer-size)))
;                (if (eof-object? size)
;                    count.outer
;                    (let loop.inner ((i 0) (count 0))
;                      (if (fx= i size)
;                          (loop.outer (fx+ count count.outer))
;                          (let ((b (bytevector-u8-ref buffer i)))
;                            (loop.inner (fx+ i 1) (if (fx= b 10) (fx+ count 1) count))))
;                      ))))))))
