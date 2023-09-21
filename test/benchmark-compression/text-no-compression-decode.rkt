#lang racket/base
(require ffi/unsafe/vm racket/list racket/pretty)
(require
  ;"../../dbk/safe-unsafe.rkt"
  racket/unsafe/ops
  )

(define enable-interrupts  (vm-primitive 'enable-interrupts))
(define disable-interrupts (vm-primitive 'disable-interrupts))

(define prefix      #"SHARE:")
(define digit-count 7)
(define len.prefix  (bytes-length prefix))
(define len         (+ len.prefix digit-count))

(define count           10000000)
(define size            (* len count))
(define input           (make-bytes size))
(define output.baseline (make-bytes size))
(define output          (make-vector count))

(define ch.zero (char->integer #\0))

(define (generate-input)
  (time (let loop ((i 0) (pos 0))
          (when (unsafe-fx< i count)
            (let ((next (make-bytes len ch.zero)))
              (unsafe-bytes-copy! next 0 prefix)
              (let loop ((i i) (j (unsafe-fx- len 1)))
                (let-values (((q r) (quotient/remainder i 10)))
                  (unsafe-bytes-set! next j (unsafe-fx+ ch.zero r))
                  (when (unsafe-fx< 0 q) (loop q (unsafe-fx- j 1)))))
              (unsafe-bytes-copy! input pos next 0 len)
              (loop (unsafe-fx+ i 1) (unsafe-fx+ pos len)))))))

(define (decode-input-baseline)
  (time (let loop ((i 0) (pos 0))
          (when (unsafe-fx< i count)
            (let ((pos.next (unsafe-fx+ pos len)))
              (unsafe-bytes-copy! output.baseline pos input pos pos.next)
              (loop (unsafe-fx+ i 1) pos.next))))))

(define (decode-input)
  (time (let loop ((i 0) (pos 0))
          (when (unsafe-fx< i count)
            (let ((t        (make-bytes len))
                  (pos.next (unsafe-fx+ pos len)))
              (unsafe-bytes-copy! t 0 input pos pos.next)
              (unsafe-vector*-set! output i t)
              (loop (unsafe-fx+ i 1) pos.next))))))

;; cpu time: 4037 real time: 4053 gc time: 499
(generate-input)

;; cpu time: 108 real time: 112 gc time: 0
(decode-input-baseline)

;; cpu time: 850 real time: 857 gc time: 690
;; or interrupts disabled:
;;   cpu time: 330 real time: 333 gc time: 0
;;   cpu time: 371 real time: 374 gc time: 371
;(disable-interrupts)
(decode-input)
;(time (enable-interrupts))

(pretty-write (subbytes input 0 100))
(pretty-write (subbytes input 10000 10100))
(pretty-write (subbytes output.baseline 0 100))
(pretty-write (subbytes output.baseline (unsafe-fx- size 100) size))
(pretty-write (map (lambda (i) (vector-ref output i)) (range 10)))
(pretty-write (map (lambda (i) (vector-ref output (- (vector-length output) (+ 1 i)))) (reverse (range 10))))
