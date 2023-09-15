#lang racket/base
(require ffi/unsafe/vm racket/fixnum racket/pretty)
(require
  ;"../../dbk/safe-unsafe.rkt"
  racket/unsafe/ops
  )

;;; Prefix compression relative to previous value

(define enable-interrupts  (vm-primitive 'enable-interrupts))
(define disable-interrupts (vm-primitive 'disable-interrupts))

(define bytevector-u32-native-ref  (vm-primitive 'bytevector-u32-native-ref))
(define bytevector-u32-native-set! (vm-primitive 'bytevector-u32-native-set!))
(define bytevector-u64-native-ref  (vm-primitive 'bytevector-u64-native-ref))
(define bytevector-u64-native-set! (vm-primitive 'bytevector-u64-native-set!))

(define prefix      #"SHARE:")
(define digit-count 7)
(define len.prefix (bytes-length prefix))
(define len        (+ len.prefix digit-count))

(define count  10000000)
(define size   (* len count))
(define input  (make-bytes size))
(define output (make-bytes size))

(define length*               (make-fxvector count len))
(define shared-prefix-length* (make-fxvector count))

(define ch.zero (char->integer #\0))

(define (generate-input)
  (time (let loop ((i 0) (pos 0) (previous (make-bytes len 0)))
          (when (unsafe-fx< i count)
            (let ((next (make-bytes len ch.zero)))
              (unsafe-bytes-copy! next 0 prefix)
              (let loop ((i i) (j (unsafe-fx- len 1)))
                (let-values (((q r) (quotient/remainder i 10)))
                  (unsafe-bytes-set! next j (unsafe-fx+ ch.zero r))
                  (when (unsafe-fx< 0 q) (loop q (unsafe-fx- j 1)))))
              (let ((shared-prefix-length
                      (let loop ((i 0))
                        (cond ((unsafe-fx= len i) len)
                              ((eq? (unsafe-bytes-ref previous i)
                                    (unsafe-bytes-ref next     i))
                               (loop (unsafe-fx+ i 1)))
                              (else i)))))
                (unsafe-fxvector-set! shared-prefix-length* i shared-prefix-length)
                (unsafe-bytes-copy! input pos next shared-prefix-length len)
                (loop (unsafe-fx+ i 1)
                      (unsafe-fx+ pos (unsafe-fx- len shared-prefix-length))
                      next)))))))

(define (decode-input)
  (time (let loop ((i 0) (start.in 0) (start.out 0) (prev.out 0))
          (when (unsafe-fx< i count)
            (let* ((shared-prefix-length (unsafe-fxvector-ref shared-prefix-length* i))
                   (end.in (unsafe-fx+ start.in (unsafe-fx- len shared-prefix-length))))
              (unsafe-bytes-copy! output start.out output prev.out
                                  (unsafe-fx+ prev.out shared-prefix-length))
              (unsafe-bytes-copy! output (unsafe-fx+ start.out shared-prefix-length) input start.in
                                  end.in)
              (loop (unsafe-fx+ i 1) end.in (unsafe-fx+ start.out len) start.out))))))

(define (pretend-decode-input)
  (time (let loop ((i 0) (start.in 0) (start.out 0) (prev.out 0))
          (when (unsafe-fx< i count)
            (let* ((shared-prefix-length (unsafe-fxvector-ref shared-prefix-length* i))
                   (end.in (unsafe-fx+ start.in (unsafe-fx- len shared-prefix-length))))
              ;(unsafe-bytes-copy! output start.out output prev.out
                                  ;(unsafe-fx+ prev.out shared-prefix-length))
              ;(unsafe-bytes-copy! output (unsafe-fx+ start.out shared-prefix-length) input start.in
                                  ;end.in)
              (loop (unsafe-fx+ i 1) end.in (unsafe-fx+ start.out len) start.out))))))

(define enumerated   (make-vector count))
(define fxenumerated (make-fxvector count))
(define one          (box 0))

(define (enumerate-output)
  (time (let loop ((i 0) (start.out 0) (acc 0))
          (if (unsafe-fx< i count)
              (let ((v       (make-bytes len))
                    (end.out (unsafe-fx+ start.out len)))
                (unsafe-bytes-copy! v 0 output start.out end.out)

                ;; cpu time: 146 real time: 149 gc time: 1
                ;(set-box! one v)

                ;; cpu time: 921 real time: 929 gc time: 759
                ;; or interrupts disabled:
                ;;   cpu time: 283 real time: 286 gc time: 0
                ;;   #"SHARE:9999999"
                ;;   cpu time: 370 real time: 377 gc time: 370
                ;(unsafe-vector*-set! enumerated i v)

                ;; cpu time: 126 real time: 129 gc time: 1
                ;; or interrupts disabled:
                ;;   cpu time: 248 real time: 254 gc time: 0
                ;;   #"SHARE:9999999"
                ;;   cpu time: 2 real time: 2 gc time: 1
                (unsafe-fxvector-set! fxenumerated i
                                      (unsafe-fx+ (unsafe-bytes-ref v 0)
                                                  (unsafe-bytes-ref v (unsafe-fx- len 1))))

                (loop (unsafe-fx+ i 1) end.out v))
              acc))))

;; cpu time: 4157 real time: 4168 gc time: 426
(generate-input)

;; cpu time: 23 real time: 23 gc time: 0
(time (unsafe-bytes-copy! output 0 input))
;; cpu time: 281 real time: 288 gc time: 0
(time (let loop ((i 0))
        (when (unsafe-fx< i size)
          (unsafe-bytes-set! output i (unsafe-bytes-ref input i))
          (loop (unsafe-fx+ i 1)))))
;; cpu time: 278 real time: 280 gc time: 0
(time (let loop ((i 0))
        (when (unsafe-fx< i size)
          (bytevector-u32-native-set! output i (bytevector-u32-native-ref input i))
          (loop (unsafe-fx+ i 4)))))
;; cpu time: 180 real time: 182 gc time: 0
(time (let loop ((i 0))
        (when (unsafe-fx< i size)
          (bytevector-u64-native-set! output i (bytevector-u64-native-ref input i))
          (loop (unsafe-fx+ i 8)))))

;; cpu time: 20 real time: 20 gc time: 0
(pretend-decode-input)

;; cpu time: 237 real time: 242 gc time: 0
(decode-input)

(pretty-write (subbytes input 0 100))
(pretty-write (subbytes input 10000 10100))
(pretty-write (subbytes output 0 100))
(pretty-write (subbytes output (unsafe-fx- size 100) size))

;(disable-interrupts)
(enumerate-output)
;(time (enable-interrupts))
