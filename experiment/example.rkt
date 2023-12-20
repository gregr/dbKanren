#lang racket/base
(require "data.rkt" racket/fixnum racket/pretty)

(define (go example)
  (let ((name (car example))
        (t*   (cadr example)))
    (displayln "================================================================")
    (displayln name)
    (displayln "================================================================")
    (displayln "uncompressed size:")
    (pretty-write
      (let ((len (vector-length t*)))
        (+ len
           (let loop ((i 0))
             (if (< i len)
                 (+ (bytes-length (vector-ref t* i))
                    (loop (+ i 1)))
                 0)))))
    (define-values (size encode) (encode-text* t*))
    (define bv (make-bytes size))
    (displayln "requested size:")
    (pretty-write size)
    (displayln "encoded size:")
    (pretty-write (encode bv 0))
    (displayln "encoded bytevector:")
    (pretty-write bv)
    (displayln "encoding tag:")
    (pretty-write (fxrshift (bytes-ref bv 0) 4))
    (displayln "byte-width bundled with encoding tag:")
    (pretty-write (fxand (bytes-ref bv 0) #b0111))
    (displayln "encoded byte codes:")
    (write (bytes->list bv))
    (newline)
    (newline)))

(define example*
  '(
    (distinct-values
      #(
        #"ABC" #"DEF" #"GHI" #"JKL" #"MNO" #"PQR" #"STU" #"VWX" #"YZ"
        ))
    (distinct-values-single-length
      #(
        #"ABC" #"DEF" #"GHI" #"JKL" #"MNO" #"PQR" #"STU" #"VWX" #"YZZ"
        ))
    (non-adjacent-duplicate-values
      #(
        #"ABC" #"DEF" #"GHI" #"JKL" #"MNO" #"PQR" #"STU" #"VWX" #"YZ"
        #"ABC" #"DEF" #"GHI" #"JKL" #"MNO" #"PQR" #"STU" #"VWX" #"YZ"
        #"ABC" #"DEF" #"GHI" #"JKL" #"MNO" #"PQR" #"STU" #"VWX" #"YZ"
        #"ABC" #"DEF" #"GHI" #"JKL" #"MNO" #"PQR" #"STU" #"VWX" #"YZ"
        #"ABC" #"DEF" #"GHI" #"JKL" #"MNO" #"PQR" #"STU" #"VWX" #"YZ"
        #"ABC" #"DEF" #"GHI" #"JKL" #"MNO" #"PQR" #"STU" #"VWX" #"YZ"
        #"ABC" #"DEF" #"GHI" #"JKL" #"MNO" #"PQR" #"STU" #"VWX" #"YZ"
        ))
    (non-adjacent-duplicate-values-single-length
      #(
        #"ABC" #"DEF" #"GHI" #"JKL" #"MNO" #"PQR" #"STU" #"VWX" #"YZZ"
        #"ABC" #"DEF" #"GHI" #"JKL" #"MNO" #"PQR" #"STU" #"VWX" #"YZZ"
        #"ABC" #"DEF" #"GHI" #"JKL" #"MNO" #"PQR" #"STU" #"VWX" #"YZZ"
        #"ABC" #"DEF" #"GHI" #"JKL" #"MNO" #"PQR" #"STU" #"VWX" #"YZZ"
        #"ABC" #"DEF" #"GHI" #"JKL" #"MNO" #"PQR" #"STU" #"VWX" #"YZZ"
        #"ABC" #"DEF" #"GHI" #"JKL" #"MNO" #"PQR" #"STU" #"VWX" #"YZZ"
        #"ABC" #"DEF" #"GHI" #"JKL" #"MNO" #"PQR" #"STU" #"VWX" #"YZZ"
        ))
    (consecutively-repeated-values
      #(
        #"ABC" #"ABC" #"ABC" #"ABC" #"ABC"
        #"DEF" #"DEF" #"DEF" #"DEF" #"DEF" #"DEF" #"DEF"
        #"GHI" #"GHI" #"GHI" #"GHI" #"GHI" #"GHI"
        #"JKL" #"JKL" #"JKL" #"JKL"
        #"MNO" #"MNO" #"MNO" #"MNO" #"MNO"
        #"PQR" #"PQR" #"PQR" #"PQR" #"PQR" #"PQR" #"PQR"
        #"STU" #"STU" #"STU"
        #"VWX" #"VWX" #"VWX" #"VWX" #"VWX" #"VWX" #"VWX"
        #"YZ"  #"YZ"  #"YZ"  #"YZ"  #"YZ"
        ))
    (consecutively-repeated-values-single-length
      #(
        #"ABC" #"ABC" #"ABC" #"ABC" #"ABC"
        #"DEF" #"DEF" #"DEF" #"DEF" #"DEF" #"DEF" #"DEF"
        #"GHI" #"GHI" #"GHI" #"GHI" #"GHI" #"GHI"
        #"JKL" #"JKL" #"JKL" #"JKL"
        #"MNO" #"MNO" #"MNO" #"MNO" #"MNO"
        #"PQR" #"PQR" #"PQR" #"PQR" #"PQR" #"PQR" #"PQR"
        #"STU" #"STU" #"STU"
        #"VWX" #"VWX" #"VWX" #"VWX" #"VWX" #"VWX" #"VWX"
        #"YZZ" #"YZZ" #"YZZ" #"YZZ" #"YZZ"
        ))
    (consecutively-repeated-values-same-number
      #(
        #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC"
        #"DEF" #"DEF" #"DEF" #"DEF" #"DEF" #"DEF" #"DEF"
        #"GHI" #"GHI" #"GHI" #"GHI" #"GHI" #"GHI" #"GHI"
        #"JKL" #"JKL" #"JKL" #"JKL" #"JKL" #"JKL" #"JKL"
        #"MNO" #"MNO" #"MNO" #"MNO" #"MNO" #"MNO" #"MNO"
        #"PQR" #"PQR" #"PQR" #"PQR" #"PQR" #"PQR" #"PQR"
        #"STU" #"STU" #"STU" #"STU" #"STU" #"STU" #"STU"
        #"VWX" #"VWX" #"VWX" #"VWX" #"VWX" #"VWX" #"VWX"
        #"YZ"  #"YZ"  #"YZ"  #"YZ"  #"YZ"  #"YZ"  #"YZ"
        ))
    (consecutively-repeated-values-same-number-single-length
      #(
        #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC"
        #"DEF" #"DEF" #"DEF" #"DEF" #"DEF" #"DEF" #"DEF"
        #"GHI" #"GHI" #"GHI" #"GHI" #"GHI" #"GHI" #"GHI"
        #"JKL" #"JKL" #"JKL" #"JKL" #"JKL" #"JKL" #"JKL"
        #"MNO" #"MNO" #"MNO" #"MNO" #"MNO" #"MNO" #"MNO"
        #"PQR" #"PQR" #"PQR" #"PQR" #"PQR" #"PQR" #"PQR"
        #"STU" #"STU" #"STU" #"STU" #"STU" #"STU" #"STU"
        #"VWX" #"VWX" #"VWX" #"VWX" #"VWX" #"VWX" #"VWX"
        #"YZZ" #"YZZ" #"YZZ" #"YZZ" #"YZZ" #"YZZ" #"YZZ"
        ))
    (consecutively-repeated-single-value
      #(
        #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC"
        #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC"
        #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC"
        #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC"
        #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC"
        #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC"
        #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC"
        #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC"
        #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC" #"ABC"
        ))
    ))

(for-each go example*)
