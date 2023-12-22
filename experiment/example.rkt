#lang racket/base
(require "data.rkt" racket/fixnum racket/pretty)

(define (go.text example)
  (let ((name (car example))
        (t*   (cadr example)))
    (displayln "================================================================")
    (display "TEXT: ")
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
    (define-values (size encode)
      (let* ((len     (vector-length t*))
             (t*.copy (make-vector len)))
        (let loop ((i 0))
          (when (< i len)
            (vector-set! t*.copy i (vector-ref t* i))
            (loop (+ i 1))))
        (encode-text* t*.copy)))
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
    (define col (column:encoding.text bv 0 (vector-length t*)))
    (define t*.decoded ((column-slice col) 0 (column-count col)))
    (unless (equal? t* t*.decoded)
      (pretty-write `((before: ,t*) (after: ,t*.decoded)))
      (displayln "FAIL!"))
    (newline)))

(define (go.int example)
  (let ((name (car example))
        (z*   (cadr example)))
    (displayln "================================================================")
    (display "INTEGER: ")
    (displayln name)
    (displayln "================================================================")
    (displayln "uncompressed size:")
    (pretty-write (* 4 (fxvector-length z*)))
    (define-values (size encode)
      (let* ((len (fxvector-length z*))
             (z*.copy (make-fxvector len)))
        (let loop ((i 0))
          (when (< i len)
            (fxvector-set! z*.copy i (fxvector-ref z* i))
            (loop (+ i 1))))
        (encode-int* z*.copy 0 len)))
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
    (define col (column:encoding.int bv 0 (fxvector-length z*)))
    (define z*.decoded (let* ((vec.z*.decoded ((column-slice col) 0 (column-count col)))
                              (len            (vector-length vec.z*.decoded))
                              (z*.decoded     (make-fxvector len)))
                         (let loop ((i 0))
                           (when (< i len)
                             (fxvector-set! z*.decoded i (vector-ref vec.z*.decoded i))
                             (loop (+ i 1))))
                         z*.decoded))
    (unless (equal? z* z*.decoded)
      (newline)
      (pretty-write `((before: ,z*) (after: ,z*.decoded)))
      (displayln "FAIL!"))
    (newline)))

(define example*.text
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

(define example*.int
  `(
    (distinct-values
      ,(fxvector 100 120 130 140 150 160 170 180)
      )
    (distinct-large-values
      ,(fxvector 100000 120000 130000 140000 150000 160000 170000 180000)
      )
    (distinct-large-values-with-negatives
      ,(fxvector 100000 -120000 130000 -140000 150000 -160000 170000 180000)
      )
    (distinct-large-values-small-range
      ,(fxvector 100000 100020 100030 100040 100050 100060 100070 100080)
      )
    (non-adjacent-duplicate-values
      ,(fxvector
         100000 120000 130000 140000 150000 160000 170000 180000
         100000 120000 130000 140000 150000 160000 170000 180000
         100000 120000 130000 140000 150000 160000 170000 180000
         100000 120000 130000 140000 150000 160000 170000 180000
         100000 120000 130000 140000 150000 160000 170000 180000
         100000 120000 130000 140000 150000 160000 170000 180000
         100000 120000 130000 140000 150000 160000 170000 180000
         ))
    (non-adjacent-duplicate-values-regular-interval-ascending
      ,(fxvector
         110000 120000 130000 140000 150000 160000 170000 180000
         110000 120000 130000 140000 150000 160000 170000 180000
         110000 120000 130000 140000 150000 160000 170000 180000
         110000 120000 130000 140000 150000 160000 170000 180000
         110000 120000 130000 140000 150000 160000 170000 180000
         110000 120000 130000 140000 150000 160000 170000 180000
         110000 120000 130000 140000 150000 160000 170000 180000
         ))
    (consecutively-repeated-values
      ,(fxvector
         100 100 100 100 100 100
         120 120 120 120 120 120 120
         130 130 130 130 130
         140 140 140 140 140 140
         150 150 150
         160 160 160 160 160
         170 170 170 170 170 170
         180 180 180 180 180 180 180
         ))
    (consecutively-repeated-values-same-number
      ,(fxvector
         100 100 100 100 100 100 100
         120 120 120 120 120 120 120
         130 130 130 130 130 130 130
         140 140 140 140 140 140 140
         150 150 150 150 150 150 150
         160 160 160 160 160 160 160
         170 170 170 170 170 170 170
         180 180 180 180 180 180 180
         ))
    (consecutively-repeated-values-regular-interval-ascending
      ,(fxvector
         110 110 110 110 110 110
         120 120 120 120 120 120 120
         130 130 130 130 130
         140 140 140 140 140 140
         150 150 150
         160 160 160 160 160
         170 170 170 170 170 170
         180 180 180 180 180 180 180
         ))
    (consecutively-repeated-values-same-number-regular-interval-ascending
      ,(fxvector
         110 110 110 110 110 110 110
         120 120 120 120 120 120 120
         130 130 130 130 130 130 130
         140 140 140 140 140 140 140
         150 150 150 150 150 150 150
         160 160 160 160 160 160 160
         170 170 170 170 170 170 170
         180 180 180 180 180 180 180
         ))
    (regular-interval-ascending-values
      ,(fxvector 110 120 130 140 150 160 170 180)
      )
    (consecutively-repeated-single-value
      ,(fxvector 100 100 100 100 100 100 100 100)
      )
    ))

(for-each go.text example*.text)
(for-each go.int example*.int)
