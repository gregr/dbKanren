#lang racket/base
(provide
  )

(require racket/fixnum racket/set racket/vector)
(require
  "../dbk/safe-unsafe.rkt"
  ;racket/unsafe/ops
  )

(define compression-type.int:nat                0)
(define compression-type.int:frame-of-reference 1)
(define compression-type.int:single-value       2)
(define compression-type.int:delta-single-value 3)
(define compression-type.int:dictionary         4)
(define compression-type.int:multiple           5)

(define compression-type.text:shared-prefix     6)
(define compression-type.text:dictionary        7)

;; Returns the pos immediately following segment
(define (text-segment-decode! bv pos t* start end)
  (let* ((type (unsafe-bytes-ref bv pos))
         (pos  (unsafe-fx+ pos 1)))
    (define (? x) (eq? type x))
    (cond
      ((? compression-type.text:shared-prefix)
       (let* ((len                   (unsafe-fx- end start))
              (length*.full          (make-vector len))
              (pos                   (int-segment-decode! bv pos length*.full 0 len))
              (length*.shared-prefix (make-vector len))
              (pos                   (int-segment-decode! bv pos length*.shared-prefix 0 len)))
         (let loop ((j 0) (pos pos) (t.prev #""))
           (if (unsafe-fx< j len)
               (let* ((t.length.full   (unsafe-vector*-ref length*.full          j))
                      (t.length.prefix (unsafe-vector*-ref length*.shared-prefix j))
                      (t.length.suffix (unsafe-fx- t.length.full t.length.prefix))
                      (t               (make-bytes t.length.full))
                      (pos.next        (unsafe-fx+ pos t.length.suffix)))
                 (unsafe-bytes-copy! t 0               t.prev 0   t.length.prefix)
                 (unsafe-bytes-copy! t t.length.prefix bv     pos pos.next)
                 (unsafe-vector*-set! t* (unsafe-fx+ start j) t)
                 (loop (unsafe-fx+ j 1) pos.next t))
               pos))))
      ((? compression-type.text:dictionary)
       (let* ((len.dict (2-unrolled-unsafe-bytes-nat-ref bv pos))
              (pos      (unsafe-fx+ pos 2))
              (t*.dict  (make-vector len.dict))
              (pos      (text-segment-decode! bv pos t*.dict 0     len.dict))
              (pos      (int-segment-decode!  bv pos t*      start end)))
         (let loop ((i start))
           (when (unsafe-fx< i end)
             (unsafe-vector*-set! t* i (unsafe-vector*-ref t*.dict (unsafe-vector*-ref t* i)))
             (loop (unsafe-fx+ i 1))))
         pos))
      (else (error "unknown text compression type" type)))))

;; Returns the pos immediately following segment
(define (text-segment-encode!/shared-prefix bv pos t* start end)
  (let* ((len                   (unsafe-fx- end start))
         (length*.full          (make-vector len))
         (length*.shared-prefix (make-vector len)))
    (let loop ((j 0) (t.prev #"") (t.prev.length 0))
      (when (unsafe-fx< j len)
        (let* ((t        (unsafe-vector*-ref t* (unsafe-fx+ start j)))
               (t.length (unsafe-bytes-length t))
               (t.end    (min t.prev.length t.length)))
          (unsafe-vector*-set! length*.full j t.length)
          (let loop ((k 0))
            (if (and (unsafe-fx< k t.end) (eq? (unsafe-bytes-ref t.prev k) (unsafe-bytes-ref t k)))
                (loop (unsafe-fx+ k 1))
                (unsafe-vector*-set! length*.shared-prefix j k)))
          (loop (unsafe-fx+ j 1) t t.length))))
    (unsafe-bytes-set! bv pos compression-type.text:shared-prefix)
    (let* ((pos (unsafe-fx+ pos 1))
           (pos (int-segment-encode! bv pos length*.full          0 len))
           (pos (int-segment-encode! bv pos length*.shared-prefix 0 len)))
      (let loop ((j 0) (pos pos))
        (if (unsafe-fx< j len)
            (let* ((t               (unsafe-vector*-ref t* (unsafe-fx+ start j)))
                   (t.length        (unsafe-bytes-length t))
                   (t.length.prefix (unsafe-vector*-ref length*.shared-prefix j)))
              (unsafe-bytes-copy! bv pos t t.length.prefix t.length)
              (loop (unsafe-fx+ j 1) (unsafe-fx+ (unsafe-fx- t.length t.length.prefix) pos)))
            pos)))))

;; Returns the pos immediately following segment
(define (text-segment-encode!/dictionary t*.dict len.dict bv pos t*.source start end)
  (unsafe-bytes-set! bv pos compression-type.text:dictionary)
  (let* ((pos      (unsafe-fx+ pos 1))
         (pos      (begin (2-unrolled-unsafe-bytes-nat-set! bv pos len.dict) (unsafe-fx+ pos 2)))
         (pos      (text-segment-encode!/shared-prefix bv pos t*.dict 0 len.dict))
         (t=>i     (let loop ((i 0) (t=>i (hash)))
                     (if (unsafe-fx< i len.dict)
                         (loop (unsafe-fx+ i 1) (hash-set t=>i (unsafe-vector*-ref t*.dict i) i))
                         t=>i)))
         (n*.len   (unsafe-fx- end start))
         (n*       (make-vector n*.len)))
    (let loop ((j 0) (i start))
      (when (unsafe-fx< j n*.len)
        (let ((t (unsafe-vector*-ref t*.source i)))
          (unsafe-vector*-set! n* j (hash-ref t=>i t))
          (loop (unsafe-fx+ j 1) (unsafe-fx+ i 1)))))
    (int-segment-encode! bv pos n* 0 n*.len)))

;; Returns the pos immediately following segment
(define text-segment-encode!
  (let ((min-count.dictionary 3))
    (lambda (bv pos t*.source start end)
      (let ((len (unsafe-fx- end start)))
        (if (or (unsafe-fx<= len min-count.dictionary) (vector-text-sorted? t*.source start end))
            (text-segment-encode!/shared-prefix bv pos t*.source start end)
            (let ((t*.dict (vector-copy t*.source start end)))
              (vector-text-sort! t*.dict 0 len)
              (let ((len.dict (vector-text-dedup-adjacent! t*.dict 0 len)))
                (text-segment-encode!/dictionary t*.dict len.dict bv pos t*.source start end))))))))

;; Returns the pos immediately following segment
(define (int-segment-decode! bv pos z* start end)
  (let* ((type (unsafe-bytes-ref bv pos))
         (pos  (unsafe-fx+ pos 1)))
    (define (? x) (eq? type x))
    (cond
      ((? compression-type.int:nat)
       (let* ((byte-width (unsafe-bytes-ref bv pos))
              (pos        (unsafe-fx+ pos 1))
              (go (lambda (n-ref)
                    (let loop ((i start) (pos pos))
                      (cond ((unsafe-fx< i end)
                             (unsafe-vector*-set! z* i (n-ref bv pos))
                             (loop (unsafe-fx+ i 1) (unsafe-fx+ pos byte-width)))
                            (else pos))))))
         (case byte-width
           ((1) (go 1-unrolled-unsafe-bytes-nat-ref))
           ((2) (go 2-unrolled-unsafe-bytes-nat-ref))
           ((3) (go 3-unrolled-unsafe-bytes-nat-ref))
           ((4) (go 4-unrolled-unsafe-bytes-nat-ref))
           ((5) (go 5-unrolled-unsafe-bytes-nat-ref))
           ((6) (go 6-unrolled-unsafe-bytes-nat-ref))
           (else (error "unsupported byte-width" byte-width)))))
      ((? compression-type.int:frame-of-reference)
       (let* ((byte-width (unsafe-bytes-ref bv pos))
              (pos        (unsafe-fx+ pos 1))
              (z.min      (unsafe-fx+ (unsafe-bytes-nat-ref byte-width bv pos)
                                      (byte-width->int-min byte-width)))
              (pos        (unsafe-fx+ pos byte-width))
              (byte-width (unsafe-bytes-ref bv pos))
              (pos        (unsafe-fx+ pos 1))
              (go (lambda (n-ref)
                    (let loop ((i start) (pos pos))
                      (cond ((unsafe-fx< i end)
                             (unsafe-vector*-set! z* i (unsafe-fx+ (n-ref bv pos) z.min))
                             (loop (unsafe-fx+ i 1) (unsafe-fx+ pos byte-width)))
                            (else pos))))))
         (case byte-width
           ((1) (go 1-unrolled-unsafe-bytes-nat-ref))
           ((2) (go 2-unrolled-unsafe-bytes-nat-ref))
           ((3) (go 3-unrolled-unsafe-bytes-nat-ref))
           ((4) (go 4-unrolled-unsafe-bytes-nat-ref))
           ((5) (go 5-unrolled-unsafe-bytes-nat-ref))
           ((6) (go 6-unrolled-unsafe-bytes-nat-ref))
           (else (error "unsupported byte-width" byte-width)))))
      ((? compression-type.int:single-value)
       (let* ((byte-width (unsafe-bytes-ref bv pos))
              (pos        (unsafe-fx+ pos 1))
              (z.sole     (unsafe-fx+ (unsafe-bytes-nat-ref byte-width bv pos)
                                      (byte-width->int-min byte-width)))
              (pos        (unsafe-fx+ pos byte-width)))
         (let loop ((i start))
           (when (unsafe-fx< i end)
             (unsafe-vector*-set! z* i z.sole)
             (loop (unsafe-fx+ i 1))))
         pos))
      ((? compression-type.int:delta-single-value)
       (let* ((byte-width (unsafe-bytes-ref bv pos))
              (pos        (unsafe-fx+ pos 1))
              (z.start    (unsafe-fx+ (unsafe-bytes-nat-ref byte-width bv pos)
                                      (byte-width->int-min byte-width)))
              (pos        (unsafe-fx+ pos byte-width))
              (byte-width (unsafe-bytes-ref bv pos))
              (pos        (unsafe-fx+ pos 1))
              (z.delta    (unsafe-fx+ (unsafe-bytes-nat-ref byte-width bv pos)
                                      (byte-width->int-min byte-width)))
              (pos        (unsafe-fx+ pos byte-width)))
         (unsafe-vector*-set! z* start z.start)
         (let loop ((i (unsafe-fx+ start 1)) (z.prev z.start))
           (when (unsafe-fx< i end)
             (let ((v (unsafe-fx+ z.prev z.delta)))
               (unsafe-vector*-set! z* i v)
               (loop (unsafe-fx+ i 1) v))))
         pos))
      ((? compression-type.int:dictionary)
       (let* ((len.dict (unsafe-bytes-ref bv pos))
              (pos      (unsafe-fx+ pos 1))
              (z*.dict  (make-vector len.dict))
              (pos      (int-segment-decode! bv pos z*.dict 0 len.dict)))
         (let loop ((i start) (pos pos))
           (if (unsafe-fx< i end)
               (let ((z (unsafe-vector*-ref z*.dict (unsafe-bytes-ref bv pos))))
                 (unsafe-vector*-set! z* i z)
                 (loop (unsafe-fx+ i 1) (unsafe-fx+ pos 1)))
               pos))))
      ((? compression-type.int:multiple)
       (let* ((len (2-unrolled-unsafe-bytes-nat-ref bv pos))
              (pos (unsafe-fx+ pos 2))
              (mid (unsafe-fx+ start len))
              (pos (int-segment-decode! bv pos z* start mid)))
         (int-segment-decode! bv pos z* mid end)))
      (else (error "unknown integer compression type" type)))))

;; Returns the pos immediately following int
(define (int-encode! bv pos z)
  (let ((byte-width (nat-min-byte-width (if (unsafe-fx< z 0)
                                            (unsafe-fx- (unsafe-fx* (unsafe-fx- z) 2) 1)
                                            z))))
    (unsafe-bytes-set! bv pos byte-width)
    (unsafe-bytes-nat-set! byte-width bv (unsafe-fx+ pos 1)
                           (unsafe-fx- z (byte-width->int-min byte-width)))
    (unsafe-fx+ pos 1 byte-width)))

;; Returns the pos immediately following segment
(define (int-segment-encode!/nat/width width bv pos n* start end)
  (unsafe-bytes-set! bv pos compression-type.int:nat)
  (let ((pos (unsafe-fx+ pos 1)))
    (unsafe-bytes-set! bv pos width)
    (let ((go (lambda (n-set!)
                (let loop ((i start) (pos (unsafe-fx+ pos 1)))
                  (cond ((unsafe-fx< i end)
                         (n-set! bv pos (unsafe-vector*-ref n* i))
                         (loop (unsafe-fx+ i 1) (unsafe-fx+ pos width)))
                        (else pos))))))
      (case width
        ((1) (go 1-unrolled-unsafe-bytes-nat-set!))
        ((2) (go 2-unrolled-unsafe-bytes-nat-set!))
        ((3) (go 3-unrolled-unsafe-bytes-nat-set!))
        ((4) (go 4-unrolled-unsafe-bytes-nat-set!))
        ((5) (go 5-unrolled-unsafe-bytes-nat-set!))
        ((6) (go 6-unrolled-unsafe-bytes-nat-set!))
        (else (error "unsupported byte-width" width))))))

;; Returns the pos immediately following segment
(define (int-segment-encode!/nat/max n.max bv pos n* start end)
  (int-segment-encode!/nat/width (nat-min-byte-width n.max) bv pos n* start end))

;; Returns the pos immediately following segment
(define (int-segment-encode!/frame-of-reference/min&width z.min width bv pos z* start end)
  (unsafe-bytes-set! bv pos compression-type.int:frame-of-reference)
  (let ((pos (int-encode! bv (unsafe-fx+ pos 1) z.min)))
    (unsafe-bytes-set! bv pos width)
    (let ((go (lambda (n-set!)
                (let loop ((i start) (pos (unsafe-fx+ pos 1)))
                  (cond ((unsafe-fx< i end)
                         (n-set! bv pos (unsafe-fx- (unsafe-vector*-ref z* i) z.min))
                         (loop (unsafe-fx+ i 1) (unsafe-fx+ pos width)))
                        (else pos))))))
      (case width
        ((1) (go 1-unrolled-unsafe-bytes-nat-set!))
        ((2) (go 2-unrolled-unsafe-bytes-nat-set!))
        ((3) (go 3-unrolled-unsafe-bytes-nat-set!))
        ((4) (go 4-unrolled-unsafe-bytes-nat-set!))
        ((5) (go 5-unrolled-unsafe-bytes-nat-set!))
        ((6) (go 6-unrolled-unsafe-bytes-nat-set!))
        (else (error "unsupported byte-width" width))))))

;; Returns the pos immediately following segment
;; Assumes a non-empty range of integer values
(define (int-segment-encode!/frame-of-reference bv pos z* start end)
  (let ((z0 (unsafe-vector*-ref z* start)))
    (let loop ((i (unsafe-fx+ start 1)) (z.min z0) (z.max z0))
      (if (unsafe-fx< i end)
          (let ((z (unsafe-vector*-ref z* i)))
            (cond ((unsafe-fx< z z.min) (loop (unsafe-fx+ i 1) z     z.max))
                  ((unsafe-fx< z.max z) (loop (unsafe-fx+ i 1) z.min z))
                  (else                 (loop (unsafe-fx+ i 1) z.min z.max))))
          (let ((width     (nat-min-byte-width (unsafe-fx- z.max z.min)))
                (width.nat (and (unsafe-fx<= 0 z.min) (nat-min-byte-width z.max))))
            (if (and width.nat (unsafe-fx<= width.nat width))
                (int-segment-encode!/nat/width                      width.nat bv pos z* start end)
                (int-segment-encode!/frame-of-reference/min&width z.min width bv pos z* start end)))))))

;; Returns the pos immediately following segment
(define (int-segment-encode!/single-value z.sole bv pos)
  (unsafe-bytes-set! bv pos compression-type.int:single-value)
  (int-encode! bv (unsafe-fx+ pos 1) z.sole))

;; Returns the pos immediately following segment
(define (int-segment-encode!/delta-single-value z.start z.delta bv pos)
  (unsafe-bytes-set! bv pos compression-type.int:delta-single-value)
  (let ((pos (int-encode! bv (unsafe-fx+ pos 1) z.start)))
    (int-encode! bv pos z.delta)))

;; Returns the pos immediately following segment
(define (int-segment-encode!/dictionary z*.dict bv pos z*.source start end)
  (unsafe-bytes-set! bv pos compression-type.int:dictionary)
  (let* ((pos      (unsafe-fx+ pos 1))
         (len.dict (unsafe-vector*-length z*.dict))
         (pos      (begin (unsafe-bytes-set! bv pos len.dict) (unsafe-fx+ pos 1)))
         (z0       (unsafe-vector*-ref z*.dict 0))
         (pos      (int-segment-encode!/frame-of-reference bv pos z*.dict 0 len.dict))
         (z=>i     (let loop ((i 0) (z=>i (hash)))
                     (if (unsafe-fx< i len.dict)
                         (loop (unsafe-fx+ i 1) (hash-set z=>i (unsafe-vector*-ref z*.dict i) i))
                         z=>i))))
    (let loop ((i start) (pos pos))
      (if (unsafe-fx< i end)
          (let ((z (unsafe-vector*-ref z*.source i)))
            (unsafe-bytes-set! bv pos (hash-ref z=>i z))
            (loop (unsafe-fx+ i 1) (unsafe-fx+ pos 1)))
          pos))))

;; Returns the pos immediately following header
(define (int-segment-encode!/multiple len bv pos)
  (unsafe-bytes-set! bv pos compression-type.int:multiple)
  (2-unrolled-unsafe-bytes-nat-set! bv (unsafe-fx+ pos 1) len)
  (unsafe-fx+ pos 3))

;; Returns the pos immediately following segment
;; Assumes a non-empty range of integer values
(define int-segment-encode!
  (let ((min-count.dictionary   1024)
        (min-count.single-value   16))
    (lambda (bv pos z* start end)
      (define (encode-for pos start end)
        (int-segment-encode!/frame-of-reference bv pos z* start end))
      (define (encode-try-dictionary pos start end)
        (if (unsafe-fx<= min-count.dictionary (unsafe-fx- end start))
            (let restart ((i start) (seen (set)))
              (let ((remaining (unsafe-fx- 256 (set-count seen))))
                (if (and (unsafe-fx< i end) (unsafe-fx< 0 remaining))
                    (let ((end.min (min (unsafe-fx+ i remaining) end)))
                      (let loop ((i i) (seen seen))
                        (if (unsafe-fx< i end.min)
                            (loop (unsafe-fx+ i 1) (set-add seen (unsafe-vector*-ref z* i)))
                            (restart i seen))))
                    (let loop ((i i))
                      (cond ((unsafe-fx= i end)
                             (let* ((z*.dict  (list->vector (sort (set->list seen) <)))
                                    (len.dict (unsafe-vector*-length z*.dict))
                                    (z.max    (unsafe-vector*-ref z*.dict (unsafe-fx- len.dict 1)))
                                    (z.min    (unsafe-vector*-ref z*.dict 0)))
                               (if (unsafe-fx< (unsafe-fx- z.max z.min) 256)
                                   (encode-for pos start end)
                                   (int-segment-encode!/dictionary z*.dict bv pos z* start end))))
                            ((set-member? seen (unsafe-vector*-ref z* i)) (loop (unsafe-fx+ i 1)))
                            (else (encode-for pos start end)))))))
            (encode-for pos start end)))
      (define (encode-try-delta-single-value pos start end)
        (let next ((pos pos) (start start) (z.start (unsafe-vector*-ref z* start)))
          (let ((end.abort (unsafe-fx- end min-count.single-value)))
            (cond
              ((unsafe-fx<= start end.abort)
               (let restart ((i (unsafe-fx+ start 1)) (start.same start) (z.start z.start))
                 (let* ((end.same (unsafe-fx+ start.same min-count.single-value))
                        (z        (unsafe-vector*-ref z* i))
                        (i        (unsafe-fx+ i 1))
                        (delta    (unsafe-fx- z z.start)))
                   (let loop ((i i) (z.prev z))
                     (if (unsafe-fx< i end.same)
                         (let ((z (unsafe-vector*-ref z* i)))
                           (cond
                             ((unsafe-fx= (unsafe-fx- z z.prev) delta) (loop (unsafe-fx+ i 1) z))
                             ((unsafe-fx<= i end.abort) (restart (unsafe-fx+ i 1) i z))
                             (else                      (encode-try-dictionary pos start end))))
                         (let ((pos (if (unsafe-fx= start start.same)
                                        pos
                                        (let ((pos (int-segment-encode!/multiple
                                                     (unsafe-fx- start.same start) bv pos)))
                                          (encode-try-dictionary pos start start.same)))))
                           (let loop ((i i) (z.prev z))
                             (if (unsafe-fx< i end)
                                 (let ((z (unsafe-vector*-ref z* i)))
                                   (if (unsafe-fx= (unsafe-fx- z z.prev) delta)
                                       (loop (unsafe-fx+ i 1) z)
                                       (let* ((pos (int-segment-encode!/multiple
                                                     (unsafe-fx- i start.same) bv pos))
                                              (pos (int-segment-encode!/delta-single-value
                                                     z.start delta bv pos)))
                                         (next pos i z))))
                                 (int-segment-encode!/delta-single-value
                                   z.start delta bv pos)))))))))
              ((unsafe-fx< 1 (unsafe-fx- end start)) (encode-try-dictionary pos start end))
              (else (int-segment-encode!/single-value z.start bv pos))))))
      (define (encode-try-single-value pos start end)
        (let next ((pos pos) (start start) (z.prev (unsafe-vector*-ref z* start)))
          (let ((end.abort (unsafe-fx- end min-count.single-value)))
            (cond
              ((unsafe-fx<= start end.abort)
               (let restart ((i (unsafe-fx+ start 1)) (start.same start) (z.prev z.prev))
                 (let ((end.same (unsafe-fx+ start.same min-count.single-value)))
                   (let loop ((i i))
                     (if (unsafe-fx< i end.same)
                         (let ((z (unsafe-vector*-ref z* i)))
                           (cond
                             ((unsafe-fx= z.prev z)     (loop (unsafe-fx+ i 1)))
                             ((unsafe-fx<= i end.abort) (restart (unsafe-fx+ i 1) i z))
                             (else                      (encode-try-delta-single-value
                                                          pos start end))))
                         (let ((pos (if (unsafe-fx= start start.same)
                                        pos
                                        (let ((pos (int-segment-encode!/multiple
                                                     (unsafe-fx- start.same start) bv pos)))
                                          (encode-try-delta-single-value pos start start.same)))))
                           (let loop ((i i))
                             (if (unsafe-fx< i end)
                                 (let ((z (unsafe-vector*-ref z* i)))
                                   (if (unsafe-fx= z.prev z)
                                       (loop (unsafe-fx+ i 1))
                                       (let* ((pos (int-segment-encode!/multiple
                                                     (unsafe-fx- i start.same) bv pos))
                                              (pos (int-segment-encode!/single-value
                                                     z.prev bv pos)))
                                         (next pos i z))))
                                 (int-segment-encode!/single-value z.prev bv pos)))))))))
              ((unsafe-fx< 1 (unsafe-fx- end start)) (encode-try-dictionary pos start end))
              (else (int-segment-encode!/single-value z.prev bv pos))))))
      (encode-try-single-value pos start end))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Codec for text, nat, and nat-array values ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (unsafe-bytes-nat-set! width bs offset n)
  (case width
    ((1)  (1-unrolled-unsafe-bytes-nat-set! bs offset n))
    ((2)  (2-unrolled-unsafe-bytes-nat-set! bs offset n))
    ((3)  (3-unrolled-unsafe-bytes-nat-set! bs offset n))
    ((4)  (4-unrolled-unsafe-bytes-nat-set! bs offset n))
    ((5)  (5-unrolled-unsafe-bytes-nat-set! bs offset n))
    ((6)  (6-unrolled-unsafe-bytes-nat-set! bs offset n))
    (else (rolled-unsafe-bytes-nat-set! width bs offset n))))
(define (rolled-unsafe-bytes-nat-set! width bs offset n)
  (let loop ((i     offset)
             (shift (unsafe-fxlshift (unsafe-fx- width 1) 3)))
    (when (unsafe-fx<= 0 shift)
      (unsafe-bytes-set! bs i (unsafe-fxand 255 (unsafe-fxrshift n shift)))
      (loop (unsafe-fx+ i     1)
            (unsafe-fx- shift 8)))))
(define 1-unrolled-unsafe-bytes-nat-set! unsafe-bytes-set!)
(define (2-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 n)))
(define (3-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 2) (unsafe-fxand 255 n)))
(define (4-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 24)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 2) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 3) (unsafe-fxand 255 n)))
(define (5-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 32)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 24)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 2) (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 3) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 4) (unsafe-fxand 255 n)))
(define (6-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 40)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 32)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 2) (unsafe-fxand 255 (unsafe-fxrshift n 24)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 3) (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 4) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 5) (unsafe-fxand 255 n)))
(define (unsafe-bytes-nat-ref width bs offset)
  (case width
    ((1)  (1-unrolled-unsafe-bytes-nat-ref bs offset))
    ((2)  (2-unrolled-unsafe-bytes-nat-ref bs offset))
    ((3)  (3-unrolled-unsafe-bytes-nat-ref bs offset))
    ((4)  (4-unrolled-unsafe-bytes-nat-ref bs offset))
    ((5)  (5-unrolled-unsafe-bytes-nat-ref bs offset))
    ((6)  (6-unrolled-unsafe-bytes-nat-ref bs offset))
    (else (rolled-unsafe-bytes-nat-ref width bs offset))))
(define (rolled-unsafe-bytes-nat-ref width bs offset)
  (let ((end (unsafe-fx+ offset width)))
    (let loop ((i offset) (n 0))
      (cond ((unsafe-fx< i end) (loop (unsafe-fx+ i 1)
                                      (unsafe-fx+ (unsafe-fxlshift n 8)
                                                  (unsafe-bytes-ref bs i))))
            (else               n)))))
(define 1-unrolled-unsafe-bytes-nat-ref unsafe-bytes-ref)
(define (2-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fx+ (unsafe-fxlshift (unsafe-bytes-ref bs             i)     8)
              (unsafe-bytes-ref                  bs (unsafe-fx+ i 1))))
(define (3-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fx+ (unsafe-fxlshift (unsafe-bytes-ref bs             i)    16)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 1))  8)
              (unsafe-bytes-ref                  bs (unsafe-fx+ i 2))))
(define (4-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fx+ (unsafe-fxlshift (unsafe-bytes-ref bs             i)    24)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 1)) 16)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 2))  8)
              (unsafe-bytes-ref                  bs (unsafe-fx+ i 3))))
(define (5-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fx+ (unsafe-fxlshift (unsafe-bytes-ref bs             i)    32)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 1)) 24)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 2)) 16)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 3))  8)
              (unsafe-bytes-ref                  bs (unsafe-fx+ i 4))))
(define (6-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fx+ (unsafe-fxlshift (unsafe-bytes-ref bs             i)    40)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 1)) 32)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 2)) 24)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 3)) 16)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 4))  8)
              (unsafe-bytes-ref                  bs (unsafe-fx+ i 5))))

(define (min-bits n)
  (let loop ((n n))
    (if (< 0 n) (+ 1 (loop (fxrshift n 1))) 0)))
(define (min-bytes n)
  (let ((bits (min-bits n)))
    (+ (quotient bits 8) (if (= 0 (remainder bits 8)) 0 1))))
(define (nat-min-byte-width nat.max) (max (min-bytes nat.max) 1))

(define (bit-width->int-min  bit-width)  (unsafe-fx- (unsafe-fxlshift 1 (unsafe-fx- bit-width 1))))
(define (byte-width->int-min byte-width) (bit-width->int-min (unsafe-fxlshift byte-width 3)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Text comparison, sorting, deduping ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (text<? a b)
  (let* ((len.a   (unsafe-bytes-length a))
         (len.b   (unsafe-bytes-length b))
         (len.min (min len.a len.b)))
    (let loop ((i 0))
      (if (unsafe-fx< i len.min)
          (let ((x.a (unsafe-bytes-ref a i)) (x.b (unsafe-bytes-ref b i)))
            (or (unsafe-fx< x.a x.b)
                (and (not (unsafe-fx< x.b x.a))
                     (loop (unsafe-fx+ i 1)))))
          (unsafe-fx< len.a len.b)))))

(define (text=? a b)
  (let ((len (unsafe-bytes-length a)))
    (and (unsafe-fx= (unsafe-bytes-length b) len)
         (let loop ((i 0))
           (or (unsafe-fx= i len)
               (and (unsafe-fx= (unsafe-bytes-ref a i) (unsafe-bytes-ref b i))
                    (loop (unsafe-fx+ i 1))))))))

(define (vector-text-sorted? t* start end)
  (or (unsafe-fx<= end start)
      (let loop ((i (unsafe-fx+ start 1)) (t.prev (unsafe-vector*-ref t* start)))
        (or (unsafe-fx= i end)
            (let ((t.next (unsafe-vector*-ref t* i)))
              (and (not (text<? t.next t.prev))
                   (loop (unsafe-fx+ i 1) t.next)))))))

(define (vector-text-sort! t* start end)
  ;; TODO: adaptive merge sort that recognizes sorted runs as in database.rkt
  (vector-sort! t* text<? start end))

;; Returns the index immediately following the last remaining value
(define (vector-text-dedup-adjacent! t* start end)
  (if (unsafe-fx< start end)
      (let loop ((t.current (unsafe-vector*-ref t* start))
                 (i         (unsafe-fx+ start 1)))
        (if (unsafe-fx= i end)
            end
            (let ((t.next (unsafe-vector*-ref t* i)))
              (if (text=? t.current t.next)
                  (let loop ((t.current t.current)
                             (target    i)
                             (i         (unsafe-fx+ i 1)))
                    (if (unsafe-fx= i end)
                        target
                        (let ((t.next (unsafe-vector*-ref t* i)))
                          (if (text=? t.current t.next)
                              (loop t.current target (unsafe-fx+ i 1))
                              (begin (unsafe-vector*-set! t* target t.next)
                                     (loop t.next (unsafe-fx+ target 1) (unsafe-fx+ i 1)))))))
                  (loop t.next (unsafe-fx+ i 1))))))
      end))

;;;;;;;;;;;;;;;;
;;; Examples ;;;
;;;;;;;;;;;;;;;;

(require racket/list racket/pretty)

(let* ((example   (list->vector (range 100)))
       (roundtrip (make-vector 100))
       (bv        (make-bytes 1000)))

  ;; int:multiple
  (let* ((pos (int-segment-encode!/multiple 49 bv 0))
         (pos (time (int-segment-encode!/frame-of-reference/min&width -10 6 bv pos example 0 49))))
    (time (int-segment-encode!/frame-of-reference/min&width -10 6 bv pos example 49 100)))

  ;; with delta:
  #;(let* ((z.start (vector-ref example 0))
         (z.delta (- (vector-ref example 1) z.start)))
    (time (int-segment-encode!/delta-single-value z.start z.delta bv 0)))

  ;; without delta:
  ;(time (int-segment-encode!/frame-of-reference/min&width -10 6 bv 0 example 0 100))

  (time (int-segment-decode! bv 0 roundtrip 0 100))

  (write roundtrip)
  (newline)
  (write bv)
  (newline)
  (write (bytes->list bv))
  (newline)
  (pretty-write (equal? example roundtrip)))

(let* ((large     5000000000)
       (small     -5000000000)
       (example   (vector large small large small large small large small large small))
       (roundtrip (make-vector (vector-length example)))
       (bv        (make-bytes 1000))
       (pos       (int-segment-encode!/dictionary (vector small large) bv 0 example 0 (vector-length example))))
  (time (int-segment-decode! bv 0 roundtrip 0 (vector-length example)))
  (write roundtrip)
  (newline)
  (write bv)
  (newline)
  (write (bytes->list bv))
  (newline)
  (pretty-write (equal? example roundtrip)))

(define (test-roundtrip buffer-size z*)
  (let ((bv        (make-bytes buffer-size))
        (roundtrip (make-vector (vector-length z*))))
    (time (int-segment-encode! bv 0 z*        0 (vector-length z*)))
    (time (int-segment-decode! bv 0 roundtrip 0 (vector-length z*)))
    (let ((success? (equal? z* roundtrip)))
      (unless #f;success?
        (write roundtrip)
        (newline)
        (write bv)
        (newline)
        (write (bytes->list bv))
        (newline))
      (pretty-write success?))))

(displayln "range-100:")
(test-roundtrip 1000 (list->vector (range 100)))

(displayln "large-small:")
(let* ((large     5000000000)
       (small     -5000000000)
       (example   (vector large small large small large small large small large small)))
  (test-roundtrip 1000 example))

(displayln "large-small-repeats:")
(let* ((large     5000000000)
       (small     -5000000000)
       (example   (vector large
                          small small small small small small small small
                          small small small small small small small small
                          small small small small small small small small
                          small small small small small small small small
                          large small large small large
                          small small small small small small small small
                          small small small small small small small small
                          small small small small small small small small
                          small small small small small small small small
                          small small small small small small small small
                          large small)))
  (test-roundtrip 1000 example))

(displayln "delta-repeats:")
(let* ((large     5000000000)
       (small     -5000000000)
       (example   (vector large
                          small small small small small small small small
                          small small small small small small small small
                          small small small small small small small small
                          small small small small small small small small
                          large small large small large
                          100 101 102 103 104 105 106 107 108 109
                          110 111 112 113 114 115 116 117 118 119
                          120 121 122 123 124 125 126 127 128 129
                          130 131 132 133 134 135 136 137 138 139
                          large small large small large
                          100 101 102 103 104 105 106 107 108 109
                          110 111 112 113 114 115 116 117 118 119
                          120 121 122 123 124 125 126 127 128 129
                          130 131 132 133 134 135 136 137 138 139
                          large small large small large
                          small small small small small small small small
                          small small small small small small small small
                          small small small small small small small small
                          small small small small small small small small
                          small small small small small small small small
                          large small)))
  (test-roundtrip 1000 example))

(displayln "dict-repeats:")
(let* ((large   5000000000)
       (small   -5000000000)
       (example (list large small large small large small large large small large
                      small large small large small large small large large small))
       (example (append example example example example example))
       (example (append example example example example example))
       (example (append example example example))
       (example (list->vector example)))
  (test-roundtrip
    2000
    ; 8000  ; need more space without dictionaries
    example))

(define (test-text-roundtrip buffer-size t*)
  (let ((bv        (make-bytes buffer-size))
        (roundtrip (make-vector (vector-length t*))))
    (time (text-segment-encode! bv 0 t*        0 (vector-length t*)))
    (time (text-segment-decode! bv 0 roundtrip 0 (vector-length t*)))
    (let ((success? (equal? t* roundtrip)))
      (unless #f;success?
        (write roundtrip)
        (newline)
        (write bv)
        (newline)
        (write (bytes->list bv))
        (newline))
      (pretty-write success?))))

(displayln "ascending text:")
(let ((example (vector #"ABCD:X0000001"
                       #"ABCD:X0000002"
                       #"ABCD:X0000003"
                       #"ABCD:X0000004"
                       #"ABCD:X0000005"
                       #"ABCD:X0000006"
                       #"ABCD:X0000007"
                       #"ABCD:X0000008"
                       #"ABCD:X0000009"
                       #"ABCD:X0000010"
                       #"ABCD:X0000011"
                       #"ABCD:X0000012"
                       #"ABCD:X0000013"
                       #"ABCD:X0000014"
                       #"ABCD:X0000015"
                       #"ABCD:X0000016"
                       #"ABCD:X0000017"
                       #"ABCD:X0000018"
                       #"ABCD:X0000019"
                       #"ABCD:X0000020")))
  (test-text-roundtrip 100 example))

(displayln "mixed text:")
(let ((example (vector #"ABCD:X0000019"
                       #"ABCD:X0000020"
                       #"ABCD:X0000001"
                       #"ABCD:X0000002"
                       #"ABCD:X0000003"
                       #"ABCD:X0000010"
                       #"ABCD:X0000013"
                       #"ABCD:X0000014"
                       #"ABCD:X0000015"
                       #"ABCD:X0000007"
                       #"ABCD:X0000008"
                       #"ABCD:X0000009"
                       #"ABCD:X0000016"
                       #"ABCD:X0000017"
                       #"ABCD:X0000018"
                       #"ABCD:X0000011"
                       #"ABCD:X0000012"
                       #"ABCD:X0000004"
                       #"ABCD:X0000005"
                       #"ABCD:X0000006"
                       #"ABCD:X0000019"
                       #"ABCD:X0000020"
                       #"ABCD:X0000001"
                       #"ABCD:X0000002"
                       #"ABCD:X0000003"
                       #"ABCD:X0000013"
                       #"ABCD:X0000014"
                       #"ABCD:X0000015"
                       #"ABCD:X0000007"
                       #"ABCD:X0000008"
                       #"ABCD:X0000009"
                       #"ABCD:X0000016"
                       #"ABCD:X0000017"
                       #"ABCD:X0000018")))
  (test-text-roundtrip 1000 example))
