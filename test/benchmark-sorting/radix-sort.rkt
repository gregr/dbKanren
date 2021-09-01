#lang racket/base
(require racket/fixnum racket/vector racket/unsafe/ops)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Enumerators
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (vector->enumerator v (start 0) (end (vector-length v)))
  (define len (min end (vector-length v)))
  (unsafe-vector->enumerator v (min start len) len))

(define ((unsafe-vector->enumerator v (start 0) (end (unsafe-vector-length v))) k!)
  (let loop ((i start))
    (when (unsafe-fx< i end)
      (k!   (unsafe-vector*-ref v i))
      (loop (unsafe-fx+ i 1)))))

(define ((enumerator-append e.0 e.1) k!)
  (e.0 k!)
  (e.1 k!))

(define ((enumerator-msd-radix-sort t->key key-byte-count en) k!)
  (define size.shift     8)
  (define mask.shift   255)
  (define count.parts  256)
  (define count.buffer  32)
  (unless (fixnum? key-byte-count) (error "key-byte-count must be a fixnum"))
  (let enumerate ((shift (* 8 (- key-byte-count 1))) (en en))
    (define parts (make-vector count.parts '()))
    (define buf   (make-vector count.parts))
    (define pos   (make-vector count.parts 0))
    (let loop ((i 0))
      (when (< i count.parts)
        (vector-set! buf i (make-vector count.buffer))
        (loop (+ i 1))))
    (en (lambda (t)
          (let* ((key     (unsafe-fxand mask.shift (fxrshift (t->key t) shift)))
                 (buf.k   (vector-ref buf key))
                 (pos.k   (vector-ref pos key))
                 (pos.k+1 (+ pos.k 1)))
            (vector-set! buf.k pos.k t)
            (if (= pos.k+1 count.buffer)
              (begin
                (vector-set! pos   key 0)
                ;; TODO: try eliminating cons allocation
                (vector-set! parts key (cons buf.k (vector-ref parts key)))
                (vector-set! buf   key (make-vector count.buffer)))
              (vector-set! pos key (+ pos.k 1))))))
    (let ((k!/en (if (= shift 0)
                   (lambda (en) (en k!))
                   (lambda (en) (enumerate (- shift size.shift) en)))))
      (let loop ((i 0))
        (when (< i count.parts)
          (let ((parts.i (vector-ref parts i))
                (buf.i   (vector-ref buf   i))
                (pos.i   (vector-ref pos   i)))
            (vector-set! parts i #f)
            (vector-set! buf   i #f)
            (k!/en (foldl (lambda (part en)
                            (enumerator-append (unsafe-vector->enumerator part 0 count.buffer) en))
                          (unsafe-vector->enumerator buf.i 0 pos.i)
                          parts.i))
            (loop (+ i 1))))))))

;; TODO: switch to using these unsafe ops for a 12% reduction in benchmark time
#;(define ((enumerator-msd-radix-sort t->key key-byte-count en) k!)
  (define size.shift     8)
  (define mask.shift   255)
  (define count.parts  256)
  (define count.buffer  32)
  (unless (fixnum? key-byte-count) (error "key-byte-count must be a fixnum" key-byte-count))
  (let enumerate ((shift (unsafe-fx* 8 (unsafe-fx- key-byte-count 1))) (en en))
    (define parts (make-vector count.parts '()))
    (define buf   (make-vector count.parts))
    (define pos   (make-vector count.parts 0))
    (let loop ((i 0))
      (when (unsafe-fx< i count.parts)
        (unsafe-vector*-set! buf i (make-vector count.buffer))
        (loop (unsafe-fx+ i 1))))
    (en (lambda (t)
          ;; TODO: we can use unsafe-fxrshift if t->key is guaranteed to produce a fixnum
          (let* ((key     (unsafe-fxand mask.shift (fxrshift (t->key t) shift)))
                 (buf.k   (unsafe-vector*-ref buf key))
                 (pos.k   (unsafe-vector*-ref pos key))
                 (pos.k+1 (unsafe-fx+ pos.k 1)))
            (unsafe-vector*-set! buf.k pos.k t)
            (if (unsafe-fx= pos.k+1 count.buffer)
              (begin
                (unsafe-vector*-set! pos   key 0)
                ;; TODO: try eliminating cons allocation
                (unsafe-vector*-set! parts key (cons buf.k (unsafe-vector*-ref parts key)))
                (unsafe-vector*-set! buf   key (make-vector count.buffer)))
              (unsafe-vector*-set! pos key (unsafe-fx+ pos.k 1))))))
    (let ((k!/en (if (unsafe-fx= shift 0)
                   (lambda (en) (en k!))
                   (lambda (en) (enumerate (unsafe-fx- shift size.shift) en)))))
      (let loop ((i 0))
        (when (unsafe-fx< i count.parts)
          (let ((parts.i (unsafe-vector*-ref parts i))
                (buf.i   (unsafe-vector*-ref buf   i))
                (pos.i   (unsafe-vector*-ref pos   i)))
            (unsafe-vector*-set! parts i #f)
            (unsafe-vector*-set! buf   i #f)
            (k!/en (foldl (lambda (part en)
                            (enumerator-append (unsafe-vector->enumerator part 0 count.buffer) en))
                          (unsafe-vector->enumerator buf.i 0 pos.i)
                          parts.i))
            (loop (unsafe-fx+ i 1))))))))

;; TODO: try lsd-radix-sort

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Benchmark
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

#|
(define key-byte-count 4)
(define count.node (arithmetic-shift 1 25))
;|#

;#|
(define key-byte-count 3)
;(define count.node (arithmetic-shift 1 24))
(define count.node (arithmetic-shift 1 22))
;|#

(define count.edge (* count.node 16))

(displayln "allocating nodes:")
(displayln count.node)
(define nodes (time (make-vector count.node)))
(displayln "allocating edges:")
(displayln count.edge)

;#|
(define edges (time (make-vector count.edge)))
(define en.edges (unsafe-vector->enumerator edges 0 count.edge))

(displayln "building edges")
(time (let loop ((i 0))
        (when (< i count.edge)
          (vector-set! edges i (random count.node))
          ;(vector-set! edges i (cons (random count.node) (random count.node)))
          (loop (+ i 1)))))
;|#

#|
(define (en.edges k!)
  (let loop ((i 0))
    (when (< i count.edge)
      (k! (random count.node))
      ;(k! (cons (random count.node) (random count.node)))
      (loop (+ i 1)))))
;|#

;(define edge->key car)
(define (edge->key e) e)

#|
(displayln "sorting edges")
(time (vector-sort!
        edges
        (lambda (e.0 e.1) (< (edge->key e.0) (edge->key e.1)))))
;|#

#|
(displayln "computing node degrees inlined")
(time (let loop ((i 0))
        (when (unsafe-fx< i count.edge)
          (let ((key (unsafe-vector*-ref edges i)))
            (unsafe-vector*-set! nodes key (unsafe-fx+ (unsafe-vector*-ref nodes key) 1)))
          (loop (unsafe-fx+ i 1)))))
;|#

#|
(displayln "computing node degrees")
(time (en.edges
        (lambda (edge)
         (define key (edge->key edge))
         ;(vector-set! nodes key (+ (vector-ref nodes key) 1))
         (unsafe-vector*-set! nodes key (unsafe-fx+ (unsafe-vector*-ref nodes key) 1))
         )))
;|#

;#|
(displayln "computing node degrees while sorting edges")
(time ((enumerator-msd-radix-sort edge->key key-byte-count en.edges)
       (lambda (edge)
         (define key (edge->key edge))
         ;(vector-set! nodes key (+ (vector-ref nodes key) 1))
         (unsafe-vector*-set! nodes key (unsafe-fx+ (unsafe-vector*-ref nodes key) 1))
         )))
;|#

#;(let ((previous #f))
  ((enumerator-msd-radix-sort edge->key key-byte-count en.edges)
   (lambda (edge)
     (when (and previous (< (edge->key edge) previous))
       (error "not sorted!" previous (edge->key edge)))
     (set! previous (edge->key edge)))))
