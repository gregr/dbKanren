#lang racket/base
(require racket/vector)

;; TODO: try unsafe ops for vectors and fixnums

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Enumerators
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define ((vector->enumerator v (start 0) (end #f)) k!)
  (define len (or (and end (min end (vector-length v))) (vector-length v)))
  (let loop ((i start))
    (when (< i len)
      (k! (vector-ref v i))
      (loop (+ i 1)))))

(define ((enumerator-append e.0 e.1) k!)
  (e.0 k!)
  (e.1 k!))

(define ((enumerator-msd-radix-sort t->key key-byte-count en) k!)
  (define size.shift     8)
  (define mask.shift   255)
  (define count.parts  256)
  (define count.buffer  32)
  (let enumerate ((shift (- (* 8 (- key-byte-count 1)))) (en en))
    (define parts (make-vector count.parts '()))
    (define buf   (make-vector count.parts))
    (define pos   (make-vector count.parts 0))
    (let loop ((i 0))
      (when (< i count.parts)
        (vector-set! buf i (make-vector count.buffer))
        (loop (+ i 1))))
    (en (lambda (t)
          (let* ((key     (bitwise-and mask.shift (arithmetic-shift (t->key t) shift)))
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
    (if (= shift 0)
      (let loop ((i 0))
        (when (< i count.parts)
          (let ((parts.i (vector-ref parts i))
                (buf.i   (vector-ref buf   i))
                (pos.i   (vector-ref pos   i)))
            (vector-set! parts i #f)
            (vector-set! buf   i #f)
            (for-each (lambda (part) ((vector->enumerator part) k!))
                      parts.i)
            (let loop.buf ((i 0))
              (when (< i pos.i)
                (k! (vector-ref buf.i i))
                (loop.buf (+ i 1))))
            (loop (+ i 1)))))
      (let loop ((i 0))
        (when (< i count.parts)
          (let ((parts.i (vector-ref parts i))
                (buf.i   (vector-ref buf   i))
                (pos.i   (vector-ref pos   i)))
            (vector-set! parts i #f)
            (vector-set! buf   i #f)
            (enumerate
              (+ shift size.shift)
              (foldl (lambda (part en)
                       (enumerator-append (vector->enumerator part) en))
                     (vector->enumerator buf.i 0 pos.i)
                     parts.i))
            (loop (+ i 1))))))))

;; TODO: try lsd-radix-sort

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Benchmark
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define key-byte-count 4)
(define count.node (arithmetic-shift 1 25))

;(define key-byte-count 3)
;(define count.node (arithmetic-shift 1 24))
;(define count.node (arithmetic-shift 1 20))

(define count.edge (* count.node 16))

(displayln "allocating nodes:")
(displayln count.node)
(define nodes (time (make-vector count.node)))
(displayln "allocating edges:")
(displayln count.edge)

;#|
(define edges (time (make-vector count.edge)))
(define en.edges (vector->enumerator edges))

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
(displayln "computing node degrees")
(time (en.edges
        (lambda (edge)
         (define key (edge->key edge))
         (vector-set! nodes key (+ (vector-ref nodes key) 1)))))
;|#

;#|
(displayln "computing node degrees while sorting edges")
(time ((enumerator-msd-radix-sort edge->key key-byte-count en.edges)
       (lambda (edge)
         (define key (edge->key edge))
         (vector-set! nodes key (+ (vector-ref nodes key) 1)))))
;|#

#;(let ((previous #f))
  ((enumerator-msd-radix-sort edge->key key-byte-count en.edges)
   (lambda (edge)
     (when (and previous (< (edge->key edge) previous))
       (error "not sorted!" previous (edge->key edge)))
     (set! previous (edge->key edge)))))
