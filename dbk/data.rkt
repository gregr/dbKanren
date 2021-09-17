#lang racket/base
(provide
  dict.empty
  dict:ordered:vector
  dict:hash
  enumerator-project
  enumerator-filter
  enumerator-sort
  enumerator-dedup
  enumerator->dict:ordered:vector
  group-fold->hash
  group-fold
  group-fold-ordered
  merge-union
  merge-join
  dict-join
  hash-join)
(require "enumerator.rkt" "misc.rkt" "order.rkt")

;; TODO: benchmark a design based on streams/iterators for comparison

(define (bisect start end i<)
  (let loop ((start start) (end end))
    (if (<= end start) end
      (let ((i (+ start (quotient (- end start) 2))))
        (if (i< i) (loop (+ 1 i) end) (loop start i))))))

(define (bisect-next start end i<)
  (define i (- start 1))
  (let loop ((offset 1))
    (define next (+ i offset))
    (cond ((and (< next end) (i< next)) (loop (arithmetic-shift offset 1)))
          (else (let loop ((i i) (o offset))
                  (let* ((o (arithmetic-shift o -1)) (next (+ i o)))
                    (cond ((= o 0)                      (+ i 1))
                          ((and (< next end) (i< next)) (loop next o))
                          (else                         (loop i    o)))))))))

(define (bisect-prev start end i>)
  (define i end)
  (let loop ((offset 1))
    (define next (- i offset))
    (cond ((and (>= next start) (i> next)) (loop (arithmetic-shift offset 1)))
          (else (let loop ((i i) (o offset))
                  (let* ((o (arithmetic-shift o -1)) (n (- i o)))
                    (cond ((= o 0)                   i)
                          ((and (>= n start) (i> n)) (loop n o))
                          (else                      (loop i o)))))))))

(define dict.empty
  (method-lambda
    ((count)                   0)
    ((== _)                    dict.empty)
    ((<= _)                    dict.empty)
    ((<  _)                    dict.empty)
    ((>= _)                    dict.empty)
    ((>  _)                    dict.empty)
    ((bstr-prefix   _)         dict.empty)
    ((bstr-contains _)         dict.empty)
    ((has-key?      _)         #f)
    ((ref _ k.found k.missing) (k.missing))
    ((enumerator/2)            (lambda _ (void)))))

(define (dict:ordered:vector rows (t->key (lambda (t) t)) (start 0) (end (vector-length rows)))
  (let loop ((start start) (end end))
    (define (key-ref i) (t->key (vector-ref rows i)))
    (define self
      (if (<= end start)
        dict.empty
        (method-lambda
          ((top-key)   (t->key (vector-ref rows start)))
          ((top-value) (vector-ref rows start))
          ((pop)       (loop (+ start 1) end))
          ((count)     (- end start))
          ((max)       (key-ref (- end 1)))
          ((min)       (key-ref start))
          ((>= key)    (loop (bisect-next start end (lambda (i) (any<?  (key-ref i) key))) end))
          ((>  key)    (loop (bisect-next start end (lambda (i) (any<=? (key-ref i) key))) end))
          ((<= key)    (loop start (bisect-prev start end (lambda (i) (any<?  key (key-ref i))))))
          ((<  key)    (loop start (bisect-prev start end (lambda (i) (any<=? key (key-ref i))))))
          ((== key)    ((self '>= key) '<= key))
          ((has-key? key)              (let ((self (self '>= key)))
                                         (and (< 0 (self 'count))
                                              (equal? (self 'min) key))))
          ((ref key k.found k.missing) (let ((self (self '>= key)))
                                         (if (< 0 (self 'count))
                                           (let ((t (self 'top-value)))
                                             (if (equal? (t->key t) key)
                                               (k.found t)
                                               (k.missing)))
                                           (k.missing))))
          ((enumerator/2)              (lambda (yield)
                                         (let loop ((i start))
                                           (when (< i end)
                                             (let ((t (vector-ref rows i)))
                                               (yield (t->key t) t)
                                               (loop (+ i 1))))))))))
    self))

(define (dict:hash k=>t)
  (let loop ((k=>t k=>t))
    (define (top)     (hash-iterate-first k=>t))
    (define (top-key) (hash-iterate-key   k=>t (top)))
    (if (= (hash-count k=>t) 0)
      dict.empty
      (method-lambda
        ((top-key)                   (top-key))
        ((top-value)                 (hash-iterate-value k=>t (top)))
        ((pop)                       (loop (hash-remove k=>t (top-key))))
        ((count)                     (hash-count k=>t))
        ((== key)                    (if (hash-has-key? k=>t key)
                                       (loop (hash key (hash-ref k=>t key)))
                                       dict.empty))
        ((has-key? key)              (hash-has-key? k=>t key))
        ((ref key k.found k.missing) (if (hash-has-key? k=>t key)
                                       (k.found (hash-ref k=>t key))
                                       (k.missing)))
        ((enumerator/2)              (hash->enumerator/2 k=>t))))))

(define ((merge-join A B) yield)
  (when (and (< 0 (A 'count))
             (< 0 (B 'count)))
    (let loop ((A   A)
               (k.A (A 'min))
               (B   B)
               (k.B (B 'min)))
      (case (compare-any k.A k.B)
        ((-1) (let ((A (A '>= k.B)))
                (when (< 0 (A 'count))
                  (loop A (A 'min) B k.B))))
        (( 1) (let ((B (B '>= k.A)))
                (when (< 0 (B 'count))
                  (loop A k.A B (B 'min)))))
        (else (let ((t.A (A 'top-value))
                    (t.B (B 'top-value))
                    (A   (A 'pop))
                    (B   (B 'pop)))
                (yield k.A t.A t.B)
                (when (and (< 0 (A 'count))
                           (< 0 (B 'count)))
                  (loop A (A 'min) B (B 'min)))))))))

(define ((merge-antijoin A B) yield)
  (if (= 0 (B 'count))
    ((A 'enumerator/2) yield)
    (when (< 0 (A 'count))
      (let loop ((A   A)
                 (k.A (A 'min))
                 (B   B)
                 (k.B (B 'min)))
        (case (compare-any k.A k.B)
          ((-1) (yield k.A (A 'top-value))
                (let ((A (A 'pop)))
                  (when (< 0 (A 'count))
                    (loop A (A 'min) B k.B))))
          (( 1) (let ((B (B '>= k.A)))
                  (if (= 0 (B 'count))
                    ((A 'enumerator/2) yield)
                    (loop A k.A B (B 'min)))))
          (else (let ((A (A 'pop))
                      (B (B 'pop)))
                  (if (= 0 (B 'count))
                    ((A 'enumerator/2) yield)
                    (when (< 0 (A 'count))
                      (loop A (A 'min) B (B 'min)))))))))))

(define ((merge-union A B unite) yield)
  (cond ((= (A 'count) 0) ((B 'enumerator/2) yield))
        ((= (B 'count) 0) ((A 'enumerator/2) yield))
        (else (let loop ((A   A)
                         (k.A (A 'min))
                         (B   B)
                         (k.B (B 'min)))
                (case (compare-any k.A k.B)
                  ((-1) (yield (A 'top-key) (A 'top-value))
                        (let ((A (A 'pop)))
                          (if (< 0 (A 'count))
                            (loop A (A 'min) B k.B)
                            ((B 'enumerator/2) yield))))
                  (( 1) (yield (B 'top-key) (B 'top-value))
                        (let ((B (B 'pop)))
                          (if (< 0 (B 'count))
                            (loop A k.A B (B 'min))
                            ((A 'enumerator/2) yield))))
                  (else (yield k.A (unite (A 'top-value) (B 'top-value)))
                        (let ((A (A 'pop))
                              (B (B 'pop)))
                          (cond ((= (A 'count) 0) ((B 'enumerator/2) yield))
                                ((= (B 'count) 0) ((A 'enumerator/2) yield))
                                (else             (loop A (A 'min) B (B 'min)))))))))))

(define (group-fold->hash en v.0 f)
  (define k=>v (hash))
  (en (lambda (k v) (set! k=>v (hash-update k=>v k
                                            (lambda (v.current) (f v v.current))
                                            v.0))))
  k=>v)

(define ((group-fold en v.0 f) yield)
  ((hash->enumerator/2 (group-fold->hash en v.0 f)) yield))

(define ((group-fold-ordered en v.0 f) yield)
  (let ((first?    #t)
        (k.current #f)
        (v.current v.0))
    (en (lambda (k v)
          (cond (first?               (set! first?    #f)
                                      (set! k.current k)
                                      (set! v.current (f v v.0)))
                ((equal? k k.current) (set! v.current (f v v.current)))
                (else                 (yield k.current v.current)
                                      (set! k.current k)
                                      (set! v.current (f v v.0))))))
    (unless first?
      (yield k.current v.current))))

(define ((enumerator-dedup en) yield)
  (define first?     #t)
  (define t.previous #f)
  (en (lambda (t)
        (cond (first?                      (set! first?     #f)
                                           (set! t.previous t)
                                           (yield t))
              ((not (equal? t t.previous)) (set! t.previous t)
                                           (yield t))))))

(define ((enumerator-project en f) yield)
  (en (lambda args (apply yield (apply f args)))))

(define ((enumerator-filter en ?) yield)
  (en (lambda (t) (when (? t) (yield t)))))

(define ((enumerator-sort en <?) yield)
  ((list->enumerator (sort (enumerator->rlist en) <?)) yield))

(define (enumerator->dict:ordered:vector en t->key)
  (dict:ordered:vector
    (enumerator->vector
      (enumerator-project
        (group-fold-ordered
          (enumerator-project (enumerator-dedup (enumerator-sort en any<?))
                              (lambda (t) (list (t->key t) t)))
          '() cons)
        (lambda (_ ts.reversed) (list (reverse ts.reversed)))))))
    (lambda (ts) (t->key (car ts)))))

(define ((hash-join en en.hash) yield)
  ((dict-join en (dict:hash (group-fold->hash en.hash '() cons)))
   (lambda (k t ts.hash)
     (for ((t.hash (in-list (reverse ts.hash))))  ; is this reversal necessary?
       (yield k t t.hash)))))

(define ((hash-antijoin en en.hash) yield)
  ((dict-antijoin en (dict:hash (group-fold->hash en.hash (void) (lambda _ (void)))))
   yield))

(define ((dict-join en d.index) yield)
  (when (< 0 (d.index 'count))
    (en (lambda (k v) (d.index 'ref k
                               (lambda (v.index) (yield k v v.index))
                               (lambda ()        (void)))))))

(define ((dict-antijoin en d.index) yield)
  (en (if (= 0 (d.index 'count))
        yield
        (lambda (k v) (unless (d.index 'has-key? k)
                        (yield k v))))))

;; TODO: computing fixed points?

(module+ test
  (require racket/pretty)

  (define (test.0 yield.0)
    (define (yield . args)
      (pretty-write `(yielding: . ,args))
      (apply yield.0 args))
    (yield 0 1)
    (yield 0 2)
    (yield 0 3)
    (yield 1 1)
    (yield 1 2)
    (yield 5 2)
    (yield 5 7))

  (define test.1 (enumerator->enumerator/2
                   (vector->enumerator '#((0 . 1)
                                          (0 . 2)
                                          (0 . 3)
                                          (1 . 1)
                                          (1 . 2)
                                          (5 . 2)
                                          (5 . 7)))))

  (displayln 'group-fold.0)
  ((group-fold test.0 0 +) (lambda (k v) (pretty-write (list k v))))

  (displayln 'group-fold-ordered.0)
  ((group-fold-ordered test.0 0 +) (lambda (k v) (pretty-write (list k v))))

  (displayln 'group-fold.1)
  ((group-fold test.1 0 +) (lambda (k v) (pretty-write (list k v))))

  (displayln 'group-fold-ordered.1)
  ((group-fold-ordered test.1 0 +) (lambda (k v) (pretty-write (list k v))))

  (displayln 'hash-join)
  ((hash-join
     (enumerator->enumerator/2 (list->enumerator '((5 . 6) (10 . 17) (8 . 33) (1 . 5) (0 . 7) (18 . 3))))
     (enumerator->enumerator/2 (list->enumerator '((7 . 61) (10 . 20) (18 . 33) (11 . 5) (0 . 77) (8 . 3)))))
   (lambda (k a b) (pretty-write (list k a b))))

  (displayln 'merge-join)
  ((merge-join
     (enumerator->dict:ordered:vector
       (list->enumerator '((5 . 6) (10 . 17) (8 . 33) (1 . 5) (0 . 7) (18 . 3)))
       car)
     (enumerator->dict:ordered:vector
       (list->enumerator '((7 . 61) (10 . 20) (18 . 33) (11 . 5) (0 . 77) (8 . 3)))
       car))
   (lambda (k a b) (pretty-write (list k a b))))

  (displayln 'merge-union)
  ((merge-union
     (enumerator->dict:ordered:vector
       (list->enumerator '((5 . 6) (10 . 17) (8 . 33) (1 . 5) (0 . 7) (18 . 3)))
       car)
     (enumerator->dict:ordered:vector
       (list->enumerator '((7 . 61) (10 . 20) (18 . 33) (11 . 5) (0 . 77) (8 . 3)))
       car)
     vector)
   (lambda (k v) (pretty-write (list k v))))

  (displayln 'hash-antijoin)
  ((hash-antijoin
     (enumerator->enumerator/2 (list->enumerator '((5 . 6) (10 . 17) (8 . 33) (1 . 5) (0 . 7) (18 . 3))))
     (enumerator->enumerator/2 (list->enumerator '((7 . 61) (10 . 20) (18 . 33) (11 . 5) (0 . 77) (8 . 3)))))
   (lambda (k v) (pretty-write (list k v))))

  (displayln 'merge-antijoin)
  ((merge-antijoin
     (enumerator->dict:ordered:vector
       (list->enumerator '((5 . 6) (10 . 17) (8 . 33) (1 . 5) (0 . 7) (18 . 3)))
       car)
     (enumerator->dict:ordered:vector
       (list->enumerator '((7 . 61) (10 . 20) (18 . 33) (11 . 5) (0 . 77) (8 . 3)))
       car))
   (lambda (k v) (pretty-write (list k v))))
  )
