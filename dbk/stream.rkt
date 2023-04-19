#lang racket/base
(provide s-next s-force s-split s-drop
         s-take/set/steps s-take/set s-take/steps s-take
         s-each s-foldr s-foldl s-split-foldl s-scan
         s-append/interleaving s-append*/interleaving
         s-append s-append* s-map/append s-map s-filter s-group s-memo s-lazy
         s-length s-enumerate s-dedup s-skip s-limit
         s-chunk s-unchunk s->list list->s)
(require ;"safe-unsafe.rkt"
         racket/unsafe/ops
         racket/function racket/match racket/set racket/vector)

(define (s-next  s) (if (procedure? s)          (s)  s))
(define (s-force s) (if (procedure? s) (s-force (s)) s))

;; lazy variant of s-drop
(define (s-skip n s)
  (cond ((= n 0)   s)
        ((pair? s) (s-skip (- n 1) (cdr s)))
        (else      (thunk (s-skip n (s))))))
;; lazy variant of s-take
(define (s-limit n s)
  (cond ((or (= n 0) (null? s)) '())
        ((pair? s)              (cons (car s) (s-limit (- n 1) (cdr s))))
        (else                   (thunk (s-limit n (s))))))

(define (s-foldl f acc s)
  (let loop ((acc acc) (s s))
    (cond ((null?      s) acc)
          ((procedure? s) (loop acc (s)))
          (else           (loop (f (car s) acc) (cdr s))))))

(define (s-split-foldl n f acc s)
  (let loop ((n n) (acc acc) (s s))
    (cond ((= n 0)        (values acc s))
          ((null?      s) (values acc '()))
          ((procedure? s) (loop n acc (s)))
          (else           (loop (- n 1) (f (car s) acc) (cdr s))))))

(define (s-split n s)
  (let-values (((rx* s.remaining) (s-split-foldl n cons '() s)))
    (values (reverse rx*) s.remaining)))

(define (s-take/set/steps steps n s)
  (if (and n (= n 0)) (set)
    (let loop ((steps steps) (s s) (acc (set)))
      (match s
        ((? procedure? s) (if (and steps (= steps 0))
                            acc
                            (loop (and steps (- steps 1)) (s) acc)))
        ('()              acc)
        ((cons x s)       (define xs (set-add acc x))
                          (if (and n (= n (set-count xs)))
                            xs
                            (loop steps s xs)))))))

(define (s-take/steps steps n s)
  (if (and n (= n 0)) '()
    (match s
      ((? procedure? s) (if (and steps (= steps 0))
                          '()
                          (s-take/steps (and steps (- steps 1)) n (s))))
      ('()              '())
      ((cons x s)       (cons x (s-take/steps steps (and n (- n 1)) s))))))

(define (s-take/set n s) (s-take/set/steps #f n s))
(define (s-take     n s) (s-take/steps #f n s))
(define (s-drop     n s) (let-values (((_ s) (s-split-foldl n (lambda (_ acc) #t) #t s))) s))

;; equivalent to (s-take #f s)
(define (s->list s)
  (cond ((null? s) '())
        ((pair? s) (cons (car s) (s->list (cdr s))))
        (else      (s->list (s)))))
;; equivalent to s-lazy for a list
(define (list->s xs)
  (thunk (if (null? xs)
           '()
           (cons (car xs) (list->s (cdr xs))))))

;; TODO: generalize to multiple streams
(define (s-foldr f acc s)
  (cond ((null? s) acc)
        ((pair? s) (f (car s) (s-foldr f acc (cdr s))))
        (else      (thunk (s-foldr f acc (s))))))

(define (s-append*/interleaving s*) (s-foldr s-append/interleaving '() s*))
;; TODO: generalize to multiple streams
(define (s-append/interleaving s1 s2)
  (cond ((null?      s1) (s2))
        ((procedure? s1) (thunk (s-append/interleaving (s2) s1)))
        (else (define d1  (cdr s1))
              (define s1^ (if (procedure? d1) d1 (thunk d1)))
              (cons (car s1) (thunk (s-append/interleaving (s2) s1^))))))

(define (s-append* ss) (s-foldr s-append '() ss))
;; TODO: generalize to multiple streams
(define (s-append a b) (s-foldr cons b a))
(define (s-filter ? s) (s-foldr (lambda (x acc) (if (? x) (cons x acc) acc))
                                '() s))

(define s-map
  (case-lambda
    ((f s)
     (let loop ((s s))
       (cond ((null?      s) '())
             ((procedure? s) (lambda () (loop (s))))
             (else           (cons (f (car s)) (loop (cdr s)))))))
    ((f s . s*)
     (let loop.outer ((s s) (s* s*))
       (cond ((null?      s) '())
             ((procedure? s) (lambda () (loop.outer (s) s*)))
             (else (let loop ((s*-pending s*) (rs* '()))
                     (if (null? s*-pending)
                         (let ((s* (reverse rs*)))
                           (cons (apply f (car s) (map car s*))
                                 (loop.outer (cdr s) (map cdr s*))))
                         (let next ((s*0 (car s*-pending)))
                           (cond ((procedure? s*0) (lambda () (next (s*0))))
                                 (else (loop (cdr s*-pending) (cons s*0 rs*)))))))))))))

;; TODO: generalize to multiple streams
(define (s-map/append f s)
  (s-foldr (lambda (x rest) (s-append (f x) rest))
           '() s))

(define (s-each p s) (let ((s (s-force s)))
                       (unless (null? s) (p (car s)) (s-each p (cdr s)))))

(define (s-scan s acc f)
  (cons acc (cond ((null? s) '())
                  ((pair? s) (s-scan (cdr s) (f (car s) acc) f))
                  (else      (thunk (s-scan (s) acc f))))))

(define (s-length s) (s-foldl (lambda (_ l) (+ l 1)) 0 s))

(define (s-group s ? @)
  (let ((@ (or @ (lambda (x) x))))
    (cond ((null? s)      '())
          ((procedure? s) (thunk (s-group (s) ? @)))
          (else (let next ((x (@ (car s))) (s s))
                  (let loop ((g (list (car s))) (s (cdr s)))
                    (cond ((null? s)      (list g))
                          ((procedure? s) (thunk (loop g (s))))
                          (else (let ((y (@ (car s))))
                                  (if (? y x) (loop (cons (car s) g) (cdr s))
                                    (cons g (next y s))))))))))))

(define (s-chunk s len.chunk)
  (cond
    ((<= len.chunk 0) (error "chunk length must be positive" len.chunk))
    ((=  len.chunk 1) (s-map vector s))
    (else (let new ((s s))
            (cond ((null?      s) '())
                  ((procedure? s) (lambda () (new (s))))
                  (else (let ((chunk (make-vector len.chunk)))
                          (unsafe-vector*-set! chunk 0 (car s))
                          (let loop ((s (cdr s)) (i 1))
                            (cond
                              ((null?      s) (list (vector-copy chunk 0 i)))
                              ((procedure? s) (lambda () (loop (s) i)))
                              (else (unsafe-vector*-set! chunk i (car s))
                                    (let ((i (unsafe-fx+ i 1)))
                                      (if (unsafe-fx< i len.chunk)
                                          (loop (cdr s) i)
                                          (cons chunk (new (cdr s)))))))))))))))

(define (s-unchunk s)
  (let next ((s s))
    (cond ((null?      s) '())
          ((procedure? s) (lambda () (next (s))))
          (else (let* ((x* (car s)) (len.chunk (vector-length x*)))
                  (let loop ((i 0))
                    (if (unsafe-fx< i len.chunk)
                        (cons (unsafe-vector*-ref x* i) (loop (unsafe-fx+ i 1)))
                        (next (cdr s)))))))))

(define (s-memo s)
  (cond ((procedure? s) (let ((v #f) (s s))
                          (thunk (when s (set! v (s-memo (s))) (set! s #f))
                                 v)))
        ((null? s)      '())
        (else           (cons (car s) (s-memo (cdr s))))))

(define (s-lazy s)
  (define (return s)
    (cond ((null? s) '())
          (else      (cons (car s) (s-lazy (cdr s))))))
  (thunk (cond ((procedure? s) (let retry ((s (s)))
                                 (cond ((procedure? s) (thunk (retry (s))))
                                       (else           (return s)))))
               (else           (return s)))))

(define (s-enumerate i s)
  (cond ((null? s) '())
        ((pair? s) (cons (cons i (car s)) (s-enumerate (+ i 1) (cdr s))))
        (else      (thunk                 (s-enumerate i           (s))))))

;; NOTE: only adjacent duplicates are removed
(define (s-dedup s)
  (define (loop x s)
    (cond ((null? s) (list x))
          ((pair? s) (if (equal? x (car s)) (loop x (cdr s))
                       (cons x (loop (car s) (cdr s)))))
          (else      (thunk (loop x (s))))))
  (cond ((null? s) '())
        ((pair? s) (loop (car s) (cdr s)))
        (else      (thunk (s-dedup (s))))))
