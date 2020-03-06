#lang racket/base
(provide s-next s-prefix s-prefix! s-take s-drop s-append
         s-filter s-map s-each s-fold s-scan s-group s-memo)
(require racket/function)

(define (s-next s) (if (procedure? s) (s) s))

(define (s-prefix n rxs s)
  (if (and n (= n 0)) (cons rxs s)
    (let ((s (s-next s)))
      (if (null? s) (cons rxs s)
        (s-prefix (and n (- n 1)) (cons (car s) rxs) (cdr s))))))

(define (s-prefix! v n s)
  (let loop ((i 0) (s s))
    (if (= i n) (cons i s)
      (let ((s (s-next s)))
        (cond ((null? s) (cons i s))
              (else (vector-set! v i (car s)) (loop (+ i 1) (cdr s))))))))

(define (s-take n s)
  (if (and n (= n 0)) '()
    (let ((s (s-next s)))
      (if (null? s) '() (cons (car s) (s-take (and n (- n 1)) (cdr s)))))))

(define (s-drop n s)
  (if (= n 0) s (let ((s (s-next s)))
                  (if (null? s) '() (s-drop (- n 1) (cdr s))))))

(define (s-append a b)
  (define (k a) (if (null? a) b (cons (car a) (s-append (cdr a) b))))
  (if (procedure? a) (thunk (k (a))) (k a)))

(define (s-filter ? s)
  (thunk (let loop ((s (s-next s)))
           (cond ((null? s)   '())
                 ((? (car s)) (cons (car s) (s-filter ? (cdr s))))
                 (else        (loop (s-next (cdr s))))))))

(define (s-map f s)
  (thunk (let ((s (s-next s)))
           (if (null? s) '() (cons (f (car s)) (s-map f (cdr s)))))))

(define (s-each s p) (let ((s (s-next s)))
                       (unless (null? s) (p (car s)) (s-each (cdr s) p))))

(define (s-fold n s acc f)
  (if (and n (= n 0)) (cons acc s)
    (let ((s (s-next s)))
      (if (null? s) (list acc)
        (s-fold (and n (- n 1)) (cdr s) (f (car s) acc) f)))))

(define (s-scan s acc f)
  (cons acc (thunk (let ((s (s-next s)))
                     (if (null? s) '() (s-scan (cdr s) (f (car s) acc) f))))))

(define (s-group s ? @)
  (let ((@ (or @ (lambda (x) x))))
    (thunk (let ((s (s-next s)))
             (if (null? s) '()
               (let next ((x (@ (car s))) (s s))
                 (let loop ((g (list (car s))) (s (cdr s)))
                   (let ((s (s-next s)))
                     (if (null? s) (list g)
                       (let ((y (@ (car s))))
                         (if (? y x) (loop (cons (car s) g) (cdr s))
                           (cons g (thunk (next y s))))))))))))))

(define (s-memo s)
  (cond ((procedure? s) (let ((v #f) (s s))
                          (thunk (when s (set! v (s-memo (s))) (set! s #f))
                                 v)))
        ((null? s)      '())
        (else           (cons (car s) (s-memo (cdr s))))))
