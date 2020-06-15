#lang racket/base
(provide s-next s-force s-prefix! s-split s-take s-drop s-append
         s-filter s-map s-each s-fold s-scan s-group s-memo s-enumerate)
(require racket/function)

(define (s-next  s) (if (procedure? s)          (s)  s))
(define (s-force s) (if (procedure? s) (s-force (s)) s))

(define (s-prefix! v n s)
  (let loop ((i 0) (s s))
    (if (= i n) (cons i s)
      (let ((s (s-force s)))
        (cond ((null? s) (cons i s))
              (else (vector-set! v i (car s)) (loop (+ i 1) (cdr s))))))))

(define (s-split n s)
  (let loop ((n n) (rxs '()) (s s))
    (if (and n (= n 0)) (cons (reverse rxs) s)
      (let ((s (s-force s)))
        (if (null? s) (cons (reverse rxs) s)
          (loop (and n (- n 1)) (cons (car s) rxs) (cdr s)))))))

(define (s-take n s)
  (if (and n (= n 0)) '()
    (let ((s (s-force s)))
      (if (null? s) '() (cons (car s) (s-take (and n (- n 1)) (cdr s)))))))

(define (s-drop n s)
  (if (= n 0) s (let ((s (s-force s)))
                  (if (null? s) '() (s-drop (- n 1) (cdr s))))))

(define (s-append a b)
  (define (k a) (if (null? a) b (cons (car a) (s-append (cdr a) b))))
  (if (procedure? a) (thunk (k (s-force a))) (k a)))

(define (s-filter ? s)
  (thunk (let loop ((s (s-force s)))
           (cond ((null? s)   '())
                 ((? (car s)) (cons (car s) (s-filter ? (cdr s))))
                 (else        (loop (s-force (cdr s))))))))

(define (s-map f s . ss)
  (thunk (let ((s (s-force s)) (ss (map s-force ss)))
           (if (null? s) '()
             (cons (apply f (car s) (map car ss))
                   (apply s-map f (cdr s) (map cdr ss)))))))

(define (s-each s p) (let ((s (s-force s)))
                       (unless (null? s) (p (car s)) (s-each (cdr s) p))))

(define (s-fold n s acc f)
  (if (and n (= n 0)) (cons acc s)
    (let ((s (s-force s)))
      (if (null? s) (list acc)
        (s-fold (and n (- n 1)) (cdr s) (f (car s) acc) f)))))

(define (s-scan s acc f)
  (cons acc (thunk (let ((s (s-force s)))
                     (if (null? s) '() (s-scan (cdr s) (f (car s) acc) f))))))

(define (s-group s ? @)
  (let ((@ (or @ (lambda (x) x))))
    (thunk (let ((s (s-force s)))
             (if (null? s) '()
               (let next ((x (@ (car s))) (s s))
                 (let loop ((g (list (car s))) (s (cdr s)))
                   (let ((s (s-force s)))
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

(define (s-enumerate i s) (thunk (let ((s (s-force s)))
                                   (if (null? s) '()
                                     (cons (cons i (car s))
                                           (s-enumerate (+ i 1) (cdr s)))))))
