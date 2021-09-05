#lang racket/base
(provide enumerator-append
         list->enumerator enumerator->list
         s->enumerator enumerator->s
         vector->enumerator unsafe-vector->enumerator
         generator->enumerator
         enumerator/2->enumerator
         hash->enumerator/2)
(require racket/control racket/unsafe/ops)

(define ((enumerator-append e.0 e.1) k!)
  (e.0 k!)
  (e.1 k!))

(define ((list->enumerator xs) k!) (for-each k! xs))

(define (enumerator->list en)
  (define xs '())
  (en (lambda (x) (set! xs (cons x xs))))
  (reverse xs))

(define ((s->enumerator s) k!)
  (let loop ((s s))
    (cond ((null? s) (void))
          ((pair? s) (k! (car s)) (loop (cdr s)))
          (else      (loop (s))))))

(define ((enumerator->s en))
  (define tag (make-continuation-prompt-tag))
  (reset-at tag
            (en (lambda (x)
                  (shift-at tag k (cons x (lambda () (k (void)))))))
            '()))

(define (vector->enumerator v (start 0) (end (vector-length v)))
  (define len (min end (vector-length v)))
  (unsafe-vector->enumerator v (min start len) len))

(define ((unsafe-vector->enumerator v (start 0) (end (unsafe-vector*-length v))) k!)
  (let loop ((i start))
    (when (unsafe-fx< i end)
      (k!   (unsafe-vector*-ref v i))
      (loop (unsafe-fx+ i 1)))))

(define ((generator->enumerator gen stop?) k!)
  (let loop ()
    (define x (gen))
    (unless (stop? x)
      (k! x)
      (loop))))

;; An enumerator/2 expects its iteratee to take two arguments
(define ((enumerator/2->enumerator en) k!)
  (en (lambda (a b) (k! (cons a b)))))

(define ((hash->enumerator/2 kvs) k!) (hash-for-each kvs k!))
