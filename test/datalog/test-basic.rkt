#lang racket/base
(require "basic-naive.rkt" racket/list racket/pretty)
(print-as-expression #f)
;(pretty-print-abbreviate-read-macros #f)

(define-syntax-rule
  (pretty-results example ...)
  (begin (begin (pretty-write 'example)
                (pretty-write '==>)
                (pretty-write example)
                (newline)) ...))

;;;;;;;;;;;;;;;;;;;;;;;
;;; Graph traversal ;;;
;;;;;;;;;;;;;;;;;;;;;;;

(define-relation (edge a b)
  (facts (list a b)
          '((a b)
            (b c)
            (d e)
            (e f)
            (b f)
            (f a)  ; comment this edge for an acyclic graph
            )))

(define-relation (path a b)
  (conde ((edge a b))
          ((fresh (mid)
            (edge a mid)
            (path mid b)))))

(pretty-results
  (run* (x)   (path 'a x))
  (run* (x)   (path x 'f))
  (run* (x y) (path x y))
  (run* (x y) (edge x y)))


;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Finite arithmetic ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;

(define-relation (+o a b c)
  (facts (list a b c)
         (append* (map (lambda (a)
                         (map (lambda (b) (list a b (+ a b)))
                              (range 100)))
                       (range 100)))))

(define-relation (*o a b c)
  (facts (list a b c)
         (append* (map (lambda (a)
                         (map (lambda (b) (list a b (* a b)))
                              (range 100)))
                       (range 100)))))

(pretty-results
  (run* (a b) (+o a b 7))
  (run* (a b) (*o a b 7))
  (run* (a b) (*o a b 18)))


;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Finite path length ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;

(define roads '((a b 1)
                (a c 7)
                (b c 1)
                (c d 1)
                (d a 50)  ; shrink this distance to illustrate bad performance
                ))

(define-relation (road source target distance)
  (facts (list source target distance) roads))

(define-relation (route s t d)
  (conde ((road s t d))
         ((fresh (mid d.0 d.rest)
            (road s mid d.0)
            (route mid t d.rest)
            (+o d.0 d.rest d)))))

(pretty-results
  (run* (s t d) (route s t d))
  (run* d (route 'a 'd d))
  (apply min (run* d (route 'a 'd d))))


;;;;;;;;;;;;;;;;;;;;;;;
;;; Mutable counter ;;;
;;;;;;;;;;;;;;;;;;;;;;;

(define count.current 0)

(define-relation (count x) (== x count.current))
(define-relation (next-count next)
  (fresh (current)
    (+o current 1 next)
    (count current)))

(define (increment!)
  (define (now) (car (run* current (count current))))
  (pretty-write `(current count: ,(now)))
  (set! count.current (car (run* next (next-count next))))
  (pretty-write `(next count: ,(now))))

(for-each (lambda (_) (increment!)) (range 10))
