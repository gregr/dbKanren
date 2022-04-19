#lang racket/base
(require "basic-naive.rkt" racket/pretty)
(print-as-expression #f)
;(pretty-print-abbreviate-read-macros #f)

(define-syntax-rule
  (pretty-results example ...)
  (begin (begin (pretty-write 'example)
                (pretty-write '==>)
                (pretty-write example)
                (newline)) ...))

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
  (run* (x y) (edge x y))
  )
