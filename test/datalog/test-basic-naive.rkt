#lang racket/base
(require "basic-naive.rkt")

(define-relation/facts (edge a b)
                       '((a b)
                         (b c)
                         (d e)
                         (e f)
                         (b f)
                         (f a)))

(define-relation (path a b)
  (conde ((edge a b))
         ((fresh (mid)
            (edge a mid)
            (path mid b)))))

(run* (x)   (path 'a x))
(run* (x)   (path x 'f))
(run* (x y) (path x y))
(run* (x y) (edge x y))
