#lang racket/base
(require "mk.rkt" racket/pretty)
(print-as-expression #f)
(pretty-print-abbreviate-read-macros #f)

(define-syntax-rule (test name e expected)
  (begin (printf "Testing ~s:\n" name)
         (pretty-print 'e)
         (let ((answer e))
           (unless (equal? answer expected)
             (printf "FAILED ~s:\n" name)
             (printf "  ANSWER:\n")
             (pretty-print answer)
             (printf "  EXPECTED:\n")
             (pretty-print expected)))))

(define-relation (appendo xs ys xsys)
  (conde ((== xs '()) (== ys xsys))
         ((fresh (a d res)
            (== `(,a . ,d)   xs)
            (== `(,a . ,res) xsys)
            (appendo d ys res)))))

(test 'appendo-forward
  (run* (z) (appendo '(1 2 3) '(4 5) z))
  '(((1 2 3 4 5))))
(test 'appendo-backward
  (run* (x y) (appendo x y '(1 2 3 4 5)))
  '((() (1 2 3 4 5))
    ((1) (2 3 4 5))
    ((1 2) (3 4 5))
    ((1 2 3) (4 5))
    ((1 2 3 4) (5))
    ((1 2 3 4 5) ())))
