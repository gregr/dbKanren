#lang racket/base
(provide m.base)

(define-dbk m.base
  (declare (member x ys)
    modes ((ys)))  ;; this mode could be inferred
  (<<= (member x (cons x ys)))
  (<<= (member x (cons y ys))
    (=/= x y) (member x ys))
  ;; member can also be defined with a single rule:
  ;(<<= (member x ys)
  ;  (exist (a d)
  ;    (== ys `(,a . ,d))
  ;    (or (== x a)
  ;        (and (=/= x a) (member x d)))))

  (declare (append xs ys xsys)
    modes ((xs) (xsys)))  ;; these modes could be inferred
  (<<= (append '() ys ys))
  (<<= (append `(,x . ,xs) ys `(,x . ,xsys))
    (append xs ys xsys))

  ;; Can this theorem be proven?
  ;(assert (all (xs ys)
  ;          (iff (append xs ys ys)
  ;               (== xs '()))))
  )
