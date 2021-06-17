#lang racket/base
(provide
  fresh-name with-fresh-names
  f:relate f:implies f:iff f:or f:and f:not f:exist f:all
  f:== f:=/= f:any<= f:any<
  t:query t:quote t:var t:prim t:app t:lambda t:if t:let
  t:map/merge t:cons t:car t:cdr t:vector t:list->vector t:vector-ref t:vector-length t:append t:not
  scm->term)
(require "misc.rkt"
         (except-in racket/match ==) racket/set)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Names
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define fresh-name-count (make-parameter #f))

(define (call-with-fresh-names thunk)
  (if (fresh-name-count)
    (thunk)
    (parameterize ((fresh-name-count 0))
      (thunk))))

(define-syntax-rule (with-fresh-names body ...)
  (call-with-fresh-names (lambda () body ...)))

(define (fresh-name name)
  (define uid.next (fresh-name-count))
  (unless uid.next (error "fresh name not available:" name))
  (fresh-name-count (+ uid.next 1))
  (cons uid.next (if (pair? name) (cdr name) name)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Abstract syntax
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define-variant formula?
  (f:relate  relation args)
  (f:implies if then)
  (f:iff     f1 f2)
  (f:or      f1 f2)
  (f:and     f1 f2)
  (f:not     f)
  (f:exist   params body)
  (f:all     params body))

(define (f:==    u v) (f:relate '(prim ==)    (list u v)))
(define (f:=/=   u v) (f:relate '(prim =/=)   (list u v)))
(define (f:any<= u v) (f:relate '(prim any<=) (list u v)))
(define (f:any<  u v) (f:relate '(prim any<)  (list u v)))

(define-variant term?
  (t:query  name formula)
  (t:quote  value)
  (t:var    name)
  (t:prim   name)
  (t:app    proc args)
  (t:lambda params body)
  (t:if     c t f)
  (t:let    bpairs body))

(define (t:map/merge f merge default xs) (t:app (t:prim 'map/merge)     (list f merge default xs)))
(define (t:cons a d)                     (t:app (t:prim 'cons)          (list a d)))
(define (t:car p)                        (t:app (t:prim 'car)           (list p)))
(define (t:cdr p)                        (t:app (t:prim 'cdr)           (list p)))
(define (t:vector . args)                (t:app (t:prim 'vector)        args))
(define (t:list->vector xs)              (t:app (t:prim 'list->vector)  (list xs)))
(define (t:vector-ref v i)               (t:app (t:prim 'vector-ref)    (list v i)))
(define (t:vector-length v)              (t:app (t:prim 'vector-length) (list v)))
(define (t:append xs ys)                 (t:app (t:prim 'append)        (list xs ys)))
(define (t:not x)                        (t:app (t:lambda '(x) (t:if (t:var 'x)
                                                                     (t:quote #f)
                                                                     (t:quote #t)))
                                                (list x)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Values and term conversion
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (atom? x)
  (or (null? x) (boolean? x) (symbol? x) (string? x) (bytes? x) (and (real? x) (exact? x)) (void? x)))

(define (scm->term x)
  (cond ((term?         x)  x)
        ((pair?         x)  (t:cons         (scm->term (car          x))
                                            (scm->term (cdr          x))))
        ((vector?       x)  (t:list->vector (scm->term (vector->list x))))
        ((atom?         x)  (t:quote        x))
        ((and (real?    x)
              (inexact? x)) (scm->term (inexact->exact x)))
        (else               (error "invalid dbk value:" x))))
