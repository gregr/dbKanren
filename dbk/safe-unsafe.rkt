#lang racket/base
(provide
  unsafe-car
  unsafe-cdr
  unsafe-fxmin
  unsafe-fx=
  unsafe-fx<=
  unsafe-fx<
  unsafe-fx+
  unsafe-fx-
  unsafe-fx*
  unsafe-fxand
  unsafe-fxior
  unsafe-fxxor
  unsafe-fxnot
  unsafe-fxlshift
  unsafe-fxrshift
  unsafe-fxquotient
  unsafe-bytes-length
  unsafe-bytes-ref
  unsafe-bytes-set!
  unsafe-bytes-copy!
  unsafe-vector*-length
  unsafe-vector*-ref
  unsafe-vector*-set!
  unsafe-fxvector-length
  unsafe-fxvector-ref
  unsafe-fxvector-set!)
(require racket/fixnum)

(define unsafe-list-ref        list-ref)
(define unsafe-car             car)
(define unsafe-cdr             cdr)
(define unsafe-fxmin           fxmin)
(define unsafe-fx=             fx=)
(define unsafe-fx<=            fx<=)
(define unsafe-fx<             fx<)
(define unsafe-fx+             fx+)
(define unsafe-fx-             fx-)
(define unsafe-fx*             fx*)
(define unsafe-fxand           fxand)
(define unsafe-fxior           fxior)
(define unsafe-fxxor           fxxor)
(define unsafe-fxnot           fxnot)
(define unsafe-fxlshift        fxlshift)
(define unsafe-fxrshift        fxrshift)
(define unsafe-fxquotient      fxquotient)
(define unsafe-bytes-length    bytes-length)
(define unsafe-bytes-ref       bytes-ref)
(define unsafe-bytes-set!      bytes-set!)
(define unsafe-bytes-copy!     bytes-copy!)
(define unsafe-vector*-length  vector-length)
(define unsafe-vector*-ref     vector-ref)
(define unsafe-vector*-set!    vector-set!)
(define unsafe-fxvector-length fxvector-length)
(define unsafe-fxvector-ref    fxvector-ref)
(define unsafe-fxvector-set!   fxvector-set!)
