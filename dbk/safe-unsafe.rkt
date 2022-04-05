#lang racket/base
(provide
  unsafe-fx=
  unsafe-fx<=
  unsafe-fx<
  unsafe-fx+
  unsafe-fx-
  unsafe-fx*
  unsafe-fxand
  unsafe-fxlshift
  unsafe-fxrshift
  unsafe-bytes-length
  unsafe-bytes-ref
  unsafe-bytes-set!
  unsafe-fxvector-length
  unsafe-fxvector-ref
  unsafe-fxvector-set!)
(require racket/fixnum)

(define unsafe-fx=             fx=)
(define unsafe-fx<=            fx<=)
(define unsafe-fx<             fx<)
(define unsafe-fx+             fx+)
(define unsafe-fx-             fx-)
(define unsafe-fx*             fx*)
(define unsafe-fxand           fxand)
(define unsafe-fxlshift        fxlshift)
(define unsafe-fxrshift        fxrshift)
(define unsafe-bytes-length    bytes-length)
(define unsafe-bytes-ref       bytes-ref)
(define unsafe-bytes-set!      bytes-set!)
(define unsafe-fxvector-length fxvector-length)
(define unsafe-fxvector-ref    fxvector-ref)
(define unsafe-fxvector-set!   fxvector-set!)