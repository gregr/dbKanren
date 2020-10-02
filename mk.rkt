#lang racket/base
(provide (all-from-out "mk/common.rkt") (all-from-out "mk/syntax.rkt")
         (all-from-out "mk/naive.rkt")
         run^ run run*)
(require "mk/common.rkt" "mk/naive.rkt" "mk/syntax.rkt" "stream.rkt")

;(define query->stream dfs:query->stream)
(define query->stream bis:query->stream)

(define-syntax run^
  (syntax-rules () ((_   body ...) (query->stream (query  body ...)))))
(define-syntax run
  (syntax-rules () ((_ n body ...) (s-take n      (run^   body ...)))))
(define-syntax run*
  (syntax-rules () ((_   body ...)                (run #f body ...))))
