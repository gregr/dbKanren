#lang racket/base
(provide (all-from-out "mk/common.rkt") (all-from-out "mk/syntax.rkt")
         (all-from-out "mk/naive.rkt")
         run^ run run*)
(require "mk/common.rkt" "mk/naive.rkt" "mk/syntax.rkt" "stream.rkt")

;(define-syntax run^
  ;(syntax-rules () ((_   body ...) (dfs:query->stream (query  body ...)))))
(define-syntax run^
  (syntax-rules () ((_   body ...) (bis:query->stream (query  body ...)))))
(define-syntax run
  (syntax-rules () ((_ n body ...) (s-take n          (run^   body ...)))))
(define-syntax run*
  (syntax-rules () ((_   body ...)                    (run #f body ...))))
