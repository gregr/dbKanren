#lang racket/base
(provide (all-from-out "mk/syntax.rkt") (all-from-out "mk/dfs.rkt")
         run^ run run*)
(require "mk/dfs.rkt" "mk/syntax.rkt" "stream.rkt")

;; TODO: move beyond DFS once other strategies are ready
(define-syntax run^
  (syntax-rules () ((_   body ...) (dfs:query->stream (query  body ...)))))
(define-syntax run
  (syntax-rules () ((_ n body ...) (s-take n          (run^   body ...)))))
(define-syntax run*
  (syntax-rules () ((_   body ...)                    (run #f body ...))))
