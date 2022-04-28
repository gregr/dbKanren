#lang racket/base
(provide run-stratified var? ==)
(require "micro-plus.rkt" (except-in racket/match ==))

(define (atom-vars atom) (filter var? atom))

(define (rule-safe?! rule)
  (let ((vars.body (apply append (map atom-vars (cdr rule)))))
    (for-each (lambda (var.head) (or (member var.head vars.body)
                                     (error "unsafe rule" rule)))
              (atom-vars (car rule)))))

(define (parse-term expr)
  (match expr
    ((? symbol?) (var expr))
    (`(quote ,c) c)
    ((cons _ _)  (error "unsupported function call" expr))
    (_           expr)))

(define (parse-atom expr) (cons (car expr) (map parse-term (cdr expr))))
(define (parse-rule expr) (let ((rule (map parse-atom expr)))
                            (rule-safe?! rule)
                            rule))

(define (run-stratified
          predicate=>proc predicate=>merge non-monotonic-predicates
          e**.rules F*)
  (define (enforce rule)
    (define (body atom)
      (let ((proc (hash-ref predicate=>proc (car atom) #f)))
        (if proc
          (compute proc (cdr atom))
          (relate atom))))
    (realize (car rule) (conj* (map body (cdr rule)))))
  (foldr (lambda (p* F*)
           (exhaust* p* predicate=>merge non-monotonic-predicates F*))
         F*
         (map (lambda (e*.rules) (map enforce (map parse-rule e*.rules)))
              e**.rules)))
