#lang racket/base
(provide
  define-dbk dbk link import input output
  current-dbk-environment
  binding:empty binding:new binding-ref binding-set
  env:empty env:new env-ref env-set
  parser-lambda parse:module parse:module-clause parse:formula parse:term)
(require "abstract-syntax.rkt" "misc.rkt" racket/match)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Environments and bindings
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define binding:empty (hash))
(define (binding:new . args)    (make-immutable-hash (plist->alist args)))
(define (binding-ref b class)   (hash-ref b class #f))
(define (binding-set b class x) (hash-set b class x))

(define env:empty (hash))
(define (env:new . args)
  (foldl (lambda (nb env)
           (match-define (cons n b) nb)
           (env-set env n b))
         env:empty
         (plist->alist args)))
(define (env-ref env n)   (hash-ref env n binding:empty))
(define (env-set env n b) (hash-set env n b))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Parsing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (literal? x) (or (number? x) (boolean? x) (string? x) (bytes? x)))
(define (literal  x) (if (and (number? x) (inexact? x)) (inexact->exact x) x))

(define-syntax parser-lambda
  (syntax-rules ()
    ((_ env (params body ...) ...)
     (lambda (env stx)
       (simple-match stx
         (params body ...) ...
         (stx    (error "invalid syntax:" stx env)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Module parsing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (parse:module env stx)
  (unless (list? stx) (error "invalid module syntax:" stx))
  (m:link (map (lambda (stx) (parse:module-clause env stx)) stx)))

(define (binding-module-clause b) (binding-ref b 'module-clause))

(define (parse:module-clause env stx)
  (match stx
    ((? symbol? name)
     (define mc.b (binding-module-clause (env-ref env name)))
     (cond ((procedure? mc.b) (mc.b env stx))
           (else              (error "unknown module clause keyword:"
                                     name (env-ref env name)))))
    (`(,operator ,@operands)
      (define mc.b (binding-module-clause (env-ref env operator)))
      (cond ((procedure? mc.b) (mc.b env stx))
            (else              (error "unknown module clause operator:"
                                      operator (env-ref env operator)))))
    ((? procedure? self-parse) (self-parse env))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Formula parsing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (binding-formula b) (binding-ref b 'formula))

(define (parse:formula env stx)
  (match stx
    ((? literal? data) (f:const (literal data)))
    ((? symbol? name)
     (define f.b (binding-formula (env-ref env name)))
     (cond ((procedure? f.b) (f.b env stx))
           (else             (f:const (if f.b f.b name)))))
    (`(,operator ,@operands)
      (define f.b (binding-formula (env-ref env operator)))
      (cond ((procedure? f.b) (f.b env stx))
            (else             (f:relate (if f.b f.b operator)
                                        (parse:term* env operands)))))
    ((? procedure? self-parse) (self-parse env))))

(define (parse:formula* env formulas)
  (map (lambda (f) (parse:formula env f)) formulas))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Term parsing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (binding-term b) (binding-ref b 'term))

(define (parse:term env stx)
  (match stx
    ((? literal? data) (t:quote (literal data)))
    ((? symbol? name)
     (define t.b (binding-term (env-ref env name)))
     (cond ((procedure? t.b) (t.b env stx))
           (else             (t:var (if t.b t.b name)))))
    (`(,operator ,@operands)
      (define t.b (binding-term (env-ref env operator)))
      (cond ((procedure? t.b) (t.b env stx))
            (else             (t:app (parse:term  env operator)
                                     (parse:term* env operands)))))
    ((? procedure? self-parse) (self-parse env))))

(define (parse:term* env stxs) (map (lambda (stx) (parse:term env stx)) stxs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Module macro expansion
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; TODO: populate initial environment
(define current-dbk-environment (make-parameter env:empty))

(define-syntax-rule (define-dbk name body ...) (define name (dbk body ...)))

(define-syntax-rule (dbk clauses ...)          (dbk-parse () clauses ...))

(define-syntax link   (syntax-rules ()))
(define-syntax import (syntax-rules ()))
(define-syntax input  (syntax-rules ()))
(define-syntax output (syntax-rules ()))

(define-syntax dbk-parse
  (syntax-rules (link import input output)
    ((_ (parsed ...)) (m:link (list parsed ...)))

    ((_ (parsed ...) (link modules ...) clauses ...)
     (dbk-parse (parsed ... (m:link (list modules ...)))
                clauses ...))

    ((_ parsed       (import)                        clauses ...)
     (dbk-parse parsed clauses ...))
    ((_ (parsed ...) (import name value imports ...) clauses ...)
     (dbk-parse (parsed ... (m:define (hash 'name (t:quote value))))
                (import imports ...) clauses ...))

    ((_ parsed       (input)                                           clauses ...)
     (dbk-parse parsed clauses ...))
    ((_ (parsed ...) (input (relation attrs ...) io-device inputs ...) clauses ...)
     (dbk-parse (parsed ...  (m:declare 'relation
                                        (hash 'attributes '(attrs ...)
                                              'input      io-device)))
                (input inputs ...) clauses ...))

    ((_ parsed       (output)                                            clauses ...)
     (dbk-parse parsed clauses ...))
    ((_ (parsed ...) (output (relation attrs ...) io-device outputs ...) clauses ...)
     (dbk-parse (parsed ...  (m:declare 'relation
                                        (hash 'attributes '(attrs ...)
                                              'output     io-device)))
                (output outputs ...) clauses ...))

    ((_ (parsed ...) clause clauses ...)
     (dbk-parse (parsed ... (parse:module-clause
                              (current-dbk-environment)
                              'clause))
                clauses ...))))
