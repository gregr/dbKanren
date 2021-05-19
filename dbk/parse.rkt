#lang racket/base
(provide
  define-dbk dbk link import input output
  current-dbk-environment
  binding:empty binding-ref binding-set binding-set* binding-remove binding-alist/class
  env:empty env-ref env-set env-set* env-set-alist
  env-bind env-bind* env-bind-alist env-map/merge env-forget-pattern-variables
  literal? literal parser-lambda
  parse:module parse:module-clause parse:formula parse:term)
(require "abstract-syntax.rkt" "misc.rkt"
         racket/list racket/match racket/set racket/struct)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Names and parameter trees
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; TODO: maybe use a dynamic uid parameter, and instead represent names as pairs of uid and symbol
(struct uname (name)
        #:methods gen:custom-write
        ((define write-proc
           (make-constructor-style-printer
             (lambda (x) 'uname)
             (lambda (x) (list (uname-name x)))))))

(define (fresh-name name)
  (if (symbol? name) (uname name) (fresh-name (uname-name name))))

(define (param-names param)
  (match param
    ((? symbol?)    (list param))
    ('()            '())
    ((cons p.a p.d) (append (param-names p.a) (param-names p.d)))
    ((? vector?)    (param-names (vector->list param)))))

(define (unique? names) (= (set-count (list->set names)) (length names)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Environments and bindings
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define binding:empty (hash))
(define (binding-ref  b class)   (hash-ref b class #f))
(define (binding-set  b class x) (hash-set b class x))
(define (binding-set* b cs    xs)
  (foldl (lambda (c x b) (binding-set b c x))
         binding:empty cs xs))
(define (binding-remove b class) (hash-remove b class))

(define (binding-alist/class class . args)
  (map (lambda (nv)
         (match-define (cons n v) nv)
         (cons n (binding-set binding:empty class v)))
       (plist->alist args)))

(define env:empty (hash))
(define (env-ref       env n)     (hash-ref env n binding:empty))
(define (env-set       env n  b)  (hash-set env n b))
(define (env-set*      env ns bs) (foldl (lambda (n b env) (env-set env n b)) env ns bs))
(define (env-set-alist env nbs)   (env-set* env (map car nbs) (map cdr nbs)))

(define (env-bind  env class name  value)
  (env-set env name (binding-set binding:empty class value)))
(define (env-bind* env class names values)
  (env-set* env names (map (lambda (v) (binding-set binding:empty class v))
                           values)))
(define (env-bind-alist env class nvs)
  (env-bind* env class (map car nvs) (map cdr nvs)))

(define (env-map/merge env default f merge)
  (if (hash-empty? env)
    default
    (let ((mapped (map f (hash->list env))))
      (foldl merge (car mapped) (cdr mapped)))))

(define (env-forget-pattern-variables env)
  (env-set-alist
    env:empty
    (env-map/merge
      env env:empty
      (lambda (nb)
        (match-define (cons n b) nb)
        (define current (binding-ref b 'term))
        (list (cons n (if (and current (not (procedure? current)))
                        (binding-remove b 'term)
                        b))))
      append)))

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
