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

(define (rule-parser type)
  (parser-lambda env
    ((_ (relation . params) . formulas)
     (define env.pattern  (env-forget-pattern-variables env))
     (define ts.params    (map (lambda (p) (parse:term env.pattern p)) params))
     (define names.params (set->list (t-free-vars-first-order* ts.params)))
     (define names.argument
       (map (lambda (i) (fresh-name (string->symbol (string-append "x." (number->string i)))))
            (range (length params))))
     (m:rule type relation names.argument
             (parse:formula:exist
               env (list* (void) names.params
                          (lambda (env)
                            (foldl f:and
                                   (f:== (t:quote #t) (t:quote #t))
                                   (map (lambda (n p) (f:== (t:var n) (parse:term env p)))
                                        names.argument params)))
                          formulas))))))

(define parse:module-clause:define
  (parser-lambda env
    ((_ (name . params) body) (m:define (hash name (parse:term:lambda env (list (void) params body)))))
    ((_ name            body) (m:define (hash name (parse:term        env                     body))))))

(define parse:module-clause:assert
  (parser-lambda env ((_ formula) (m:assert (parse:formula env formula)))))

(define bindings.initial.module
  (binding-alist/class
    'module-clause
    'define  parse:module-clause:define
    ;; TODO:
    ;'declare

    '<<=     (rule-parser '<<=)
    '<<+     (rule-parser '<<+)
    '<<-     (rule-parser '<<-)
    '<<~     (rule-parser '<<~)
    'assert  parse:module-clause:assert))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Formula parsing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; TODO: parse relation argument underscores as implicit existentials

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

(define parse:formula:or
  (parser-lambda env
    ((_ disjunct)             (parse:formula env disjunct))
    ((_ disjunct . disjuncts) (f:or (parse:formula env disjunct)
                                    (parse:formula:or env (cons (void) disjuncts))))))

(define parse:formula:and
  (parser-lambda env
    ((_ conjunct)             (parse:formula env conjunct))
    ((_ conjunct . conjuncts) (f:and (parse:formula env conjunct)
                                     (parse:formula:and env (cons (void) conjuncts))))))

(define parse:formula:not
  (parser-lambda env
    ((_ f) (f:not (parse:formula env f)))))

(define parse:formula:exist
  (parser-lambda env
    ((_ params . body)
     (define names (param-names params))
     (unless (unique? names)
       (error "existential quantifier parameter names must be unique:" names))
     (define unames (map fresh-name names))
     (f:exist unames
              (parse:formula:and
                (env-bind* env 'term names unames)
                (cons (void) body))))))

(define parse:formula:=/=
  (parser-lambda env
    ((_ u v) (f:=/= (parse:term env u) (parse:term env v)))))

(define bindings.initial.formula
  (binding-alist/class
    'formula
    '=/=   parse:formula:=/=
    'or    parse:formula:or
    'and   parse:formula:and
    'not   parse:formula:not
    'exist parse:formula:exist))

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

(define parse:term:query
  (parser-lambda env
    ((_ param-pattern . body)
     (cond ((symbol? param-pattern)
            (define param (fresh-name param-pattern))
            (t:query param (parse:formula:and
                             (env-bind env 'term param-pattern param)
                             (cons (void) body))))
           (else (define param (fresh-name 'q.0))
                 (define names (param-names param-pattern))
                 (define (assign-param-pattern env)
                   (f:== param (parse:term:simple env param-pattern)))
                 (t:query param (parse:formula:exist
                                  env (list* (void) names assign-param-pattern body))))))))

(define (parse:term:simple env pattern)
  (let loop ((pattern pattern))
    (match pattern
      ((? symbol?)    (t:var (binding-term (env-ref env pattern))))
      ('()            (t:quote '()))
      ((cons p.a p.d) (t:cons (loop p.a) (loop p.d)))
      ((? vector?)    (t:list->vector (loop (vector->list pattern)))))))

(define parse:term:quote
  (parser-lambda env ((_ value) (t:quote value))))

(define parse:term:quasiquote
  (parser-lambda env
    ((_ template)
     (define ((keyword? k) n) (eq? k (binding-ref (env-ref env n) 'quasiquote)))
     (define (lift tag e)     (t:cons (t:quote tag) (t:cons e (t:quote '()))))
     ;; NOTE: unquote-splicing support requires a safe definition of append
     (let loop ((t template) (level 0))
       (match t
         ((list (? (keyword? 'unquote)    k) e) (if (= level 0)
                                                  (parse:term env e)
                                                  (lift k (loop e (- level 1)))))
         ((list (? (keyword? 'quasiquote) k) t) (lift k (loop t (+ level 1))))
         (`(,t.a . ,t.d)                        (t:cons (loop t.a level) (loop t.d level)))
         ((? vector?)                           (t:list->vector (loop (vector->list t) level)))
         ((or (? (keyword? 'quasiquote))
              (? (keyword? 'unquote)))          (error "invalid quasiquote:" t template))
         (v                                     (t:quote v)))))))

(define parse:term:lambda
  (parser-lambda env
    ((_ params body)
     (define names (param-names params))
     (unless (unique? names)
       (error "lambda parameter names must be unique:" names))
     (define unames (map fresh-name names))
     (t:lambda unames (parse:term (env-bind* env 'term names unames)
                                  body)))))

(define bindings.initial.quasiquote
  (binding-alist/class
    'quasiquote
    'quasiquote 'quasiquote
    'unquote    'unquote))

(define bindings.initial.term
  (binding-alist/class
    'term
    'query      parse:term:query
    'quote      parse:term:quote
    'quasiquote parse:term:quasiquote
    'lambda     parse:term:lambda))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Module macro expansion
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define current-dbk-environment
  (make-parameter (env-set-alist env:empty (append bindings.initial.quasiquote
                                                   bindings.initial.term
                                                   bindings.initial.formula
                                                   bindings.initial.module))))

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
