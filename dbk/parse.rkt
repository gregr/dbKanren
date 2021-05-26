#lang racket/base
(provide
  define-dbk dbk link import input output
  dbk-environment dbk-environment-update with-dbk-environment-update with-fresh-names
  env:empty env:new env-ref env-set env-set* env-remove env-remove* env-bind env-bind* env-union
  literal? literal simple-parser
  parse:module* parse:module parse:formula parse:term)
(require "abstract-syntax.rkt" "misc.rkt"
         racket/hash racket/list racket/match racket/set racket/struct)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Names and parameter trees
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

(define (param-names param)
  (match param
    ((? symbol?)    (list param))
    ('()            '())
    ((cons p.a p.d) (append (param-names p.a) (param-names p.d)))
    ((? vector?)    (param-names (vector->list param)))))

(define (unique? names) (= (set-count (list->set names)) (length names)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Environments with vocabularies
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define env:empty (hash))

(define (env-ref     env vocab n)     (hash-ref (hash-ref env n (hash)) vocab #f))
(define (env-set     env vocab n  v)  (hash-update env n (lambda (vocab=>v) (hash-set vocab=>v vocab v)) (hash)))
(define (env-set*    env vocab ns vs) (foldl (lambda (n v env) (env-set env vocab n v)) env ns vs))

(define (env-remove  env       n)     (hash-remove env n))
(define (env-remove* env       ns)    (foldl (lambda (n e) (env-remove env n)) env ns))

(define (env-bind    env vocab n  v)  (env-set  (env-remove  env n)  vocab n  v))
(define (env-bind*   env vocab ns vs) (env-set* (env-remove* env ns) vocab ns vs))

(define (env-union   env . envs)      (foldl (lambda (e e.0)
                                               (hash-union e.0 e #:combine
                                                           (lambda (vocab=>v.0 vocab=>v)
                                                             (hash-union vocab=>v.0 vocab=>v #:combine
                                                                         (lambda (v.0 v) v)))))
                                             env envs))

(define (env:new vocab . args)
  (define nvs (plist->alist args))
  (env-set* env:empty vocab
            (map car nvs)
            (map cdr nvs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Parsing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (literal? x) (or (number? x) (boolean? x) (string? x) (bytes? x)))
(define (literal  x) (if (and (number? x) (inexact? x)) (inexact->exact x) x))

(define (binding-pairs?! bps)
  (unless (and (list? bps)
               (andmap (lambda (bp) (and (list? bp)
                                         (= 2 (length bp))))
                       bps))
    (error "invalid binding pairs:" bps)))

(define ((simple-parser proc) stx)
  (cond ((list? stx) (apply proc (cdr stx)))
        (else        (error "simple-parser expects list syntax:" stx))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Module parsing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define ((parse:module* stx) env)
  (unless (list? stx) (error "invalid module syntax:" stx))
  (with-fresh-names
    (m:link (map (lambda (stx) ((parse:module stx) env)) stx))))

(define ((parse:module stx) env)
  (with-fresh-names
    (match stx
      ((? symbol? name)
       (define mc.b (env-ref env 'module name))
       (cond ((procedure? mc.b) ((mc.b stx) env))
             (else              (error "unknown module clause keyword:" name mc.b))))
      (`(,operator ,@operands)
        (define mc.b (env-ref env 'module operator))
        (cond ((procedure? mc.b) ((mc.b stx) env))
              (else              (error "unknown module clause operator:" operator mc.b))))
      ((? procedure? self-parse) (self-parse env)))))

(define (rule-parser type)
  (simple-parser
    (simple-match-lambda
      (((relation . params) . formulas)
       (lambda (env)
         ;; NOTE: extracting variables in first-order positions as pattern
         ;; variables may be brittle.  It may be better to introduce a pattern
         ;; matching vocabulary to explicitly identify pattern variables.
         (define ts.params    (map (lambda (p) ((parse:term p) env)) params))
         (define names.params (set->list (t-free-vars-first-order* ts.params)))
         (define names.argument
           (map (lambda (i) (fresh-name (string->symbol (string-append "x." (number->string i)))))
                (range (length params))))
         (m:rule type relation names.argument
                 ((apply parse:formula:exist names.params
                         (lambda (env) (foldl f:and
                                              (f:== (quote-literal #t) (quote-literal #t))
                                              (map (lambda (n p) (f:== (t:var n) ((parse:term p) env)))
                                                   names.argument params)))
                         formulas)
                  env)))))))

(define parse:module:define
  (simple-match-lambda
    (((name . params) body) (lambda (env) (m:define (hash name ((parse:term:lambda params body) env)))))
    ((name            body) (lambda (env) (m:define (hash name ((parse:term               body) env)))))))

(define parse:module:declare
  (simple-match-lambda
    (((relation . attrs) . args)      (lambda (env) (m:link (list (m:declare relation (hash 'attributes attrs))
                                                                  ((apply parse:module:declare relation args)
                                                                   env)))))
    ((relation)                       (lambda (env) (m:declare relation (hash))))
    ((relation property value . args) (lambda (env)
                                        (define p.b (env-ref env 'declare property))
                                        (m:declare relation
                                                   (cond ((procedure? p.b) (match-define (cons p v) ((p.b value) env))
                                                                           (hash p v))
                                                         (else             (hash (if p.b p.b property) value))))))))

(define parse:module:assert
  (simple-match-lambda ((formula) (lambda (env) (m:assert ((parse:formula formula) env))))))

(define parse:declare:indexes
  (simple-match-lambda
    ((projections) (lambda (env) (cons 'indexes (map (lambda (proj) ((parse:term* proj) env))
                                                     projections))))))

(define env.initial.module.declare
  (env:new
    'declare
    'indexes parse:declare:indexes))

(define env.initial.module.clause
  (env:new
    'module
    'define          (simple-parser parse:module:define)
    'declare         (simple-parser parse:module:declare)
    'assert          (simple-parser parse:module:assert)
    '<<=             (rule-parser '<<=)
    '<<+             (rule-parser '<<+)
    '<<-             (rule-parser '<<-)
    '<<~             (rule-parser '<<~)
    ;; miniKanren style module clauses
    'define-relation (rule-parser '<<=)))

(define env.initial.module (env-union env.initial.module.declare
                                      env.initial.module.clause))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Formula parsing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define ((parse:formula stx) env)
  (with-fresh-names
    (match stx
      ((? literal? data) (f:const (literal data)))
      ((? symbol? name)
       (define f.b (env-ref env 'formula name))
       (cond ((procedure? f.b) ((f.b stx) env))
             (else             (f:const (if f.b f.b name)))))
      (`(,operator ,@operands)
        (define f.b (env-ref env 'formula operator))
        ((cond ((procedure? f.b) (f.b stx))
               (else             (parse:formula:relate (if f.b f.b operator) operands)))
         env))
      ((? procedure? self-parse) (self-parse env)))))

(define ((parse:formula* formulas) env)
  (map (lambda (f) ((parse:formula f) env)) formulas))

(define anonymous-vars (make-parameter #f))

(define-syntax formula/anonymous-vars
  (syntax-rules ()
    ((_ body ...) (parameterize ((anonymous-vars '()))
                    (define f (let () body ...))
                    (if (null? (anonymous-vars))
                      f
                      (f:exist (anonymous-vars) f))))))

(define ((parse:formula:relate relation operands) env)
  (formula/anonymous-vars (f:relate relation ((parse:term* operands) env))))

(define parse:formula:or
  (simple-match-lambda
    ((disjunct)             (parse:formula disjunct))
    ((disjunct . disjuncts) (lambda (env) (f:or ((parse:formula disjunct)           env)
                                                ((apply parse:formula:or disjuncts) env))))))

(define parse:formula:and
  (simple-match-lambda
    ((conjunct)             (parse:formula conjunct))
    ((conjunct . conjuncts) (lambda (env) (f:and ((parse:formula conjunct)            env)
                                                 ((apply parse:formula:and conjuncts) env))))))

(define parse:formula:not
  (simple-match-lambda ((f) (lambda (env) (f:not ((parse:formula f) env))))))

(define (parse:formula:quantifier f:quantifier msg.name)
  (simple-match-lambda
    ((params . body) (define names (param-names params))
                     (unless (unique? names)
                       (error (string-append msg.name " parameter names must be unique:") names))
                     (lambda (env)
                       (define unames (map fresh-name names))
                       (f:quantifier unames ((apply parse:formula:and body)
                                             (env-bind* env 'term names unames)))))))

(define parse:formula:exist (parse:formula:quantifier f:exist "existential quantifier"))
(define parse:formula:all   (parse:formula:quantifier f:all   "universal quantifier"))

(define parse:formula:implies
  (simple-match-lambda
    ((hypothesis conclusion) (lambda (env) (f:implies ((parse:formula hypothesis) env)
                                                      ((parse:formula conclusion) env))))))

(define parse:formula:iff
  (simple-match-lambda
    ((f.a f.b) (lambda (env) (f:iff ((parse:formula f.a) env)
                                    ((parse:formula f.b) env))))))

;; miniKanren style formulas
(define parse:formula:fresh (parse:formula:quantifier f:exist "fresh"))

(define parse:formula:conde
  (simple-match-lambda
    (clauses (apply parse:formula:or (map (lambda (conjuncts) (apply parse:formula:and conjuncts))
                                          clauses)))))

(define env.initial.formula
  (env:new
    'formula
    'or      (simple-parser parse:formula:or)
    'and     (simple-parser parse:formula:and)
    'not     (simple-parser parse:formula:not)
    'implies (simple-parser parse:formula:implies)
    'iff     (simple-parser parse:formula:iff)
    'exist   (simple-parser parse:formula:exist)
    'all     (simple-parser parse:formula:all)
    ;; miniKanren style formulas
    'fresh   (simple-parser parse:formula:fresh)
    'conde   (simple-parser parse:formula:conde)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Term parsing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (quote-literal v) (t:quote (literal v)))

(define ((parse:term stx) env)
  (with-fresh-names
    (match stx
      ((? literal? data) (quote-literal data))
      ((? symbol?  name) ((parse:term:ref name) env))
      (`(,operator ,@operands)
        (define t.b (env-ref env 'term operator))
        ((cond ((procedure? t.b) (t.b stx))
               (else             (parse:term:app operator operands)))
         env))
      ((? procedure? self-parse) (self-parse env)))))

(define ((parse:term* stxs) env)
  (map (lambda (stx) ((parse:term stx) env)) stxs))

(define ((parse:term:ref name) env)
  (define t.b (env-ref env 'term name))
  (cond ((procedure? t.b) ((t.b name) env))
        (else             (t:var (if t.b t.b name)))))

(define parse:term:query
  (simple-match-lambda
    ((param-pattern . body)
     (lambda (env)
       (cond ((symbol? param-pattern)
              (define param (fresh-name param-pattern))
              (t:query param ((apply parse:formula:and body)
                              (env-bind env 'term param-pattern param))))
             (else (define param (fresh-name 'q.0))
                   (define names (param-names param-pattern))
                   (define (assign-param-pattern env)
                     (f:== param ((parse:term:simple param-pattern) env)))
                   (t:query param ((apply parse:formula:exist names assign-param-pattern body)
                                   env))))))))

(define ((parse:term:simple pattern) env)
  (let loop ((pattern pattern))
    (match pattern
      ((? symbol?)    ((parse:term:ref pattern) env))
      ('()            (quote-literal '()))
      ((cons p.a p.d) (t:cons (loop p.a) (loop p.d)))
      ((? vector?)    (t:list->vector (loop (vector->list pattern)))))))

(define parse:term:quote
  (simple-match-lambda ((value) (lambda (_) (quote-literal value)))))

(define parse:term:quasiquote
  (simple-match-lambda
    ((template)
     (lambda (env)
       (define ((keyword? k) n) (eq? k (env-ref env 'quasiquote n)))
       (define (lift tag e)     (t:cons (quote-literal tag) (t:cons e (quote-literal '()))))
       ;; NOTE: unquote-splicing support requires a safe definition of append
       (let loop ((t template) (level 0))
         (match t
           ((list (? (keyword? 'unquote)    k) e) (if (= level 0)
                                                    ((parse:term e) env)
                                                    (lift k (loop e (- level 1)))))
           ((list (? (keyword? 'quasiquote) k) t) (lift k (loop t (+ level 1))))
           (`(,t.a . ,t.d)                        (t:cons (loop t.a level) (loop t.d level)))
           ((? vector?)                           (t:list->vector (loop (vector->list t) level)))
           ((or (? (keyword? 'quasiquote))
                (? (keyword? 'unquote)))          (error "invalid quasiquote:" t template))
           (v                                     (quote-literal v))))))))

(define parse:term:app
  (simple-match-lambda
    ((proc args) (lambda (env) (t:app ((parse:term proc)  env)
                                      ((parse:term* args) env))))))

(define parse:term:lambda
  (simple-match-lambda
    ((params body) (define names (param-names params))
                   (unless (unique? names)
                     (error "lambda parameter names must be unique:" names))
                   (lambda (env)
                     (define unames (map fresh-name names))
                     (t:lambda unames ((parse:term body)
                                       (env-bind* env 'term names unames)))))))

(define parse:term:if
  (simple-match-lambda
    ((c t f) (lambda (env) (t:if ((parse:term c) env)
                                 ((parse:term t) env)
                                 ((parse:term f) env))))))

(define parse:term:let
  (simple-match-lambda
    ((bps body) (binding-pairs?! bps)
                (parse:term:app (parse:term:lambda (map car bps) body)
                                (map cadr bps)))))

(define parse:term:letrec
  (simple-match-lambda
    ((bps body) (binding-pairs?! bps)
                (define names (param-names (map car bps)))
                (unless (unique? names)
                  (error "letrec parameter names must be unique:" names))
                (lambda (env)
                  (define unames (map fresh-name names))
                  (define rhss ((parse:term* (map cadr bps)) env))
                  (t:letrec (map cons unames rhss)
                            ((parse:term body)
                             (env-bind* env 'term names unames)))))))

(define parse:term:and
  (simple-match-lambda
    (()           (lambda (_) (quote-literal #t)))
    ((arg)        (parse:term arg))
    ((arg . args) (parse:term:if arg
                                 (apply parse:term:and args)
                                 (lambda (_) (quote-literal #f))))))

(define parse:term:or
  (simple-match-lambda
    (()           (lambda (_) (quote-literal #f)))
    ((arg)        (parse:term arg))
    ((arg . args) (lambda (env)
                    ((parse:term:let (list (list 'temp arg))
                                     (parse:term:if (parse:term:ref 'temp)
                                                    (parse:term:ref 'temp)
                                                    (lambda (_) ((apply parse:term:or args)
                                                                 env))))
                     env)))))

(define parse:term:anonymous-var
  (simple-match-lambda
    ((stx) (lambda (_)
             (unless (anonymous-vars) (error "misplaced anonymous variable:" stx))
             (define name (fresh-name '_))
             (anonymous-vars (cons name (anonymous-vars)))
             (t:var name)))))

(define (parse:term:prim name)
  (simple-match-lambda
    (((_ . args)) (lambda (env) (t:app (t:prim name) ((parse:term* args) env))))
    ((_)          (lambda (env) (t:prim name)))))

(define env.initial.term.quasiquote
  (env:new
    'quasiquote
    'quasiquote 'quasiquote
    'unquote    'unquote))

(define env.initial.term.primitive
  (apply env-union env:empty
         (map (lambda (name) (env:new 'term name (parse:term:prim name)))
              ;; TODO: some of these can be derived rather than primitive
              '(apply
                 cons car cdr
                 list->vector vector vector-ref vector-length
                 bytes-ref bytes-length bytes->string string->bytes
                 symbol->string string->symbol
                 floor + - * / =
                 equal? not
                 <= < >= >
                 any<= any< any>= any>
                 .< .<= .> .>=  ; polymorphic point-wise monotonic comparisons

                 ;; TODO: can some of these be defined relationally?
                 set set-count set-member? set-union set-intersect set-subtract
                 dict dict-count dict-ref dict-set dict-update dict-remove dict-union dict-intersect

                 min max sum length
                 map/merge map merge filter foldl foldr))))

(define env.initial.term.special
  (env:new
    'term
    '_          parse:term:anonymous-var
    'query      (simple-parser parse:term:query)
    'quote      (simple-parser parse:term:quote)
    'quasiquote (simple-parser parse:term:quasiquote)

    'if         (simple-parser parse:term:if)
    'lambda     (simple-parser parse:term:lambda)
    'let        (simple-parser parse:term:let)
    'letrec     (simple-parser parse:term:letrec)

    'and        (simple-parser parse:term:and)
    'or         (simple-parser parse:term:or)))

(define env.initial.term (env-union env.initial.term.quasiquote
                                    env.initial.term.primitive
                                    env.initial.term.special))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Module macro expansion
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define dbk-environment (make-parameter (env-union env.initial.term
                                                   env.initial.formula
                                                   env.initial.module)))

(define (dbk-environment-update env->env) (dbk-environment (env->env    (dbk-environment))))

(define-syntax-rule (with-dbk-environment-update env->env body ...)
  (parameterize ((dbk-environment (env->env    (dbk-environment))))
    body ...))

(define-syntax-rule (define-dbk name body ...) (define name (dbk body ...)))

(define-syntax-rule (dbk clauses ...)          (with-fresh-names (dbk-parse () clauses ...)))

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
     (dbk-parse (parsed ... (m:define (hash 'name (quote-literal value))))
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
     (dbk-parse (parsed ... ((parse:module 'clause) (dbk-environment)))
                clauses ...))))
