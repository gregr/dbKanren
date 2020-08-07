#lang racket/base
(provide
  (struct-out make-query)
  (struct-out disj)
  (struct-out conj)
  (struct-out constrain)
  (struct-out make-use)
  (struct-out var)

  make-relation relations relations-ref relations-set! relations-set*!
  relation letrec-relation define-relation
  relation/stream letrec-relation/stream define-relation/stream
  conj* disj* fresh conde use query
  == =/= absento symbolo numbero stringo
  <=o +o *o string<=o string-appendo string-symbolo string-numbero
  retrieve

  ground?
  make-pretty pretty)
(require racket/match racket/vector)

(struct query     (term g)         #:prefab #:name make-query
                                   #:constructor-name make-query)
;; goals
(struct disj      (g1 g2)          #:prefab)
(struct conj      (g1 g2)          #:prefab)
(struct constrain (op terms)       #:prefab)
;; terms
(struct use       (proc args desc) #:prefab #:name make-use
                                   #:constructor-name make-use)
(struct var       (name))

(define-syntax define-constraint
  (syntax-rules ()
    ((_ (op params ...)) (define (op params ...)
                           (constrain 'op (list params ...))))))
(define-constraint (==             t1 t2))
(define-constraint (=/=            t1 t2))
;; TODO: with a better strategy, this can be implemented as a normal relation
(define-constraint (absento        t1 t2))
;; TODO: derive these from (define-constraint (typeo          type term))
;; in fact, typeo itself can derive from uses of <=anyo and =/=
(define-constraint (symbolo        t))
(define-constraint (numbero        t))
(define-constraint (stringo        t))
(define-constraint (byteso         t))
(define-constraint (vectoro        t))
;; TODO: derive these from (define-constraint (<=anyo         t1 t2))
(define-constraint (<=o            t1 t2))
(define-constraint (string<=o      t1 t2))
(define-constraint (+o             t1 t2 t3))
(define-constraint (*o             t1 t2 t3))
;; TODO: can consider conversions between lists and strings/vectors
(define-constraint (string-appendo t1 t2 t3))
;; TODO: derive these from (define-constraint (casto          type1 type2 term))
(define-constraint (string-symbolo t1 t2))
;; TODO: probably omit this
(define-constraint (string-numbero t1 t2))
(define (retrieve stream args) (constrain `(retrieve ,stream) args))
(define (relate proc args) (constrain proc args))

(define relation-registry          (make-weak-hasheq '()))
(define (relations)                (hash->list relation-registry))
(define (relations-ref   proc)     (hash-ref relation-registry proc))
(define (relations-set!  proc k v) (relations-set*! proc `((,k . ,v))))
(define (relations-set*! proc alist)
  (hash-set! relation-registry proc
             (foldl (lambda (kv acc) (hash-set acc (car kv) (cdr kv)))
                    (relations-ref proc) alist)))
(define (make-relation name attributes)
  (define n ((make-syntax-introducer) (datum->syntax #f name)))
  (define r (eval-syntax #`(letrec ((#,n (lambda args (relate #,n args))))
                             #,n)))
  (hash-set! relation-registry r (make-immutable-hash
                                   `((name            . ,name)
                                     (attribute-names . ,attributes))))
  r)

(define-syntax relation
  (syntax-rules ()
    ((_ name (param ...) g ...)
     (let ((r (make-relation 'name '(param ...))))
       (relations-set! r 'expand (lambda (param ...) (fresh () g ...)))
       r))))
(define-syntax relation/stream
  (syntax-rules ()
    ((_ name (param ...) stream)
     (relation name (param ...) (retrieve stream param ...)))))
(define-syntax letrec-relation
  (syntax-rules ()
    ((_ (((name param ...) g ...) ...) body ...)
     (letrec ((name (relation name (param ...) g ...)) ...) body ...))))
(define-syntax letrec-relation/stream
  (syntax-rules ()
    ((_ (((name param ...) stream) ...) body ...)
     (letrec ((name (relation/stream name (param ...) stream)) ...)
       body ...))))
(define-syntax define-relation
  (syntax-rules ()
    ((_ (name param ...) g ...)
     (define name (relation name (param ...) g ...)))))
(define-syntax define-relation/stream
  (syntax-rules ()
    ((_ (name param ...) stream)
     (define name (relation/stream name (param ...) stream)))))
(define success (== #t #t))
(define failure (== #f #t))
(define-syntax conj*
  (syntax-rules ()
    ((_)           success)
    ((_ g)         g)
    ((_ gs ... gN) (conj (conj* gs ...) gN))))
(define-syntax disj*
  (syntax-rules ()
    ((_)           failure)
    ((_ g)         g)
    ((_ g0 gs ...) (disj g0 (disj* gs ...)))))
(define-syntax let/fresh
  (syntax-rules ()
    ((_ (x ...) e ...) (let ((x (var 'x)) ...) e ...))))
(define-syntax fresh
  (syntax-rules ()
    ((_ (x ...) g0 gs ...) (let/fresh (x ...) (conj* g0 gs ...)))))
(define-syntax conde
  (syntax-rules ()
    ((_ (g gs ...) (h hs ...) ...)
     (disj* (conj* g gs ...) (conj* h hs ...) ...))))
(define-syntax use
  (syntax-rules ()
    ((_ (x ...) body ...) (make-use (lambda (x ...) body ...)
                                    (list x ...)
                                    `((x ...) body ...)))))
(define-syntax query
  (syntax-rules ()
    ((_ (x ...) g0 gs ...)
     (let/fresh (x ...) (make-query (list x ...) (conj* g0 gs ...))))
    ((_ x       g0 gs ...)
     (let/fresh (x)     (make-query x            (conj* g0 gs ...))))))

(define (ground? t)
  (cond ((var?    t)  #f)
        ((pair?   t) (and (ground? (car t)) (ground? (cdr t))))
        ((vector? t) (andmap ground? (vector->list t)))
        ((use?    t) (andmap ground? (use-args t)))
        (else        #t)))

(define (make-pretty)
  (define var=>id (make-hash))
  (define (pretty-term t)
    (cond ((pair? t)   (cons (pretty-term (car t)) (pretty-term (cdr t))))
          ((vector? t) (vector-map pretty-term t))
          ((var? t)    `#s(var ,(var-name t)
                               ,(let ((id (hash-ref   var=>id t #f))
                                      (c  (hash-count var=>id)))
                                  (or id (begin (hash-set! var=>id t c) c)))))
          ((use? t)    `#s(let ,(map list
                                     (car (use-desc t))
                                     (map pretty-term (use-args t)))
                            ,@(cdr (use-desc t))))
          (else        t)))
  (define (pretty-goal g)
    (match g
      (`#s(disj ,g1 ,g2)         `#s(disj ,(pretty-goal g1) ,(pretty-goal g2)))
      (`#s(conj ,g1 ,g2)         `#s(conj ,(pretty-goal g1) ,(pretty-goal g2)))
      (`#s(constrain ,op ,terms) `(,op ,(map pretty-term terms)))))
  (lambda (x)
    (match x
      (`#s(query ,t ,g)
        `#s(query ,(pretty-term t) ,(pretty-goal g)))
      (_ (if (or (disj? x) (conj? x) (constrain? x))
           (pretty-goal x)
           (pretty-term x))))))
(define (pretty x) ((make-pretty) x))
