#lang racket/base
(provide relation-kind relation-arity relation-properties relation-properties-set! relation-dirty! relation-clean!
         define-relation define-relation/table define-relation/input)
(require "abstract-syntax.rkt" "concrete-syntax.rkt" "misc.rkt" "stream.rkt"
         racket/struct)

(define (relation-kind            r)     ((relation-method r) 'kind))
(define (relation-arity           r)     ((relation-method r) 'arity))
(define (relation-properties      r)     ((relation-method r) 'properties))
(define (relation-properties-set! r k v) ((relation-method r) 'properties-set! k v))
(define (relation-dirty!          r)     ((relation-method r) 'dirty!))
(define (relation-clean!          r)     ((relation-method r) 'clean!))

(struct relation (method)
        #:methods gen:custom-write
        ((define write-proc
           (make-constructor-style-printer
             (lambda (r) 'relation)
             (lambda (r) (list (cons 'kind  (relation-kind       r))
                               (cons 'arity (relation-arity      r))
                               (relation-properties r))))))
        #:property prop:procedure
        (lambda (r . args)
          (unless (= (relation-arity r) (length args))
            (error "relation called with invalid number of arguments" args r))
          (with-term-vocabulary (f:relate r (map scm->term args)))))

(define (make-relation kind arity parent->self)
  (define properties (hash))
  (relation
    (parent->self
      (method-lambda
        ((kind)                   kind)
        ((arity)                  arity)
        ((properties)             properties)
        ((properties-set!    k v) (when (eq? k 'attributes)
                                    (unless (and (list? v) (= arity (length v)))
                                      (error "invalid number of attributes for arity:" arity v)))
                                  (set! properties (hash-set    properties k v)))
        ((properties-remove! k)   (set! properties (hash-remove properties k)))
        ((dirty!)                 (void))
        ((clean!)                 (void))))))

(define (relation/rule arity rule)
  (make-relation
    'rule arity
    (lambda (parent)
      (method-lambda
        ((apply . args) (define len (length args))
                        (unless (= len arity) (error "invalid number of arguments:" arity args))
                        (apply rule args))
        (else           parent)))))

(define (relation/table arity path)
  ;; TODO: if path is #f, use temporary storage
  (make-relation
    'table arity
    (lambda (parent)
      (method-lambda
        ;; TODO:
        ;; Define instantiation of table controllers that manage their own column constraints.
        ;; A single query might instantiate multiple controllers for the same table, each with
        ;; a different set of constraints/bounds.
        ;; Controller interface provides:
        ;; - retrieval of statistics:
        ;;   - total tuple count
        ;;   - per-column cardinality
        ;;   - per-column bounds
        ;; - update of per-column bounds
        ;; - index descriptions

        ;; TODO:
        ;; Define an interface for updating table content.
        ;; Maintain a log of insertions and deletions.
        ;; Possibly support log subscriptions to allow other processes to observe changes.
        ((path) path)
        (else   parent)))))

(define (relation/input arity produce)
  (make-relation
    'input arity
    (lambda (parent)
      (method-lambda
        ((produce) (s-enumerate 0 (produce)))
        (else      parent)))))

(define-syntax-rule (define-relation (r param ...) f ...)
  (begin (define r (relation/rule (length '(param ...))
                                  (lambda (param ...) (with-formula-vocabulary (conj f ...)))))
         (relation-properties-set! r 'name       'r)
         (relation-properties-set! r 'attributes '(param ...))
         (relation-properties-set! r 'rule       '((r param ...) :- f ...))))

(define-syntax-rule (define-relation/table (r param ...) path)
  (begin (define r (relation/table (length '(param ...)) path))
         (relation-properties-set! r 'name       'r)
         (relation-properties-set! r 'attributes '(param ...))))

(define-syntax-rule (define-relation/input (r param ...) produce)
  (begin (define r (relation/input (length '(param ...)) produce))
         (relation-properties-set! r 'name       'r)
         (relation-properties-set! r 'attributes '(param ...))))
