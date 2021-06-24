#lang racket/base
(provide relation-kind relation-arity relation-properties relation-properties-set! relation-dirty! relation-clean!
         relation/rule relation/table relation/input)
(require "misc.rkt" "stream.rkt")

(define (relation-kind            r)     (r 'kind))
(define (relation-arity           r)     (r 'arity))
(define (relation-properties      r)     (r 'properties))
(define (relation-properties-set! r k v) (r 'properties-set! k v))
(define (relation-dirty!          r)     (r 'dirty!))
(define (relation-clean!          r)     (r 'clean!))

(define (relation self kind arity)
  (define properties (hash))
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
    ((clean!)                 (void))))

(define (relation/rule arity rule)
  (define self
    (method-lambda
      ((apply . args) (define len (length args))
                      (unless (= len arity) (error "invalid number of arguments:" arity args))
                      (apply rule args))
      (else           parent)))
  (define parent (relation self 'rule arity))
  self)

(define (relation/table arity path)
  ;; TODO: if path is #f, use temporary storage
  (define self
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
      (else   parent)))
  (define parent (relation self 'table arity))
  self)

(define (relation/input arity produce)
  (define self
    (method-lambda
      ((produce) (s-enumerate 0 (produce)))
      (else      parent)))
  (define parent (relation self 'input arity))
  self)
