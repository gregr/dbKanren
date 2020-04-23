#lang racket/base
(provide
  (struct-out make-query)
  (struct-out make-use)
  (struct-out disj)
  (struct-out conj)
  (struct-out constrain)
  (struct-out relate)

  relations relations-ref define-relation let-relations
  conj* disj* fresh conde use query
  == =/= absento symbolo numbero stringo
  <=o +o *o string<=o string-appendo string-symbolo string-numbero
  )

(struct query          (g var desc)     #:prefab #:name make-query
                                        #:constructor-name make-query)
(struct use            (proc args desc) #:prefab #:name make-use
                                        #:constructor-name make-use)
(struct disj           (g1 g2)          #:prefab)
(struct conj           (g1 g2)          #:prefab)
(struct constrain      (op terms)       #:prefab)
(struct relate         (thunk desc)     #:prefab)

(define-syntax define-constraint
  (syntax-rules ()
    ((_ (op params ...)) (define (op params ...)
                           (constrain 'op (list params ...))))))
(define-constraint (==             t1 t2))
(define-constraint (=/=            t1 t2))
(define-constraint (absento        t))
(define-constraint (symbolo        t))
(define-constraint (numbero        t))
(define-constraint (stringo        t))
(define-constraint (<=o            t1 t2))
(define-constraint (+o             t1 t2 t3))
(define-constraint (*o             t1 t2 t3))
(define-constraint (string<=o      t1 t2))
(define-constraint (string-appendo t1 t2 t3))
(define-constraint (string-symbolo t1 t2))
(define-constraint (string-numbero t1 t2))

(define relation-registry    (make-weak-hasheq '()))
(define (relations)          (hash->list relation-registry))
(define (relations-ref proc) (hash-ref relation-registry proc))
(define (relations-register! proc signature)
  (hash-set! relation-registry proc
             (make-hash `((signature . ,(list->vector signature))
                          (analysis  . #f)))))

(define-syntax let-relations
  (syntax-rules ()
    ((_ (((name param ...) g ...) ...) body ...)
     (let-values (((name ...) (let ()
                                (define-relation (name param ...) g ...) ...
                                ;; TODO: specify an appropriate caching mode
                                (values name ...))))
       body ...))))
(define-syntax define-relation
  (syntax-rules ()
    ((_ (name param ...) g ...)
     (begin
       (define (name param ...)
         (relate (lambda () (fresh () g ...)) `(,name name ,param ...)))
       (relations-register! name '(name param ...))))))
(define succeed (== #t #t))
(define fail    (== #f #t))
(define-syntax conj*
  (syntax-rules ()
    ((_)           succeed)
    ((_ g)         g)
    ((_ g0 gs ...) (conj g0 (conj* gs ...)))))
(define-syntax disj*
  (syntax-rules ()
    ((_)           fail)
    ((_ g)         g)
    ((_ g0 gs ...) (disj g0 (disj* gs ...)))))
(define-syntax fresh
  (syntax-rules ()
    ((_ (x ...) g0 gs ...)
     (let ((x (var/fresh 'x)) ...) (conj* g0 gs ...)))))
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
     (let ((initial-var (var/fresh #f)))
       (make-query (fresh (x ...) (== (list x ...) initial-var) g0 gs ...)
                   initial-var
                   `((x ...) g0 gs ...))))))
