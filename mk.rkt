#lang racket/base
(provide
  (struct-out make-query)
  (struct-out make-use)
  (struct-out disj)
  (struct-out conj)
  (struct-out constrain)
  (struct-out relate)

  relations relations-ref relations-register!
  define-relation let-relations
  conj* disj* fresh conde use query run^ run run*
  == =/= absento symbolo numbero stringo
  <=o +o *o string<=o string-appendo string-symbolo string-numbero
  retrieve

  pretty-query pretty-goal pretty-term
  )

(require "method.rkt" "stream.rkt" racket/function racket/match racket/vector)

(struct query     (g var desc)     #:prefab #:name make-query
                                   #:constructor-name make-query)
(struct use       (proc args desc) #:prefab #:name make-use
                                   #:constructor-name make-use)
(struct relate    (proc args desc) #:prefab)
(struct disj      (g1 g2)          #:prefab)
(struct conj      (g1 g2)          #:prefab)
(struct constrain (op terms)       #:prefab)

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
(define (retrieve s args) (constrain `(retrieve ,s) args))

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
     (begin (define (name param ...)
;; TODO: thunk may need to invoke new logic after precomputation or analysis
              (relate (lambda (param ...) (fresh () g ...)) (list param ...)
                      `(,name . name)))
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
(define-syntax run^
  (syntax-rules () ((_   body ...) (query->stream (query  body ...)))))
(define-syntax run
  (syntax-rules () ((_ n body ...) (s-take n      (run^   body ...)))))
(define-syntax run*
  (syntax-rules () ((_   body ...)                (run #f body ...))))

;; TODO: move beyond DFS once other strategies are ready
(define (query->stream q)
  (match-define `#s(query ,g ,x ,desc) q)
  (thunk
    (let loop ((st (state-empty)) (g g) (gs '()))
      (define (fail)   (state-undo! st) '())
      (define (return) (cond ((null? gs) (define result (pretty-term x))
                                         (state-undo! st)
                                         (list result))
                             (else       (loop st (car gs) (cdr gs)))))
      (match g
        (`#s(conj ,g1 ,g2) (loop st g1 (cons g2 gs)))
        (`#s(disj ,g1 ,g2) (s-append (loop (state-new st) g1 gs)
                                     (thunk (loop st g2 gs))))
        (`#s(relate   ,proc  ,args ,desc) (loop st (relate-expand g) gs))
        (`#s(constrain (retrieve ,s) ,args)
          (let ((s (s-next s)))
            (if (null? s) fail
              (loop st (disj (== (car s) args)
                             (constrain `(retrieve ,(cdr s)) args))
                    gs))))
        (`#s(constrain == (,t1 ,t2)) ((if (unify* st t1 t2) return fail)))))))

(struct state (assignments constraints) #:mutable)
(define (state-empty)  (state '() '()))
;; TODO: what should be preserved?  Should this link to the parent state?
(define (state-new st) (state-empty))
(define (state-assign! st x t)
  (set-state-assignments! st (cons (cons x (var-value x))
                                   (state-assignments st)))
  (set-var-value! x t))
(define (state-undo! st)
  (set-state-assignments!
    st (map (lambda (kv) (let* ((x (car kv)) (t (var-value x)))
                           (set-var-value! x (cdr kv))
                           (cons x t)))
            (state-assignments st))))
(define (state-redo! st) (state-undo! st))  ;; coincidentally, for now

;; TODO: variable lattice attributes supporting general constraints
#|
* type domains: #t top, #f bottom, lattice vector for domain sums
  * (), #t, #f domains need no representation beyond being top or bottom
  * pair domains are all represented as concrete values
    * though pairs may contain variables
  * symbol, string, bytes, and vector domains are represented as discrete sets
    * discrete sets are sorted lists of concrete values
      * though vectors may contain variables
  * number domains are represented as interval sets (ordered ranges)
|#
;(struct vspec (domain constraints) #:prefab)
;(define vtop (vspec #t '()))
;; TODO: register constrained/specified variables in a priority queue?

(struct var (name (value #:mutable)) #:prefab)
(define (var/fresh name) (var name (void)))  ;; TODO: use TOP instead of void
(define (var-assign! st x t) (and (not (occurs? x t)) (state-assign! st x t)))
(define (var-walk vr)
  (let ((val (var-value vr)))
    (cond ((var?  val) (let ((val^ (var-walk val)))
                         (unless (eq? val val^) (set-var-value! vr val^))
                         val^))
          ((void? val) vr)
          (else        val))))
(define (walk tm) (if (var? tm) (var-walk tm) tm))
(define (walk* t)
  (let ((t (walk t)))
    (cond ((pair? t)   (cons (walk* (car t)) (walk* (cdr t))))
          ((vector? t) (vector-map walk* t))
          ((use? t)    (apply (use-proc t) (walk* (use-args t))))
          (else        t))))
(define (occurs? x t)
  (cond ((pair? t)   (or (occurs? x (walk (car t)))
                         (occurs? x (walk (cdr t)))))
        ((vector? t) (let loop ((i (- (vector-length t) 1)))
                       (and (<= 0 i) (or (occurs? x (walk (vector-ref t i)))
                                         (loop (- i 1))))))
        (else        (eq? x t))))
(define (unify* st t1 t2) (unify st (walk* t1) (walk* t2)))
(define (unify st t1 t2)
  (let ((t1 (walk t1)) (t2 (walk t2)))
    (cond ((eqv? t1 t2) #t)
          ((var? t1)    (var-assign! st t1 t2))
          ((var? t2)    (var-assign! st t2 t1))
          ((pair? t1)   (and (pair? t2)
                             (unify st (car t1) (car t2))
                             (unify st (cdr t1) (cdr t2))))
          ((vector? t1) (and (vector? t2)
                             (= (vector-length t1) (vector-length t2))
                             (let loop ((i (- (vector-length t1) 1)))
                               (or (< i 0) (and (unify st
                                                       (vector-ref t1 i)
                                                       (vector-ref t2 i))
                                                (loop (- i 1)))))))
          ((string? t1) (and (string? t2) (string=? t1 t2)))
          (else         #f))))
;; TODO: walk* decision should be made by relate-proc instead
(define (relate-expand r) (apply (relate-proc r) (walk* (relate-args r))))
;; TODO: constraint satisfaction

(define (pretty-printer)
  (define st (state-empty))
  (define var-count 0)
  (define (pretty-var x)
    (define v `#s(var ,(var-name x) ,var-count))
    (set! var-count (+ var-count 1))
    (var-assign! st x v)
    v)
  (define (pretty-term t)
    (let ((t (walk t)))
      (cond ((pair? t)   (cons (pretty-term (car t)) (pretty-term (cdr t))))
            ((vector? t) (vector-map pretty-term t))
            ((var? t)    (pretty-var t))
            ((use? t)    `(let ,(map list
                                     (car (use-desc t))
                                     (map pretty-term (use-args t)))
                            . ,(cdr (use-desc t))))
            (else        t))))
  (define (pretty-goal g)
    (match g
      (`#s(disj ,g1 ,g2)         `(disj ,(pretty-goal g1) ,(pretty-goal g2)))
      (`#s(conj ,g1 ,g2)         `(conj ,(pretty-goal g1) ,(pretty-goal g2)))
      (`#s(constrain ,op ,terms) `(,op . ,(map pretty-term terms)))
      (`#s(relate ,_ ,args (,_ . ,name))
        `(relate ,name . ,(map pretty-term args)))))
  (define (pretty-query q)
    (match q
      (`#s(query ,g ,x (,params . ,_))
        `(query ,params ,(pretty-term x) ,(pretty-goal g)))))
  (define (return x) (state-undo! st) x)
  (method-lambda
    ((query q) (return (pretty-query q)))
    ((term t)  (return (pretty-term t)))
    ((goal g)  (return (pretty-goal g)))))

(define (pretty-query q) ((pretty-printer) 'query q))
(define (pretty-goal  g) ((pretty-printer) 'goal  g))
(define (pretty-term  t) ((pretty-printer) 'term  t))
