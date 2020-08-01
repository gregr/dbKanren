#lang racket/base
(provide (all-from-out "mk/syntax.rkt")
         dfs:walk* dfs:retrieve
         run^ run run*)
(require "mk/syntax.rkt" "stream.rkt"
         racket/function (except-in racket/match ==) racket/vector)

(define-syntax run^
  (syntax-rules () ((_   body ...) (dfs:query->stream (query  body ...)))))
(define-syntax run
  (syntax-rules () ((_ n body ...) (s-take n          (run^   body ...)))))
(define-syntax run*
  (syntax-rules () ((_   body ...)                    (run #f body ...))))

;; TODO: move beyond DFS once other strategies are ready
(define (dfs:query->stream q) ((dfs:query q) (state-empty)))
(define (dfs:query q)
  (match-define `#s(query ,x ,g) q)
  (define (return st) (let ((result (pretty (dfs:walk* x))))
                        (state-undo! st)
                        (list result)))
  (dfs:goal g return))
(define (fail st) (state-undo! st) '())
(define (mplus k1 k2) (lambda (st) (s-append (k1 (state-new st))
                                             (thunk (k2 st)))))
(define (dfs:retrieve s args k)
  (lambda (st) (let ((s (s-force s)))
                 ((if (null? s) fail
                    (mplus (dfs:==       (car s) args k)
                           (dfs:retrieve (cdr s) args k)))
                  st))))
(define (dfs:goal g k)
  (define loop dfs:goal)
  (match g
    (`#s(conj ,g1 ,g2) (loop g1 (loop g2 k)))
    (`#s(disj ,g1 ,g2) (mplus (loop g1 k) (loop g2 k)))
    (`#s(constrain ,(? procedure? proc) ,args)
      (define r (relations-ref proc))
      (define apply/dfs (hash-ref r 'apply/dfs #f))
      (cond (apply/dfs (apply/dfs k args))
            (else (define ex (hash-ref r 'expand #f))
                  (unless ex (error "no interpretation for:" proc args))
                  (lambda (st) ((loop (apply ex (dfs:walk* args)) k) st)))))
    (`#s(constrain == (,t1 ,t2)) (dfs:== t1 t2 k))))
(define ((dfs:== t1 t2 k) st) ((if (unify* st t1 t2) k fail) st))

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
;(define (var/fresh name) (var name (void)))  ;; TODO: use TOP instead of void
(define (var-assign! st x t) (and (not (occurs? x t)) (state-assign! st x t)))
(define (var-walk vr)
  (let ((val (var-value vr)))
    (cond ((var?  val) (let ((val^ (var-walk val)))
                         (unless (eq? val val^) (set-var-value! vr val^))
                         val^))
          ((void? val) vr)
          (else        val))))
(define (walk tm) (if (var? tm) (var-walk tm) tm))
(define (dfs:walk* t)
  (let ((t (walk t)))
    (cond ((pair? t)   (cons (dfs:walk* (car t)) (dfs:walk* (cdr t))))
          ((vector? t) (vector-map dfs:walk* t))
          ((use? t)    (apply (use-proc t) (dfs:walk* (use-args t))))
          (else        t))))
(define (occurs? x t)
  (cond ((pair? t)   (or (occurs? x (walk (car t)))
                         (occurs? x (walk (cdr t)))))
        ((vector? t) (let loop ((i (- (vector-length t) 1)))
                       (and (<= 0 i) (or (occurs? x (walk (vector-ref t i)))
                                         (loop (- i 1))))))
        (else        (eq? x t))))
(define (unify* st t1 t2) (unify st (dfs:walk* t1) (dfs:walk* t2)))
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
;; TODO: constraint satisfaction
