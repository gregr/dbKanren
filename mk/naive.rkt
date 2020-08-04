#lang racket/base
(provide naive:walk*
         bis:query->stream bis:retrieve
         dfs:query->stream dfs:retrieve)
(require "../stream.rkt" "syntax.rkt"
         (except-in racket/match ==)
         racket/function racket/vector)

(define (bis:query->stream q) ((bis:query q) state-empty))
(define (bis:query q)
  (match-define `#s(query ,x ,g) q)
  (define (return st) (list (pretty (naive:walk* st x))))
  (bis:goal g return))
(define ((bis:mplus k1 k2) st)
  (let loop ((s1 (k1 st)) (s2 (thunk (k2 st))))
    (cond ((null?      s1) (s2))
          ((procedure? s1) (thunk (loop (s2) s1)))
          (else (define d1  (cdr s1))
                (define s1^ (if (procedure? d1) d1 (thunk d1)))
                (cons (car s1) (thunk (loop (s2) s1^)))))))
(define ((bis:retrieve s args k) st)
  (let ((s (s-force s)))
    (if (null? s) '() ((bis:mplus (k:==         (car s) args k)
                                  (bis:retrieve (cdr s) args k))
                       st))))
(define (bis:goal g k)
  (define loop bis:goal)
  (match g
    ;; TODO: bias the interleaving in the usual miniKanren way
    (`#s(conj ,g1 ,g2) (loop g1 (loop g2 k)))
    (`#s(disj ,g1 ,g2) (bis:mplus (loop g1 k) (loop g2 k)))
    (`#s(constrain ,(? procedure? proc) ,args)
      (define r (relations-ref proc))
      (define apply/bis (hash-ref r 'apply/bis #f))
      (cond (apply/bis (apply/bis k args))
            (else (define ex (hash-ref r 'expand #f))
                  (unless ex (error "no interpretation for:" proc args))
                  (lambda (st) ((loop (apply ex (naive:walk* st args)) k)
                                st)))))
    (`#s(constrain == (,t1 ,t2)) (k:== t1 t2 k))))

(define (dfs:query->stream q) ((dfs:query q) state-empty))
(define (dfs:query q)
  (match-define `#s(query ,x ,g) q)
  (define (return st) (list (pretty (naive:walk* st x))))
  (dfs:goal g return))
(define ((dfs:mplus k1 k2) st) (s-append (k1 st) (thunk (k2 st))))
(define ((dfs:retrieve s args k) st)
  (let ((s (s-force s)))
    (if (null? s) '() ((dfs:mplus (k:==         (car s) args k)
                                  (dfs:retrieve (cdr s) args k))
                       st))))
(define (dfs:goal g k)
  (define loop dfs:goal)
  (match g
    (`#s(conj ,g1 ,g2) (loop g1 (loop g2 k)))
    (`#s(disj ,g1 ,g2) (dfs:mplus (loop g1 k) (loop g2 k)))
    (`#s(constrain ,(? procedure? proc) ,args)
      (define r (relations-ref proc))
      (define apply/dfs (hash-ref r 'apply/dfs #f))
      (cond (apply/dfs (apply/dfs k args))
            (else (define ex (hash-ref r 'expand #f))
                  (unless ex (error "no interpretation for:" proc args))
                  (lambda (st) ((loop (apply ex (naive:walk* st args)) k)
                                st)))))
    (`#s(constrain == (,t1 ,t2)) (k:== t1 t2 k))))

(define ((k:== t1 t2 k) st) (let ((st (unify st t1 t2))) (if st (k st) '())))

(struct state (var=>cx))
(define state-empty (state (hash)))
(define (state-var-assign st x t) (state (hash-set (state-var=>cx st) x t)))
(define (state-var-ref    st x)   (hash-ref (state-var=>cx st) x (void)))

;; TODO: variable lattice attributes supporting general constraints
;* type domains: #t top, #f bottom, lattice vector for domain sums
;  * (), #t, #f domains need no representation beyond being top or bottom
;  * pair domains are all represented as concrete values
;    * though pairs may contain variables
;  * symbol, string, bytes, and vector domains are represented as discrete sets
;    * discrete sets are sorted lists of concrete values
;      * though vectors may contain variables
;  * number domains are represented as interval sets (ordered ranges)
;(struct vspec (domain constraints) #:prefab)
;(define vtop (vspec #t '()))
;; TODO: register constrained/specified variables in a priority queue?
;(define (var/fresh name) (var name (void)))  ;; TODO: use TOP instead of void

(define (var-assign st x t) (and (not (occurs? st x t))
                                 (state-var-assign st x t)))
(define (var-walk st x)
  (define val (state-var-ref st x))
  (cond ((var?  val) (var-walk st val))
        ((void? val) x)
        (else        val)))
(define (walk st t)
  (define (walk* t) (naive:walk* st t))
  (cond ((var? t) (var-walk st t))
        ;; TODO: later, uses will be re-scheduled if args are not yet ground
        ((use? t) (apply (use-proc t) (map walk* (use-args t))))
        (else     t)))
(define (naive:walk* st t)
  (let loop ((term t))
    (define t (walk st term))
    (cond ((pair?   t) (cons (loop (car t)) (loop (cdr t))))
          ((vector? t) (vector-map loop t))
          (else        t))))
(define (occurs? st x t)
  (let oc? ((t t))
    (cond ((pair?   t) (or (oc? (walk st (car t))) (oc? (walk st (cdr t)))))
          ((vector? t) (let vloop ((i (- (vector-length t) 1)))
                         (and (<= 0 i) (or (oc? (walk st (vector-ref t i)))
                                           (vloop (- i 1))))))
          (else        (eq? x t)))))
(define (unify st t1 t2)
  (let ((t1 (walk st t1)) (t2 (walk st t2)))
    (cond ((eqv? t1 t2) st)
          ((var?    t1) (var-assign st t1 t2))
          ((var?    t2) (var-assign st t2 t1))
          ((pair?   t1) (and (pair? t2)
                             (let ((st (unify st (car t1) (car t2))))
                               (and st (unify st (cdr t1) (cdr t2))))))
          ((vector? t1) (and (vector? t2) (= (vector-length t1)
                                             (vector-length t2))
                             (unify st (vector->list t1) (vector->list t2))))
          ((string? t1) (and (string? t2) (string=? t1 t2) st))
          (else         #f))))
;; TODO: constraint satisfaction
