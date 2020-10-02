#lang racket/base
(provide state.empty walk* unify disunify reify)
(require "syntax.rkt" racket/vector)

;; TODO:

;; implementation phases:
;;   pre-minimal implementation:
;;     ==: values as constraints
;;     =/=: =/=.atom, =/=.rhs, =/=*, optional subsumption checking
;;   minimal implementation must support:
;;     tables: ub, lb, lb-inclusive? ub-inclusive?, table-domains table-arcs
;;       also, subsumption and functional dependencies
;;   extended implementations may support:
;;     recursions and disjunctions as constraints: can replace =/=*
;;       recursion approximations
;;       watching 2 disjunction clauses
;;     one-var any<=o constraints (for constraining type): pair/vector sub-constraints
;;     general any<=o constraints: cycle checking, pair/vector decomposition into disjunction
;;   we can support the other constraints later

;; extra solvers, beyond bounds checking/limiting for domains and arcs:
;;   table state shared between indexes
;;   any<=o: cycles become ==
;;     incremental approach:
;;       maintain a topological sort
;;       if a new any<=o is added that doesn't respect the sort, re-sort
;;         SCCs are ==
;;   +o, *o:
;;     for generality, could use a substitute and simplify model
;;       if a + b = c, then replace c whenever it appears
;;       still need some functional dependencies (only forward?)
;;          e.g., a + b = c and a + b = d implies c = d
;;          also true that a + b = c and a + d = c implies b = d
;;          however, substituting for c and simplifying covers this:
;;            a + b = a + d, and simplifying shows b = d
;;          similar for *o, but case split with 0
;;     in special cases, can do linear programming
;;       incremental simplex
;;       keep in mind: flooro, X-lengtho, X-refo introduce integer constraints
;;     in other cases: difference equations, polynomials...
;;     calculus for optimization
;;     etc.
;;   (uninterpreted) functional dependencies:
;;     == propagation forward
;;       e.g., if (f a b = c) and (f a b = d) then (== c d)
;;     =/= propagation in reverse
;;       e.g., if (f a b = c) and (f a d = e) and (c =/= e) then (b =/= d)
;;         note: this is not a bi-implication unless the function is one-to-one
;;         (expresssed as two opposing functional dependency constraints)
;;     also can be used to encode some forms of subsumption checking
;;   X-refo: maintain minimum length and partial mapping
;;   string==byteso: look for impossible utf-8 bytes (possibly partial mapping)

;; priority/event-based constraint scheduling:
;;   immediate:
;;     ==
;;   high:
;;     potentially any event (assignment, lb, ub):
;;       table relation domains
;;       disjunction domains
;;   mid or low:
;;     non-ground assignment:
;;       wait, these imply target var would be constrained, though
;;       =/=, any<=o, vector-refo, +o
;;     ground assignment:
;;       =/=
;;       may cause assignment:
;;         +o and *o
;;         X-refo, X-lengtho, symbol==stringo, string==byteso
;;         table relation unsorted arcs
;;           schedule constraints for next indexed variable
;;         disjunction unsorted arcs
;;     lb and/or ub:
;;       =/=
;;       may cause assignment:
;;         any<=o, flooro (first position)
;;         table relation sorted arcs
;;         disjunction sorted arcs
;;       does not likely cause assignment:
;;         flooro (second position)

;; satisfiability loop for a variable constraint graph:
;;   constraint propagation loop:
;;     we have high and low priority queues of updated vars
;;       initially empty first time through the satisfiability loop
;;     we have a set of new constraints produced by the program step just taken
;;       these are sorted in order of priority: immediate, high, mid, low
;;     new constraints are processed in order of priority
;;       completed/subsumed constraints are discarded, otherwise attached to
;;       appropriate variables, possibly modifying them, where any modified
;;       variable constraints are queued: high for assignments, otherwise low
;;     updated vars are then processed from job queues until quiessence
;;       if high priority queue is empty, fill with all of low priority queue
;;       if both queues are empty, we've reached quiessence
;;       otherwise, pop a var-update job off the high priority queue
;;         iterate its domain constraints to a fixed point
;;         then update the bounds of its arc constraint targets
;;           queue up any updated targets: high for assignments, otherwise low
;;       re-enter constraint propagation loop
;;     backtrack and learn clauses if conflict is detected at any point
;;
;;   after propagation quiessence, attempt to divide and conquer
;;     this is attempted once each time through the satisfiability loop because
;;       new assignments may lead to disconnection, allowing more decomposition
;;     decompose into subproblem per subgraph of connected variables
;;       connection comes from dependency arcs implied by shared constraints
;;       each subgraph can be satisfied independently
;;       subgraph solutions can be enumerated and composed
;;
;;   for each variable constraint subgraph:
;;     while there are unresolved variables with possible assignments:
;;       choose variable with lowest assignment-set cardinality
;;       choose an assignment for the variable
;;         may introduce/expand new constraints by stepping into a disj clause
;;       re-enter satisfiability loop with any new constraints
;;       if enumerating or loop fails, choose the next assignment
;;       if no more assignments are available, fail
;;     once no more unresolved variables, succeed
;;   compose subgraph solutions
;;     if any subgraph failed completely, composition also fails


;; initial implementation scratch notes:
;
;(struct watchers (high mid:== mid:lb mid:ub low))
;(define watchers.empty (watchers '() '() '() '() '()))
;
;(struct bounds (lb lb-inclusive? ub ub-inclusive?))
;(define bounds.any (bounds term.min #t term.max #t #f))
;
;;; TODO:
;;; bounds -> bounds?
;(define (table-intersect/ub table ub inclusive?)
;  ;; update a table given new ub
;  ;; NOTE: this does not handle initial table constraint evaluation, which might already be within bounds
;
;  ;; TODO: inclusive? chooses <= or <
;
;  ;; TODO: this produces #f OR a new table with a possibly-changed ub
;  (table '<= ub)
;
;  ;; if #f, fail
;  ;; otherwise continue with new table
;  ;; if ub has changed, also activate ub watchers
;  )
;
;(struct vcx (bounds cardinality watchers))
;(define vcx.empty (vcx bounds.any #f watchers.empty))
;
;(struct mvcx (pending? bounds.old bounds.new cardinality watchers) #:mutable)
;(define (vcx->mvcx vcx update)
;  (mvcx #t (vcx-bounds vcx) (update (vcx-bounds vcx))
;        (vcx-cardinality vcx) (vcx-watchers vcx)))
;(define (mvcx->cx mvcx)
;  (define value (mvcx-bounds-new mvcx))
;  (if (bounds? value) (vcx value (mvcx-cardinality mvcx) (mvcx-watchers mvcx))
;    ;; TODO: assert cardinality=1 and watchers is empty
;    value))
;
;(define hash.empty (hash))
;(struct state (var=>cx store modified pending.high pending.low))  ;; TODO: when pending.high is empty, promote pending.low
;(define state.empty (state hash.empty hash.empty '() '()))
;(define (state-store-ref st k _) (hash-ref (state-store st) k _))
;(define (state-store-set st k v) (state (state-var=>cx st)
;                                        (hash-set (state-store st) k v)
;                                        (state-modified st)
;                                        (state-pending-high st)
;                                        (state-pending-low st)))
;(define (state-finish-propagation st)
;  (define v=>cx (foldl (lambda (v+mv v=>cx)
;                         (hash-set v=>cx (car v+mv) (mvcx->cx (cdr v+mv))))
;                       (state-var=>cx st) (state-modified st)))
;  ;; TODO: assert pending.high and pending.low are empty
;  (state v=>cx (state-store st) '() '()))
;
;;; TODO: make sure that propagation can be interrupted, so that it doesn't DoS other search threads
;(define (state-propagate-step st)
;  st
;  )
;(define (state-satisfy-step st) st)
;
;(define (state-modify-new-high st x new)
;  (state (hash-set (state-var=>cx st) x new)
;         (state-store st)
;         (cons (cons x new) (state-modified st))
;         (cons
;
;           ;; TODO: this should be wrapped in a job procedure that will process watchers
;           (lambda ()
;             new
;             )
;
;           (state-pending-high st))
;         (state-pending-low st))
;
;;(vcx->mvcx vcx update)
;
;  )
;
;;; TODO:
;;(state-modify-new-low st x new)
;
;
;(define (combine-cxs)
;  ;; TODO: check for self =/=?
;  ;; TODO: may also be able to simplify some arithmetic constraints
;  )
;(define (state-var-bound st x b)
;  )
;;; TODO: recast this as a modification event
;(define (state-var=>cx-set st x t)
;
;  ;; TODO: some of this should be factored out for assignment resulting from propagation?
;
;  (define (simple-update v=>cx)
;    ;; TODO: still have to check for self =/=?
;    ;; TODO: may also be able to simplify some arithmetic constraints
;    (state v=>cx (state-store st) (state-modified st)
;           (state-pending-high st) (state-pending-low st)))
;  (define v=>cx (state-var=>cx st))
;  (define v.x (hash-ref v=>cx x x))
;  (define v.t (and (var? t) (hash-ref v=>cx t t)))
;
;  (cond ((vcx? v.x)
;         (if (var? t)
;           (cond ((vcx? v.t)
;                  ;; TODO: combine both constraints
;                  )
;                 ((mvcx? v.t)
;                  ;; TODO: combine both constraints
;                  )
;                 (else (simple-update (hash-set v=>cx t x))))
;
;           ;; TODO: update x with assignment to t
;
;           (state-modify-
;
;             )
;           ))
;        ((mvcx? v.x)
;         (if (var? t)
;           (cond ((vcx? v.t)
;                  ;; TODO: combine both constraints
;                  )
;                 ((mvcx? v.t)
;                  ;; TODO: combine both constraints
;                  )
;                 (else (simple-update (hash-set v=>cx t x))))
;           ;; TODO: update x with assignment to t
;           (state-modify-
;
;             )))
;        (else (simple-update (hash-set v=>cx x t))))
;
;  (state (hash-set (state-var=>cx st) x t)
;                                         (state-store st)
;                                         ;; TODO: update these
;                                         (state-modified st)
;                                         (state-pending-high st)
;                                         (state-pending-low st)))
;
;;(define (state-var-walk/cx st x)
;  ;(let loop ((v=>cx (state-var=>cx st)) (x x))
;    ;(define val (hash-ref v=>cx x (void)))
;    ;(cond ((var?  val) (loop st val))
;          ;((void? val) x)
;          ;(else        val))))
;;(define (walk/cx st t) (if (var? t) (state-var-walk/cx st t) t))
;
;;; TODO: occurs check for =/= and vector-ref

(define hash.empty (hash))

(struct vcx (=/=*))
(define vcx.empty (vcx '()))

(struct mvcx () #:mutable)

(struct state (var=>cx))
(define state.empty (state hash.empty))
(define (state-var=>cx-set st x t) (state (hash-set (state-var=>cx st) x t)))

(define (assign st x t)
  (define v=>cx (state-var=>cx st))
  (and (not (occurs? st x t))
       (let ((vcx.x              (hash-ref v=>cx x vcx.empty))
             (vcx.t (if (var? t) (hash-ref v=>cx t vcx.empty) vcx.empty))
             (st    (state-var=>cx-set st x t)))
         (disunify** st (append (vcx-=/=* vcx.t) (vcx-=/=* vcx.x))))))
(define (walk st t)
  (if (var? t)
    (let ((v=>cx (state-var=>cx st)))
      (let loop ((x t))
        (define val (hash-ref v=>cx x vcx.empty))
        (cond ((var? val)                  (loop val))
              ((or (vcx? val) (mvcx? val)) x)
              (else                        val))))
    t))
(define (walk* st t)
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
          ((var?    t1) (assign st t1 t2))
          ((var?    t2) (assign st t2 t1))
          ((pair?   t1) (and (pair? t2)
                             (let ((st (unify st (car t1) (car t2))))
                               (and st (unify st (cdr t1) (cdr t2))))))
          ((vector? t1) (and (vector? t2) (= (vector-length t1)
                                             (vector-length t2))
                             (unify st (vector->list t1) (vector->list t2))))
          ((string? t1) (and (string? t2) (string=? t1 t2) st))
          ((bytes?  t1) (and (bytes?  t2) (bytes=?  t1 t2) st))
          (else         #f))))
(define (disunify st t1 t2) (disunify* st (list (cons t1 t2))))
(define (disunify* st =/=*)
  (define (assign st ==* x t) (and (not (occurs? st x t))
                                   (cons (state-var=>cx-set st x t)
                                         (cons (cons x t) ==*))))
  (define (unify st ==* t1 t2)
    (let ((t1 (walk st t1)) (t2 (walk st t2)))
      (cond ((eqv? t1 t2) (cons st ==*))
            ((var?    t1) (assign st ==* t1 t2))
            ((var?    t2) (assign st ==* t2 t1))
            ((pair?   t1) (and (pair? t2)
                               (let ((st+ (unify st ==* (car t1) (car t2))))
                                 (and st+ (unify (car st+) (cdr st+)
                                                 (cdr t1) (cdr t2))))))
            ((vector? t1) (and (vector? t2) (= (vector-length t1)
                                               (vector-length t2))
                               (unify st ==*
                                      (vector->list t1) (vector->list t2))))
            ((string? t1) (and (string? t2) (string=? t1 t2) (cons st ==*)))
            ((bytes?  t1) (and (bytes?  t2) (bytes=?  t1 t2) (cons st ==*)))
            (else         #f))))
  (let loop ((=/=* =/=*) (st st))
    (and (pair? =/=*)
         (let ((st+ (unify st '() (caar =/=*) (cdar =/=*))))
           (cond ((not st+)         st)
                 ((null? (cdr st+)) (loop (cdr =/=*) st))
                 (else (let* ((=/=*  (append (cdr st+) (cdr =/=*)))
                              (y     (caar =/=*))
                              (vcx.y (hash-ref (state-var=>cx st) y vcx.empty))
                              (vcx.y (vcx (cons =/=* (vcx-=/=* vcx.y)))))
                         (state-var=>cx-set st y vcx.y))))))))
(define (disunify** st =/=**)
  (if (null? =/=**) st
    (let ((st (disunify* st (car =/=**))))
      (and st (disunify** st (cdr =/=**))))))

(define (reify st t) (pretty (walk* st t)))
