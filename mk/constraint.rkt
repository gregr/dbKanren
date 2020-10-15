#lang racket/base
(provide state.empty walk* unify disunify use state-enumerate reify)
(require "../order.rkt" "../stream.rkt" "syntax.rkt"
         (except-in racket/match ==)
         racket/function racket/list racket/set racket/vector)

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
;;       watching 2 disjunction branches
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
;;         may introduce/expand new constraints by stepping into a disj branch
;;       re-enter satisfiability loop with any new constraints
;;       if enumerating or loop fails, choose the next assignment
;;       if no more assignments are available, fail
;;     once no more unresolved variables, succeed
;;   compose subgraph solutions
;;     if any subgraph failed completely, composition also fails


;; initial implementation scratch notes:
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

(define (foldl/and f acc xs)
  (let loop ((acc acc) (xs xs))
    (if (null? xs) acc
      (and acc (loop (f (car xs) acc) (cdr xs))))))

(define hasheq.empty (hash))
(define seteq.empty  (set))

(struct bounds (lb lb-inclusive? ub ub-inclusive?))
(define bounds.any (bounds term.min #t term.max #t))

(struct vcx (bounds domain arc =/=* ==/use))
(define vcx.empty (vcx bounds.any '() '() '() '()))
(define (vcx-=/=*-clear x)    (vcx (vcx-bounds x) (vcx-domain x) (vcx-arc x)
                                   '() (vcx-==/use x)))
(define (vcx-=/=*-add x =/=*) (vcx (vcx-bounds x) (vcx-domain x) (vcx-arc x)
                                   (cons =/=* (vcx-=/=* x)) (vcx-==/use x)))
(define (vcx-==/use-add x u)  (vcx (vcx-bounds x) (vcx-domain x) (vcx-arc x)
                                   (vcx-=/=* x) (cons u (vcx-==/use x))))

(struct mvcx (pending? var vcx) #:mutable)
(define (mvcx-new x vcx) (mvcx #t x vcx))

;; tables: any finite       relations where a row    *must* be chosen
;; disjs:  any search-based relations where a branch *must* be chosen
(struct state (var=>cx store tables disjs uses mvcxs pending.high pending.low))
(define state.empty (state hasheq.empty hasheq.empty seteq.empty seteq.empty
                           seteq.empty '() '() '()))
(define (state-var=>cx-set st x t)
  (state (hash-set (state-var=>cx st) x t) (state-store st) (state-tables st)
         (state-disjs st) (state-uses st) (state-mvcxs st)
         (state-pending.high st) (state-pending.low st)))
(define (state-store-ref st k _) (hash-ref (state-store st) k _))
(define (state-store-set st k v)
  (state (state-var=>cx st) (hash-set (state-store st) k v) (state-tables st)
         (state-disjs st) (state-uses st) (state-mvcxs st)
         (state-pending.high st) (state-pending.low st)))
(define (state-tables-add st t)
  (state (state-var=>cx st) (state-store st) (set-add (state-tables st) t)
         (state-disjs st) (state-uses st) (state-mvcxs st)
         (state-pending.high st) (state-pending.low st)))
(define (state-tables-remove st t)
  (state (state-var=>cx st) (state-store st) (set-remove (state-tables st) t)
         (state-disjs st) (state-uses st) (state-mvcxs st)
         (state-pending.high st) (state-pending.low st)))
(define (state-uses-add st u)
  (state (state-var=>cx st) (state-store st) (state-tables st) (state-disjs st)
         (set-add (state-uses st) u) (state-mvcxs st)
         (state-pending.high st) (state-pending.low st)))
(define (state-uses-remove* st us)
  (state (state-var=>cx st) (state-store st) (state-tables st)
         (state-disjs st)
         (foldl (lambda (u us) (set-remove us u)) (state-uses st) us)
         (state-mvcxs st) (state-pending.high st) (state-pending.low st)))
(define (state-uses-empty?! st)
  (unless (set-empty? (state-uses st))
    (match-define `#s(==/use ,l ,deps ,r ,desc) (set-first (state-uses st)))
    (error ":== dependencies are not ground:"
           (pretty (==/use (walk* st l) (walk* st deps) r desc)))))

(define (state-mvcxs-add st m)
  (state (state-var=>cx st) (state-store st) (state-tables st) (state-disjs st)
         (state-uses st) (cons m (state-mvcxs st))
         (state-pending.high st) (state-pending.low st)))
(define (state-mvcxs-clear st)
  (let ((st (foldl (lambda (m st)
                     (define x (walk st (mvcx-var m)))
                     (if x (state-var=>cx-set st x (mvcx-vcx m)) st))
                   st (state-mvcxs st))))
    (state (state-var=>cx st) (state-store st) (state-tables st)
           (state-disjs st) (state-uses st) '()
           (state-pending.high st) (state-pending.low st))))

(define (state-pending-push-high st job)
  (state (state-var=>cx st) (state-store st) (state-tables st) (state-disjs st)
         (state-uses st) (state-mvcxs st)
         (cons job (state-pending.high st)) (state-pending.low st)))
(define (state-pending-push-low st job)
  (state (state-var=>cx st) (state-store st) (state-tables st) (state-disjs st)
         (state-uses st) (state-mvcxs st)
         (state-pending.high st) (cons job (state-pending.low st))))
(define (state-pending-pop st)
  (define (state/pending high low)
    (state (state-var=>cx st) (state-store st) (state-tables st)
           (state-disjs st) (state-uses st) (state-mvcxs st) high low))
  (define pending (state-pending.high st))
  (define pending.low (state-pending.low st))
  (if (null? pending)
    (if (null? pending.low) #f
      (let ((pending (reverse pending.low)))
        (cons (car pending) (state/pending (cdr pending) '()))))
    (cons (car pending) (state/pending (cdr pending) pending.low))))
(define (state-pending-run st)
  (let loop ((st st))
    (define job+st (state-pending-pop st))
    (if job+st
      (let ((st ((car job+st) (cdr job+st))))
        (and st (state-pending-run st)))
      (state-mvcxs-clear st))))

(define (state-choose st xs.observable)
  (define x=>stats
    (foldl
      (lambda (t x=>stats)
        (foldl (lambda (xstat x=>stats)
                 (match-define (cons x cardinality.new) xstat)
                 (hash-update x=>stats x
                              (lambda (stats)
                                (match-define (cons cardinality count) stats)
                                (cons (if cardinality
                                        (min cardinality.new cardinality)
                                        cardinality.new)
                                      (+ count 1)))
                              '(#f . 0)))
               x=>stats (t 'variables)))
      hasheq.empty (set->list (state-tables st))))
  ;; TODO: if we don't make subsequent use of sorted xccs, just use a linear
  ;; scan to find x.best instead of sorting.
  (define xccs
    (sort (hash->list x=>stats)
          (lambda (a b)
            (match-define `(,x.a . (,card.a . ,count.a)) a)
            (match-define `(,x.b . (,card.b . ,count.b)) b)
            ;; Sort by increasing cardinality and decreasing count.
            ;; Prefer members of xs.enum.
            (or (< card.a card.b)
                (and (= card.a card.b)
                     (or (< count.b count.a)
                         (and (= count.b count.a)
                              (set-member? xs.observable x.a)
                              (not (set-member? xs.observable x.b)))))))))
  ;; TODO: also consider paths provided by available table indexes, maybe via
  ;; prioritized topological sort of SCCs.
  (define x.best (caar xccs))
  ;; TODO: it might be better to loop the entire state-choose.  It may be
  ;; unlikely, but pruning the domain of x.best could affect cardinalities
  ;; and/or counts of other variables.
  (let loop ((st st))
    (define v=>cx (state-var=>cx st))
    (define t (bounds-lb (vcx-bounds (hash-ref v=>cx x.best vcx.empty))))
    (define st.new (assign st x.best t))
    (define (s-rest)
      (define st.skip (disunify st x.best t))
      (if st.skip (loop st.skip) '()))
    (if st.new (cons st.new s-rest) s-rest)))

(define (state-enumerate st.0 term)
  (define st (state-pending-run st.0))
  (if st
    (if (set-empty? (state-tables st))
      (begin (state-uses-empty?! st) (list st))
      ;; TODO: term-vars walk* efficiency
      (let* ((xs.observable (set->list (term-vars (walk* st term))))
             (sts.all (s-append*
                        (s-map (lambda (st) (state-enumerate st xs.observable))
                               (state-choose st xs.observable)))))
        (if (null? xs.observable) (s-limit 1 sts.all) sts.all)))
    '()))

(define (assign/vcx st x t vcx.x =/=*.t)
  (let* ((=/=**   (append =/=*.t (vcx-=/=* vcx.x)))
         (==/use* (vcx-==/use vcx.x))
         (st      (state-var=>cx-set st x t))
         (st      (state-uses-remove* st ==/use*))
         (st      (use* st ==/use*)))
    (and st (disunify** st =/=**))))

(define (assign st x t)
  (define v=>cx (state-var=>cx st))
  (and (not (occurs? st x t))
       (let* ((vcx.x                (hash-ref v=>cx x vcx.empty))
              (vcx.t   (if (var? t) (hash-ref v=>cx t vcx.empty) vcx.empty))
              (=/=*.t  (vcx-=/=* vcx.t))
              (st      (if (eq? vcx.empty vcx.t) st
                         (state-var=>cx-set st t (vcx-=/=*-clear vcx.t)))))
         (assign/vcx st x t vcx.x =/=*.t))))

(define (use st u)
  (match-define `#s(==/use ,lhs ,args ,rhs ,desc) u)
  ;; TODO: performance
  ;; * can interleave walk* and term-vars
  ;; * can stop after finding just one var
  (let* ((t  (walk* st args))
         (xs (term-vars t)))
    (if (set-empty? xs)
      (unify st lhs (apply rhs t))
      (let* ((y     (set-first xs))
             (u     (==/use lhs t rhs desc))
             (vcx.y (hash-ref (state-var=>cx st) y vcx.empty))
             (vcx.y (vcx-==/use-add vcx.y u))
             (st    (state-uses-add st u)))
        (state-var=>cx-set st y vcx.y)))))
(define (use* st ==/use*) (foldl/and (lambda (u st) (use st u)) st ==/use*))

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

(define (assign/log st ==* x t) (and (not (occurs? st x t))
                                     (cons (state-var=>cx-set st x t)
                                           (cons (cons x t) ==*))))
(define (unify/log st ==* t1 t2)
  (let ((t1 (walk st t1)) (t2 (walk st t2)))
    (cond ((eqv? t1 t2) (cons st ==*))
          ((var?    t1) (assign/log st ==* t1 t2))
          ((var?    t2) (assign/log st ==* t2 t1))
          ((pair?   t1) (and (pair? t2)
                             (let ((st+ (unify/log st ==* (car t1) (car t2))))
                               (and st+ (unify/log (car st+) (cdr st+)
                                                   (cdr t1) (cdr t2))))))
          ((vector? t1) (and (vector? t2) (= (vector-length t1)
                                             (vector-length t2))
                             (unify/log st ==*
                                        (vector->list t1) (vector->list t2))))
          ((string? t1) (and (string? t2) (string=? t1 t2) (cons st ==*)))
          ((bytes?  t1) (and (bytes?  t2) (bytes=?  t1 t2) (cons st ==*)))
          (else         #f))))
(define (disunify* st =/=*)
  (let loop ((=/=* =/=*))
    (and (pair? =/=*)
         (let ((st+ (unify/log st '() (caar =/=*) (cdar =/=*))))
           (cond ((not st+)         st)
                 ((null? (cdr st+)) (loop (cdr =/=*)))
                 (else (let* ((=/=*  (append (cdr st+) (cdr =/=*)))
                              (y     (caar =/=*))
                              (vcx.y (hash-ref (state-var=>cx st) y vcx.empty))
                              (vcx.y (vcx-=/=*-add vcx.y =/=*)))
                         (state-var=>cx-set st y vcx.y))))))))
(define (disunify** st =/=**)
  (foldl/and (lambda (=/=* st) (disunify* st =/=*)) st =/=**))
(define (disunify st t1 t2) (disunify* st (list (cons t1 t2))))

(define (reify st term)
  (define t.0 (walk* st term))
  (define xs (term-vars t.0))
  (define v=>cx (state-var=>cx st))
  (define (v->=/=* x) (vcx-=/=* (hash-ref v=>cx x vcx.empty)))
  (define =/=**.0 (append* (set-map xs v->=/=*)))
  (define (disunify*/full =/=*)
    (let loop ((=/=* =/=*) (=/=*.new '()))
      (if (null? =/=*) =/=*.new
        (let ((st+ (unify/log st '() (caar =/=*) (cdar =/=*))))
          (and st+ (if (null? (cdr st+)) (loop (cdr =/=*) =/=*.new)
                     (let ((=/=*.0 (cdr st+)))
                       ;; irrelevant variables imply irrelevant constraints
                       (and (set-empty? (set-subtract (term-vars =/=*.0) xs))
                            (loop (cdr =/=*) (append =/=*.0 =/=*.new))))))))))
  (define =/=**.1 (filter-not not (map disunify*/full =/=**.0)))
  ;; pretty variables are comparable via term<?
  (match-define `(,t . ,=/=**.2) (pretty `(,t.0 . ,=/=**.1)))
  ;; normalize order of each =/= involving variables on both lhs and rhs
  (define =/=**.3
    (map (lambda (=/=*)
           (map (lambda (=/=) (sort (list (car =/=) (cdr =/=)) term<?))
                =/=*))
         =/=**.2))
  ;; eliminate subsumed =/=*s and sort final result
  (define (set-count<? a b) (< (set-count a) (set-count b)))
  (define =/=**
    (sort (map (lambda (=/=*) (sort (set->list =/=*) term<?))
               (foldl
                 (lambda (=/=* =/=**)
                   (let loop ((=/=** =/=**))
                     (if (null? =/=**) (list =/=*)
                       (let ((=/=*.0 (car =/=**)))
                         (cond ((subset? =/=*.0 =/=*) =/=**)
                               ((subset? =/=* =/=*.0) (loop (cdr =/=**)))
                               (else (cons =/=*.0 (loop (cdr =/=**)))))))))
                 '() (sort (map list->set =/=**.3) set-count<?)))
          term<?))
  ;; This whole sort-and-subset? process for subsumption is ad hoc and fragile.
  ;; TODO: define general subsumption that works with other constraints.  Save
  ;; multiple impossible variations of the current state, and check constraints
  ;; against this impossible set.  Any that fail are subsumed.  Any that do not
  ;; fail should be added to the impossibility set.  Make sure to order
  ;; constraints such that later ones wouldn't subsume those already
  ;; contributing to the impossible set.
  (if (null? =/=**) t `#s(cx ,t (=/=** . ,=/=**))))
