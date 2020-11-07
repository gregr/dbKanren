#lang racket/base
(provide materialized-relation define-materialized-relation)
(require "codec.rkt" "method.rkt" "mk.rkt" "stream.rkt" "table.rkt"
         (except-in racket/match ==) racket/function racket/list racket/vector)

(define (alist-ref alist key (default (void)))
  (define kv (assoc key alist))
  (cond (kv              (cdr kv))
        ((void? default) (error "missing key in association list:" key alist))
        (else            default)))
(define (alist-remove alist key)
  (filter (lambda (kv) (not (equal? (car kv) key))) alist))

;; * extensional relation:
;;   * schema:
;;     * heading: set of attributes and their types
;;     * degree constraints (generalized functional dependencies)
;;     * possibly join and inclusion dependencies
;;   * body: finite set of tuples

;; bodies, aka data sources:
;;   * single stream
;;   OR
;;   * main table and supporting (index) tables
;;   OR
;;   * extension of an existing relation to include an implicit position

;; streams (e.g., csv/tsv files) are not tables, can't efficiently bisect
;; but prepending a position column to each tuple implicitly sorts them

;; constraints:
;;   * functional dependency expressed as degree constraint of 1
;;   * uniqueness expressed as functional dependency

;; Non-prefix columns may be guaranteed to be sorted if they vary monotonically
;; with the table prefix.  This means the same table to be used as a virtual
;; index for multiple prefixes.  The virtual table reorders its columns to put
;; an alternate sorted column first.

;; TODO: no real need to specify sorted columns since we can add virtual tables
;; TODO: could also use virtual tables for implicit position column, but the
;;       number of virtual tables needed could grow linearly with number of
;;       relation attributes (since position is always sorted in any column
;;       location, after any number of prefix attributes become known, we
;;       always have an index on position available),
;;       and indices would be normal tables that explicitly mention the
;;       position column.  Is this acceptable?
;; TODO: index subsumption

;; TODO: should we interpret degree constraints to find useful special cases?
;; * functional dependency
;; * bijection (one-to-one mapping via opposing functional dependencies)
;; * uniqueness (functional dependency to full set of of attributes)

;; example degree constraints
;; TODO: are range lower bounds useful?
;'#(1 1 #(w x y z) #(pos))
;'#(1 1 #(x y z)   #(pos))  ;; even after truncating w, there are no duplicates

(define (degree lb ub domain range)
  ;; TODO: ub is #f or lb <= ub; domain and range are disjoint
  (vector lb ub domain range))
(define (degree-lower-bound d) (vector-ref d 0))
(define (degree-upper-bound d) (vector-ref d 1))
(define (degree-domain      d) (vector-ref d 2))
(define (degree-range       d) (vector-ref d 3))

(define (materialized-relation kwargs)
  (define directory-path? (alist-ref kwargs 'path   #f))
  (define source?         (alist-ref kwargs 'source #f))
  (cond (directory-path? (materialized-relation/path directory-path? kwargs))
        (source?         (materialized-relation/source source? kwargs))
        (else (error "missing relation path or source:" kwargs))))

(define (materialized-relation/path directory-path kwargs)
  (define name           (alist-ref kwargs 'relation-name))
  (define retrieval-type (alist-ref kwargs 'retrieval-type 'disk))
  (define dpath (if #f (path->string (build-path "TODO: configurable base"
                                                 directory-path))
                  directory-path))
  (define info-alist
    (let/files ((in (path->string (build-path dpath "metadata.scm")))) ()
      (read in)))
  (when (eof-object? info-alist) (error "corrupt relation metadata:" dpath))
  (define info (make-immutable-hash info-alist))
  (define attribute-names    (hash-ref info 'attribute-names))
  (define attribute-types    (hash-ref info 'attribute-types))
  (define primary-info-alist (hash-ref info 'primary-table))
  (define primary-info       (make-immutable-hash primary-info-alist))
  (define primary-t (table/metadata retrieval-type dpath primary-info-alist))
  (define primary-key-name     (hash-ref primary-info 'key-name))
  (define primary-column-names (primary-t 'columns))
  (define index-ts
    (map (lambda (info) (table/metadata retrieval-type dpath info))
         (hash-ref info 'index-tables '())))
  (relation/tables name attribute-names primary-key-name primary-t index-ts))

(define (materialized-relation/source source kwargs)
  (define name   (alist-ref kwargs 'relation-name))
  (define sort?  (alist-ref kwargs 'sort?  #t))
  (define dedup? (alist-ref kwargs 'dedup? #t))
  (define info (make-immutable-hash kwargs))
  (define attribute-names (hash-ref info 'attribute-names))
  (define attribute-types (hash-ref info 'attribute-types
                                    (map (lambda (_) #f) attribute-names)))
  (define primary-info (make-immutable-hash
                         (hash-ref info 'primary-table
                                   `((column-names . ,attribute-names)
                                     (key-name     . #t)))))
  (define primary-key-name (hash-ref primary-info 'key-name))
  (define name-type-alist (make-immutable-hash
                            (map cons attribute-names attribute-types)))
  (let ((kt (hash-ref name-type-alist primary-key-name 'nat)))
    (unless (or (not kt) (eq? kt 'nat)
                (and (vector? kt) (eq? (vector-ref kt 0) 'nat)))
      (error "invalid key type:" kt kwargs)))
  (define name=>type (hash-set name-type-alist primary-key-name 'nat))
  (define (name->type n) (hash-ref name=>type n))
  (define primary-column-names (hash-ref primary-info 'column-names))
  (define primary-column-types (map name->type primary-column-names))
  (define primary-source-names (if primary-key-name
                                 (cons primary-key-name primary-column-names)
                                 primary-column-names))
  (unless (vector? source) (error "invalid source vector:" kwargs source))
  (when sort? (vector-table-sort! primary-column-types source))
  (define primary-v (if dedup? (vector-dedup source) source))
  (define primary-t (table/vector primary-key-name primary-column-names
                                  primary-column-types primary-v))
  (define index-ts
    (let* ((ss.sources (generate-temporaries primary-source-names))
           (name=>ss (make-immutable-hash
                       (map cons primary-source-names ss.sources)))
           (name->ss (lambda (n) (hash-ref name=>ss n))))
      (map (lambda (info)
             (define key-name       (alist-ref info 'key-name #f))
             (define sorted-columns (alist-ref info 'sorted-columns '()))
             (define column-names   (alist-ref info 'column-names))
             (define column-types   (map name->type column-names))
             (define ss.columns     (map name->ss   column-names))
             (define index-src
               (cond (primary-key-name
                       (define transform
                         (eval-syntax
                           #`(lambda (#,(car ss.sources) row)
                               (match-define (vector #,@(cdr ss.sources)) row)
                               (vector #,@ss.columns))))
                       (define iv (make-vector (vector-length primary-v)))
                       (for ((i   (in-range (vector-length primary-v)))
                             (row (in-vector primary-v)))
                         (vector-set! iv i (transform i row)))
                       iv)
                     (else (define transform
                             (eval-syntax
                               #`(lambda (row)
                                   (match-define (vector #,@ss.sources) row)
                                   (vector #,@ss.columns))))
                           (vector-map transform primary-v))))
             (vector-table-sort! column-types index-src)
             (table/vector key-name column-names column-types
                           (vector-dedup index-src)))
           (hash-ref info 'index-tables '()))))
  (relation/tables name attribute-names primary-key-name primary-t index-ts))

(define-syntax define-materialized-relation
  (syntax-rules ()
    ((_ name kwargs) (define name (materialized-relation
                                    `((relation-name . name) . ,kwargs))))))

(define (relation/tables
          relation-name attribute-names primary-key-name primary-t index-ts)
  (define primary-column-names (primary-t 'columns))
  (define key-name (and (member primary-key-name attribute-names)
                        primary-key-name))
  ;; TODO: consider sorted-columns for out-of-order satifying
  (define (advance-table env col=>ts t)
    (define cols (t 'columns))
    (cond ((= 0 (t 'length)) #f)
          ((null? cols)      col=>ts)
          ((equal? (car cols) primary-key-name)
           (hash-update col=>ts (car cols) (lambda (ts) (cons t ts)) '()))
          (else (define col (car cols))
                (define v (hash-ref env col))
                (if (ground? v)
                  (advance-table env col=>ts (table-project t v))
                  (hash-update col=>ts col (lambda (ts) (cons t ts)) '())))))
  (define (advance-tables env col=>ts ts)
    (foldl (lambda (t col=>ts) (and col=>ts (advance-table env col=>ts t)))
           col=>ts ts))
  (define (loop cols t0 env col=>ts0 acc)
    (define k+ts
      (and col=>ts0
           (let ((ts (hash-ref col=>ts0 primary-key-name '())))
             (if (null? ts) (list (t0 'key))
               (table-intersect-start
                 (cons ((car ts) 'drop< (t0 'key)) (cdr ts)))))))
    (define col=>ts
      (and k+ts (hash-set col=>ts0 primary-key-name (cdr k+ts))))
    (define t (and k+ts (t0 'drop-key< (car k+ts))))
    (cond ((or (not k+ts) (= (t 'length) 0)) '())
          ((null? cols)
           (s-map (lambda (suffix) (foldl cons suffix acc))
                  (if key-name (s-enumerate (t 'key) (t 'stream)) '(()))))
          (else (define col          (car cols))
                (define v            (hash-ref    env     col))
                (define col=>ts//col (hash-remove col=>ts col))
                (define ixts         (hash-ref    col=>ts col '()))
                (if (ground? v) (loop (cdr cols) (table-project t v) env
                                      (advance-tables env col=>ts//col ixts)
                                      acc)
                  (let ((v+ts (table-intersect-start (cons t ixts))))
                    (if (not v+ts) '()
                      (let* ((v    (car v+ts))
                             (t    (cadr v+ts))
                             (ixts (cddr v+ts)))
                        (thunk (s-append
                                 (let* ((env (hash-set env col v))
                                        (col=>ts (advance-tables
                                                   env col=>ts//col ixts)))
                                   (loop (cdr cols) (table-project t v) env
                                         col=>ts (cons v acc)))
                                 (loop cols (t 'drop<= v) env
                                       (advance-tables env col=>ts//col ixts)
                                       acc))))))))))
  (define r (make-relation relation-name attribute-names))
  (define (apply/expand . args)
    (unless (= (length args) (length attribute-names))
      (error "invalid number of arguments:" attribute-names args))
    (define env (make-immutable-hash (map cons attribute-names args)))
    (define (ref name) (hash-ref env name))
    (define key (and key-name (ref key-name)))
    (cond ((and (integer? key) (<= 0 key) (< key (primary-t 'length)))
           (== (vector->list (primary-t 'ref* key))
               (map ref primary-column-names)))
          ((and key-name (not (var? key))) (== #t #f))
          (else (define ordered-attributes
                  (if key-name
                    (append primary-column-names (list key-name))
                    primary-column-names))
                (define ordered-args
                  (filter-not ground? (map ref ordered-attributes)))
                (define result-stream
                  (loop primary-column-names primary-t env
                        (advance-tables env (hash) index-ts) '()))
                (retrieve result-stream ordered-args))))
  (relations-set! r 'apply/expand apply/expand)
  r)

;; TODO: mk constraint integration
;; When a variable is unified, its constraints are queued for re-evaluation
;; (e.g., bounds refinement, propagation).
;; When an index constraint incorporates a unified value as prefix, it needs to
;; update itself, attaching to the next prefix variable(s) (might be more than
;; one variable due to either sorted columns (multiple index orders) or if a
;; column includes pair/vector values) in the index (if any).

;; constraint info:
;; * descriptions used for subsumption
;;   * #(,relation ,attributes-satisfied ,attributes-pending)
;;   * within a relation, table constraint A subsumes B if
;;     B's attributes-pending is a prefix of A's
;;     AND
;;     B does not have any attributes-satisfied that A does not have
;; * lower and upper domain bounds

;; Relations built on streams do not provide typical constraints since they
;; only support linear scanning.  Accessing them should probably be treated as
;; search (as with conde).

;; example: safe-drug -(predicate)-> gene
#|
(run* (D->G)
  (fresh (D G D-is-safe P)
    ;; D -(predicate)-> G
    (concept D 'category 'drug)   ;; probably not low cardinality          (6? optional if has-tradename already guarantees this, but is it helpful?) known D Ologn if indexed; known Ologn
    (edge D->G 'subject   D)      ;;                                       (5) 'subject range known Ologn; known D O(n) via scan
    (edge D->G 'object    G)      ;; probably lowest cardinality for D->G  (3) 'object range known Ologn; known D->G Ologn
    (edge D->G 'predicate P)      ;; probably next lowest, but unsorted    (4) known |D->G| O(|P|+logn) (would be known D->G O(nlogn)); known D->G O(nlognlog(|P|)) via scan
    (membero P predicates)        ;; P might also have low cardinality     (2) known P O(|P|)
    (== G 1234)                   ;; G should have lowest cardinality      (1) known G O1
    ;(concept G 'category 'gene)
    ;; D -(has-trade-name)-> _
    (edge D-is-safe 'subject   D) ;;                                       (7) 'subject range known Ologn; known D-is-safe Ologn
    (edge D-is-safe 'predicate 'has-tradename))) ;;                        (8) known Ologn
|#
;; about (4, should this build an intermediate result for all Ps, iterate per P, or just enumerate and filter P?):
;; * iterate        (global join on P): O(1)   space; O(|P|*(lg(edge G) + (edge P)))    time; join order is G(==), P(membero), D->G(edge, edge)
;; * intermediate    (local join on P): O(|P|) space; O(|(edge P)|lg(|P|) + lg(edge G)) time; compute D->G chunk offsets, virtual heap-sort, then join order is G(==), D->G(edge, intermediate)
;; * filter (delay consideration of P): O(1)   space; O((edge G)*lg(|P|)) time; join order is G(==), D->G(edge), P
;; (edge P) is likely larger than (edge G)
;; even if |P| is small, |(edge P)| is likely to be large
;; if |P| is small, iterate or intermediate
;; if |P| is large, iterate or filter; the larger the |P|, the better filtering becomes (negatives become less likely)
;; intermediate is rarely going to be good due to large |(edge P)|
;; iterate is rarely going to be very bad, but will repeat work for |P| > 1

;; TODO:
;; In this example query family, the query graph is tree-shaped, and the
;; smallest cardinalities will always start at leaves of the tree, so pausing
;; when cardinality spikes, and resuming at another leaf makes sense.  But what
;; happens if the smallest cardinality is at an internal node of the tree,
;; cardinality spikes, and the smallest cardinality is now along a different
;; branch/subtree?  In other words, resolving the internal node can be seen as
;; removing it, which disconnects the query graph.  The disconnected subgraphs
;; are independent, and so could be solved independently to avoid re-solving
;; each one multiple times.  This is sort of an on-the-fly tree decomposition.
;; Before fully solving each independent problem, ensure that each is
;; satisfiable.  Existential-only paths can stop after satisfiability check.

;; Comparing join structures to loop structures:
;; * iterative joining is naturally right-associative (think of nested for loops
;;   in a result position)
;;   * for x in (intersect ...):
;;       for y in (intersect ...):
;;         for z in (intersect ...):
;;         ...
;; * intermediate computations correspond to left-factorings (prefix of joins)
;;   (think of nested for loops in a generating position)
;;   * intermediate = (for x in (intersect ...):
;;                       for y in (intersect ...):
;;                         return (x, y))
;;     for z in (intersect ... intermediate ...):
;;       ...
;;   * computing intermediates reduces downstream duplication of effort, but
;;     risks useless effort up front, and the intermediate result uses extra
;;     space and probably requires indexing to integrate with the rest of the
;;     computation
