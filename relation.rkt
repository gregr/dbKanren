#lang racket/base
(provide relation/stream define-relation/stream define-relation/tables
         materializer materialized-relation define-materialized-relation)
(require "codec.rkt" "method.rkt" "mk.rkt" "stream.rkt" "table.rkt"
         racket/file racket/function racket/list racket/pretty racket/set)

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

;; example sources
;`#(stream ,s pos  #(w x y z) ())  ;; not even a 0 because cannot guarantee sorting
;; what about the tuple types for a stream?

;`#(table ,t1 #f   #(w x y z) (0))   ;; unnamed pos
;`#(table ,t1 pos  #(w x y z) (0 1)) ;; main storage, suffix (x y z) (i.e., prefix truncated by 1) is also sorted in the same pos order
;`#(table ,t2 #f   #(y pos)   (0))   ;; index
;`#(table ,t3 pos2 #(z pos)   (0))   ;; index, names its own implicit position pos2 (theoretically usable by other indices)
;`#(table ,t4 #f   #(w pos2)  (0))   ;; index of index (is this even useful? maybe to simulate an index for (w z pos))

;; TODO: should we be more structured about sources?
;; single stream vs. table + indices
;; or is this just specifying a particular realization of relation-instance's method-lambda?
;`#(table ,t1 #(w x y z) pos
         ;(#(,t1-virtual #(x w y z)))
         ;(#(,t2 #(y #f))
          ;#(,t3 #(z #f))))

(define (source type data position-name? attributes sorted-offsets)
  (unless (member type '(table stream)) (error "invalid source type:" type))
  (define as  (vector->list attributes))
  (define pas (cons position-name? as))
  (define bad (filter-not symbol? as))
  (unless (null? bad) (error "invalid attribute names:" bad))
  (unless (or (not position-name?) (symbol? position-name?))
    (error "invalid optional position attribute name:" position-name?))
  (unless (= (length (remove-duplicates pas)) (length pas))
    (error "duplicate attributes:" pas))
  (foldl (lambda (i prev)
           (unless (and (exact? i) (integer? i)
                        (< prev i (vector-length attributes)))
             (error "invalid sorted column offsets:" sorted-offsets))
           i) -1 sorted-offsets)
  (vector type data position-name? attributes sorted-offsets))
(define (source-type           s) (vector-ref s 0))
(define (source-data           s) (vector-ref s 1))
(define (source-position-name? s) (vector-ref s 2))
(define (source-attributes     s) (vector-ref s 3))
(define (source-sorted-offsets s) (vector-ref s 4))

;; TODO: no real need to specify sorted columns since we can add virtual tables
;; TODO: could also use virtual tables for implicit position column, but the
;;       number of virtual tables needed could grow linearly with number of
;;       relation attributes (since position is always sorted in any column
;;       location, after any number of prefix attributes become known, we
;;       always have an index on position available),
;;       and indices would be normal tables that explicitly mention the
;;       position column.  Is this acceptable?
;; TODO: index subsumption

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

;; TODO: should we specify sources directly, or pass a pre-built body that may
;;       have been constructed in an arbitrary way?
(define (relation-instance attribute-names attribute-types degrees sources)
  (unless (= (vector-length attribute-names) (vector-length attribute-types))
    (error "mismatching attribute names and types:"
           attribute-names attribute-types))
  (define as (vector->list attribute-names))
  (unless (= (length (remove-duplicates as)) (length as))
    (error "duplicate attributes:" as))
  (when (null? sources) (error "empty relation sources for:" attribute-names))
  (unless (andmap (let ((sas (vector->list (source-attributes (car sources)))))
                    (lambda (a) (member a sas))) as)
    (error "main source attributes do not cover relation attributes:"
           (source-attributes (car sources))
           attribute-names))
  (define ras (list->set as))
  ;; TODO:
  ;; guarantee attribute-types match? how?

  ;; TODO: should we interpret degree constraints to find useful special cases?
  ;; * functional dependency
  ;; * bijection (one-to-one mapping via opposing functional dependencies)
  ;; * uniqueness (functional dependency to full set of of attributes)

  (method-lambda
    ((attribute-names) attribute-names)
    ((attribute-types) attribute-types)
    ;((degrees)         degrees)
    ;; TODO:
    ))

(define (make-relation/stream attribute-names attribute-types s)
  ;; TODO: optional type validation of stream data?
  (method-lambda
    ((attribute-names) attribute-names)
    ((attribute-types) attribute-types)
    ((apply args)      (constrain `(retrieve ,s) args))))

(define-syntax relation/stream
  (syntax-rules ()
    ;; TODO: specify types
    ((_ name (attr ...) se)
     (let ((r/s (make-relation/stream '(attr ...) '(attr ...)
                                      (s-enumerate 0 se))))
       (relation/proc name (attr ...)
                      (lambda (attr ...) (r/s 'apply (list attr ...))))))))
(define-syntax define-relation/stream
  (syntax-rules ()
    ;; TODO: specify types
    ((_ (name attr ...) se)
     (define name (relation/stream name (attr ...) se)))))

(define (materializer source-names buffer-size directory-path
                      attribute-names attribute-types key table-descriptions)
  (define (unique?! as) (unless (= (length (remove-duplicates as)) (length as))
                          (error "duplicates:" as)))
  (unique?! source-names)
  (unique?! attribute-names)
  (unless (subset? (set-remove attribute-names key) source-names)
    (error "missing source names for attributes:"
           source-names (set-remove attribute-names key)))
  (unless (= (length attribute-names) (length attribute-types))
    (error "mismatching attribute names and types:"
           attribute-names attribute-types))
  (define name=>type (make-immutable-hash
                       (map cons attribute-names attribute-types)))
  (when (null? table-descriptions)
    (error "empty list of table descriptions for:" attribute-names))
  (define index-tds            (cdr table-descriptions))
  (define primary-td           (car table-descriptions))
  (define primary-column-names (car primary-td))
  (define primary-column-types (map (lambda (n) (hash-ref name=>type n))
                                    primary-column-names))
  (define primary-source-names (if key (cons key primary-column-names)
                                 primary-column-names))
  (unique?! primary-column-names)
  (when (or (member key primary-column-names) (member key source-names))
    (error "key name must be distinct:" key primary-column-names source-names))
  (unless (equal? (set-remove (list->set attribute-names) key)
                  (list->set primary-column-names))
    (error "primary columns must include all non-key attributes:"
           (set->list (set-remove (list->set attribute-names) key))
           (set->list (list->set primary-column-names))))
  (define dpath (if #f (path->string (build-path "TODO: configurable base"
                                                 directory-path))
                  directory-path))
  (make-directory* dpath)
  (define metadata-fname (path->string (build-path dpath "metadata.scm")))
  (define primary-fname  (path->string (build-path dpath "primary")))
  (define index-fnames
    (map (lambda (i)
           (path->string (build-path dpath (string-append
                                             "index." (number->string i)))))
         (range (length index-tds))))
  (define metadata-out (open-output-file metadata-fname))
  (define primary-t
    (tabulator source-names buffer-size primary-fname
               primary-column-names primary-column-types
               key (cdr primary-td)))
  (method-lambda
    ((put x) (primary-t 'put x))
    ((close) (define primary-info (primary-t 'close))
             (define key-type (nat-type/max
                                (cdr (assoc 'length primary-info))))
             (define index-ts
               (let* ((name=>type (hash-set name=>type key key-type))
                      (name->type (lambda (n) (hash-ref name=>type n))))
                 (map (lambda (fname td)
                        (define column-names   (car td))
                        (define sorted-columns (cdr td))
                        (tabulator primary-source-names buffer-size fname
                                   column-names (map name->type column-names)
                                   #f sorted-columns))
                      index-fnames index-tds)))
             (let/files ((in (value-table-file-name primary-fname))) ()
               (define primary-s (s-decode in primary-column-types))
               (s-each (if key (s-enumerate 0 primary-s) primary-s)
                       (lambda (x) (for-each (lambda (t) (t 'put x))
                                             index-ts))))
             (define index-infos  (map (lambda (t) (t 'close)) index-ts))
             (pretty-write `((attribute-names . ,attribute-names)
                             (attribute-types . ,attribute-types)
                             (primary-table   . ,primary-info)
                             (index-tables    . ,index-infos))
                           metadata-out)
             (close-output-port metadata-out))))

(define (materialized-relation relation-name retrieval-type directory-path)
  (define dpath (if #f (path->string (build-path "TODO: configurable base"
                                                 directory-path))
                  directory-path))
  (let/files ((in (path->string (build-path dpath "metadata.scm")))) ()
    (define info-alist (read in))
    (when (eof-object? info-alist) (error "corrupt relation metadata:" dpath))
    (define info (make-immutable-hash info-alist))
    (define attribute-names    (hash-ref info 'attribute-names))
    (define attribute-types    (hash-ref info 'attribute-types))
    (define primary-info-alist (hash-ref info 'primary-table))
    (define primary-info       (make-immutable-hash primary-info-alist))
    (define fn.primary (path->string (build-path dpath "primary")))
    (define primary-t
      (table/metadata retrieval-type fn.primary primary-info-alist))
    (define primary-key-name     (hash-ref primary-info 'key-name))
    (define primary-column-names (primary-t 'columns))
    (define key-name (and (member primary-key-name attribute-names)
                          primary-key-name))
    (define index-info-alists (hash-ref info 'index-tables))
    (define index-infos (map make-immutable-hash index-info-alists))
    (define index-ts
      (map (lambda (i info)
             (define fn.index
               (path->string (build-path dpath (string-append
                                                 "index."
                                                 (number->string i)))))
             (table/metadata retrieval-type fn.index info))
           (range (length index-info-alists))
           index-info-alists))
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
    (make-relation/proc
      relation-name attribute-names
      (lambda args
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
                    (constrain `(retrieve ,result-stream) ordered-args)))))))

(define-syntax define-materialized-relation
  (syntax-rules ()
    ((_ name retrieval-type directory-path)
     (define name (materialized-relation 'name retrieval-type directory-path)))))

;; TODO: attribute-types should be verified with tables
(define (make-relation/tables attribute-names attribute-types attrs/tables)
  (when (null? attrs/tables)
    (error "relation/tables must include at least one table:"
           attribute-names attribute-types))
  (define attrs/main-table    (car attrs/tables))
  (define attrss/index-tables (cdr attrs/tables))
  (let ((main-attrs (car attrs/main-table)))
    (for-each
      (lambda (name)
        (unless (member name main-attrs)
          (error "missing attribute in primary table:" name attrs/main-table)))
      attribute-names)
    (for-each
      (lambda (name)
        (unless (member name attribute-names)
          (error "unknown attribute in primary table:" name attribute-names)))
      main-attrs))
  (method-lambda
    ((attribute-names) attribute-names)
    ((attribute-types) attribute-types)
    ((apply args)
     ;; TODO: make use of index tables; later, use the constraint system
     ;; for now, index whatever possible from main table, then stream the rest
     (define env (map cons attribute-names args))
     (let loop ((attrs (car attrs/main-table)) (t (cdr attrs/main-table)))
       (define (finish) (constrain `(retrieve ,(t 'stream))
                                   (map (lambda (attr) (cdr (assoc attr env)))
                                        attrs)))
       (cond ((null? attrs) (finish))
             (else (define v (cdr (assoc (car attrs) env)))
                   (if (not (ground? v)) (finish)
                     (loop (cdr attrs) (table-project t v)))))))))

(define-syntax relation/tables
  (syntax-rules ()
    ;; TODO: specify types
    ((_ name (attr ...) as/ts)
     (let ((r/s (make-relation/tables '(attr ...) '(attr ...) as/ts)))
       (relation/proc name (attr ...)
                      (lambda (attr ...) (r/s 'apply (list attr ...))))))))

;; TODO: need a higher level interface than this
(define-syntax define-relation/tables
  (syntax-rules ()
    ;; TODO: specify types
    ((_ (name attr ...) as/ts)
     (define name (relation/tables name (attr ...) as/ts)))))

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
;; each one multiple times.
