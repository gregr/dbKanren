#lang racket/base
(provide)
(require "method.rkt" racket/list)

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
