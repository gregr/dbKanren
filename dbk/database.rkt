#lang racket/base
(provide
  ;; TODO: move these
  set-fixed-point
  build-tsv-relation
  unsafe-bytes-split-tab
  bytes-base10->fxnat

  database
  database-path
  database-commit!
  database-revert!
  database-trash-empty!
  database-relation-names
  database-relation-name?
  database-relation
  database-relation-builder
  database-relation-new
  database-relation-add!
  relation-database
  relation-has-name?
  relation-name
  relation-attributes
  relation-type
  relation-indexes
  relation-delete!
  relation-name-set!
  relation-attributes-set!
  relation-assign!
  relation-index-add!
  relation-index-remove!
  relation-full-compact!
  relation-incremental-compact!
  R.empty R+ R-
  auto-empty-trash?
  current-batch-size)
(require "heap.rkt" "logging.rkt" "misc.rkt" "storage.rkt" "stream.rkt"
         racket/file racket/fixnum racket/hash racket/list racket/match
         racket/set racket/struct racket/unsafe/ops racket/vector)

;(define-syntax-rule (performance-log description body ...) (let () body ...))
(define-syntax-rule (performance-log description body ...) (let ()
                                                             (pretty-log description)
                                                             (time/pretty-log body ...)))

;; TODO: move these
(define (bytes<=? a b) (not (bytes<? b a)))

(define (set-fixed-point xs.initial step)
  (let loop ((current (set))
             (next    xs.initial))
    (let ((new (set-subtract next current)))
      (if (set-empty? new)
        current
        (loop (set-union current new)
              (step      new))))))

(define (build-tsv-relation db type file-name)
  (let-values (((insert! finish) (database-relation-builder db '(int text text))))
    (call-with-input-file
      file-name
      (lambda (in)
        (read-bytes-line in 'any)  ; drop header line
        (time
          (let tuple-loop ((i.tuple 0))
            (let ((line (read-bytes-line in 'any)))
              (unless (eof-object? line)
                (insert! (let ((fields (unsafe-bytes-split-tab line)))
                           (cons (bytes-base10->fxnat (car fields)) (cdr fields))))
                (tuple-loop (unsafe-fx+ i.tuple 1))))))
        (time (finish))))))

(define (unsafe-bytes-split-tab bs)
  (let loop ((end    (unsafe-bytes-length bs))
             (i      (unsafe-fx- (unsafe-bytes-length bs) 1))
             (fields '()))
    (cond ((unsafe-fx< i 0)                       (cons (subbytes bs 0 end) fields))
          ((unsafe-fx= (unsafe-bytes-ref bs i) 9) (loop i   (unsafe-fx- i 1) (cons (subbytes bs i end) fields)))
          (else                                   (loop end (unsafe-fx- i 1) fields)))))

(define (bytes-base10->fxnat bs)
  (define len (bytes-length bs))
  (unless (< 0 len 19)
    (when (= len 0)  (error "natural number must contain at least one digit" bs))
    (when (< 18 len) (error "natural number must contain at most 18 digits (to safely fit in a fixnum)" bs)))
  (let loop ((i 0) (n 0))
    (if (unsafe-fx< i len)
      (let ((b (unsafe-bytes-ref bs i)))
        (unless (unsafe-fx<= 48 b 57)
          (error "natural number must contain only base10 digits" bs))
        (loop (unsafe-fx+ i 1)
              (unsafe-fx+ (unsafe-fx* n 10)
                          (unsafe-fx- b 48))))
      n)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Persistent databases ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Limitations:
;; - A single database will not scale well to enormous numbers of relations.
;; - This implementation will not scale well to high frequency database updates.
;; - A single database should not be used by multiple host processes running concurrently.
;;   Initializing the same database from concurrent processes may lead to data corruption.
;; - Multiple threads of a single process can concurrently read from the same database, but it is
;;   not safe for a thread to read concurrently with another thread's update to the same database.
;; - It is not safe for multiple threads to perform concurrent updates on the same database.
;; - While it should be possible to recover from typical process failure and interruption, sudden
;;   host system failure and interruption may corrupt data.

;; A database is a persistent collection of extensional relations, and is represented as a
;; filesystem directory managed by an instance of checkpointing storage.

;; An extensional relation is a uniquely named, finite set of ordered tuples.  Each tuple in the set
;; contains the same number of fields.  For each field, every tuple assigns the same name and type
;; to that field.  The list of field types is called the relation's type.  The field names are
;; called the relation's attributes, and each name must be unique within the relation.

;; For efficient querying, a relation can be indexed on one or more permutations of a subset of its
;; attributes.  An index provides an efficient way to filter and enumerate a subset of the
;; relation's tuples.  To filter the tuples, constraints are applied to a subset of the relation's
;; attributes, eliminating tuples that do not satisfy the constraints.  The index's attribute
;; permutation is the order in which attributes will be constrained.  Constraining attributes in
;; this order is efficient due to the representation of the index, which is the result of sorting
;; the relation's tuples lexicographically according to the attribute permutation.

;; Databases and relations can be modified:
;; - New relations can be added to, and existing relations can be removed from, a database.
;; - Relations and their attributes can be renamed.
;; - Indexes can be added to, or removed from, a relation.
;; - Tuples can be inserted into, or deleted from, a relation.
;; - Relations may be compacted to improve lookup efficiency.

;; Modifications to a database and its relations can be used immediately, but will not persist until
;; they are explicitly committed.  Uncommitted modifications can be reverted, restoring the most
;; recently committed version of the database.

(define version.current '2022-2-22)

(define auto-empty-trash?  (make-parameter #f))
(define current-batch-size (make-parameter (expt 2 29)))

(define (database-path             db)                      ((wrapped-database-controller db) 'path))
(define (database-commit!          db)                      ((wrapped-database-controller db) 'commit!))
(define (database-revert!          db)                      ((wrapped-database-controller db) 'revert!))
(define (database-trash-empty!     db)                      ((wrapped-database-controller db) 'trash-empty!))
(define (database-relation-names   db)                      ((wrapped-database-controller db) 'relation-names))
(define (database-relation-name?   db name)                 ((wrapped-database-controller db) 'relation-name? name))
(define (database-relation         db name)                 ((wrapped-database-controller db) 'relation       name))
(define (database-relation-builder db type (batch-size #f)) ((wrapped-database-controller db)
                                                             'relation-builder type (or batch-size (current-batch-size))))
(define (database-relation-new     db type)                 (let-values (((_ finish) (database-relation-builder db type 0)))
                                                              (finish)))
(define (database-relation-add!    db name attrs type)      (let ((r (database-relation-new db type)))
                                                              (relation-name-set!       r name)
                                                              (relation-attributes-set! r attrs)
                                                              r))

(define (relation-database        r)       ((wrapped-relation-controller r) 'database))
(define (relation-has-name?       r)       ((wrapped-relation-controller r) 'has-name?))
(define (relation-name            r)       ((wrapped-relation-controller r) 'name))
(define (relation-attributes      r)       ((wrapped-relation-controller r) 'attributes))
(define (relation-type            r)       ((wrapped-relation-controller r) 'type))
(define (relation-indexes         r)       ((wrapped-relation-controller r) 'indexes))
(define (relation-delete!         r)       ((wrapped-relation-controller r) 'delete!))
(define (relation-name-set!       r name)  ((wrapped-relation-controller r) 'name-set!       name))
(define (relation-attributes-set! r attrs) ((wrapped-relation-controller r) 'attributes-set! attrs))
(define (relation-assign!         r expr)  ((wrapped-relation-controller r) 'assign!         expr))
(define (relation-index-add!      r . ixs) ((wrapped-relation-controller r) 'index-add!      ixs))
(define (relation-index-remove!   r . ixs) ((wrapped-relation-controller r) 'index-remove!   ixs))
(define (relation-full-compact!        r)  ((wrapped-relation-controller r) 'full-compact!))
(define (relation-incremental-compact! r)  ((wrapped-relation-controller r) 'incremental-compact!))

(struct wrapped-database (controller)
        #:methods gen:custom-write
        ((define write-proc (make-constructor-style-printer
                              (lambda (db) 'database)
                              (lambda (db) (list (database-path db)))))))

(struct wrapped-relation (key controller)
        #:methods gen:custom-write
        ((define write-proc (make-constructor-style-printer
                              (lambda (r)
                                (if ((wrapped-relation-controller r) 'valid?)
                                  'relation
                                  'deleted-relation))
                              (lambda (r)
                                (if (and ((wrapped-relation-controller r) 'valid?)
                                         (relation-has-name? r))
                                  (list (relation-name r))
                                  '()))))))

(define R.empty     '())
(define (R+ . args) (cons '+ args))
(define (R- e0 e1)  `(- ,e0 ,e1))

(define T.empty     '())
(define (T+ . args) (cons '+ args))
(define (T- e0 e1)  `(- ,e0 ,e1))

(define (database path.db)
  (define (make-relation id.self)
    (define (invalidate!)
      (hash-remove! id=>R (list id.self))
      (set! self #f))
    (define (remove-name!)
      (when (R-has-name? id.self)
        (stg-update! 'name=>relation-id (lambda (n=>rid) (hash-remove n=>rid (R-name id.self))))))
    (define (index-signature->ordering ix)
      (valid-attributes?! ix)
      (let* ((attrs   (R-attrs id.self))
             (a->i    (lambda (attr) (let ((i (index-of attrs attr)))
                                       (unless i (error "invalid index attribute" attr ix attrs))
                                       (+ i 1))))
             (ord.0   (map a->i ix))
             (missing (set-subtract (list->set attrs) (list->set ix))))
        (cond ((set-empty? missing)      ord.0)
              ((= (set-count missing) 1) (append ord.0 (list (a->i (car (set->list missing))))))
              (else                      (append ord.0 '(0))))))
    (define (update-indexes! update)
      (stg-update! 'relation-id=>indexes (lambda (rid=>os) (hash-update rid=>os id.self update))))
    (define self
      (method-lambda
        ((valid?)      #t)
        ((database)    db)
        ((invalidate!) (invalidate!))
        ((has-name?)   (R-has-name? id.self))
        ((name)        (R-name      id.self))
        ((attributes)  (R-attrs     id.self))
        ((type)        (R-type      id.self))
        ((table-expr)  (R-texpr     id.self))
        ((indexes)     (hash-keys (hash-ref (stg-ref 'relation-id=>indexes)    id.self)))
        ((name-set! name)
         (unless (and (R-has-name? id.self) (equal? (R-name id.self) name))
           (new-relation?! name)
           (remove-name!)
           (stg-update! 'name=>relation-id (lambda (n=>rid) (hash-set n=>rid name id.self)))
           (stg-update! 'relation-id=>name (lambda (rid=>n) (hash-set rid=>n id.self name)))))
        ((attributes-set! attrs)
         (valid-attributes?! attrs)
         (let ((type (R-type id.self)))
           (unless (= (length attrs) (length type))
             (error "number of attributes must match the relation type arity"
                    attrs type)))
         (stg-update! 'relation-id=>attributes (lambda (rid=>as) (hash-set rid=>as id.self attrs))))
        ((assign! expr)      (R-assign-r! id.self expr))
        ((index-add!    ixs) (update-indexes! (lambda (os)
                                                (foldl (lambda (ordering os) (hash-set os ordering #t))
                                                       os
                                                       (map index-signature->ordering ixs)))))
        ((index-remove! ixs) (update-indexes! (lambda (os)
                                                (foldl (lambda (ordering os) (hash-remove os ordering))
                                                       os
                                                       (map index-signature->ordering ixs)))))
        ((full-compact!)        (stg-update! 'relations-to-fully-compact
                                             (lambda (rids) (hash-set rids id.self #t))))
        ((incremental-compact!) (stg-update! 'relations-to-incrementally-compact
                                             (lambda (rids) (hash-set rids id.self #t))))
        ((delete!)
         (set-remove! rids.new id.self)
         (remove-name!)
         (stg-update! 'relation-id=>name       (lambda (rid=>n)  (hash-remove rid=>n  id.self)))
         (stg-update! 'relation-id=>attributes (lambda (rid=>as) (hash-remove rid=>as id.self)))
         (stg-update! 'relation-id=>type       (lambda (rid=>t)  (hash-remove rid=>t  id.self)))
         (stg-update! 'relation-id=>table-expr (lambda (rid=>te) (hash-remove rid=>te id.self)))
         (stg-update! 'relation-id=>indexes    (lambda (rid=>os) (hash-remove rid=>os id.self)))
         (invalidate!))))
    (lambda args (apply (or self (method-lambda
                                   ((valid?) #f)))
                        args)))

  (define (relation-builder id.R batch-size.bytes)
    (define R (id->R id.R))
    (define checkpoint.current (storage-checkpoint-count stg))
    (define (valid?)
      (and ((wrapped-relation-controller R) 'valid?)
           (equal? (storage-checkpoint-count stg) checkpoint.current)))
    (define (invalidate!) (set! checkpoint.current #f))
    (cond
      ((= 0 batch-size.bytes)
       (values #f (lambda ()
                    (unless (valid?) (error "cannot use a stale relation builder"))
                    (invalidate!)
                    R)))
      ((< 0 batch-size.bytes)
       (define (start-batch!)
         (hash-clear! text=>id)
         (set! i.tuple   0)
         (set! size.text 0))
       (define (insert! tuple)
         (for-each (lambda (field proj v.col)
                     (fxvector-set! v.col i.tuple (proj field)))
                   tuple projections vs.col)
         (set! i.tuple (unsafe-fx+ i.tuple 1))
         (when (or (unsafe-fx=  column-size      i.tuple)
                   (unsafe-fx<= batch-size.bytes size.text))
           (finish-batch!)
           (start-batch!)))
       (define (finish!)
         (when (< 0 i.tuple) (finish-batch!))
         (R-assign-t! id.R (apply T+ (reverse tables)))
         (invalidate!)
         R)
       (define (finish-batch!)
         (unless (valid?) (error "cannot use a stale relation builder"))
         (define column-id.text
           (and (ormap (lambda (type.col) (eqv? type.col 'text)) column-types)
                (let* ((width.pos        (nat-min-byte-width size.text))
                       (count.ids        (hash-count text=>id))
                       (id=>id           (make-fxvector count.ids))
                       (cid.text.value   (fresh-column-id))
                       (cid.text.pos     (fresh-column-id))
                       (cid.text         (fresh-column-id))
                       (bname.text.value (cons 'column cid.text.value))
                       (bname.text.pos   (cons 'column cid.text.pos)))
                  (define pos.final
                    (call-with-output-file
                      (storage-block-new! stg bname.text.value)
                      (lambda (out.text.value)
                        (call-with-output-file
                          (storage-block-new! stg bname.text.pos)
                          (lambda (out.text.pos)
                            (define (write-pos)
                              (write-byte-width-nat width.pos out.text.pos (file-position out.text.value)))
                            (let ((t&id* (performance-log
                                           `(sorting ,(hash-count text=>id) text values)
                                           (sort (hash->list text=>id)
                                                 (lambda (a b) (bytes<? (car a) (car b)))))))
                              (performance-log
                                `(writing text column: ,size.text bytes)
                                (write-pos)
                                (let loop ((i 0) (t&id* t&id*))
                                  (unless (null? t&id*)
                                    (let* ((t&id (car t&id*))
                                           (text (car t&id))
                                           (id   (cdr t&id)))
                                      (write-bytes text out.text.value)
                                      (write-pos)
                                      (unsafe-fxvector-set! id=>id id i)
                                      (loop (unsafe-fx+ i 1) (cdr t&id*)))))
                                (file-position out.text.value))))))))
                  (add-columns! cid.text.value (hash 'class     'block
                                                     'name      bname.text.value
                                                     'bit-width 8
                                                     'count     size.text
                                                     'offset    0
                                                     'min       0
                                                     'max       255)
                                cid.text.pos   (hash 'class     'block
                                                     'name      bname.text.pos
                                                     'bit-width (* 8 width.pos)
                                                     'count     (+ 1 count.ids)
                                                     'offset    0
                                                     'min       0
                                                     'max       pos.final)
                                cid.text       (hash 'class     'text
                                                     'position  cid.text.pos
                                                     'value     cid.text.value))
                  (for-each (lambda (type v.col)
                              (when (eqv? type 'text)
                                (let loop ((i (unsafe-fx- i.tuple 1)))
                                  (when (unsafe-fx<= 0 i)
                                    (unsafe-fxvector-set! v.col i
                                                          (unsafe-fxvector-ref
                                                            id=>id
                                                            (unsafe-fxvector-ref v.col i)))
                                    (loop (unsafe-fx- i 1))))))
                            column-types vs.col)
                  cid.text)))
         (define count.tuples.unique (performance-log `(sorting ,i.tuple tuples)
                                                      (table-sort-and-dedup! i.tuple vs.col)))
         (let ((table-id (build-table column-types column-id.text vs.col count.tuples.unique)))
           (pretty-log `(inserted batch of ,count.tuples.unique unique tuples)
                       `(,size.text bytes for ,(hash-count text=>id) unique text values))
           (set! tables (cons table-id tables))))
       (define (text->id bs)
         (or (hash-ref text=>id bs #f)
             (let ((id (hash-count text=>id)))
               (hash-set! text=>id bs id)
               (set! size.text (+ size.text (bytes-length bs)))
               id)))
       (define (identity x) x)
       (define column-types (relation-type R))
       (define column-size  (max (quotient batch-size.bytes (* (length column-types) 8)) 2))
       (define tables       '())
       (define i.tuple      0)
       (define size.text    0)
       (define text=>id     (make-hash))
       (define vs.col       (map (lambda (_) (make-fxvector column-size)) column-types))
       (define projections  (map (lambda (ctype) (if (eqv? ctype 'text)
                                                   text->id
                                                   identity))
                                 column-types))
       (start-batch!)
       (values insert! finish!))
      (else (error "invalid batch size" batch-size.bytes))))

  (define (build-table column-types column-id.text vecs.col row-count)
    (let ((table-id        (fresh-table-id))
          (cid.primary-key (fresh-column-id)))
      (add-columns! cid.primary-key (hash 'class  'line
                                          'count  row-count
                                          'offset 0
                                          'step   1))
      (let ((cids.attrs (map (lambda (type.col vec.col)
                               (let ((id.col (performance-log `(writing column: ,row-count values)
                                                              (write-fx-column vec.col row-count))))
                                 (cond ((eqv? type.col 'text)
                                        (let ((id.remap (fresh-column-id)))
                                          (add-columns! id.remap (hash 'class  'remap
                                                                       'local  id.col
                                                                       'global column-id.text))
                                          id.remap))
                                       (else id.col))))
                             column-types vecs.col)))
        (stg-update! 'table-id=>column-ids
                     (lambda (tid=>cids)
                       (hash-set tid=>cids table-id (cons cid.primary-key cids.attrs)))))
      table-id))

  (define (build-table-indexes! ordering tids)
    (let ((prefixes (ordering->prefixes ordering)))
      (for-each
        (lambda (tid)
          (define (has-index-prefix? prefix)
            (hash-has-key? (stg-ref 'index-prefix=>key-column-id) (cons tid prefix)))
          (unless (has-index-prefix? (car prefixes))
            (define prefixes.needed  (map (lambda (p) (and (not (has-index-prefix? p)) p))
                                          (reverse prefixes)))
            (define cid=>desc        (stg-ref 'column-id=>column))
            (define descs.col        (map (lambda (cid) (hash-ref cid=>desc cid))
                                          (let ((cids (hash-ref (stg-ref 'table-id=>column-ids) tid)))
                                            (map (lambda (pos) (list-ref cids pos))
                                                 (car prefixes)))))
            (define vs.col           (performance-log `(reading ,(length descs.col) columns)
                                                      (map read-fx-column descs.col)))
            (define count.table      (performance-log `(sorting ,(fxvector-length (car vs.col)) tuples)
                                                      (table-sort-and-dedup! (fxvector-length (car vs.col)) vs.col)))
            (define vs.pos           (map (lambda (p) (and p (make-fxvector (+ count.table 1))))
                                          (cdr prefixes.needed)))
            (for-each (lambda (v.pos) (when v.pos (unsafe-fxvector-set! v.pos 0 0)))
                      vs.pos)
            (define counts.key
              (performance-log
                `(grouping keys for ,count.table tuples)
                (let loop.main ((vs.key vs.col)
                                (vs.pos vs.pos)
                                (pos*   (make-list (length vs.col) 0))
                                (start  0)
                                (end    count.table))
                  (if (null? vs.pos)
                    (list (unsafe-fx+ (car pos*) (unsafe-fx- end start)))  ; final key column is already deduplicated
                    (let ((v.key (car vs.key)) (v.pos (car vs.pos)))
                      (let loop.key ((pos (car pos*)) (pos* (cdr pos*)) (start start) (end end))
                        (if (unsafe-fx= start end)
                          (cons pos pos*)
                          (let* ((key       (unsafe-fxvector-ref v.key start))
                                 (start.new (unsafe-bisect-next
                                              start end (lambda (i) (unsafe-fx<= (unsafe-fxvector-ref v.key i)
                                                                                 key)))))
                            (unsafe-fxvector-set! v.key pos key)
                            (let ((pos* (loop.main (cdr vs.key) (cdr vs.pos) pos* start start.new)))
                              (when v.pos (unsafe-fxvector-set! v.pos (unsafe-fx+ pos 1) (car pos*)))
                              (loop.key (unsafe-fx+ pos 1) pos* start.new end))))))))))
            (for-each
              (lambda (prefix.needed v.col v.pos count.key)
                (when prefix.needed
                  (define iprefix (cons tid prefix.needed))
                  (stg-update! 'index-prefix=>key-column-id
                               (lambda (iprefix=>cid)
                                 (hash-set iprefix=>cid iprefix
                                           (performance-log
                                             `(writing key column: ,count.key values)
                                             (write-fx-column v.col count.key)))))
                  (when v.pos
                    (stg-update! 'index-prefix=>position-column-id
                                 (lambda (iprefix=>cid)
                                   (hash-set iprefix=>cid iprefix
                                             (performance-log
                                               `(writing position column: ,(+ count.key 1) values)
                                               (write-fx-column v.pos (+ count.key 1)))))))))
              prefixes.needed vs.col (cons #f vs.pos) counts.key)
            (pretty-log `(indexed table: ,tid ordering: ,ordering))
            (checkpoint!)))
        tids)))

  (define (merge-text-columns cids.text.original)
    (match cids.text.original
      ('()             (values #f       (hash)))
      ((list cid.text) (values cid.text (hash)))
      (_ (define custodian.gs (make-custodian))
         (define *g&count&id=>id
           (let ((cid=>c (stg-ref 'column-id=>column)))
             (parameterize ((current-custodian custodian.gs))
               (map (lambda (cid)
                      (define desc.text (hash-ref cid=>c cid))
                      (define count     (column-count desc.text))
                      (define s         ((column->start->s desc.text) 0))
                      (define id=>id    (make-fxvector count))
                      (list (and (< 0 count)
                                 (let loop ((id 0) (s s))
                                   (match (s) ; assume uniform stream
                                     ((cons text s)  (cons text (lambda (i)
                                                                  (unsafe-fxvector-set! id=>id id i)
                                                                  (loop (unsafe-fx+ id 1) s))))
                                     (_              #f))))
                            count
                            id=>id))
                    cids.text.original))))
         (define gs               (map car   *g&count&id=>id))
         (define counts           (map cadr  *g&count&id=>id))
         (define id=>ids          (map caddr *g&count&id=>id))
         (define vec.pos          (make-fxvector (foldl + 0 counts)))
         (define cid.text.value   (fresh-column-id))
         (define cid.text         (fresh-column-id))
         (define bname.text.value (cons 'column cid.text.value))
         (define count.ids        (let ((i.pos 0))
                                    (call-with-output-file
                                      (storage-block-new! stg bname.text.value)
                                      (lambda (out)
                                        (define (write-pos)
                                          (fxvector-set! vec.pos i.pos (file-position out))
                                          (set! i.pos (unsafe-fx+ i.pos 1)))
                                        (write-pos)
                                        (performance-log
                                          `(merging text columns with counts: . ,counts)
                                          ((unsafe-multi-merge (lambda (g.0 g.1) (bytes<? (car g.0) (car g.1)))
                                                               (filter-not not gs)
                                                               not
                                                               car
                                                               (lambda (g i) ((cdr g) i)))
                                           (lambda (text)
                                             (write-bytes text out)
                                             (write-pos))))))))
         (custodian-shutdown-all custodian.gs) ; close all block file ports
         (add-columns! cid.text.value (hash 'class     'block
                                            'name      bname.text.value
                                            'bit-width 8
                                            'count     (fxvector-ref vec.pos count.ids)
                                            'offset    0
                                            'min       0
                                            'max       255)
                       cid.text       (hash 'class     'text
                                            'position  (write-fx-column vec.pos (+ count.ids 1))
                                            'value     cid.text.value))
         (values cid.text (make-immutable-hash (map cons cids.text.original id=>ids))))))

  (define (merge-text-columns/tables tids.original)
    (define (cid->text-cid cid) (column->text-cid (hash-ref cid=>c cid)))
    (define tid=>cids (stg-ref 'table-id=>column-ids))
    (define cid=>c    (stg-ref 'column-id=>column))
    (define cids.text.original
      (set->list
        (list->set
          (append*
            (map (lambda (tid)
                   (filter-not not (map cid->text-cid (hash-ref tid=>cids tid))))
                 tids.original)))))
    (define-values (cid.text.new cid.text=>id=>id) (merge-text-columns cids.text.original))
    (define cid.text=>cid.global.new
      (make-immutable-hash
        (hash-map cid.text=>id=>id
                  (lambda (cid.text id=>id)
                    (cons cid.text
                          (let ((cid.global.new (fresh-column-id)))
                            (add-columns! cid.global.new
                                          (hash 'class  'remap
                                                'local  (write-fx-column id=>id (fxvector-length id=>id))
                                                'global cid.text.new))
                            cid.global.new))))))
    (make-immutable-hash
      (map (lambda (tid)
             (let* ((cids.original (hash-ref tid=>cids tid))
                    (cids.new
                      (map (lambda (cid.original)
                             (or (let ((desc.original (hash-ref cid=>c cid.original)))
                                   (let loop ((desc desc.original))
                                     (case (hash-ref desc 'class)
                                       ((remap) (let* ((cid.global  (hash-ref desc 'global))
                                                       (desc.global (hash-ref cid=>c cid.global)))
                                                  (let ((cid.global.new
                                                          (case (hash-ref desc.global 'class)
                                                            ((text) (hash-ref cid.text=>cid.global.new cid.global #f))
                                                            (else   (loop desc.global)))))
                                                    (and cid.global.new
                                                         (let ((cid.new (fresh-column-id)))
                                                           (add-columns! cid.new (hash 'class  'remap
                                                                                       'local  (hash-ref desc 'local)
                                                                                       'global cid.global.new))
                                                           cid.new)))))
                                       (else    #f))))
                                 cid.original))
                           cids.original)))
               (cons tid (if (equal? cids.original cids.new)
                           tid
                           (let ((tid.new (fresh-table-id)))
                             (stg-update! 'table-id=>column-ids
                                          (lambda (tid=>cids) (hash-set tid=>cids tid.new cids.new)))
                             tid.new)))))
           tids.original)))

  (define (merge-table-expr type.table texpr)
    (let* ((cid=>c    (stg-ref 'column-id=>column))
           (tid=>cids (stg-ref 'table-id=>column-ids))
           (cid.text  ; All tables must depend on the same text value column, if any
             (let ((i.text-col (ormap (lambda (type.col i) (and (eqv? type.col 'text) i))
                                      type.table (range 1 (+ (length type.table) 1)))))
               (and i.text-col
                    (let loop ((texpr texpr))
                      (match texpr
                        ('()          #f)
                        (`(+ . ,ts)   (ormap loop ts))
                        (`(- ,t0 ,t1) (or (loop t0) (loop t1)))
                        (table-id     (column->text-cid (hash-ref cid=>c (list-ref (hash-ref tid=>cids table-id)
                                                                                   i.text-col)))))))))
           (dict.tuples
             (let loop ((texpr texpr))
               (match texpr
                 ('()          dict.empty)
                 (`(+ . ,ts)   (apply dict:union unsafe-int-tuple<? (lambda _ '()) (map loop ts)))
                 (`(- ,t0 ,t1) (dict:diff unsafe-int-tuple<? 1 (loop t0) (loop t1)))
                 (table-id     (let ((cids (hash-ref tid=>cids table-id)))
                                 (dict:monovec (table->monovec
                                                 (map (lambda (cid) (hash-ref cid=>c cid))
                                                      (cdr cids))) ; (car cids) is the row id, not tuple data
                                               (lambda (_) '())
                                               0
                                               (column-count (hash-ref cid=>c (car cids)))))))))
           (count.worst-case
             (let loop ((texpr texpr))
               (match texpr
                 ('()          0)
                 (`(+ . ,ts)   (foldl + 0 (map loop ts)))
                 (`(- ,t0 ,t1) (loop t0))
                 (table-id     (column-count (hash-ref cid=>c (car (hash-ref tid=>cids table-id))))))))
           (vecs.col (map (lambda (_) (make-fxvector count.worst-case))
                          type.table))
           (i.tuple  0))
      (performance-log
        `(merging table expr: ,texpr)
        ((dict-key-enumerator dict.tuples)
         (lambda (tuple)
           ;; TODO: work with vectors instead of lists, for efficiency: see table->monovec
           (for-each (lambda (vec.col value) (unsafe-fxvector-set! vec.col i.tuple value))
                     vecs.col tuple)
           (set! i.tuple (unsafe-fx+ i.tuple 1)))))
      (if (< 0 i.tuple)
        (build-table type.table cid.text vecs.col i.tuple)
        '())))

    ;; TODO: test these
    ;; - merge text columns, producing id=>id remappings
    ;; - apply each id=>id remapping to columns and rebuilt original tables
    ;; - merge the rebuilt tables (according to a table-expr)
    ;; TODO: implement these operations:
    ;; - text value gc
    ;;   - compute a table's reachable text ids
    ;;     - treat each text column as a 1-column table, merge those, and enumerate the sorted/deduped ids
    ;;   - drop ids from a text column
    ;;     - (text-remove-ids desc.text ids)  ==>  desc.text.new
    ;;       - id=>id remapping is implied by the set of removed ids

  (define (compact-relations! rids)
    (unless (null? rids)
      (pretty-log `(fully compacting ,(length rids) relations))
      (let ((tid=>tid (merge-text-columns/tables
                        (set->list
                          (list->set
                            (append* (map (lambda (rid) (table-expr->table-ids (R-texpr rid)))
                                          rids)))))))
        (for-each (lambda (rid) (R-assign-t! rid (table-expr-map (lambda (tid) (hash-ref tid=>tid tid))
                                                                 (R-texpr rid))))
                  rids))
      (checkpoint!)
      (for-each compact-relation-fully! rids)
      ;; TODO: garbage collect unreachable shared text values
      ;; TODO: after text value gc and applying remappings, eliminate those remappings by rewriting the affected columns
      ))

  (define (compact-relation-fully! rid)
    (let ((texpr (R-texpr rid)))
      (when (pair? texpr)
        (when (R-has-name? rid) (pretty-log `(fully compacting relation: ,(R-name rid))))
        (R-assign-t! rid (merge-table-expr (R-type rid) texpr))
        (checkpoint!))))

  (define (compact-relation-incrementally! rid)
    (when (R-has-name? rid) (pretty-log `(incrementally compacting relation: ,(R-name rid))))
    ;; TODO:
    ;; - identify portion of table-expr to compact
    ;; - consolidate relevant text columns into one shared text column
    ;; - merge tables and update table-expr
    (void))

  (define (collect-garbage!)
    (define (remove-unreachable! stg-key reachable)
      (stg-update! stg-key
                   (lambda (h)
                     (foldl (lambda (k h) (hash-remove h k))
                            h
                            (set->list (set-subtract (list->set (hash-keys h)) reachable))))))
    (let* ((cid=>c (stg-ref 'column-id=>column))
           (table-ids.reachable
             (list->set (append* (map table-expr->table-ids (hash-values (stg-ref 'relation-id=>table-expr))))))
           (index-prefixes.reachable
             (list->set (append* (hash-map (stg-ref 'relation-id=>indexes)
                                           (lambda (rid indexes)
                                             (let ((ordering-prefixes
                                                     (append* (map ordering->prefixes (hash-keys indexes)))))
                                               (append* (map (lambda (tid)
                                                               (map (lambda (oprefix) (cons tid oprefix))
                                                                    ordering-prefixes))
                                                             (table-expr->table-ids (R-texpr rid))))))))))
           (stgkey.ixp=>cid->cids
             (lambda (stgkey)
               (list->set (filter-not not (set-map index-prefixes.reachable
                                                   (lambda (iprefix) (hash-ref (stg-ref stgkey) iprefix #f)))))))
           (column-ids.reachable
             (set-fixed-point
               (set-union
                 (list->set (append* (set-map table-ids.reachable
                                              (lambda (tid) (hash-ref (stg-ref 'table-id=>column-ids) tid)))))
                 (stgkey.ixp=>cid->cids 'index-prefix=>key-column-id)
                 (stgkey.ixp=>cid->cids 'index-prefix=>position-column-id))
               (lambda (cids) (apply set-union (set)
                                     (set-map cids (lambda (cid)
                                                     (let loop ((cid cid))
                                                       (let ((desc (hash-ref cid=>c cid)))
                                                         (case (hash-ref desc 'class)
                                                           ((remap) (set-union (set cid)
                                                                               (loop (hash-ref desc 'local))
                                                                               (loop (hash-ref desc 'global))))
                                                           ((text)  (set-union (set cid)
                                                                               (loop (hash-ref desc 'position))
                                                                               (loop (hash-ref desc 'value))))
                                                           (else    (set cid))))))))))))
      (apply storage-block-remove-names!
             stg
             (set->list (set-subtract (list->set (storage-block-names stg))
                                      (apply set-union (set)
                                             (set-map column-ids.reachable
                                                      (lambda (cid)
                                                        (let ((desc (hash-ref cid=>c cid)))
                                                          (case (hash-ref desc 'class)
                                                            ((block) (set (hash-ref desc 'name)))
                                                            (else    (set))))))))))
      (remove-unreachable! 'table-id=>column-ids             table-ids.reachable)
      (remove-unreachable! 'index-prefix=>key-column-id      index-prefixes.reachable)
      (remove-unreachable! 'index-prefix=>position-column-id index-prefixes.reachable)
      (remove-unreachable! 'column-id=>column                column-ids.reachable))
    (checkpoint!))

  (define (text-column->text=>id desc) (let ((count (column-count desc)))
                                         (dict:ref (column->ref desc) bytes<? bytes<=?
                                                   (column->ref (hash 'class  'line
                                                                      'count  count
                                                                      'offset 0
                                                                      'step   1))
                                                   0 count)))
  (define (text-column->id=>text desc) (let ((count (column-count desc)))
                                         (dict:monovec
                                           (column->monovec (hash 'class  'line
                                                                  'count  count
                                                                  'offset 0
                                                                  'step   1))
                                           (column->ref desc)
                                           0 count)))

  (define (block-desc->path desc.col) (storage-block-path stg (hash-ref desc.col 'name)))

  (define (column-count desc.col)
    (case (hash-ref desc.col 'class)
      ((line block) (hash-ref desc.col 'count))
      ((text)       (- (column-count (hash-ref (stg-ref 'column-id=>column) (hash-ref desc.col 'position))) 1))
      ((remap)         (column-count (hash-ref (stg-ref 'column-id=>column) (hash-ref desc.col 'local))))
      (else         (error "column-count unimplemented for column class" desc.col))))

  (define ((column->start->s desc.col) start)
    (case (hash-ref desc.col 'class)
      ((line)  (let* ((count (max (- (hash-ref desc.col 'count) start) 0))
                      (step  (hash-ref desc.col 'step)))
                 (let loop ((i 0) (value (+ (hash-ref desc.col 'offset) (* step start))))
                   (lambda ()
                     (if (unsafe-fx< i count)
                       (cons value (loop (unsafe-fx+ i 1) (unsafe-fx+ value step)))
                       '())))))
      ((block) (let* ((count.0 (hash-ref desc.col 'count))
                      (count   (max (- count.0 start) 0))
                      (width   (unsafe-fxrshift (hash-ref desc.col 'bit-width) 3))
                      (offset  (hash-ref desc.col 'offset))
                      (in      (open-input-file (block-desc->path desc.col))))
                 (file-position in (* width (min count.0 start)))
                 (let loop ((i 0))
                   (lambda ()
                     (if (unsafe-fx< i count)
                       (cons (unsafe-bytes-nat-ref width (read-bytes width in) 0)
                             (loop (unsafe-fx+ i 1)))
                       '())))))
      ((text)  (let* ((cid=>c (stg-ref 'column-id=>column))
                      (s.pos  ((column->start->s (hash-ref cid=>c (hash-ref desc.col 'position)))
                               (+ start 1)))
                      (in     (open-input-file (block-desc->path (hash-ref cid=>c (hash-ref desc.col 'value))))))
                 (let loop ((s.pos s.pos) (pos.current 0))
                   (lambda ()
                     (match (s.pos) ; assume uniform stream
                       ((cons pos.next s.pos) (cons (read-bytes (unsafe-fx- pos.next pos.current) in)
                                                    (loop s.pos pos.next)))
                       (_                     '()))))))
      ((remap) (let ((s   ((column->start->s (hash-ref (stg-ref 'column-id=>column) (hash-ref desc.col 'local)))
                           start))
                     (ref (column->ref (hash-ref (stg-ref 'column-id=>column) (hash-ref desc.col 'global)))))
                 (values count (s-map ref s))))
      (else    (error "column->start->s unimplemented for column class" desc.col))))

  (define (table->monovec descs)
    ;; TODO: work with vectors instead of lists, for efficiency
    (let* ((unique?s    (map (lambda (d) (and (eq? (hash-ref d 'class) 'line)
                                              (unsafe-fx< 0 (hash-ref d 'step))))
                             descs))
           (monovs      (map column->monovec    descs))
           (refs        (map monovec-ref        monovs))
           (unref-nexts (map monovec-unref-next monovs))
           (unref-prevs (map monovec-unref-prev monovs)))
      (monovec (lambda (i) (map (lambda (ref) (ref i)) refs))
               (lambda (inclusive? i.start i.end vs)
                 (let loop ((v        (car vs))
                            (vs       (cdr vs))
                            (un       (car unref-nexts))
                            (uns      (cdr unref-nexts))
                            (unique?  (car unique?s))
                            (unique?s (cdr unique?s))
                            (start    i.start)
                            (end      i.end))
                   (if (null? vs)
                     (un inclusive? start end v)
                     (let ((start.next (un #t start end v)))
                       (if (unsafe-fx<= end start.next)
                         end
                         (let ((end.next (if unique?
                                           (unsafe-fx+ start.next 1)
                                           (un #f start.next end v))))
                           (if (unsafe-fx<= end end.next)
                             end
                             (loop (car vs)       (cdr vs)
                                   (car uns)      (cdr uns)
                                   (car unique?s) (cdr unique?s)
                                   start.next     end.next))))))))
               (lambda (inclusive? i.start i.end vs)
                 (let loop ((v        (car vs))
                            (vs       (cdr vs))
                            (un       (car unref-prevs))
                            (uns      (cdr unref-prevs))
                            (unique?  (car unique?s))
                            (unique?s (cdr unique?s))
                            (start    i.start)
                            (end      i.end))
                   (if (null? vs)
                     (un inclusive? start end v)
                     (let ((end.prev (un #t start end v)))
                       (if (unsafe-fx<= end.prev start)
                         start
                         (let ((start.prev (if unique?
                                             (unsafe-fx- end.prev 1)
                                             (un #f start end.prev v))))
                           (if (unsafe-fx<= start.prev start)
                             start
                             (loop (car vs)       (cdr vs)
                                   (car uns)      (cdr uns)
                                   (car unique?s) (cdr unique?s)
                                   start.prev     end.prev)))))))))))

  (define (column->text-cid desc)
    (case (hash-ref desc 'class)
      ((remap)           (let* ((cid  (hash-ref desc 'global))
                                (desc (hash-ref (stg-ref 'column-id=>column) cid)))
                           (case (hash-ref desc 'class)
                             ((text) cid)
                             (else   (column->text-cid desc)))))
      (else              #f)))

  (define (column->ref desc)
    (case (hash-ref desc 'class)
      ((text) (let* ((cid=>c  (stg-ref 'column-id=>column))
                     (ref.pos (column->ref (hash-ref cid=>c (hash-ref desc 'position))))
                     (in      (open-input-file (block-desc->path (hash-ref cid=>c (hash-ref desc 'value))))))
                (lambda (i) (let ((pos.i   (ref.pos i))
                                  (pos.i+1 (ref.pos (unsafe-fx+ i 1))))
                              (file-position in pos.i)
                              (read-bytes (unsafe-fx- pos.i+1 pos.i) in)))))
      (else   (match-define (monovec ref fnext fprev) (column->monovec desc))
              (match (column->text-cid desc)
                (#f       ref)
                (cid.text (let ((ref.text (column->ref (hash-ref (stg-ref 'column-id=>column) cid.text))))
                            (lambda (i) (ref.text (ref i)))))))))

  (define (column->monovec desc)
    (define (ref->monovec ref) (monovec ref
                                        (find-next:ref ref unsafe-fx< unsafe-fx<=)
                                        (find-prev:ref ref unsafe-fx< unsafe-fx<=)))
    (case (hash-ref desc 'class)
      ((line)  (let ((step   (hash-ref desc 'step))
                     (offset (hash-ref desc 'offset)))
                 (if (unsafe-fx= step 0)
                   (ref->monovec (lambda (_) offset))
                   (monovec (lambda (i) (unsafe-fx+ (unsafe-fx* step i) offset))
                            (find-next:line offset step)
                            (find-prev:line offset step)))))
      ((block) (let* ((width  (unsafe-fxrshift (hash-ref desc 'bit-width) 3))
                      (offset (hash-ref desc 'offset))
                      (in     (open-input-file (block-desc->path desc)))
                      (ref    (lambda (i)
                                (file-position in (* width i))
                                (unsafe-bytes-nat-ref width (read-bytes width in) 0))))
                 (ref->monovec ref)))
      ((text)  #f)
      ((remap) (let* ((cid=>c      (stg-ref 'column-id=>column))
                      (desc.global (hash-ref cid=>c (hash-ref desc 'global)))
                      (monov.local (column->monovec (hash-ref cid=>c (hash-ref desc 'local)))))
                 (match (column->monovec desc.global)
                   (#f monov.local)
                   ((monovec ref.global fnext.global fprev.global)
                    (match-define (monovec ref.local fnext.local fprev.local) monov.local)
                    (define (ref i) (ref.global (ref.local i)))
                    (case (hash-ref desc.global 'class)
                      ;; We use inverse lookup for lines because it should be more efficient.
                      ;; TODO: test that this is worth the extra complexity.
                      ((line) (if (unsafe-fx= (hash-ref desc.global 'step) 0)
                                (ref->monovec ref)
                                (let ((end.global (hash-ref desc.global 'count)))
                                  (monovec ref
                                           (lambda (inclusive? i.start i.end v)
                                             (let ((i.global (fnext.global #t 0 end.global v)))
                                               (if (unsafe-fx< i.global end.global)
                                                 (fnext.local (or inclusive? (unsafe-fx< v (ref.global i.global)))
                                                              i.start i.end i.global)
                                                 i.end)))
                                           (lambda (inclusive? i.start i.end v)
                                             (let ((i.global (fprev.global #t 0 end.global v)))
                                               (if (unsafe-fx< 0 i.global)
                                                 (fprev.local (or inclusive? (unsafe-fx< (ref.global i.global) v))
                                                              i.start i.end i.global)
                                                 i.start)))))))
                      (else   (ref->monovec ref)))))))
      (else    (error "column->monovec unimplemented for column class" desc))))

  (define (read-fx-column desc.col)
    (case (hash-ref desc.col 'class)
      ((line)  (let* ((count   (hash-ref desc.col 'count))
                      (step    (hash-ref desc.col 'step))
                      (vec.col (make-fxvector count)))
                 (let loop ((i 0) (value (hash-ref desc.col 'offset)))
                   (when (unsafe-fx< i count)
                     (unsafe-fxvector-set! vec.col i value)
                     (loop (unsafe-fx+ i 1) (unsafe-fx+ value step))))
                 vec.col))
      ((block) (let* ((count   (hash-ref desc.col 'count))
                      (width   (unsafe-fxrshift (hash-ref desc.col 'bit-width) 3))
                      (offset  (hash-ref desc.col 'offset))
                      (vec.col (make-fxvector count))
                      (bs      (file->bytes (block-desc->path desc.col))))
                 (let loop ((i 0) (j 0))
                   (when (< i count)
                     (unsafe-fxvector-set! vec.col i (+ (unsafe-bytes-nat-ref width bs j) offset))
                     (loop (+ i 1) (+ j width))))
                 vec.col))
      ((remap) (read-fx-column (hash-ref (stg-ref 'column-id=>column)
                                         (hash-ref desc.col 'local))))
      (else    (error "read-fx-column unimplemented for column class" desc.col))))

  (define (write-fx-column vec.col count)
    (define (write-line count offset step)
      (let ((id.col (fresh-column-id)))
        (add-columns! id.col (hash 'class  'line
                                   'count  count
                                   'offset offset
                                   'step   step))
        id.col))
    (define (write-block min.col max.col)
      (define-values (width.col offset.col)
        (let* ((diff.col  (- max.col min.col))
               (size.diff (nat-min-byte-width diff.col))
               (size.max  (nat-min-byte-width max.col)))
          (if (or (< min.col   0)
                  (< size.diff size.max))
            (values size.diff min.col)
            (values size.max  0))))
      (let* ((id.col     (fresh-column-id))
             (name.block (cons 'column id.col))
             (path.block (storage-block-new! stg name.block))
             (bs.col     (make-bytes (* count width.col))))
        (let loop ((i 0) (j 0))
          (when (unsafe-fx< i count)
            (unsafe-bytes-nat-set! width.col bs.col j (- (fxvector-ref vec.col i) offset.col))
            (loop (unsafe-fx+ i 1) (unsafe-fx+ j width.col))))
        (display-to-file bs.col path.block)
        (add-columns! id.col (hash 'class     'block
                                   'name      name.block
                                   'bit-width (* 8 width.col)
                                   'count     count
                                   'offset    offset.col
                                   'min       min.col
                                   'max       max.col))
        id.col))
    (define (write-remapped-block min.col max.col alphabet)
      (let* ((count.alphabet (set-count alphabet))
             (alphabet       (sort (set->list alphabet) unsafe-fx<))
             (vec.alphabet   (make-fxvector count.alphabet))
             (n=>n           (let loop ((i        0)
                                        (alphabet alphabet)
                                        (n=>n     (hash)))
                               (if (unsafe-fx< i count.alphabet)
                                 (let ((n.next (car alphabet)))
                                   (unsafe-fxvector-set! vec.alphabet i n.next)
                                   (loop (unsafe-fx+ i 1)
                                         (cdr alphabet)
                                         (hash-set n=>n n.next i)))
                                 n=>n)))
             ;; TODO: using full-blown write-fx-column here is a little wasteful
             (id.alphabet    (write-fx-column vec.alphabet count.alphabet))
             (id.remap       (fresh-column-id)))
        (let loop ((i count))
          (when (unsafe-fx<= 0 i)
            (unsafe-fxvector-set! vec.col i (hash-ref n=>n (unsafe-fxvector-ref vec.col i)))
            (loop (unsafe-fx- i 1))))
        (add-columns! id.remap (hash 'class  'remap
                                     'local  (write-block 0 (- (set-count alphabet) 1))
                                     'global id.alphabet))
        id.remap))
    (if (= count 1)
      (write-line 1 (unsafe-fxvector-ref vec.col 0) 0)
      (let* ((n.0                (unsafe-fxvector-ref vec.col 0))
             (n.1                (unsafe-fxvector-ref vec.col 1))
             (offset             n.0)
             (step               (- n.1 n.0)))
        (if (let loop ((i      2)
                       (n.prev n.1))
              (or (unsafe-fx= i count)
                  (let ((n.next (unsafe-fxvector-ref vec.col i)))
                    (and (unsafe-fx= (unsafe-fx- n.next n.prev) step)
                         (loop (unsafe-fx+ i 1) n.next)))))
          (write-line count offset step)
          (let loop ((i       (unsafe-fx- count 1))
                     (min.col (min n.0 n.1))
                     (max.col (max n.0 n.1)))
            (if (unsafe-fx= i 1)
              (let ((count.alphabet.max (max-remap-global-count (nat-min-byte-width (- max.col min.col)) count)))
                (let loop ((i        (unsafe-fx- count 1))
                           (alphabet (set n.0 n.1)))
                  (if (unsafe-fx= i 1)
                    (write-remapped-block min.col max.col alphabet)
                    (let ((alphabet.next (set-add alphabet (unsafe-fxvector-ref vec.col i))))
                      (if (unsafe-fx< count.alphabet.max (set-count alphabet.next))
                        (write-block min.col max.col)
                        (loop (unsafe-fx- i 1) alphabet.next))))))
              (let ((n.next (unsafe-fxvector-ref vec.col i)))
                (loop (unsafe-fx- i 1)
                      (min min.col n.next)
                      (max max.col n.next)))))))))

  (define (table-expr type rexpr)
    ;; TODO: simplify and normalize resulting table expression
    (let loop ((rexpr rexpr))
      (match rexpr
        ('()                     T.empty)
        (`(+ ,@rs)               (apply T+ (map loop rs)))
        (`(- ,r0 ,r1)            (T- (loop r0) (loop r1)))
        ((? wrapped-relation? R) (let* ((R    (wrapped-relation-controller R))
                                        (path (database-path (R 'database))))
                                   (unless (equal? path (storage-path stg))
                                     (error "cannot combine relations from different databases"
                                            path (storage-path stg)))
                                   (unless (equal? (R 'type) type)
                                     (error "type mismatch" (R 'type) type))
                                   (R 'table-expr))))))

  (define (table-expr->table-ids texpr)
    (let loop ((texpr texpr) (tids '()))
      (match texpr
        ('()          tids)
        (`(+ . ,ts)   (foldl loop tids ts))
        (`(- ,t0 ,t1) (loop t1 (loop t0 tids)))
        (table-id     (cons table-id tids)))))

  (define (table-expr-map tid->tid texpr)
    (let loop ((texpr texpr))
      (match texpr
        ('()          '())
        (`(+ . ,ts)   `(+ . ,(map loop ts)))
        (`(- ,t0 ,t1) `(- ,(loop t0) ,(loop t1)))
        (table-id     (tid->tid table-id)))))

  (define rids.new (mutable-set))
  (define id=>R    (make-weak-hash))
  (define (name->R name)
    (id->R (hash-ref (name=>relation-id) name
                     (lambda () (error "unknown relation" name (storage-path stg))))))
  (define (id->R id.R)
    (let* ((key.R (list id.R))
           (R     (hash-ref id=>R key.R #f)))
      (if R
        (wrapped-relation (hash-ref-key id=>R key.R) R)
        (wrapped-relation key.R                      (let ((R (make-relation id.R)))
                                                       (hash-set! id=>R key.R R)
                                                       R)))))
  (define (stg-ref      key)        (storage-description-ref     stg key))
  (define (stg-set!     key value)  (storage-description-set!    stg key value))
  (define (stg-update!  key update) (storage-description-update! stg key update))
  (define (R-anonymous)             (error "anonymous relation has no name"))
  (define (R-valid?     id)         (hash-has-key? (stg-ref 'relation-id=>type)  id))
  (define (R-has-name?  id)         (hash-has-key? (stg-ref 'relation-id=>name)  id))
  (define (R-name       id)         (hash-ref (stg-ref 'relation-id=>name)       id R-anonymous))
  (define (R-attrs      id)         (hash-ref (stg-ref 'relation-id=>attributes) id))
  (define (R-type       id)         (hash-ref (stg-ref 'relation-id=>type)       id))
  (define (R-texpr      id)         (hash-ref (stg-ref 'relation-id=>table-expr) id))
  (define (R-assign-t!  id texpr)   (stg-update! 'relation-id=>table-expr
                                                 (lambda (rid=>ts) (hash-set rid=>ts id texpr))))
  (define (R-assign-r!  id rexpr)   (R-assign-t! id (table-expr (R-type id) rexpr)))
  (define (name=>relation-id)       (stg-ref 'name=>relation-id))
  (define (relation-name? name)     (hash-has-key? (name=>relation-id) name))
  (define (new-relation?! name)     (when (relation-name? name)
                                      (error "relation already exists" name (storage-path stg))))
  (define (fresh-relation-id)       (fresh-uid))
  (define (fresh-table-id)          (fresh-uid))
  (define (fresh-column-id)         (fresh-uid))
  (define (fresh-uid)               (let ((uid (stg-ref 'next-uid)))
                                      (stg-set! 'next-uid (+ uid 1))
                                      uid))
  (define (add-columns! id desc . id&descs)
    (stg-update! 'column-id=>column
                 (lambda (cid=>desc) (apply hash-set* cid=>desc id desc id&descs))))
  (define (checkpoint!)
    (when (storage-checkpoint-pending? stg)
      (storage-checkpoint! stg))
    (when (and (auto-empty-trash?)
               (not (storage-trash-empty? stg)))
      (storage-trash-empty! stg)))
  (define (revert!)
    (for-each (lambda (rid) (let ((R (hash-ref id=>R (list rid) #f)))
                              (when R (R 'invalidate!))))
              (set->list rids.new))
    (set-clear! rids.new)
    (storage-revert! stg))
  (define (commit!)
    (for-each (lambda (rid)
                (let ((R (hash-ref id=>R (list rid) #f)))
                  (when (and R (not (R-has-name? rid)))
                    (R 'delete!))))  ; Delete anonymous relations before checkpoint
              (set->list rids.new))
    (checkpoint!)
    (set-clear! rids.new)
    (perform-pending-jobs!))
  (define (perform-pending-jobs!)
    (compact-relations! (filter R-valid? (hash-keys (stg-ref 'relations-to-fully-compact))))
    (stg-set! 'relations-to-fully-compact (hash))
    (checkpoint!)
    (for-each compact-relation-incrementally!
              (filter R-valid? (hash-keys (stg-ref 'relations-to-incrementally-compact))))
    (stg-set! 'relations-to-incrementally-compact (hash))
    (checkpoint!)
    (for-each
      (lambda (rid&texpr)
        (match-define (cons rid texpr) rid&texpr)
        (let ((tids (set->list (list->set (table-expr->table-ids texpr)))))
          (for-each
            (lambda (ordering) (build-table-indexes! ordering tids))
            ;; sorting by descending-length makes it easier to share common index building work
            (sort (hash-keys (hash-ref (stg-ref 'relation-id=>indexes) rid))
                  (lambda (o1 o2) (> (length o1) (length o2)))))))
      (hash->list (stg-ref 'relation-id=>table-expr)))
    (checkpoint!)
    (collect-garbage!))

  (define stg (storage:filesystem path.db))
  (let ((version (storage-description-ref stg 'database-format-version #f)))
    (unless (equal? version version.current)
      (when version (error "unknown version" version))
      (stg-set! 'database-format-version            version.current)
      (stg-set! 'name=>relation-id                  (hash))
      (stg-set! 'relation-id=>name                  (hash))
      (stg-set! 'relation-id=>attributes            (hash))
      (stg-set! 'relation-id=>type                  (hash))
      ;; relation-id => table-expr
      (stg-set! 'relation-id=>table-expr            (hash))
      ;; relation-id => (ordering => #t)
      (stg-set! 'relation-id=>indexes               (hash))
      ;; table-id => (list column-id ...)
      (stg-set! 'table-id=>column-ids               (hash))
      ;; (cons table-id ordering-prefix) => column-id
      (stg-set! 'index-prefix=>key-column-id        (hash))
      ;; (cons table-id ordering-prefix) => column-id  ; for forming intervals
      (stg-set! 'index-prefix=>position-column-id   (hash))
      ;; desc.column:
      ;;  (hash 'class  'line
      ;;        'count  nat
      ;;        'offset int
      ;;        'step   int)
      ;; OR
      ;;  (hash 'class     'block
      ;;        'name      block-name
      ;;        'bit-width nat
      ;;        'count     nat
      ;;        'offset    int
      ;;        'min       int
      ;;        'max       int)
      ;; OR
      ;;  (hash 'class  'remap  ; monotonic injection from local to global namespace, possibly for compression
      ;;        'local  column-id
      ;;        'global column-id)
      ;; OR
      ;;  (hash 'class    'text
      ;;        'position column-id
      ;;        'value    column-id)
      ;; column-id => desc.column
      (stg-set! 'column-id=>column                  (hash))
      ;; relation-id => #t
      (stg-set! 'relations-to-fully-compact         (hash))
      ;; relation-id => #t
      (stg-set! 'relations-to-incrementally-compact (hash))
      (stg-set! 'next-uid                           0)
      (checkpoint!))
    (perform-pending-jobs!))

  (define db
    (wrapped-database
      (method-lambda
        ((path)                (storage-path stg))
        ((relation-names)      (hash-keys (name=>relation-id)))
        ((relation-name? name) (relation-name? name))
        ((relation       name) (name->R name))
        ((relation-builder type batch-size)
         (valid-relation-type?! type)
         (define id.R (fresh-relation-id))
         (set-add! rids.new id.R)
         (stg-update! 'relation-id=>attributes (lambda (rid=>as) (hash-set rid=>as id.R (range (length type)))))
         (stg-update! 'relation-id=>type       (lambda (rid=>t)  (hash-set rid=>t  id.R type)))
         (stg-update! 'relation-id=>indexes    (lambda (rid=>is) (hash-set rid=>is id.R (hash))))
         (R-assign-t! id.R T.empty)
         (relation-builder id.R batch-size))
        ((commit!)      (commit!))
        ((revert!)      (revert!))
        ((trash-empty!) (storage-trash-empty! stg)))))
  db)

(define (valid-attributes?! attrs)
  (unless (list? attrs)
    (error "attributes must be a list" attrs))
  (unless (= (length attrs) (set-count (list->set attrs)))
    (error "attributes must be unique" attrs)))

(define (valid-relation-type?! type)
  (when (null? type) (error "relation must include at least one attribute type"))
  (for-each (lambda (t) (unless (member t '(int text))
                          (error "invalid attribute type" t 'in type)))
            type))

(define (ordering->prefixes ordering)
  (let ((rcols (reverse ordering))) ; prefixes ordered from longest to shortest
    (let loop ((c0 (car rcols)) (rcols (cdr rcols)))
      (if (null? rcols)
        (list (list c0))
        (cons (reverse (cons c0 rcols))
              (loop (car rcols) (cdr rcols)))))))

(define (int-tuple<? a b)
  (let loop ((a a) (b b))
    (and (not (null? a))
         (or (< (car a) (car b))
             (and (= (car a) (car b))
                  (loop (cdr a) (cdr b)))))))
(define (unsafe-int-tuple<? a b)
  (let loop ((a a) (b b))
    (and (not (null? a))
         (or (unsafe-fx< (car a) (car b))
             (and (unsafe-fx= (car a) (car b))
                  (loop (cdr a) (cdr b)))))))

(define (table-sort-and-dedup! count.tuples.initial vs.columns)
  (define table (make-vector count.tuples.initial))
  (let loop ((i (unsafe-fx- count.tuples.initial 1)))
    (when (<= 0 i)
      (vector-set! table i (map (lambda (vec.col) (unsafe-fxvector-ref vec.col i))
                                vs.columns))
      (loop (unsafe-fx- i 1))))
  (vector-sort! table unsafe-int-tuple<?)
  (define (columns-set! j tuple) (for-each (lambda (vec.col value.col)
                                             (unsafe-fxvector-set! vec.col j value.col))
                                           vs.columns tuple))
  (define count.tuples.unique
    (if (= 0 count.tuples.initial)
      0
      (let ((t0 (vector-ref table 0)))
        (columns-set! 0 t0)
        (let loop ((prev t0) (i 1) (j 1))
          (if (< i count.tuples.initial)
            (let ((next (vector-ref table i)))
              (cond ((equal? prev next) (loop prev (unsafe-fx+ i 1) j))
                    (else               (columns-set! j next)
                                        (loop next (unsafe-fx+ i 1) (unsafe-fx+ j 1)))))
            j)))))
  count.tuples.unique)

(define (nat-min-byte-width nat.max) (max (min-bytes nat.max) 1))

(define (max-remap-global-count width.local count.local)
  (define scale.max 2/3)
  (define size.min  (expt 2 20))
  (if (< (* count.local width.local) size.min)
    0
    (let loop ((width.global (- width.local 1)) (count.max 0))
      (if (= width.global 0)
        count.max
        (let ((count.candidate (min (expt 256 width.global)
                                    (floor (/ (* count.local (- (* scale.max width.local) width.global))
                                              width.local)))))
          (loop (- width.global 1) (max count.max count.candidate)))))))

(define (write-byte-width-nat width out n)
  (define bs (make-bytes width))
  (unsafe-bytes-nat-set! width bs 0 n)
  (write-bytes bs out))

(define (unsafe-bytes-nat-set! width bs offset n)
  (case width
    ((1)  (1-unrolled-unsafe-bytes-nat-set! bs offset n))
    ((2)  (2-unrolled-unsafe-bytes-nat-set! bs offset n))
    ((3)  (3-unrolled-unsafe-bytes-nat-set! bs offset n))
    ((4)  (4-unrolled-unsafe-bytes-nat-set! bs offset n))
    ((5)  (5-unrolled-unsafe-bytes-nat-set! bs offset n))
    ((6)  (6-unrolled-unsafe-bytes-nat-set! bs offset n))
    (else (rolled-unsafe-bytes-nat-set! width bs offset n))))

(define (rolled-unsafe-bytes-nat-set! bs offset width n)
  (let loop ((i     offset)
             (shift (unsafe-fxlshift (unsafe-fx- width 1) 3)))
    (when (unsafe-fx<= 0 shift)
      (unsafe-bytes-set! bs i (unsafe-fxand 255 (unsafe-fxrshift n shift)))
      (loop (unsafe-fx+ i     1)
            (unsafe-fx- shift 8)))))
(define (1-unrolled-unsafe-bytes-nat-set! bs i n) (unsafe-bytes-set! bs i n))
(define (2-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 n)))
(define (3-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 2) (unsafe-fxand 255 n)))
(define (4-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 24)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 2) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 3) (unsafe-fxand 255 n)))
(define (5-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 32)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 24)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 2) (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 3) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 4) (unsafe-fxand 255 n)))
(define (6-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 40)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 32)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 2) (unsafe-fxand 255 (unsafe-fxrshift n 24)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 3) (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 4) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 5) (unsafe-fxand 255 n)))

(define (unsafe-bytes-nat-ref width bs offset)
  (case width
    ((1)  (1-unrolled-unsafe-bytes-nat-ref bs offset))
    ((2)  (2-unrolled-unsafe-bytes-nat-ref bs offset))
    ((3)  (3-unrolled-unsafe-bytes-nat-ref bs offset))
    ((4)  (4-unrolled-unsafe-bytes-nat-ref bs offset))
    ((5)  (5-unrolled-unsafe-bytes-nat-ref bs offset))
    ((6)  (6-unrolled-unsafe-bytes-nat-ref bs offset))
    (else (rolled-unsafe-bytes-nat-ref width bs offset))))

(define (rolled-unsafe-bytes-nat-ref width bs offset)
  (let ((end (unsafe-fx+ offset width)))
    (let loop ((i offset) (n 0))
      (cond ((unsafe-fx< i end) (loop (unsafe-fx+ i 1)
                                      (unsafe-fx+ (unsafe-fxlshift n 8)
                                                  (unsafe-bytes-ref bs i))))
            (else               n)))))
(define (1-unrolled-unsafe-bytes-nat-ref bs i) (unsafe-bytes-ref bs i))
(define (2-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fx+ (unsafe-fxlshift (unsafe-bytes-ref bs             i)     8)
              (unsafe-bytes-ref                  bs (unsafe-fx+ i 1))))
(define (3-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fx+ (unsafe-fxlshift (unsafe-bytes-ref bs             i)    16)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 1))  8)
              (unsafe-bytes-ref                  bs (unsafe-fx+ i 2))))
(define (4-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fx+ (unsafe-fxlshift (unsafe-bytes-ref bs             i)    24)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 1)) 16)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 2))  8)
              (unsafe-bytes-ref                  bs (unsafe-fx+ i 3))))
(define (5-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fx+ (unsafe-fxlshift (unsafe-bytes-ref bs             i)    32)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 1)) 24)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 2)) 16)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 3))  8)
              (unsafe-bytes-ref                  bs (unsafe-fx+ i 4))))
(define (6-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fx+ (unsafe-fxlshift (unsafe-bytes-ref bs             i)    40)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 1)) 32)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 2)) 24)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 3)) 16)
              (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 4))  8)
              (unsafe-bytes-ref                  bs (unsafe-fx+ i 5))))

(define (unsafe-bisect-next start end i<)
  (define i (unsafe-fx- start 1))
  (let loop ((offset 1))
    (define next (unsafe-fx+ i offset))
    (cond ((and (unsafe-fx< next end) (i< next)) (loop (arithmetic-shift offset 1)))
          (else (let loop ((i i) (o offset))
                  (let* ((o (arithmetic-shift o -1)) (next (unsafe-fx+ i o)))
                    (cond ((unsafe-fx= o 0)                      (unsafe-fx+ i 1))
                          ((and (unsafe-fx< next end) (i< next)) (loop next o))
                          (else                                  (loop i    o)))))))))

(define (bisect-next start end i<)
  (define i (- start 1))
  (let loop ((offset 1))
    (define next (+ i offset))
    (cond ((and (< next end) (i< next)) (loop (arithmetic-shift offset 1)))
          (else (let loop ((i i) (o offset))
                  (let* ((o (arithmetic-shift o -1)) (next (+ i o)))
                    (cond ((= o 0)                      (+ i 1))
                          ((and (< next end) (i< next)) (loop next o))
                          (else                         (loop i    o)))))))))
(define (bisect-prev start end i>)
  (define i end)
  (let loop ((offset 1))
    (define next (- i offset))
    (cond ((and (>= next start) (i> next)) (loop (arithmetic-shift offset 1)))
          (else (let loop ((i i) (o offset))
                  (let* ((o (arithmetic-shift o -1)) (n (- i o)))
                    (cond ((= o 0)                   i)
                          ((and (>= n start) (i> n)) (loop n o))
                          (else                      (loop i o)))))))))

(define ((unsafe-multi-merge <? gens gen-empty? gen-first gen-rest) yield)
  (define h   (list->vector gens))
  (define end (vector-length h))
  (heap! <? h end)
  (define (re-insert end gen)
    (cond ((gen-empty? gen) (heap-remove!  <? h end)
                            (unsafe-fx- end 1))
          (else             (heap-replace! <? h end gen)
                            end)))
  (if (unsafe-fx< 0 end)
    (let ((g.top (heap-top h)))
      (let loop.new ((g.top g.top)
                     (x     (gen-first g.top))
                     (i     0)
                     (end   end))
        (yield x)
        (let loop.duplicate ((end (re-insert end (gen-rest g.top i))))
          (if (unsafe-fx< 0 end)
            (let* ((g.top (heap-top h))
                   (y     (gen-first g.top)))
              (if (equal? x y)
                (loop.duplicate (re-insert end (gen-rest g.top i)))
                (loop.new       g.top y (unsafe-fx+ i 1) end)))
            (unsafe-fx+ i 1)))))
    0))

(define ((multi-merge <? gens gen-empty? gen-first gen-rest) yield)
  (define h   (list->vector gens))
  (define end (vector-length h))
  (heap! <? h end)
  (define (re-insert end gen)
    (cond ((gen-empty? gen) (heap-remove!  <? h end)
                            (- end 1))
          (else             (heap-replace! <? h end gen)
                            end)))
  (if (< 0 end)
    (let ((g.top (heap-top h)))
      (let loop.new ((g.top g.top)
                     (x     (gen-first g.top))
                     (i     0)
                     (end   end))
        (yield x)
        (let loop.duplicate ((end (re-insert end (gen-rest g.top i))))
          (if (< 0 end)
            (let* ((g.top (heap-top h))
                   (y     (gen-first g.top)))
              (if (equal? x y)
                (loop.duplicate (re-insert end (gen-rest g.top i)))
                (loop.new       g.top y (+ i 1) end)))
            (+ i 1)))))
    0))

(struct monovec (ref unref-next unref-prev) #:prefab)

(define ((ref-map   ref f)      i) (f                          (ref i)))
(define ((ref-remap ref id=>id) i) (unsafe-fxvector-ref id=>id (ref i)))

(define ((find-next:ref ref <? <=?) inclusive? i.start i.end v)
  (let ((<? (if inclusive? <? <=?)))
    (bisect-next i.start i.end (lambda (i) (<? (ref i) v)))))
(define ((find-prev:ref ref <? <=?) inclusive? i.start i.end v)
  (let ((<? (if inclusive? <? <=?)))
    (bisect-prev i.start i.end (lambda (i) (<? v (ref i))))))

(define ((find-next:line offset step) inclusive? i.start i.end v)
  (let* ((i.0 (/ (- v offset) step))
         (i.1 (ceiling i.0))
         (i   (if (and (not inclusive?) (eqv? i.0 i.1))
                (+ i.1 1)
                i.1)))
    (max i.start (min i.end i))))
(define ((find-prev:line offset step) inclusive? i.start i.end v)
  (let* ((i.0 (/ (- v offset) step))
         (i.1 (floor   i.0))
         (i   (if (and (not inclusive?) (eqv? i.0 i.1))
                (- i.1 1)
                i.1)))
    (max i.start (min i.end i))))

(define (dict-count     d)                 (d 'count))
(define (dict-min       d)                 (d 'min))
(define (dict-min-value d)                 (d 'min-value))
(define (dict-min-pop   d)                 (d 'min-pop))
(define (dict-min-find  d inclusive? key)  (d 'min-find inclusive? key))
(define (dict-max       d)                 (d 'max))
(define (dict-max-find  d inclusive? key)  (d 'max-find inclusive? key))
(define (dict-empty?    d)                 (eqv? 0 (dict-count d)))
(define (dict->=        d key)             (dict-min-find d #t key))
(define (dict->         d key)             (dict-min-find d #f key))
(define (dict-<=        d key)             (dict-max-find d #t key))
(define (dict-<         d key)             (dict-max-find d #f key))
(define (dict-ref d key k.found k.missing) (let ((d (dict->= d key)))
                                             (if (or (dict-empty? d)
                                                     (not (equal? (dict-min d) key)))
                                               (k.missing)
                                               (k.found (dict-min-value d)))))
(define ((dict-enumerator     d) yield)    (let loop ((d d))
                                             (unless (dict-empty? d)
                                               (yield (dict-min d) (dict-min-value d))
                                               (loop (dict-min-pop d)))))
(define ((dict-key-enumerator d) yield)    (let loop ((d d))
                                             (unless (dict-empty? d)
                                               (yield (dict-min d))
                                               (loop (dict-min-pop d)))))

(define dict.empty
  (method-lambda
    ((count)                   0)
    ((min-find inclusive? key) dict.empty)
    ((max-find inclusive? key) dict.empty)))

(define (dict:monovec monovec.key ref.value start end)
  (match-define (monovec ref.key find-next find-prev) monovec.key)
  (dict:basic ref.key find-next find-prev ref.value start end))

(define (dict:ref ref.key <?.key <=?.key ref.value start end)
  (dict:basic ref.key
              (find-next:ref ref.key <?.key <=?.key)
              (find-prev:ref ref.key <?.key <=?.key)
              ref.value
              start end))

(define (dict:basic ref.key find-next find-prev ref.value start end)
  (let loop ((start start) (end end))
    (if (unsafe-fx<= end start)
      dict.empty
      (method-lambda
        ((count)                   (unsafe-fx- end start))
        ((min)                     (ref.key   start))
        ((min-value)               (ref.value start))
        ((min-pop)                 (loop (unsafe-fx+ start 1) end))
        ((max)                     (ref.key   (unsafe-fx- end 1)))
        ((min-find inclusive? key) (loop (find-next inclusive? start end key) end))
        ((max-find inclusive? key) (loop start (find-prev inclusive? start end key)))))))

(define (dict:union <? combine-values . ds)
  (define (<=? a b) (not (<? b a)))
  (define (d<? d0 d1) (<? (dict-min d0) (dict-min d1)))
  (define h (list->vector ds))
  (heap! d<? h (vector-length h))
  (let loop.dict ((hcount (vector-length h)))
    (define (dict:h hcount key.min value.min)
      (define self
        (method-lambda
          ((count)     (let loop ((i (unsafe-fx- hcount 1)) (count 1))
                         (if (unsafe-fx<= 0 i)
                           (loop (unsafe-fx- i 1)
                                 (unsafe-fx+ (dict-count (vector-ref h i)) count))
                           count)))
          ((min)       key.min)
          ((min-value) value.min)
          ((min-pop)   (loop.dict hcount))
          ((max)       (let loop ((i (unsafe-fx- hcount 1)) (key.max key.min))
                         (if (unsafe-fx<= 0 i)
                           (loop (unsafe-fx- i 1)
                                 (max (dict-max (vector-ref h i)) key.max))
                           key.max)))
          ((min-find inclusive? key)
           (let ((<? (if inclusive? <=? <?)))
             (if (<? key key.min)
               self
               (let loop ((hcount hcount))
                 (if (unsafe-fx= hcount 0)
                   dict.empty
                   (let ((d.top (heap-top h)))
                     (if (<? key (dict-min d.top))
                       (loop.dict hcount)
                       (let ((d.top (dict-min-find d.top)))
                         (cond ((dict-empty? d.top) (heap-remove!  d<? h hcount)
                                                    (loop (unsafe-fx- hcount 1)))
                               (else                (heap-replace! d<? h hcount d.top)
                                                    (loop hcount)))))))))))
          ((max-find inclusive? key)
           (if ((if inclusive? <? <=?) key key.min)
             dict.empty
             (let* ((ds     (list->vector
                              (filter-not dict-empty?
                                          (map (lambda (d) (dict-max-find d inclusive? key))
                                               (vector->list (vector-copy h 0 hcount))))))
                    (hcount (vector-length ds)))
               (vector-fill! h #f)
               (vector-copy! h 0 ds)
               (heap! d<? h hcount)
               (dict:h hcount key.min value.min))))))
      self)
    (case hcount
      ((0)  dict.empty)
      ((1)  (vector-ref h 0))
      (else (let* ((d.top   (heap-top h))
                   (key.min (dict-min d.top)))
              (let loop ((hcount hcount) (vs.min '()))
                (define (finish) (dict:h hcount key.min (apply combine-values vs.min)))
                (if (unsafe-fx= hcount 0)
                  (finish)
                  (let ((d.top (heap-top h)))
                    (if (<? key.min (dict-min d.top))
                      (finish)
                      (let ((vs.min (cons (dict-min-value d.top) vs.min))
                            (d.top  (dict-min-pop d.top)))
                        (cond ((dict-empty? d.top) (heap-remove!  d<? h hcount)
                                                   (loop (unsafe-fx- hcount 1) vs.min))
                              (else                (heap-replace! d<? h hcount d.top)
                                                   (loop hcount                vs.min)))))))))))))

(define (dict:diff <? count.keys d.positive d.negative)
  (let loop/count.keys ((count.keys count.keys) (d.pos d.positive) (d.neg d.negative))
    (let loop ((d.pos d.pos) (d.neg d.neg))
      (define (shared d.pos d.neg)
        (method-lambda
          ((count)                   (dict-count d.pos))
          ((min)                     (dict-min   d.pos))
          ((max)                     (dict-max   d.pos))
          ((min-find inclusive? key) (loop (dict-min-find d.pos inclusive? key)
                                           (dict-min-find d.neg inclusive? key)))
          ((max-find inclusive? key) (loop (dict-max-find d.pos inclusive? key)
                                           (dict-max-find d.neg inclusive? key)))))
      (define (less)
        (let ((super (shared d.pos d.neg)))
          (method-lambda
            ((min-value) (dict-min-value d.pos))
            ((min-pop)   (loop (dict-min-pop d.pos d.neg)))
            (else        super))))
      (define (same)
        (if (= count.keys 1)
          (loop (dict-min-pop d.pos) (dict-min-pop d.neg))
          (let ((d.pos.top (loop/count.keys (unsafe-fx- count.keys 1)
                                            (dict-min-value d.pos)
                                            (dict-min-value d.neg))))
            (if (dict-empty? d.pos.top)
              (loop (dict-min-pop d.pos) (dict-min-pop d.neg))
              (let ((super (shared d.pos d.neg)))
                (method-lambda
                  ((min-value) d.pos.top)
                  ((min-pop)   (loop (dict-min-pop d.pos) (dict-min-pop d.neg)))
                  (else        super)))))))
      (cond ((dict-empty? d.pos) dict.empty)
            ((dict-empty? d.neg) d.pos)
            (else (let ((min.pos (dict-min d.pos)) (min.neg (dict-min d.neg)))
                    (cond ((<? min.pos min.neg) (less))
                          ((<? min.neg min.pos) (loop d.pos (dict-min-find d.neg #t min.pos)))
                          (else                 (same)))))))))
