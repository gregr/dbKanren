#lang racket/base
(provide
  ;; TODO: move these
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
        ((table-expr)             (hash-ref (stg-ref 'relation-id=>table-expr) id.self))
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
         (set! size.text 0)
         (set! table-id  (fresh-table-id)))
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
         (define id.primary-key      (fresh-column-id))
         (add-columns! id.primary-key (hash 'class  'line
                                            'count  count.tuples.unique
                                            'offset 0
                                            'step   1))
         (define column-ids.attrs
           (map (lambda (type.col v.col)
                  (let ((id.col (performance-log `(writing column: ,count.tuples.unique values)
                                                 (write-fx-column v.col count.tuples.unique))))
                    (cond ((eqv? type.col 'text)
                           (let ((id.remap (fresh-column-id)))
                             (add-columns! id.remap (hash 'class  'remap
                                                          'local  id.col
                                                          'global column-id.text))
                             id.remap))
                          (else id.col))))
                column-types vs.col))
         (define column-ids (cons id.primary-key column-ids.attrs))
         (stg-update! 'table-id=>column-ids (lambda (tid=>cids) (hash-set tid=>cids table-id column-ids)))
         (pretty-log `(inserted batch of ,count.tuples.unique unique tuples)
                     `(,size.text bytes for ,(hash-count text=>id) unique text values))
         (set! tables (cons table-id tables)))
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
       (define table-id     #f)
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

  (define (build-table-indexes! ordering tids)
    (let ((prefixes (let ((rcols (reverse ordering))) ; prefixes ordered from longest to shortest
                      (let loop ((c0 (car rcols)) (rcols (cdr rcols)))
                        (if (null? rcols)
                          (list (list c0))
                          (cons (reverse (cons c0 rcols))
                                (loop (car rcols) (cdr rcols))))))))
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

  (define (merge-text-columns descs.text)
    (define custodian.gs (make-custodian))
    (define *g&count&id=>id
      (parameterize ((current-custodian custodian.gs))
        (map (lambda (desc.text)
               (define count  (column-count desc.text))
               (define s      ((column->start->s desc.text) 0))
               (define id=>id (make-fxvector count))
               (list (and (< 0 count)
                          (let loop ((id 0) (s s))
                            (match (s) ; assume uniform stream
                              ((cons value s) (cons value (lambda (i)
                                                            (unsafe-fxvector-set! id=>id id i)
                                                            (loop (unsafe-fx+ id 1) s))))
                              (_              #f))))
                     count
                     id=>id))
             descs.text)))
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
                                      (lambda (value)
                                        (write-bytes value out)
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
    (hash 'text       cid.text
          'remappings id=>ids))

  (define (column-count desc.col)
    (case (hash-ref desc.col 'class)
      ((line block) (hash-ref desc.col 'count))
      ((text)       (column-count (hash-ref (stg-ref 'column-id=>column) (hash-ref desc.col 'position))))
      ((remap)      (column-count (hash-ref (stg-ref 'column-id=>column) (hash-ref desc.col 'local))))
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
                      (in      (open-input-file (storage-block-path stg (hash-ref desc.col 'name)))))
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
                      (in     (open-input-file (storage-block-path stg (hash-ref cid=>c (hash-ref desc.col 'value))))))
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

  (define (column->ref desc.col)
    (case (hash-ref desc.col 'class)
      ((line)  (let ((step   (hash-ref desc.col 'step))
                     (offset (hash-ref desc.col 'offset)))
                 (lambda (i) (unsafe-fx+ (unsafe-fx* step i) offset))))
      ((block) (let ((width  (unsafe-fxrshift (hash-ref desc.col 'bit-width) 3))
                     (offset (hash-ref desc.col 'offset))
                     (in     (open-input-file (storage-block-path stg (hash-ref desc.col 'name)))))
                 (lambda (i)
                   (file-position in (* width i))
                   (unsafe-bytes-nat-ref width (read-bytes width in) 0))))
      ((text)  (column-count (hash-ref (stg-ref 'column-id=>column) (hash-ref desc.col 'position))))
      ((remap) (column-count (hash-ref (stg-ref 'column-id=>column) (hash-ref desc.col 'local))))
      (else    (error "column-count unimplemented for column class" desc.col))))

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
                      (bs      (time (file->bytes (storage-block-path stg (hash-ref desc.col 'name))))))
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
      (let* ((count.alphabet.max (expt 2 (- (nat-min-byte-width count) 1)))
             (n.0                (unsafe-fxvector-ref vec.col 0))
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
          (let loop ((i        (unsafe-fx- count 1))
                     (min.col  (min n.0 n.1))
                     (max.col  (max n.0 n.1))
                     (alphabet (set n.0 n.1)))
            (cond ((unsafe-fx< 1 i)
                   (let ((n.next (unsafe-fxvector-ref vec.col i)))
                     (loop (unsafe-fx- i 1)
                           (min min.col n.next)
                           (max max.col n.next)
                           (and alphabet
                                (let ((alphabet.next (set-add alphabet n.next)))
                                  (and (unsafe-fx<= (set-count alphabet.next) count.alphabet.max)
                                       alphabet.next))))))
                  ((and alphabet (< (nat-min-byte-width (- (set-count alphabet) 1))
                                    (nat-min-byte-width (- max.col min.col))))
                   (write-remapped-block min.col max.col alphabet))
                  (else (write-block min.col max.col))))))))

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

  (define rids.new           (mutable-set))
  (define id=>R              (make-weak-hash))
  (define (name->R name)
    (id->R (hash-ref (name=>relation-id) name
                     (lambda () (error "unknown relation" name (storage-path stg))))))
  (define (id->R id.R)
    (let* ((key.R (list id.R)))
      (wrapped-relation
        key.R
        (or (hash-ref id=>R key.R #f)
            (let ((R (make-relation id.R)))
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
    ;; TODO:
    ;; - collect table garbage
    (define (rids-remove-invalid rids)
      (foldl (lambda (rid rids)
               (if (R-valid? rid)
                 rids
                 (hash-remove rids rid)))
             rids
             (hash-keys rids)))
    (stg-update! 'relations-to-fully-compact
                 ;; TODO: perform full compaction with text gc
                 ;; For now, just remove deleted relations
                 rids-remove-invalid)
    (stg-update! 'relations-to-incrementally-compact
                 ;; TODO: perform incremental compaction
                 ;; For now, just remove deleted relations
                 rids-remove-invalid)
    ;; - collect index garbage
    ;; - collect text garbage
    (checkpoint!)
    (for-each
      (lambda (rid&texpr)
        (match-define (cons rid texpr) rid&texpr)
        (let ((tids (table-expr->table-ids texpr)))
          (for-each
            (lambda (ordering) (build-table-indexes! ordering tids))
            ;; sorting by descending-length makes it easier to share common index building work
            (sort (hash-keys (hash-ref (stg-ref 'relation-id=>indexes) rid))
                  (lambda (o1 o2) (> (length o1) (length o2)))))))
      (hash->list (stg-ref 'relation-id=>table-expr)))
    (checkpoint!))

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
      ;;  (hash 'class 'fxvector
      ;;        'value #fx(int ...)
      ;;        'min   int
      ;;        'max   int)
      ;; OR
      ;;  (hash 'class 'bigint-vector
      ;;        'value #(int ...)
      ;;        'min   int
      ;;        'max   int)
      ;; OR
      ;;  (hash 'class     'block
      ;;        'name      block-name
      ;;        'bit-width nat
      ;;        'count     nat
      ;;        'offset    int
      ;;        'min       int
      ;;        'max       int)
      ;; OR
      ;;  (hash 'class  'remap      ; monotonic injection from local to global namespace, for compression
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

(define (int-tuple<? a b)
  (let loop ((a a) (b b))
    (and (not (null? a))
         (or (< (car a) (car b))
             (and (= (car a) (car b))
                  (loop (cdr a) (cdr b)))))))

(define (table-sort-and-dedup! count.tuples.initial vs.columns)
  (define table (make-vector count.tuples.initial))
  (let loop ((i (unsafe-fx- count.tuples.initial 1)))
    (when (<= 0 i)
      (vector-set! table i (map (lambda (vec.col) (unsafe-fxvector-ref vec.col i))
                                vs.columns))
      (loop (unsafe-fx- i 1))))
  (vector-sort! table int-tuple<?)
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
