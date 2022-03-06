#lang racket/base
(provide
  begin-update
  database
  database-path
  database-relation-names
  database-relation-name?
  database-relation
  database-relation-builder
  database-relation-new
  database-relation-add!
  database-trash-empty!
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
  relation-compact!
  R.empty R+ R-
  auto-empty-trash?
  current-batch-size)
(require "logging.rkt" "misc.rkt" "storage.rkt"
         racket/fixnum racket/hash racket/list racket/match racket/set
         racket/struct racket/unsafe/ops racket/vector)

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
;; permutation is the order in which attributes will be constrained.  This order of constraining
;; attributes is efficient due to the representation of the index, which is the result of sorting
;; the relation's tuples lexicographically according to the attribute permutation.

;; Databases and relations can be modified:
;; - New relations can be added to, and existing relations can be removed from, a database.
;; - Relations and their attributes can be renamed.
;; - Indexes can be added to, or removed from, a relation.
;; - Tuples can be inserted into, or deleted from, a relation.
;; Database and relation modifications always occur in the context of an atomic database update, and
;; multiple such modifications can be performed as part of the same update.  This means that
;; multiple relations can be modified simultaneously, each being modified in one or more ways,
;; during such update.  Updates are atomic in the sense that they never partially succeed: either
;; all specified modifications are performed, or none are performed.

(define version.current '2022-2-22)

(define auto-empty-trash?  (make-parameter #f))
(define current-batch-size (make-parameter (expt 2 26)))

(define (database-path             db)                      ((wrapped-database-controller db) 'path))
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
(define (relation-compact!        r)       ((wrapped-relation-controller r) 'compact!))

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
      (let ((attrs (R-attrs id.self)))
        (map (lambda (attr) (let ((i (index-of attrs attr)))
                              (if i i (error "invalid index attribute" attr ix attrs))))
             ix)))
    (define (update-indexes! update)
      (claim-update!)
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
         (claim-update!)
         (unless (and (R-has-name? id.self) (equal? (R-name id.self) name))
           (new-relation?! name)
           (remove-name!)
           (stg-update! 'name=>relation-id (lambda (n=>rid) (hash-set n=>rid name id.self)))
           (stg-update! 'relation-id=>name (lambda (rid=>n) (hash-set rid=>n id.self name)))))
        ((attributes-set! attrs)
         (claim-update!)
         (valid-attributes?! attrs)
         (let ((type (R-type id.self)))
           (unless (= (length attrs) (length type))
             (error "number of attributes must match the relation type arity"
                    attrs type)))
         (stg-update! 'relation-id=>attributes (lambda (rid=>as) (hash-set rid=>as id.self attrs))))
        ((assign! expr)
         (claim-update!)
         (R-assign-r! id.self expr))
        ((index-add!    ixs) (update-indexes! (lambda (os)
                                                (foldl (lambda (ordering os)
                                                         (hash-set os ordering #t))
                                                       os
                                                       (map index-signature->ordering ixs)))))
        ((index-remove! ixs) (update-indexes! (lambda (os)
                                                (foldl (lambda (ordering os)
                                                         (hash-remove os ordering))
                                                       os
                                                       (map index-signature->ordering ixs)))))
        ((compact!)
         (claim-update!)
         (stg-update! 'relations-to-compact (lambda (rids) (hash-set rids id.self #t))))
        ((delete!)
         (claim-update!)
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

  (define (relation-builder id.R batch-size)
    (define R (id->R id.R))
    (define checkpoint.current (storage-checkpoint-count stg))
    (define (valid?)
      (and ((wrapped-relation-controller R) 'valid?)
           (current-update-details)
           (equal? (storage-checkpoint-count stg) checkpoint.current)))
    (define (invalidate!) (set! checkpoint.current #f))
    (cond
      ((= 0 batch-size)
       (values #f (lambda ()
                    (unless (valid?) (error "cannot use a stale relation builder"))
                    (invalidate!)
                    R)))
      ((< 0 batch-size)
       (define (start-batch!)
         (hash-clear! text=>id)
         (set! i.tuple   0)
         (set! size.text 0)
         (set! table-id  (fresh-uid)))
       (define (insert! tuple)
         (for-each (lambda (field proj v.col)
                     (fxvector-set! v.col i.tuple (proj field)))
                   tuple projections vs.col)
         (set! i.tuple (unsafe-fx+ i.tuple 1))
         (when (unsafe-fx= i.tuple column-size)
           (finish-batch!)
           (start-batch!)))
       (define (finish!)
         (finish-batch!)
         (R-assign-t! id.R (apply T+ (reverse tables)))
         (invalidate!)
         R)
       (define (finish-batch!)
         (unless (valid?) (error "cannot use a stale relation builder"))
         (define width.pos (nat-min-byte-width size.text))
         (define count.ids (hash-count text=>id))
         (define id=>id    (make-fxvector count.ids))
         (let ((text&id*.sorted (sort (hash->list text=>id)
                                      (lambda (a b) (bytes<? (car a) (car b))))))
           ;; TODO: check whether we have text before creating these columns
           (let* ((id.text.value  (fresh-uid))
                  (id.text.pos    (fresh-uid))
                  (out.text.value (storage-block-new! stg (cons 'column id.text.value)))
                  (out.text.pos   (storage-block-new! stg (cons 'column id.text.pos))))
             (define (write-pos)
               (write-byte-width-nat out.text.pos (file-position out.text.value) width.pos))
             (write-pos)
             (let loop ((i 0) (t&id* text&id*.sorted))
               (unless (null? t&id*)
                 (let* ((t&id (car t&id*))
                        (text (car t&id))
                        (id   (cdr t&id)))
                   (write-bytes text out.text.value)
                   (write-pos)
                   (unsafe-fxvector-set! id=>id id i)
                   (loop (+ i 1) (cdr t&id*)))))
             (close-output-port out.text.value)
             (close-output-port out.text.pos)
             ;; TODO: use size.text width.pos count.ids
             (define desc.text.value 'TODO)
             (define desc.text.pos   'TODO)
             (stg-update! 'column-id=>column
                          (lambda (cid=>desc)
                            (hash-set* cid=>desc
                                       id.text.value desc.text.value
                                       id.text.pos   desc.text.pos)))))
         (for-each (lambda (type v.col)
                     (when (eqv? type 'text)
                       (let loop ((i (unsafe-fx- i.tuple 1)))
                         (when (<= 0 i)
                           (unsafe-fxvector-set! v.col i
                                                 (unsafe-fxvector-ref
                                                   id=>id
                                                   (unsafe-fxvector-ref v.col i)))
                           (loop (unsafe-fx- i 1))))))
                   column-types vs.col)
         (define count.tuples.unique (table-sort-and-dedup! i.tuple vs.col))
         ;; TODO: column-ids are just raw columns; need to account for possible remaps before describing table
         (define column-ids (map (lambda (v.col) (write-fx-column v.col count.tuples.unique)) vs.col))
         (stg-update! 'table-id=>column-ids (lambda (tid=>cids) (hash-set tid=>cids table-id column-ids)))
         (set! tables (cons table-id tables)))
       (define (text->id bs)
         (or (hash-ref text=>id bs #f)
             (let ((id (hash-count text=>id)))
               (hash-set! text=>id bs id)
               (set! size.text (+ size.text (bytes-length bs)))
               id)))
       (define (identity x) x)
       (define column-types (relation-type R))
       (define column-size  (max (quotient batch-size (length column-types)) 2))
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
      (else (error "invalid batch size" batch-size))))

  (define (write-fx-column vec.col count)
    ;; TODO: check for a line before committing to writing a block
    ;; TODO: calculate: offset.col width.col min.col max.col
    (define offset.col 0)
    (define width.col  4)
    (let* ((id.col  (fresh-uid))
           (out.col (storage-block-new! stg (cons 'column id.col))))
      (let loop ((i 0))
        (when (unsafe-fx< i count)
          (write-byte-width-nat out.col (- (fxvector-ref vec.col i) offset.col) width.col)
          (loop (unsafe-fx+ i 1))))
      (close-output-port out.col)
      (define desc.col 'TODO:desc.col)
      (stg-update! 'column-id=>column (lambda (cid=>desc) (hash-set cid=>desc id.col desc.col)))
      id.col))

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
  (define (fresh-uid)               (let ((uid (stg-ref 'next-uid)))
                                      (stg-set! 'next-uid (+ uid 1))
                                      uid))
  (define (claim-update!)
    (let* ((details  (or (current-update-details)
                         (error "cannot update database outside of a begin-update context"
                                (storage-path stg))))
           (commit!? (hash-ref details 'commit!)))
      (if (not commit!?)
        (current-update-details (hash-set* details
                                           'commit! commit!
                                           'revert! revert!))
        (unless (equal? commit!? commit!)
          (error "cannot update multiple databases simultaneously" (storage-path stg))))))
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
    (stg-update! 'relations-to-compact
                 ;; For now, just remove deleted relations
                 (lambda (rids)
                   (foldl
                     (lambda (rid rids)
                       (if (R-valid? rid)
                         rids
                         (hash-remove rids rid)))
                     rids
                     (hash-keys rids))
                   ;; TODO: perform compaction and intra-text gc
                   ))
    ;; - collect index garbage
    ;; - collect text garbage
    ;; - checkpoint
    ;; - build new indexes
    (checkpoint!))

  (define stg (storage:filesystem path.db))
  (let ((version (storage-description-ref stg 'database-format-version #f)))
    (unless (equal? version version.current)
      (when version (error "unknown version" version))
      (stg-set! 'database-format-version       version.current)
      (stg-set! 'name=>relation-id             (hash))
      (stg-set! 'relation-id=>name             (hash))
      (stg-set! 'relation-id=>attributes       (hash))
      (stg-set! 'relation-id=>type             (hash))
      ;; relation-id => table-expr
      (stg-set! 'relation-id=>table-expr       (hash))
      ;; relation-id => (ordering => #t)
      (stg-set! 'relation-id=>indexes          (hash))
      ;; table-id => (list column-id ...)
      (stg-set! 'table-id=>column-ids             (hash))
      ;; (cons table-id ordering-prefix) => column-id
      (stg-set! 'index-prefix=>key-column-id      (hash))
      ;; (cons table-id ordering-prefix) => column-id  ; for forming intervals
      (stg-set! 'index-prefix=>position-column-id (hash))
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
      (stg-set! 'column-id=>column             (hash))
      ;; relation-id => #t
      (stg-set! 'relations-to-compact          (hash))
      (stg-set! 'next-uid                      0)
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
         (claim-update!)
         (valid-relation-type?! type)
         (define id.R (fresh-uid))
         (set-add! rids.new id.R)
         (stg-update! 'relation-id=>attributes (lambda (rid=>as) (hash-set rid=>as id.R (range (length type)))))
         (stg-update! 'relation-id=>type       (lambda (rid=>t)  (hash-set rid=>t  id.R type)))
         (stg-update! 'relation-id=>indexes    (lambda (rid=>is) (hash-set rid=>is id.R (hash))))
         (R-assign-t! id.R T.empty)
         (relation-builder id.R batch-size))
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

(define current-update-details (make-parameter #f))

(define-syntax-rule (begin-update body ...)
  (let ((k (lambda () body ...)))
    (if (current-update-details)
      (k)
      (parameterize ((current-update-details (hash 'commit! #f
                                                   'revert! #f)))
        (with-handlers (((lambda (_) #t)
                         (lambda (e)
                           (let ((revert! (hash-ref (current-update-details) 'revert!)))
                             (when revert! (revert!)))
                           (raise e))))
          (k)
          (let ((commit! (hash-ref (current-update-details) 'commit!)))
            (when commit! (commit!))))))))

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

(define (write-byte-width-nat out n width)
  (define bs (make-bytes width))
  (unsafe-bytes-byte-width-nat-set! bs n width 0)
  (write-bytes bs out))

(define (unsafe-bytes-byte-width-nat-set! bs n width offset)
  (case width
    ((1)  (1-unrolled-unsafe-bytes-nat-set! n width bs offset))
    ((2)  (2-unrolled-unsafe-bytes-nat-set! n width bs offset))
    ((3)  (3-unrolled-unsafe-bytes-nat-set! n width bs offset))
    ((4)  (4-unrolled-unsafe-bytes-nat-set! n width bs offset))
    ((5)  (5-unrolled-unsafe-bytes-nat-set! n width bs offset))
    ((6)  (6-unrolled-unsafe-bytes-nat-set! n width bs offset))
    (else (let loop ((i     offset)
                     (shift (unsafe-fx* 8 (unsafe-fx- width 1))))
            (when (unsafe-fx<= 0 shift)
              (bytes-set! bs i (unsafe-fxand 255 (unsafe-fxrshift n shift)))
              (loop (unsafe-fx+ i     1)
                    (unsafe-fx- shift 8)))))))

(define (1-unrolled-unsafe-bytes-nat-set! n width bs i)
  (bytes-set! bs i n))

(define (2-unrolled-unsafe-bytes-nat-set! n width bs i)
  (bytes-set! bs i       (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (bytes-set! bs (+ i 1) (unsafe-fxand 255 n)))

(define (3-unrolled-unsafe-bytes-nat-set! n width bs i)
  (bytes-set! bs i       (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (bytes-set! bs (+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (bytes-set! bs (+ i 2) (unsafe-fxand 255 n)))

(define (4-unrolled-unsafe-bytes-nat-set! n width bs i)
  (bytes-set! bs i       (unsafe-fxand 255 (unsafe-fxrshift n 24)))
  (bytes-set! bs (+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (bytes-set! bs (+ i 2) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (bytes-set! bs (+ i 3) (unsafe-fxand 255 n)))

(define (5-unrolled-unsafe-bytes-nat-set! n width bs i)
  (bytes-set! bs i       (unsafe-fxand 255 (unsafe-fxrshift n 32)))
  (bytes-set! bs (+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 24)))
  (bytes-set! bs (+ i 2) (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (bytes-set! bs (+ i 3) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (bytes-set! bs (+ i 4) (unsafe-fxand 255 n)))

(define (6-unrolled-unsafe-bytes-nat-set! n width bs i)
  (bytes-set! bs i       (unsafe-fxand 255 (unsafe-fxrshift n 36)))
  (bytes-set! bs (+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 32)))
  (bytes-set! bs (+ i 2) (unsafe-fxand 255 (unsafe-fxrshift n 24)))
  (bytes-set! bs (+ i 3) (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (bytes-set! bs (+ i 4) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (bytes-set! bs (+ i 5) (unsafe-fxand 255 n)))
