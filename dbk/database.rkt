#lang racket/base
(provide
  begin-update
  database
  database-path
  database-relation-names
  database-relation-name?
  database-relation
  database-relation-new
  database-relation-add!
  database-trash-empty!
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
  R.empty R+ R-
  auto-empty-trash?
  current-batch-size)
(require "logging.rkt" "misc.rkt" "storage.rkt"
         racket/hash racket/list racket/match racket/set racket/struct)

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
(define current-batch-size (make-parameter (expt 2 24)))

(define (database-path             db)      ((wrapped-database-controller db) 'path))
(define (database-trash-empty!     db)      ((wrapped-database-controller db) 'trash-empty!))
(define (database-relation-names   db)      ((wrapped-database-controller db) 'relation-names))
(define (database-relation-name?   db name) ((wrapped-database-controller db) 'relation-name? name))
(define (database-relation         db name) ((wrapped-database-controller db) 'relation       name))
(define (database-relation-new     db type) ((wrapped-database-controller db) 'relation-new   type))
(define (database-relation-add!    db name attrs type)
  (let ((r (database-relation-new db type)))
    (relation-name-set!       r name)
    (relation-attributes-set! r attrs)
    r))

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
      (stg-update! 'relation-id=>indexes (lambda (rid=>os) (hash-update rid=>os id.self update))))
    (define self
      (method-lambda
        ((valid?)      #t)
        ((path)        path.db)
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
        ((assign! expr) (R-assign! id.self expr))
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

  (define (table-expr type rexpr)
    ;; TODO: simplify and normalize resulting table expression
    (let loop ((rexpr rexpr))
      (match rexpr
        ('()                     '())
        (`(+ ,@rs)               `(+ . ,(map loop rs)))
        (`(- ,r0 ,r1)            `(- ,(loop r0) ,(loop r1)))
        ((? wrapped-relation? R) (let ((R (wrapped-relation-controller R)))
                                   (unless (equal? (R 'path) path.db)
                                     (error "cannot combine relations from different databases"
                                            (R 'path) path.db))
                                   (unless (equal? (R 'type) type)
                                     (error "type mismatch" (R 'type) type))
                                   (R 'table-expr))))))

  (define rids.new (mutable-set))
  (define id=>R    (make-weak-hash))
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
  (define (R-has-name?  id)         (hash-has-key? (stg-ref 'relation-id=>name)  id))
  (define (R-name       id)         (hash-ref (stg-ref 'relation-id=>name)       id R-anonymous))
  (define (R-attrs      id)         (hash-ref (stg-ref 'relation-id=>attributes) id))
  (define (R-type       id)         (hash-ref (stg-ref 'relation-id=>type)       id))
  (define (R-assign!    id rexpr)   (stg-update!
                                      'relation-id=>table-expr
                                      (lambda (rid=>ts)
                                        (hash-set rid=>ts id (table-expr (R-type id) rexpr)))))
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
    ;; - perform compaction and intra-text gc
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
      ;; table-id => (list desc.column ...)
      (stg-set! 'table-id=>columns             (hash))
      ;; (cons table-id ordering-prefix) => desc.column
      (stg-set! 'index-prefix=>key-column      (hash))
      ;; (cons table-id ordering-prefix) => desc.column  ; for forming intervals
      (stg-set! 'index-prefix=>position-column (hash))
      ;; desc.column:
      ;;  (hash 'type   'line
      ;;        'count  nat
      ;;        'offset int
      ;;        'step   int)
      ;; OR
      ;;  (hash 'type      'block   ; implicit block name is (cons table-id column-position)
      ;;        'bit-width nat
      ;;        'count     nat
      ;;        'offset    int
      ;;        'min       int
      ;;        'max       int)
      ;; Indirects are optional and provide dictionary compression:
      ;; table-id => (list (OR #f desc.column) ...)  ; one per column
      (stg-set! 'table-id=>indirects           (hash))
      ;; table-id => text-id
      (stg-set! 'table-id=>text-id             (hash))
      ;; text-id => (hash 'value desc.column 'position desc.column)
      (stg-set! 'text-id=>text                 (hash))
      ;; relation-id => #t
      (stg-set! 'pending:full-compactions      (hash))
      ;; relation-id => #t
      (stg-set! 'pending:minor-compactions     (hash))
      (stg-set! 'next-uid                      0)
      (checkpoint!))
    (perform-pending-jobs!))

  (wrapped-database
    (method-lambda
      ((path)                (storage-path stg))
      ((relation-names)      (hash-keys (name=>relation-id)))
      ((relation-name? name) (relation-name? name))
      ((relation       name) (name->R name))
      ((relation-new   type)
       (claim-update!)
       (valid-relation-type?! type)
       (define id.R (fresh-uid))
       (set-add! rids.new id.R)
       (stg-update! 'relation-id=>attributes (lambda (rid=>as) (hash-set rid=>as id.R (range (length type)))))
       (stg-update! 'relation-id=>type       (lambda (rid=>t)  (hash-set rid=>t  id.R type)))
       (stg-update! 'relation-id=>indexes    (lambda (rid=>is) (hash-set rid=>is id.R (hash))))
       (R-assign! id.R R.empty)
       (id->R id.R))
      ((trash-empty!) (storage-trash-empty! stg)))))

(define (valid-attributes?! attrs)
  (unless (list? attrs)
    (error "attributes must be a list" attrs))
  (unless (= (length attrs) (set-count (list->set attrs)))
    (error "attributes must be unique" attrs)))

(define (valid-relation-type?! type)
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
