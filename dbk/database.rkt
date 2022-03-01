#lang racket/base
(provide
  begin-update
  database
  database-path
  database-relation-names
  database-relation-name?
  database-relation
  database-relation-add!
  database-relation-remove!
  database-trash-empty!
  relation-name
  relation-attributes
  relation-type
  relation-delete!
  auto-empty-trash?
  current-batch-size)
(require "logging.rkt" "misc.rkt" "storage.rkt"
         racket/list racket/set racket/struct)

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

(define (database-path             db)                 ((wrapped-database-controller db) 'path))
(define (database-relation-names   db)                 ((wrapped-database-controller db) 'relation-names))
(define (database-relation-name?   db name)            ((wrapped-database-controller db) 'relation-name?   name))
(define (database-relation         db name)            ((wrapped-database-controller db) 'relation         name))
(define (database-relation-add!    db name attrs type) ((wrapped-database-controller db) 'relation-add!    name attrs type))
(define (database-relation-remove! db name)            (relation-delete! (database-relation db name)))
(define (database-trash-empty!     db)                 ((wrapped-database-controller db) 'trash-empty!))

(define (relation-name       r) ((wrapped-relation-controller r) 'name))
(define (relation-attributes r) ((wrapped-relation-controller r) 'attributes))
(define (relation-type       r) ((wrapped-relation-controller r) 'type))
(define (relation-delete!    r) ((wrapped-relation-controller r) 'delete!))

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
                                (if ((wrapped-relation-controller r) 'valid?)
                                  (list (relation-name r))
                                  '()))))))

(define (database path.db)
  (define (make-relation id.self)

    (define (valid?) (not (not id.self)))
    (define (invalidate!)
      (set! id.self #f)
      (hash-remove! id=>R (list id.self)))
    (method-lambda
      ((valid?)      (valid?))
      ((invalidate!) (invalidate!))
      ((name)        (R-name  id.self))
      ((attributes)  (R-attrs id.self))
      ((type)        (R-type  id.self))
      ((delete!)
       (let ((name  (R-name  id.self))
             (attrs (R-attrs id.self))
             (type  (R-type  id.self)))
         (claim-update!)
         (apply pretty-log `(deleting relation ,name)
                (map (lambda (a t) `(,a : ,t)) attrs type))
         (stg-update! 'name=>relation-id       (lambda (n=>rid) (hash-remove n=>rid  name)))
         (stg-update! 'relation-id=>name       (lambda (rid=>n)  (hash-remove rid=>n  id.self)))
         (stg-update! 'relation-id=>attributes (lambda (rid=>as) (hash-remove rid=>as id.self)))
         (stg-update! 'relation-id=>type       (lambda (rid=>t)  (hash-remove rid=>t  id.self)))
         (stg-update!
           'pending:relation-id=>tables
           (lambda (rid=>tables)
             (for-each decref-table!
                       (map cdr (hash-ref rid=>tables id.self '())))
             (hash-remove rid=>tables id.self)))
         (stg-update!
           'relation-id=>tables
           (lambda (rid=>tables)
             (define tables.current (map cdr (hash-ref rid=>tables id.self)))
             (for-each decref-table! tables.current)
             (stg-update!
               'relation-id=>indexes
               (lambda (rid=>indexes)
                 (stg-update!
                   'pending:index-id=>delta
                   (lambda (iid=>delta)
                     (foldl
                       (lambda (iid iid=>delta)
                         (hash-update iid=>delta iid (lambda (delta) (- delta 1)) 0))
                       iid=>delta
                       (append*
                         (map (lambda (ordering)
                                (map (lambda (tid) (cons tid ordering)) tables.current))
                              (hash-keys (hash-ref rid=>indexes id.self)))))))
                 (hash-remove rid=>indexes id.self)))
             (hash-remove rid=>tables id.self)))
         (invalidate!)))
      ))

  (define rids.new '())
  (define id=>R    (make-weak-hash))
  (define (make-R name)
    (let* ((id.R  (hash-ref (name=>relation-id) name
                            (lambda () (error "unknown relation" name (storage-path stg)))))
           (key.R (list id.R)))
      (wrapped-relation
        key.R
        (or (hash-ref id=>R key.R #f)
            (let ((R (make-relation id.R)))
              (hash-set! id=>R key.R R)
              R)))))
  (define (stg-ref      key)        (storage-description-ref     stg key))
  (define (stg-set!     key value)  (storage-description-set!    stg key value))
  (define (stg-update!  key update) (storage-description-update! stg key update))
  (define (R-invalid)               (error "cannot use deleted relation"))
  (define (R-name       id)         (hash-ref (stg-ref 'relation-id=>name)       id R-invalid))
  (define (R-attrs      id)         (hash-ref (stg-ref 'relation-id=>attributes) id R-invalid))
  (define (R-type       id)         (hash-ref (stg-ref 'relation-id=>type)       id R-invalid))
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
                              (when R ((wrapped-relation-controller R) 'invalidate!))))
              rids.new)
    (set! rids.new '())
    (storage-revert! stg))
  (define (commit!)
    (checkpoint!)
    (set! rids.new '())
    (perform-pending-jobs!))
  (define (perform-pending-jobs!)
    (stg-update!
      'pending:relation-id=>tables
      (lambda (rid=>tables.new)
        (stg-update!
          'relation-id=>tables
          (lambda (rid=>tables.current)
            (foldl
              (lambda (ridts rid=>ts)
                (let ((rid    (car ridts))
                      (ts.new (cdr ridts)))
                  (hash-update
                    rid=>ts rid
                    (lambda (ts.current)
                      ;; TODO: automatically request compaction if appropriate
                      (append ts.current ts.new)))))
              rid=>tables.current
              (hash->list rid=>tables.new))))
        (hash)))
    ;; TODO: perform any compaction before building new indexes
    (stg-update!
      'pending:index-id=>delta
      (lambda (iid=>delta)
        (stg-update!
          'index-id=>refcount
          (lambda (iid=>rc)
            (foldl
              (lambda (iidd iid=>rc)
                (let* ((iid     (car iidd))
                       (delta   (cdr iidd))
                       (iid=>rc (hash-update
                                  iid=>rc iid
                                  (lambda (rc)
                                    (let ((rc.new (+ rc delta)))
                                      (cond ((and (= rc 0) (< 0 rc.new))
                                             ;; TODO: build a new index
                                             )
                                            ((and (= rc.new 0) (< 0 rc))
                                             ;; TODO: deallocate index
                                             ))
                                      rc.new))
                                  0)))
                  (if (= 0 (hash-ref iid=>rc iid))
                    (hash-remove iid=>rc iid)
                    iid=>rc)))
              iid=>rc
              (hash->list iid=>delta))))
        (hash)))
    (checkpoint!))

  ;; TODO: bulk process this stg-update! instead, as is done with index-id=>refcount
  (define (decref-table! tid)
    (stg-update!
      'table-id=>refcount
      (lambda (tid=>rc)
        (let ((tid=>rc
                (hash-update
                  tid=>rc tid
                  (lambda (rc)
                    (let ((rc (- rc 1)))
                      (when (= rc 0)
                        ;; TODO: deallocate columns
                        ;; TODO: and deref any associated text-id
                        (void))
                      rc)))))
          (if (= 0 (hash-ref tid=>rc tid))
            (hash-remove tid=>rc tid)
            tid=>rc)))))

  (define stg (storage:filesystem path.db))
  (let ((version (storage-description-ref stg 'database-format-version #f)))
    (unless (equal? version version.current)
      (when version (error "unknown version" version))
      (stg-set! 'database-format-version       version.current)
      (stg-set! 'name=>relation-id             (hash))
      (stg-set! 'relation-id=>name             (hash))
      (stg-set! 'relation-id=>attributes       (hash))
      (stg-set! 'relation-id=>type             (hash))
      ;; relation-id => (list (cons (OR 'insert 'delete) table-id) ...)
      (stg-set! 'relation-id=>tables           (hash))
      ;; relation-id => (ordering => ())
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
      ;; relation-id => (list (cons (OR 'insert 'delete) table-id) ...)
      (stg-set! 'pending:relation-id=>tables   (hash))
      ;; (cons table-id ordering) => delta
      (stg-set! 'pending:index-id=>delta       (hash))
      ;; table-id => nat
      (stg-set! 'table-id=>refcount            (hash))
      ;; (cons table-id ordering) => nat
      (stg-set! 'index-id=>refcount            (hash))
      ;; text-id => nat
      (stg-set! 'text-id=>refcount             (hash))
      (stg-set! 'next-uid                      0)
      (checkpoint!))
    (perform-pending-jobs!))

  (wrapped-database
    (method-lambda
      ((path)                (storage-path stg))
      ((relation-names)      (hash-keys (name=>relation-id)))
      ((relation-name? name) (relation-name? name))
      ((relation       name) (make-R name))
      ((relation-add! name attrs type)
       (claim-update!)
       (new-relation?! name)
       (valid-attributes?! attrs)
       (valid-relation-type?! type)
       (unless (= (length attrs) (length type))
         (error "number of attributes must match the relation type arity"
                name attrs type))
       (apply pretty-log `(creating relation ,name)
              (map (lambda (a t) `(,a : ,t)) attrs type))
       (define id.R (fresh-uid))
       (set! rids.new (cons id.R rids.new))
       (stg-update! 'name=>relation-id       (lambda (n=>rid)  (hash-set n=>rid  name id.R)))
       (stg-update! 'relation-id=>name       (lambda (rid=>n)  (hash-set rid=>n  id.R name)))
       (stg-update! 'relation-id=>attributes (lambda (rid=>as) (hash-set rid=>as id.R attrs)))
       (stg-update! 'relation-id=>type       (lambda (rid=>t)  (hash-set rid=>t  id.R type)))
       (stg-update! 'relation-id=>tables     (lambda (rid=>ts) (hash-set rid=>ts id.R '())))
       (stg-update! 'relation-id=>indexes    (lambda (rid=>is) (hash-set rid=>is id.R (hash)))))
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
