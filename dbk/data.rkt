#lang racket/base
(provide
  column:const
  column:vector
  column:table
  column:indirect
  column:interval
  column:bytes:nat
  column:port
  column:port-indirect
  column:port-string
  interval->dict:ordered
  dict.empty
  dict:ordered
  dict:ordered:vector
  dict:hash
  enumerator-project
  enumerator-filter
  enumerator-sort
  enumerator-dedup
  enumerator->dict:ordered:vector-flat
  enumerator->dict:ordered:vector-group
  group-fold->hash
  group-fold
  group-fold-ordered
  merge-key-union
  merge-antijoin
  merge-join
  dict-join-unordered
  dict-join-ordered
  hash-join
  dict-antijoin-unordered
  dict-antijoin-ordered
  hash-antijoin
  dict-key-union-unordered
  dict-key-union-ordered
  dict-subtract-unordered
  dict-subtract-ordered

  database
  database-path
  database-metadata
  database-relation
  database-relation-names
  database-relation-has?
  database-relation-add!
  database-relation-remove!
  database-compact!
  database-import!
  database-export!

  relation-name
  relation-metadata
  relation-copy!
  relation-rename!
  relation-delete!
  relation-rename-attributes!
  relation-index-add!
  relation-index-remove!
  relation-index-dict
  relation-domain-dicts
  relation-compact!
  )
(require "codec.rkt" "enumerator.rkt" "heap.rkt" "misc.rkt" "order.rkt" "stream.rkt"
         racket/file racket/list racket/match racket/pretty racket/set racket/struct racket/vector)

;; TODO:
;bscm.rkt
;bscm:read bscm:write ?
;or favor a bytes-ref/bytes-set! style interface with bscm types?

;; TODO: use these definitions to replace the logging defined in config.rkt
(define (pretty-log/port out . args)
  (define seconds (current-seconds))
  (define d       (seconds->date seconds #f))
  (define d-parts (list seconds 'UTC
                        (date-year d) (date-month  d) (date-day    d)
                        (date-hour d) (date-minute d) (date-second d)))
  (pretty-write (cons d-parts args) out))

(define (pretty-logf/port out message . args) (pretty-log/port out (apply format message args)))

(define current-log-port (make-parameter (current-error-port)))

(define (pretty-log  . args) (apply pretty-log/port  (current-log-port) args))
(define (pretty-logf . args) (apply pretty-logf/port (current-log-port) args))

(define-syntax-rule (time/pretty-log body ...)
  (let-values (((results time.cpu time.real time.gc) (time-apply (lambda () body ...) '())))
    (pretty-log `(time cpu ,time.cpu real ,time.real gc ,time.gc))
    (apply values results)))

;;;;;;;;;;;;;;;;;;;;;;;
;;; Metadata format ;;;
;;;;;;;;;;;;;;;;;;;;;;;
;;
;;desc.database
;;(hash
;;  'relations      (hash NAME desc.relation ...)
;;  'current        (hash PATH desc.arbitrary ...)
;;  'pending        (list JOB ...)
;;  )
;;
;;;; supports copying by sharing table and index descriptions
;;desc.relation
;;(hash
;;  'attributes (list attribute-names ...)
;;  'type       (list high-level-types ...)
;;  'tables     (list path.table ...)
;;  'indexes    (hash ordering (list path.table-index ...) ...)
;;  )
;;
;;desc.domain
;;(hash
;;  'text path-to-domain-text
;;  ...
;;  )
;;
;;desc.domain-text
;;(hash
;;  'count         num-elements
;;  'size.position num-bytes
;;  'index         optional-path-to-index
;;  )
;;
;;TODO:
;;desc.domain-text-index
;;(hash
;;  'domain path.domain
;;  )
;;
;;desc.column
;;(hash
;;  'type  'nat|'text
;;  'count num-elements
;;  'size  nat-bytes
;;  'min   _
;;  'max   _
;;  )
;;
;;desc.table
;;(hash
;;  'direction 'insert|'delete
;;  'domain    desc.domain
;;  'count     tuple-count
;;  'columns   (list desc.column ...)
;;  )
;;
;;desc.table-index
;;(hash
;;  'table            path.table
;;  'ordering         (list 3 1 2 etc.)
;;  'columns.value    (list desc.column ...)
;;  'columns.indirect (list desc.column ...)  ; length is (- (length columns) 1)
;;  )

(define fnsuffix.key        ".key")
(define fnsuffix.indirect   ".indirect")
(define fnsuffix.next       ".next")
(define fnsuffix.initial    ".initial")
(define fn.metadata         "metadata.scm")
(define fn.value            "value")
(define fn.pos              "position")
(define fn.tuple            "tuple")
(define fn.col              "column")
(define fn.metadata.initial (string-append fn.metadata fnsuffix.initial))

(define metadata-format-version "0")

(define (write-metadata path.metadata metadata)
  (let/files () ((out.metadata path.metadata))
    (pretty-write
      (hash-set metadata 'format-version metadata-format-version)
      out.metadata)))

(define desc.database.empty
  (hash 'relations (hash)
        'data      (hash)))

;; TODO: store redundant metadata
;; - for optional consistency checking
;; - also for independent file/folder exploration (e.g., analyzing trash)
;;   - file explorer:
;;     - display both logical and physical values
;;     - indicate min/max
;;     - jump to row, jump to column
;;       - absolute or relative
;;     - search for logical/physical value
;;       - local (fast), or various forms of global (slow)
;;       - optionally use full-text search indexes
;; - "previous" directory supports time travel as long as most recent groups are not deleted

(define (valid-attributes?! attrs)
  (unless (list? attrs)
    (error "attributes must be a list" attrs))
  (for-each (lambda (a) (unless (symbol? a) (error "attribute must be a symbol" a 'in attrs)))
            attrs)
  (unless (= (length attrs) (set-count (list->set attrs)))
    (error "attributes must be unique" attrs)))

(struct wrapped-database (controller)
        #:methods gen:custom-write
        ((define write-proc
           (make-constructor-style-printer
             (lambda (db) 'database)
             (lambda (db) (list (database-path db)
                                (hash-ref (database-metadata db) 'relations)))))))

(struct wrapped-relation (controller)
        #:methods gen:custom-write
        ((define write-proc
           (make-constructor-style-printer
             (lambda (r) 'relation)
             (lambda (r) (list (relation-name     r)
                               (relation-metadata r)))))))

;; Racket doesn't seem to have a weak-value hash table, so we
;; suboptimally use a weak-key hash table as a self-cleaning list.
(define all-databases (make-weak-hash))
(define (database path.db)
  (let ((path.db (normalize-path path.db)))
    (or (ormap (lambda (db) (and (equal? path.db (database-path db)) db))
               (hash-keys all-databases))
        (let ((db (make-database path.db)))
          (hash-set! all-databases db (void))
          db))))

(define (make-database path.db)
  (define (db-path   name) (path->string (build-path path.db      name)))
  (define (data-path name) (path->string (build-path path.current name)))

  (define (relations-update! f) (set! metadata (hash-update metadata 'relations f)))
  (define (data-update!      f) (set! metadata (hash-update metadata 'data      f)))

  (define (reachable (relation-names #f))
    (define data               (hash-ref metadata 'data))
    (define name=>relation     (hash-ref metadata 'relations))
    (define descs.relation     (if relation-names
                                 (map (lambda (name) (hash-ref name=>relation name)) relation-names)
                                 (hash-values name=>relation)))
    (define lpaths.table       (list->set (append* (map (lambda (desc.r) (hash-ref desc.r 'tables))
                                                        descs.relation))))
    (define lpaths.table-index (list->set (append* (map (lambda (desc.r) (append* (hash-values (hash-ref desc.r 'indexes))))
                                                        descs.relation))))
    (define descs.table        (set-map lpaths.table (lambda (lp) (hash-ref data lp))))
    (define lpaths.domain-text (list->set (map (lambda (desc.t) (hash-ref (hash-ref desc.t 'domain) 'text))
                                               descs.table)))
    (set-union lpaths.table lpaths.table-index lpaths.domain-text))

  (define (clean!)
    (define pending-previous   (path->string (build-path path.db          "pending-previous")))
    (define metadata.previous  (path->string (build-path pending-previous fn.metadata)))
    (define lpaths.old         (if (and (directory-exists? pending-previous)
                                        (file-exists?      metadata.previous))
                                 (list->set (hash-keys (hash-ref (call-with-input-file metadata.previous read)
                                                                 'data)))
                                 (set)))
    (define lpaths.known       (set-union lpaths.old (list->set (hash-keys (hash-ref metadata 'data)))))
    (define lpaths.reachable   (reachable))
    (define lpaths.all         (list->set (map path->string (directory-list path.current))))
    (define lpaths.trash       (set->list (set-subtract lpaths.all   lpaths.known)))
    (define lpaths.unreachable (set->list (set-subtract lpaths.known lpaths.reachable)))
    (unless (null? lpaths.unreachable)
      (apply pretty-log '(archiving unreachable data) lpaths.unreachable)
      (make-directory* pending-previous)
      (for-each (lambda (lp)
                  (define apath (data-path lp))
                  (when (directory-exists? apath)
                    (rename-file-or-directory apath (build-path pending-previous lp))))
                lpaths.unreachable))
    (when (directory-exists? pending-previous)
      (define upath (unique-previous-path))
      (pretty-log `(moving ,pending-previous to ,upath))
      (rename-file-or-directory pending-previous upath))
    (define pending-trash (path->string (build-path path.db "pending-trash")))
    (when (file-exists? path.metadata.next)
      (pretty-log '(moving interrupted checkpoint to trash) path.metadata.next)
      (make-directory* pending-trash)
      (rename-file-or-directory path.metadata.next
                                (build-path pending-trash (string-append fn.metadata fnsuffix.next))))
    (unless (null? lpaths.trash)
      (apply pretty-log '(moving unknown data to trash) lpaths.trash)
      (make-directory* pending-trash)
      (for-each (lambda (lp) (rename-file-or-directory (data-path lp) (build-path pending-trash lp)))
                lpaths.trash))
    (when (directory-exists? pending-trash)
      (define upath (unique-trash-path))
      (pretty-log `(moving ,pending-trash to ,upath))
      (rename-file-or-directory pending-trash upath)))

  (define (checkpoint!)
    (define metadata.previous (call-with-input-file path.metadata read))
    (when   (equal? metadata metadata.previous) (pretty-log '(no checkpoint necessary)))
    (unless (equal? metadata metadata.previous)
      (define lpaths.known       (list->set (hash-keys (hash-ref metadata 'data))))
      (define lpaths.reachable   (reachable))
      (define lpaths.unreachable (set->list (set-subtract lpaths.known lpaths.reachable)))
      (unless (null? lpaths.unreachable)
        (data-update! (lambda (data) (foldl (lambda (lp data) (hash-remove data lp))
                                            data lpaths.unreachable))))
      (call-with-output-file path.metadata.next (lambda (out) (pretty-write metadata out)))
      (define pending-previous (build-path path.db "pending-previous"))
      (make-directory* pending-previous)
      (pretty-log `(archiving ,path.metadata))
      (rename-file-or-directory path.metadata (build-path pending-previous fn.metadata))
      (pretty-log '(checkpointing metadata) metadata)
      (rename-file-or-directory path.metadata.next path.metadata)
      (clean!)))

  ;; TODO: later, consolidate subsequent inserts/deletes
  (define (compact!)
    (pretty-log `(compacting ,path.db))
    (define data           (hash-ref metadata 'data))
    (define descs.relation (hash-values (hash-ref metadata 'relations)))
    (match-define (list paths.table descs.table paths.domain-text/duplicates)
      (let* ((paths.table       (append* (map (lambda (desc.relation) (hash-ref desc.relation 'tables))
                                              descs.relation)))
             (descs.table       (map (lambda (path.table) (hash-ref data path.table)) paths.table))
             (paths.domain-text (map (lambda (d) (hash-ref (hash-ref d 'domain) 'text))
                                     descs.table))
             (pdps              (map (lambda (path.table desc.table path.domain-text)
                                       (and path.domain-text (list path.table desc.table path.domain-text)))
                                     paths.table descs.table paths.domain-text)))
        (apply map list (filter-not not pdps))))
    (define paths.domain-text (set->list (list->set paths.domain-text/duplicates)))
    (unless (< 1 (length paths.domain-text)) (pretty-log '(no compaction necessary)))
    (when (< 1 (length paths.domain-text))
      (define (type=>id=>id path.domain-text)
        (hash 'text (hash-ref path.domain-text=>id=>id path.domain-text)))
      (define path.domain-text.new     (unique-path "domain-text"))
      (define descs.domain-text        (map (lambda (p) (hash-ref data p)) paths.domain-text))
      (define compaction               (compact-text-domains
                                         (data-path path.domain-text.new)
                                         (map data-path paths.domain-text)
                                         descs.domain-text))
      (define desc.domain-text.new     (hash-ref compaction 'domain-text))
      (define path.domain-text=>id=>id (make-immutable-hash
                                         (map cons paths.domain-text (hash-ref compaction 'remappings))))
      (define paths.table.new          (map (lambda (path.table desc.table path.domain-text)
                                              (if (remap-table? desc.table (type=>id=>id path.domain-text))
                                                (unique-path "table")
                                                path.table))
                                            paths.table descs.table paths.domain-text/duplicates))
      (define path.t=>path.t.new       (make-immutable-hash (map cons paths.table paths.table.new)))
      (define descs.table.new
        (map (lambda (path.table path.table.new desc.table path.domain-text)
               (define desc.domain.new (hash 'text path.domain-text.new))
               (if (equal? path.table path.table.new)
                 (begin (pretty-log '(no need to remap table) path.table)
                        (hash-set desc.table 'domain desc.domain.new))
                 (remap-table (data-path path.table)
                              (data-path path.table.new)
                              desc.table
                              desc.domain.new
                              (type=>id=>id path.domain-text))))
             paths.table paths.table.new descs.table paths.domain-text/duplicates))
      (define paths.table-index
        (append* (map (lambda (desc.relation)
                        (filter (lambda (path.table-index)
                                  (define desc.table-index (hash-ref data path.table-index))
                                  (member (hash-ref desc.table-index 'table) paths.table))
                                (append* (hash-values (hash-ref desc.relation 'indexes)))))
                      descs.relation)))
      (define descs.table-index     (map (lambda (path.ti) (hash-ref data path.ti)) paths.table-index))
      (define paths.table-index.new (map (lambda (path.ti desc.ti)
                                           (define path.table  (hash-ref desc.ti 'table))
                                           (define desc.domain (hash-ref (hash-ref data path.table) 'domain))
                                           (define path.dt     (hash-ref desc.domain 'text))
                                           (if (remap-table-index? desc.ti (type=>id=>id path.dt))
                                             (unique-path "table-index")
                                             path.ti))
                                         paths.table-index descs.table-index))
      (define path.ti=>path.ti.new  (make-immutable-hash (map cons paths.table-index paths.table-index.new)))
      (define (replace-p.ti p.ti) (hash-ref path.ti=>path.ti.new p.ti p.ti))
      (define descs.table-index.new
        (map (lambda (path.table-index path.table-index.new desc.table-index)
               (define path.table       (hash-ref desc.table-index   'table))
               (define path.table.new   (hash-ref path.t=>path.t.new path.table))
               (define path.domain-text (hash-ref (hash-ref (hash-ref data path.table) 'domain) 'text))
               (if (equal? path.table-index path.table-index.new)
                 (begin (pretty-log '(no need to remap table index) path.table-index)
                        desc.table-index)
                 (remap-table-index (data-path path.table-index)
                                    (data-path path.table-index.new)
                                    desc.table-index
                                    path.table.new
                                    (type=>id=>id path.domain-text))))
             paths.table-index paths.table-index.new descs.table-index))
      (pretty-log '(installing remapped data))
      (data-update!
        (lambda (data)
          (apply hash-set* data
                 path.domain-text.new desc.domain-text.new
                 (append (append* (map list paths.table.new       descs.table.new))
                         (append* (map list paths.table-index.new descs.table-index.new))))))
      (pretty-log '(updating relations with remapped data))
      (relations-update!
        (lambda (rs)
          (make-immutable-hash
            (hash-map rs (lambda (name.r desc.r)
                           (let* ((desc.r (hash-update desc.r 'tables
                                                       (lambda (paths.table)
                                                         (map (lambda (p) (hash-ref path.t=>path.t.new   p p))
                                                              paths.table))))
                                  (desc.r (hash-update desc.r 'indexes
                                                       (lambda (ordering=>paths.table-index)
                                                         (make-immutable-hash
                                                           (hash-map ordering=>paths.table-index
                                                                     (lambda (o ps.ti)
                                                                       (cons o (map replace-p.ti ps.ti)))))))))
                             (cons name.r desc.r)))))))
      (checkpoint!)))

  (define (unique-path str.type)
    (define data    (hash-ref metadata 'data))
    (define seconds (number->string (current-seconds)))
    (let loop ((id.local 0))
      (define candidate (string-append str.type "-" seconds "-" (number->string id.local)))
      (cond ((hash-has-key? data candidate) (loop (+ id.local 1)))
            (else                           (make-directory* (data-path candidate))
                                            (data-update! (lambda (data) (hash-set data candidate #f)))
                                            candidate))))
  (define (unique-directory path.root str.type)
    (define seconds (number->string (current-seconds)))
    (let loop ((id.local 0))
      (define candidate (string-append str.type "-" seconds "-" (number->string id.local)))
      (define apath (path->string (build-path path.root candidate)))
      (cond ((directory-exists? apath) (loop (+ id.local 1)))
            (else                      apath))))
  (define (unique-previous-path) (unique-directory path.previous "previous"))
  (define (unique-trash-path)    (unique-directory path.trash    "trash"))

  (define (new-relation?! name) (when (hash-has-key? name=>relation name)
                                  (error "relation already exists" name path.db)))

  (define (make-relation name)
    (define (description)           (hash-ref (hash-ref metadata 'relations) name))
    (define (description-update! f) (relations-update! (lambda (rs) (hash-update rs name f))))
    (define self
      (method-lambda
        ((name)                     name)
        ((metadata)                 (description))
        ((copy!   name.new)         (pretty-log `(copying relation ,name to ,name.new))
                                    (new-relation?! name.new)
                                    (set! name=>relation
                                      (hash-set name=>relation name.new (hash-ref name=>relation name)))
                                    (relations-update! (lambda (rs) (hash-set rs name.new (hash-ref rs name))))
                                    (checkpoint!))
        ((rename! name.new)         (pretty-log `(renaming relation ,name to ,name.new))
                                    (new-relation?! name.new)
                                    (set! name=>relation (let ((r    (hash-ref    name=>relation name))
                                                               (n=>r (hash-remove name=>relation name)))
                                                           (hash-set n=>r name.new r)))
                                    (relations-update! (lambda (rs) (let* ((r  (hash-ref    rs name))
                                                                           (rs (hash-remove rs name)))
                                                                      (hash-set rs name.new r))))
                                    (set! name name.new)
                                    (checkpoint!))
        ((delete!)                  (pretty-log `(deleting relation ,name))
                                    (set! self           #f)
                                    (set! name=>relation (hash-remove name=>relation name))
                                    (relations-update! (lambda (rs) (hash-remove rs name)))
                                    (checkpoint!))
        ((rename-attributes! attrs) (pretty-log `(renaming ,name attributes)
                                                `(old: ,(hash-ref (description) 'attributes))
                                                `(new: ,attrs))
                                    (valid-attributes?! attrs)
                                    (let ((attrs.old (hash-ref (description) 'attributes)))
                                      (unless (= (length attrs) (length attrs.old))
                                        (error "cannot change the number of attributes"
                                               name 'new attrs 'old attrs.old)))
                                    (relations-update! (lambda (rs) (let* ((r (hash-ref rs name))
                                                                           (r (hash-set r 'attributes attrs)))
                                                                      (hash-set rs name r))))
                                    (checkpoint!))
        ((index-add!    signatures) (apply pretty-log `(adding indexes for ,name) signatures)
                                    (let* ((desc         (description))
                                           (data         (hash-ref metadata 'data))
                                           (attrs        (hash-ref desc     'attributes))
                                           (lpaths.table (hash-ref desc     'tables))
                                           (descs.table  (map (lambda (lp) (hash-ref data lp)) lpaths.table))
                                           (orderings    (map (attrs->signature->ordering attrs) signatures))
                                           (orderings    (normalize-table-index-orderings (length attrs) orderings))
                                           (ords.current (list->set (hash-keys (hash-ref desc 'indexes))))
                                           (ords.skipped (set->list (set-intersect (list->set orderings) ords.current)))
                                           (ords.new     (set->list (set-subtract  (list->set orderings) ords.current))))
                                      (define (ords->sigs ords)
                                        (map (lambda (ordering) (map (lambda (i) (list-ref attrs i))
                                                                     (filter-not (lambda (i) (eq? #t i))
                                                                                 ordering)))
                                             ords))
                                      (apply pretty-log '(normalizing table index signatures) (ords->sigs orderings))
                                      (unless (null? ords.skipped)
                                        (apply pretty-log '(skipping existing table indexes)
                                               (ords->sigs ords.skipped)))
                                      (when   (null? ords.new) (pretty-log '(no table indexes to build)))
                                      (unless (null? ords.new)
                                        (define lpaths*.ti
                                          (apply map list
                                                 (map (lambda (lpath.table desc.table)
                                                        (define lpaths.ti (map (lambda (_) (unique-path "table-index")) ords.new))
                                                        (define descs.ti  (build-table-indexes
                                                                            path.current lpaths.ti lpath.table desc.table ords.new))
                                                        (data-update!
                                                          (lambda (data) (apply hash-set* data
                                                                                (append* (map list lpaths.ti descs.ti)))))
                                                        lpaths.ti)
                                                      lpaths.table descs.table)))
                                        (description-update!
                                          (lambda (desc) (hash-update desc 'indexes
                                                                      (lambda (ordering=>lps)
                                                                        (apply hash-set* ordering=>lps
                                                                               (append* (map list ords.new lpaths*.ti)))))))
                                        (checkpoint!))))
        ((index-remove! signatures) (apply pretty-log `(removing indexes for ,name) signatures)
                                    (let* ((desc         (description))
                                           (data         (hash-ref metadata 'data))
                                           (attrs        (hash-ref desc     'attributes))
                                           (orderings    (map (attrs->signature->ordering attrs) signatures))
                                           (orderings    (normalize-table-index-orderings (length attrs) orderings))
                                           (ords.current (list->set (hash-keys (hash-ref desc 'indexes))))
                                           (ords.missing (set->list (set-subtract  (list->set orderings) ords.current)))
                                           (ords.found   (set->list (set-intersect (list->set orderings) ords.current))))
                                      (define (ords->sigs ords)
                                        (map (lambda (ordering) (map (lambda (i) (list-ref attrs i))
                                                                     (filter-not (lambda (i) (eq? #t i))
                                                                                 ordering)))
                                             ords))
                                      (apply pretty-log '(normalizing table index signatures) (ords->sigs orderings))
                                      (unless (null? ords.missing)
                                        (apply pretty-log '(skipping table indexes that do not exist)
                                               (ords->sigs ords.missing)))
                                      (when   (null? ords.found) (pretty-log '(no table indexes to remove)))
                                      (unless (null? ords.found)
                                        (description-update!
                                          (lambda (desc) (hash-update desc 'indexes
                                                                      (lambda (ordering=>lps)
                                                                        (foldl (lambda (o o=>lps) (hash-remove o=>lps o))
                                                                               ordering=>lps ords.found)))))
                                        (checkpoint!))))
        ((index-dict signature)     (let* ((desc      (description))
                                           (data      (hash-ref metadata 'data))
                                           (attrs     (hash-ref desc     'attributes))
                                           (ordering  ((attrs->signature->ordering attrs) signature))
                                           (ordering  (car (normalize-table-index-orderings (length attrs) (list ordering))))
                                           (lpaths.ti (hash-ref (hash-ref desc 'indexes) ordering
                                                                (lambda () (error "no relation index matches signature"
                                                                                  name signature)))))
                                      (match lpaths.ti
                                        ('() dict.empty)
                                        ((list lpath.ti)
                                         (define (descs->cols fnsuffix descs.col)
                                           (map (lambda (j desc.col)
                                                  (and desc.col
                                                       (let* ((fname (string-append "column." (number->string j) fnsuffix))
                                                              (apath (build-path (data-path lpath.ti) fname)))
                                                         (column:port (open-input-file apath) `#(nat ,(hash-ref desc.col 'size)))
                                                         ;; Optionally load index columns into memory instead
                                                         ;(time (column:bytes:nat (file->bytes apath) (hash-ref desc.col 'size)))
                                                         )))
                                                (range (length descs.col)) descs.col))
                                         (define desc.ti       (hash-ref data lpath.ti))
                                         (define descs.col.key (hash-ref desc.ti 'columns.key))
                                         (define cols.key      (descs->cols ".key"      descs.col.key))
                                         (define cols.indirect (descs->cols ".indirect" (hash-ref desc.ti 'columns.indirect)))
                                         (let loop ((start         0)
                                                    (end           (hash-ref (car descs.col.key) 'count))
                                                    (cols.key      cols.key)
                                                    (cols.indirect cols.indirect))
                                           (define (next start end) (loop start end (cdr cols.key) (cdr cols.indirect)))
                                           (define i->key   (car cols.key))
                                           (define i->value (if (null? cols.indirect)
                                                              (column:const '())
                                                              (let ((ci (car cols.indirect)))
                                                                (if ci
                                                                  (column:interval ci next)
                                                                  (lambda (i) (next i (+ i 1)))))))
                                           (dict:ordered i->key i->value start end)))
                                        ;; TODO: multiple table-indexes, possibly with deletions
                                        (_ (error "multi-table indexes are not yet supported" name lpaths.ti)))))
        ;; TODO: share domain dicts when the domains are shared across relations
        ((domain-dicts)             (let* ((desc         (description))
                                           (data         (hash-ref metadata 'data))
                                           (lpaths.table (hash-ref desc     'tables))
                                           (descs.table  (map (lambda (lp) (hash-ref data lp)) lpaths.table))
                                           (descs.domain (map (lambda (desc) (hash-ref desc 'domain)) descs.table)))
                                      (define (domain->dict-pair.text desc.domain)
                                        (define lpath.dt     (hash-ref desc.domain 'text))
                                        (define desc.dt      (hash-ref data        lpath.dt))
                                        (define size.pos     (hash-ref desc.dt     'size.position))
                                        (define count        (hash-ref desc.dt     'count))
                                        (define apath.dt     (data-path lpath.dt))
                                        ;; TODO: should col.pos be shared like this, or duplicated across dicts for safety?
                                        (define col.pos      (column:port (open-input-file (build-path apath.dt "position")) `#(nat ,size.pos)))
                                        ;; Optionally load positions into memory instead
                                        ;(define col.pos      (time (column:bytes:nat (file->bytes (build-path apath.dt "position")) size.pos)))
                                        (define id->str      (column:port-string col.pos (open-input-file (build-path apath.dt "value"))))
                                        (define dict.str=>id (dict:ordered id->str (lambda (id) id) 0 count))
                                        (define dict.id=>str (dict:ordered (lambda (id) id) id->str 0 count))
                                        (cons dict.str=>id dict.id=>str))
                                      (define dict-pairs.text (map domain->dict-pair.text descs.domain))
                                      (cons (hash 'text (map car dict-pairs.text))
                                            (hash 'text (map cdr dict-pairs.text)))))
        ;; TODO: only spend effort compacting this relation
        ((compact!)                 (compact!))))
    (wrapped-relation (lambda args
                        (unless self (error "cannot use deleted relation" name))
                        (apply self args))))

  (define path.current       (db-path "current"))
  (define path.previous      (db-path "previous"))
  (define path.trash         (db-path "trash"))
  (define path.pending       (db-path "pending"))
  (define path.metadata      (db-path fn.metadata))
  (define path.metadata.next (string-append path.metadata fnsuffix.next))
  (for-each make-directory* (list path.db path.current path.previous path.trash path.pending))
  (define metadata
    (cond ((file-exists? path.metadata)      (call-with-input-file path.metadata read))
          ((file-exists? path.metadata.next) (pretty-log '(checkpointing metadata after interrupted swap))
                                             (rename-file-or-directory path.metadata.next path.metadata)
                                             (call-with-input-file path.metadata read))
          (else                              (pretty-log '(creating new database) path.db)
                                             (call-with-output-file
                                               path.metadata
                                               (lambda (out) (pretty-write desc.database.empty out)))
                                             desc.database.empty)))
  (pretty-log '(loaded metadata for) path.db metadata)
  ;; TODO: migrate metadata if format-version is old
  (clean!)

  (define name=>relation (make-immutable-hash
                           (hash-map (hash-ref metadata 'relations)
                                     (lambda (name desc.relation)
                                       (pretty-log `(loading relation ,name) desc.relation)
                                       (cons name (make-relation name))))))

  (wrapped-database
    (method-lambda
      ((path)                              path.db)
      ((metadata)                          metadata)
      ((reachable names)                   (reachable names))
      ((relation name)                     (hash-ref name=>relation name
                                                     (lambda () (error "unknown relation" name path.db))))
      ((relation-add! name attrs type src) (apply pretty-log `(creating relation ,name)
                                                  (map (lambda (a t) `(,a : ,t)) attrs type))
                                           (new-relation?! name)
                                           (valid-attributes?! attrs)
                                           (for-each (lambda (t) (unless (member t '(nat bytes string symbol))
                                                                   (error "invalid attribute type" t 'in type)))
                                                     type)
                                           (unless (= (length attrs) (length type))
                                             (error "number of attributes must match the relation type arity"
                                                    name attrs type))
                                           (define path.domain-text (unique-path "domain-text"))
                                           (define path.table       (unique-path "table"))
                                           (define desc.ingest      (ingest-relation-source
                                                                      path.current path.domain-text path.table type src))
                                           (define desc.domain-text (hash-ref (hash-ref desc.ingest 'domain) 'text))
                                           (define desc.table       (hash-ref desc.ingest 'table))
                                           (define desc.relation    (hash 'attributes attrs
                                                                          'type       type
                                                                          'tables     (list path.table)
                                                                          'indexes    (hash)))
                                           (data-update!      (lambda (data) (hash-set* data
                                                                                        path.domain-text desc.domain-text
                                                                                        path.table       desc.table)))
                                           (relations-update! (lambda (rs)   (hash-set  rs name desc.relation)))
                                           (checkpoint!)
                                           (set! name=>relation (hash-set name=>relation name (make-relation name))))
      ((import! db.in names.in)            (pretty-log `(importing relations . ,names.in) 'from: (db.in 'path) 'into: path.db)
                                           (define path.current.in   (path->string (build-path (db.in 'path) "current")))
                                           (define name=>relation.in (hash-ref (db.in 'metadata) 'relations))
                                           (define lpath=>data.in    (hash-ref (db.in 'metadata) 'data))
                                           (define descs.relation.in (map (lambda (name)
                                                                            (unless (hash-has-key? name=>relation.in name)
                                                                              (error "cannot import non-existent relation"
                                                                                     name (db.in 'path)))
                                                                            (new-relation?! name)
                                                                            (hash-ref name=>relation.in name))
                                                                          names.in))
                                           (define lpaths.reachable (set->list (db.in 'reachable names.in)))
                                           (for-each (lambda (lpath)
                                                       (define apath.in  (path->string (build-path path.current.in lpath)))
                                                       (define apath.out (data-path lpath))
                                                       ;; TODO: validate that skipped data is identical
                                                       ;; TODO: support renaming paths when non-identical data collision occurs
                                                       (cond ((directory-exists? apath.out) (pretty-log '(skipping import copy) apath.in apath.out))
                                                             (else                          (pretty-log '(copying for import)   apath.in apath.out)
                                                                                            (copy-directory/files apath.in apath.out
                                                                                                                  #:keep-modify-seconds? #t))))
                                                     lpaths.reachable)
                                           (data-update!      (lambda (data)
                                                                (foldl (lambda (lpath data)
                                                                         (hash-set data lpath (hash-ref lpath=>data.in lpath)))
                                                                       data lpaths.reachable)))
                                           (relations-update! (lambda (rs)
                                                                (foldl (lambda (name desc rs) (hash-set rs name desc))
                                                                       rs names.in descs.relation.in)))
                                           (checkpoint!))
      ((compact!)                          (compact!)))))

(define (database-path             db)              ((wrapped-database-controller db) 'path))
(define (database-metadata         db)              ((wrapped-database-controller db) 'metadata))
(define (database-relation         db name)         ((wrapped-database-controller db) 'relation         name))
(define (database-relation-names   db)              (hash-keys     (hash-ref (database-metadata db) 'relations)))
(define (database-relation-has?    db name)         (hash-has-key? (hash-ref (database-metadata db) 'relations) name))
(define (database-relation-add!    db name . pargs) ((wrapped-database-controller db) 'relation-add!    name
                                                                                      (plist-ref pargs 'attributes)
                                                                                      (plist-ref pargs 'type)
                                                                                      (plist-ref pargs 'source)))
(define (database-relation-remove! db name)         (relation-delete! (database-relation db name)))
(define (database-compact!         db)              ((wrapped-database-controller db) 'compact!))

(define (database-import! db db.in    . relation-names.in)
  ((wrapped-database-controller db) 'import! (wrapped-database-controller db.in) relation-names.in))
(define (database-export! db path.out . relation-names.out)
  (let ((path.out (normalize-path path.out)))
    (when (or (file-exists? path.out) (directory-exists? path.out))
      (error "export destination already exists" path.out))
    (apply database-import! (database path.out) db relation-names.out)))

(define (relation-name                r)              ((wrapped-relation-controller r) 'name))
(define (relation-metadata            r)              ((wrapped-relation-controller r) 'metadata))
(define (relation-copy!               r name.new)     ((wrapped-relation-controller r) 'copy!              name.new))
(define (relation-rename!             r name.new)     ((wrapped-relation-controller r) 'rename!            name.new))
(define (relation-delete!             r)              ((wrapped-relation-controller r) 'delete!))
(define (relation-rename-attributes!  r attrs.new)    ((wrapped-relation-controller r) 'rename-attributes! attrs.new))
(define (relation-index-add!          r . signatures) ((wrapped-relation-controller r) 'index-add!         signatures))
(define (relation-index-remove!       r . signatures) ((wrapped-relation-controller r) 'index-remove!      signatures))
(define (relation-index-dict          r signature)    ((wrapped-relation-controller r) 'index-dict         signature))
(define (relation-domain-dicts        r)              ((wrapped-relation-controller r) 'domain-dicts))
(define (relation-compact!            r)              ((wrapped-relation-controller r) 'compact!))

;; TODO: in-place sorting of multiple columns
(define (nat-tuple<? a b)
  (let loop ((a a) (b b))
    (and (not (null? a))
         (or (< (car a) (car b))
             (and (= (car a) (car b))
                  (loop (cdr a) (cdr b)))))))

(define (sorted-tuples count.tuples columns)
    (pretty-log `(building ,count.tuples tuples from ,(length columns) columns))
    (define tuples (make-vector count.tuples))
    (time/pretty-log
      (let loop ((i 0))
        (when (< i count.tuples)
          (vector-set! tuples i (map (lambda (col) (vector-ref col i))
                                     columns))
          (loop (+ i 1)))))
    (pretty-log '(sorting tuples))
    (time/pretty-log (vector-sort! tuples nat-tuple<?))
    tuples)

(define (min-nat-bytes nat.max) (max (min-bytes nat.max) 1))

(define (column-paths path.table column-ids)
  (map (lambda (i) (path->string
                     (build-path path.table (string-append fn.col "." (number->string i)))))
       column-ids))

(define (ingest-relation-source apath.root lpath.domain-text lpath.table type s.in)
  (define bytes=>id             (make-hash))
  (define size.bytes            0)
  (define count.tuples.initial  0)
  (define apath.domain.value    (path->string (build-path apath.root lpath.domain-text fn.value)))
  (define apath.domain.pos      (path->string (build-path apath.root lpath.domain-text fn.pos)))
  (define apath*.column         (column-paths (build-path apath.root lpath.table) (range (length type))))
  (define apath*.column.initial (map (lambda (p.c) (string-append p.c fnsuffix.initial))
                                     apath*.column))
  ;; TODO: can store (null)/bools/ints too, which will be physically shifted into min/max range
  ;; if min/max range is singleton (guaranteed for null), nothing needs to be stored for that column
  (define type.tuple           (map (lambda (_) 'nat) type))
  (define (insert-bytes! b)
    (or (hash-ref bytes=>id b #f)
        (let ((id (hash-count bytes=>id)))
          (hash-set! bytes=>id b id)
          (set! size.bytes (+ size.bytes (bytes-length b)))
          id)))
  (define row->tuple
    (let ((col->num* (map (lambda (i t.col)
                            (match t.col
                              ('nat    (lambda (x)
                                         (unless (nat?    x) (error "invalid nat"    `(column: ,i) x))
                                         x))
                              ('bytes  (lambda (x)
                                         (unless (bytes?  x) (error "invalid bytes"  `(column: ,i) x))
                                         (insert-bytes!                                      x)))
                              ('string (lambda (x)
                                         (unless (string? x) (error "invalid string" `(column: ,i) x))
                                         (insert-bytes! (string->bytes/utf-8                 x))))
                              ('symbol (lambda (x)
                                         (unless (symbol? x) (error "invalid symbol" `(column: ,i) x))
                                         (insert-bytes! (string->bytes/utf-8 (symbol->string x)))))
                              (_ (error "(currently) unsupported type"                `(column: ,i) t.col))))
                          (range (length type))
                          type)))
      (lambda (row)
        (set! count.tuples.initial (+ count.tuples.initial 1))
        (let loop ((col* row) (col->num* col->num*))
          (match* (col* col->num*)
            (((cons col col*) (cons col->num col->num*)) (cons (col->num col) (loop col* col->num*)))
            (('()             '())                       '())
            ((_               _)                         (error "incorrect number of columns" row type)))))))

  (apply pretty-log '(ingesting rows and writing initial tuple columns) apath*.column.initial)
  (call/files
    '() apath*.column.initial
    (lambda outs.column.initial
      (time/pretty-log
        (s-each (lambda (row) (map encode outs.column.initial type.tuple (row->tuple row)))
                s.in))))

  (define size.pos  (min-nat-bytes size.bytes))
  (define count.ids (hash-count bytes=>id))
  (define id=>id    (make-vector count.ids))
  (pretty-log `(ingested ,count.tuples.initial tuples))
  (pretty-log `(sorting ,(hash-count bytes=>id) strings -- ,size.bytes bytes total))
  (let ((bytes&id*.sorted (time/pretty-log (sort (hash->list bytes=>id)
                                                 (lambda (a b) (bytes<? (car a) (car b)))))))
    (pretty-log '(writing sorted strings to) apath.domain.value
                '(writing positions to) apath.domain.pos)
    (let/files () ((out.bytes.value apath.domain.value)
                   (out.bytes.pos   apath.domain.pos))
      (define (write-pos)
        (write-bytes (nat->bytes size.pos (file-position out.bytes.value)) out.bytes.pos))
      (write-pos)
      (time/pretty-log
        (let loop ((i 0) (b&id* bytes&id*.sorted))
          (unless (null? b&id*)
            (let* ((b&id (car b&id*))
                   (b    (car b&id))
                   (id   (cdr b&id)))
              (write-bytes b out.bytes.value)
              (write-pos)
              (vector-set! id=>id id i)
              (loop (+ i 1) (cdr b&id*))))))))
  (define desc.domain-text
    (hash 'count         count.ids
          'size.position size.pos))
  (write-metadata (build-path apath.root lpath.domain-text fn.metadata.initial) desc.domain-text)

  (pretty-log '(remapping columns))
  (define column-vmms
    (map (lambda (t.col apath.in)
           (define col->col
             (match t.col
               ('nat                        (lambda (n)  n))
               ((or 'bytes 'string 'symbol) (lambda (id) (vector-ref id=>id id)))))
           (define (read-element in) (col->col (decode in 'nat)))
           (match-define (list vec.col min.col max.col)
             (read-column/bounds apath.in count.tuples.initial read-element))
           (pretty-log `(deleting ,apath.in))
           (delete-file apath.in)
           (list vec.col min.col max.col))
         type apath*.column.initial))
  (define columns (map car column-vmms))

  (define tuples  (sorted-tuples count.tuples.initial columns))
  (pretty-log '(deduplicating tuples))
  (define (columns-set! j tuple) (for-each (lambda (vec.col value.col)
                                             (vector-set! vec.col j value.col))
                                           columns tuple))
  (define count.tuples.unique
    (time/pretty-log
      (when (< 0 count.tuples.initial)
        (define t0 (vector-ref tuples 0))
        (columns-set! 0 t0)
        (let loop ((prev t0) (i 1) (j 1))
          (if (< i count.tuples.initial)
            (let ((next (vector-ref tuples i)))
              (cond ((equal? prev next) (loop prev (+ i 1) j))
                    (else (columns-set! j next)
                          (loop next (+ i 1) (+ j 1)))))
            j)))))

  (define column-descriptions
    (map (lambda (t.col vec.col min.col max.col apath.out)
           (define size.col (write-column apath.out count.tuples.unique vec.col min.col max.col))
           (hash 'type  (match t.col
                          ('nat                        'nat)
                          ((or 'bytes 'string 'symbol) 'text))
                 'count count.tuples.unique
                 'size  size.col
                 'min   min.col
                 'max   max.col))
         type columns (map cadr column-vmms) (map caddr column-vmms) apath*.column))
  (define desc.table
    (hash 'direction 'insert
          'domain    (hash 'text lpath.domain-text)
          'count     count.tuples.unique
          'columns   column-descriptions))
  (write-metadata (build-path apath.root lpath.table fn.metadata.initial) desc.table)

  (hash 'domain (hash 'text desc.domain-text)
        'table  desc.table))

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

(define (compact-text-domains apath.domain-text.new apaths.domain-text descs.domain-text)
  (define apath.value  (path->string (build-path apath.domain-text.new fn.value)))
  (define apath.pos    (path->string (build-path apath.domain-text.new fn.pos)))
  (define size.bytes   (sum (map (lambda (apath.in) (file-size (build-path apath.in fn.value)))
                                 apaths.domain-text)))
  (define size.pos     (min-nat-bytes size.bytes))
  (define id=>ids      (map (lambda (desc.in) (make-vector (hash-ref desc.in 'count)))
                            descs.domain-text))
  (define custodian.gs (make-custodian))
  (define gs           (parameterize ((current-custodian custodian.gs))
                         (map (lambda (apath.in desc.in id=>id)
                                (define count       (hash-ref desc.in 'count))
                                (define size.pos    (hash-ref desc.in 'size.position))
                                (define apath.value (build-path apath.in fn.value))
                                (define apath.pos   (build-path apath.in fn.pos))
                                (and (< 0 count)
                                     (let ((in.value (open-input-file apath.value))
                                           (in.pos   (open-input-file apath.pos)))
                                       (define (read-pos) (bytes-nat-ref (read-bytes size.pos in.pos)
                                                                         size.pos
                                                                         0))
                                       (let loop ((id 0) (pos.current (read-pos)))
                                         (let ((pos.next (read-pos)))
                                           (cons (read-bytes (- pos.next pos.current) in.value)
                                                 (lambda (i)
                                                   (vector-set! id=>id id i)
                                                   (and (< (+ id 1) count)
                                                        (loop (+ id 1) pos.next)))))))))
                              apaths.domain-text descs.domain-text id=>ids)))
  (pretty-log '(merging domains) (map cons apaths.domain-text descs.domain-text)
              '(writing merge-sorted strings to) apath.value
              '(writing positions to) apath.pos)
  (define count.ids    (let/files () ((out.value apath.value)
                                      (out.pos   apath.pos))
                         (define (write-pos) (write-bytes (nat->bytes size.pos
                                                                      (file-position out.value))
                                                          out.pos))
                         (write-pos)
                         (time/pretty-log
                           ((multi-merge (lambda (g.0 g.1) (bytes<? (car g.0) (car g.1)))
                                         (filter-not not gs)
                                         not
                                         car
                                         (lambda (g i) ((cdr g) i)))
                            (lambda (bs)
                              (write-bytes bs out.value)
                              (write-pos))))))
  (custodian-shutdown-all custodian.gs)
  ;; replace identity mappings with #f, indicating no remapping is necessary
  (define remappings   (map (lambda (id=>id)
                              (let loop ((i (- (vector-length id=>id) 1)))
                                (and (<= 0 i)
                                     (if (= i (vector-ref id=>id i))
                                       (loop (- i 1))
                                       id=>id))))
                            id=>ids))
  (define desc.domain-text
    (hash 'count         count.ids
          'size.position size.pos))
  (write-metadata (build-path apath.domain-text.new fn.metadata.initial) desc.domain-text)
  (hash 'domain-text desc.domain-text
        'remappings  remappings))

(define ((attrs->signature->ordering attrs) signature)
  (map (lambda (attr) (let ((i (index-of attrs attr)))
                        (if i i (error "invalid signature attribute" attr signature))))
       signature))

(define (normalize-table-index-orderings count.columns orderings)
  (for-each (lambda (ordering) (unless (and (not (null? ordering))
                                            (list? ordering)
                                            (andmap (lambda (i) (and (nat? i) (<= 0 i) (< i count.columns)))
                                                    ordering)
                                            (= (length ordering) (set-count (list->set ordering))))
                                 (error "invalid index" ordering)))
            orderings)
  (remove-duplicates
    (map (lambda (ordering)
           (define len (length ordering))
           (cond ((= (+ len 1) count.columns) (append ordering (set-subtract (range count.columns)
                                                                             ordering)))
                 ((<    len    count.columns) (append ordering '(#t)))
                 (else                                ordering)))
         orderings)))

(define (build-table-indexes apath.root lpath*.index lpath.table desc.table orderings)
  (define apath.root.table  (path->string (build-path apath.root lpath.table)))
  (define apath*.root.index (map (lambda (lpath.index) (path->string (build-path apath.root lpath.index)))
                                 lpath*.index))
  (define count.tuples      (hash-ref desc.table 'count))
  (define desc*.column      (hash-ref desc.table 'columns))
  (define count.columns     (length desc*.column))
  (pretty-log '(building indexes for) apath.root.table orderings desc.table apath*.root.index)
  (define key-used?       (ormap (lambda (ordering) (member #t ordering))
                                 orderings))
  (define column-ids.used (set->list (set-remove (foldl (lambda (ordering col-ids)
                                                          (set-union col-ids (list->set ordering)))
                                                        (set) orderings)
                                                 #t)))
  (define size.pos        (min-nat-bytes (- count.tuples 1)))
  (define i=>desc.col     (make-immutable-hash
                            (append (if key-used?
                                      (list (cons #t (hash 'type  'nat
                                                           'count count.tuples
                                                           'size  size.pos
                                                           'min   0
                                                           'max   (- count.tuples 1))))
                                      '())
                                    (map cons (range count.columns) desc*.column))))
  (define i=>col          (make-immutable-hash
                            (append (if key-used?
                                      (list (cons #t (let ((column.key (make-vector count.tuples)))
                                                       (let loop ((i 0))
                                                         (when (< i count.tuples)
                                                           (vector-set! column.key i i)
                                                           (loop (+ i 1))))
                                                       column.key)))
                                      '())
                                    (map (lambda (i.col apath.in)
                                           (define desc.col (hash-ref i=>desc.col i.col))
                                           (define size.in  (hash-ref desc.col    'size))
                                           (define (read-element in)
                                             (bytes-nat-ref (read-bytes size.in in) size.in 0))
                                           (cons i.col (read-column apath.in count.tuples read-element)))
                                         column-ids.used
                                         (column-paths apath.root.table column-ids.used)))))
  (map (lambda (apath.root.index ordering)
         (pretty-log '(building index) apath.root.index '(with ordering) ordering)
         (define columns.used        (map (lambda (i.col) (hash-ref i=>col      i.col)) ordering))
         (define descs.used          (map (lambda (i.col) (hash-ref i=>desc.col i.col)) ordering))
         (define sizes.used          (map (lambda (desc)  (hash-ref desc        'size)) descs.used))
         (define tuples              (sorted-tuples count.tuples columns.used))
         (define apath*.col.key      (map (lambda (apath.col) (string-append apath.col fnsuffix.key))
                                          (column-paths apath.root.index (range    (length ordering)))))
         (define apath*.col.indirect (map (lambda (apath.col) (string-append apath.col fnsuffix.indirect))
                                          (column-paths apath.root.index (range (- (length ordering) 1)))))
         (pretty-log '(writing index columns))
         (define counts
           (call/files
             '()
             apath*.col.key
             (lambda out*.key
               (call/files
                 '()
                 apath*.col.indirect
                 (lambda out*.indirect
                   (time/pretty-log
                     (when (< 0 count.tuples)
                       (for-each (lambda (out) (write-bytes (nat->bytes size.pos 0) out))
                                 out*.indirect)
                       (let loop.keys ((i*.key        (range (length ordering)))
                                       (size*.key     sizes.used)
                                       (out*.key      out*.key)
                                       (out*.indirect out*.indirect)
                                       (pos*          (make-list (length out*.key) 0))
                                       (start         0)
                                       (end           count.tuples))
                         (let ((i.key (car i*.key)) (i*.key (cdr i*.key)) (size.key (car size*.key)))
                           (define (key-ref i) (list-ref (vector-ref tuples i) i.key))
                           (let ((out.key (car out*.key)))
                             (define (write-key key) (write-bytes (nat->bytes size.key key) out.key))
                             (if (null? i*.key)
                               (let loop.final ((i start))
                                 (cond ((< i end) (write-key (key-ref i))
                                                  (loop.final (+ i 1)))
                                       (else      (list (+ (car pos*) (- i start))))))
                               (let ((out.indirect (car out*.indirect)))
                                 (let loop.key ((pos (car pos*)) (pos* (cdr pos*)) (start start) (end end))
                                   (if (< start end)
                                     (let ((key (key-ref start)))
                                       (write-key key)
                                       (let ((start.new (bisect-next start end (lambda (i) (<= (key-ref i) key)))))
                                         (let ((pos* (loop.keys i*.key
                                                                (cdr size*.key)
                                                                (cdr out*.key)
                                                                (cdr out*.indirect)
                                                                pos*
                                                                start
                                                                start.new)))
                                           (write-bytes (nat->bytes size.pos (car pos*)) out.indirect)
                                           (loop.key (+ pos 1) pos* start.new end))))
                                     (cons pos pos*)))))))))))))))
         (define descs.column.key      (map (lambda (desc count) (hash-set desc 'count count))
                                            descs.used counts))
         (define descs.column.indirect (map (lambda (apath.indirect count.current count.next)
                                              (cond ((= count.current count.next)
                                                     (pretty-log '(deleting identity indirection) apath.indirect)
                                                     (delete-file apath.indirect)
                                                     #f)
                                                    (else (hash 'type  'nat
                                                                'count count.current
                                                                'size  size.pos
                                                                'min   0
                                                                'max   count.next))))
                                            apath*.col.indirect
                                            (reverse (cdr (reverse counts)))
                                            (cdr counts)))
         (define desc.table-index      (hash 'table            lpath.table
                                             'ordering         ordering
                                             'columns.key      descs.column.key
                                             'columns.indirect descs.column.indirect))
         (write-metadata (build-path apath.root.index fn.metadata.initial) desc.table-index)
         desc.table-index)
       apath*.root.index orderings))

(define (read-column/bounds apath.in count read-element)
  (define vec.col (make-vector count))
  (pretty-log `(reading ,count elements and computing min/max from) apath.in)
  (let/files ((in apath.in)) ()
    (time/pretty-log
      (let loop ((i 0) (min.col #f) (max.col 0))
        (cond ((< i count)
               (define value (read-element in))
               (vector-set! vec.col i value)
               (loop (+ i 1)
                     (if min.col (min min.col value) value)
                     (max max.col value)))
              (else (list vec.col (or min.col 0) max.col)))))))

(define (read-column apath.in count read-element)
  (define vec.col (make-vector count))
  (pretty-log `(reading ,count elements from) apath.in)
  (let/files ((in apath.in)) ()
    (time/pretty-log
      (let loop ((i 0))
        (cond ((< i count) (vector-set! vec.col i (read-element in))
                           (loop (+ i 1)))
              (else        vec.col))))))

(define (write-column apath.out count vec.col min.col max.col)
  ;; TODO: consider offseting column values
  ;; - if storing ints
  ;; - if (- max.col min.col) supports a smaller nat size
  (define size.col (min-nat-bytes max.col))
  (pretty-log `(writing ,count elements to) apath.out
              `(nat-size: ,size.col min: ,min.col max: ,max.col))
  (let/files () ((out apath.out))
    (time/pretty-log
      (let loop ((i 0))
        (when (< i count)
          (write-bytes (nat->bytes size.col (vector-ref vec.col i)) out)
          (loop (+ i 1))))))
  size.col)

(define (remap-column?      desc.col   type=>id=>id) (not (not (hash-ref type=>id=>id (hash-ref desc.col 'type) #f))))
(define (remap-table?       desc.table type=>id=>id) (ormap (lambda (desc.col) (remap-column? desc.col type=>id=>id))
                                                            (hash-ref desc.table 'columns)))
(define (remap-table-index? desc.ti    type=>id=>id) (ormap (lambda (desc.col) (remap-column? desc.col type=>id=>id))
                                                            (hash-ref desc.ti 'columns.key)))

(define (remap-column apath.in apath.out desc.in type=>id=>id)
  (pretty-log `(remapping ,apath.in to ,apath.out) desc.in)
  (define type    (hash-ref desc.in 'type))
  (define count   (hash-ref desc.in 'count))
  (define size.in (hash-ref desc.in 'size))
  (define id=>id  (hash-ref type=>id=>id type #f))
  (cond (id=>id (match-define (list vec.col min.col max.col)
                  (read-column/bounds apath.in count
                                      (lambda (in)
                                        (define v.in (bytes-nat-ref (read-bytes size.in in) size.in 0))
                                        (vector-ref id=>id v.in))))
                (define size.col (write-column apath.out (vector-length vec.col) vec.col min.col max.col))
                (hash-set* desc.in 'size size.col 'min min.col 'max max.col))
        (else (pretty-log '(copying verbatim due to identity remapping))
              (time/pretty-log (copy-file apath.in apath.out))
              desc.in)))

(define (remap-table apath.in apath.out desc.table.in desc.domain.new type=>id=>id)
  (pretty-log `(remapping ,apath.in to ,apath.out) desc.table.in)
  (define columns.in     (hash-ref desc.table.in 'columns))
  (define columns.out    (map (lambda (apath.in.col apath.out.col desc.in.col)
                                (remap-column apath.in.col apath.out.col desc.in.col type=>id=>id))
                              (column-paths apath.in  (range (length columns.in)))
                              (column-paths apath.out (range (length columns.in)))
                              columns.in))
  (define desc.table.out (hash 'domain  desc.domain.new
                               'count   (hash-ref desc.table.in 'count)
                               'columns columns.out))
  (write-metadata (build-path apath.out fn.metadata.initial) desc.table.out)
  desc.table.out)

(define (remap-table-index apath.in apath.out desc.table-index.in lpath.table.new type=>id=>id)
  (pretty-log `(remapping ,apath.in to ,apath.out) desc.table-index.in)
  (define (remap fnsuffix desc*.in)
    (map (lambda (apath.in.col apath.out.col desc.in.col)
           (and desc.in.col (remap-column apath.in.col apath.out.col desc.in.col type=>id=>id)))
         (map (lambda (apath.col) (string-append apath.col fnsuffix))
              (column-paths apath.in  (range (length desc*.in))))
         (map (lambda (apath.col) (string-append apath.col fnsuffix))
              (column-paths apath.out (range (length desc*.in))))
         desc*.in))
  (define columns.key.in       (hash-ref desc.table-index.in 'columns.key))
  (define columns.indirect.in  (hash-ref desc.table-index.in 'columns.indirect))
  (define columns.key.out      (remap fnsuffix.key      columns.key.in))
  (define columns.indirect.out (remap fnsuffix.indirect columns.indirect.in))
  (define desc.table-index.out (hash-set* desc.table-index.in
                                          'table            lpath.table.new
                                          'columns.key      columns.key.out
                                          'columns.indirect columns.indirect.out))
  (write-metadata (build-path apath.out fn.metadata.initial) desc.table-index.out)
  desc.table-index.out)


;; TODO: benchmark a design based on streams/iterators for comparison

;; TODO:
;; simple edb-relations vs. idb-relations w/ fixed-point iteration materializations (current + next-delta + now-being-produced)

(define (bisect start end i<)
  (let loop ((start start) (end end))
    (if (<= end start) end
      (let ((i (+ start (quotient (- end start) 2))))
        (if (i< i) (loop (+ 1 i) end) (loop start i))))))

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

;; TODO: remove tables, or at least reduce their scope?
(define table.empty
  (method-lambda
    ((length)                         0)
    ((subtable start.sub (end.sub 0)) table.empty)
    ((columns  start.col (end.col 0)) table.empty)
    ((copy)                           table.empty)
    ((dedup)                          table.empty)
    ((dedup!)                         (void))
    ((sort)                           table.empty)
    ((sort!)                          (void))))

(define (table columns (start 0) (end (vector-length (vector-ref columns 0))))
  (if (= 0 (vector-length columns))
    table.empty
    (let loop ((start start) (end end))
      (define (self-length)         (- end start))
      (define (self-width)          (vector-length columns))
      (define (self-column col)     (vector-ref    columns           col))
      (define (self-ref  col row)   (vector-ref    (self-column col) row))
      (define (self-set! col row v) (vector-set!   (self-column col) row v))
      (define (self-copy)           (table (vector-map (lambda (v.col) (vector-copy v.col start end))
                                                       columns)))
      (define (self-dedup!)
        (define width (self-width))
        (let dedup ((row.prev start) (row (+ start 1)))
          (if (= row end)
            (when (< (+ row.prev 1) end)
              (set! end (+ row.prev 1)))
            (if (let duplicate? ((col 0))
                  (or (= col width)
                      (let ((v.col (self-column col)))
                        (and (equal? (vector-ref v.col row.prev)
                                     (vector-ref v.col row))
                             (duplicate? (+ col 1))))))
              (dedup row.prev (+ row 1))
              (let ((row.prev (+ row.prev 1)))
                (unless (= row.prev row)
                  (let swap! ((col 0))
                    (when (< col width)
                      (let ((v.col (self-column col)))
                        (vector-set! v.col row.prev (vector-ref v.col row))
                        (swap! (+ col 1))))))
                (dedup row.prev (+ row 1)))))))
      (if (<= end start)
        table.empty
        (method-lambda
          ((width)                                      (self-width))
          ((length)                                     (self-length))
          ((ref  col row)                               (self-ref  col row))
          ((set! col row v)                             (self-set! col row v))
          ((subtable start.sub (end.sub (self-length))) (loop (+ start start.sub) (+ start end.sub)))
          ((columns  start.col (end.col (self-width)))  (table (vector-copy columns start.col end.col) start end))
          ((copy)                                       (self-copy))
          ((dedup)                                      (let ((t (self-copy)))
                                                          (t 'dedup!)
                                                          t))
          ((dedup!)                                     (self-dedup!))
          ;; TODO:
          ;((sort ))
          ;((sort! ))
          )))))

(define (table-width    t)                              (t 'width))
(define (table-length   t)                              (t 'length))
(define (table-ref      t col row)                      (t 'ref      col row))
(define (table-set!     t col row v)                    (t 'set!     col row v))
(define (table-subtable t start (end (table-length t))) (t 'subtable start end))
(define (table-columns  t start (end (table-width  t))) (t 'columns  start end))
(define (table-dedup    t)                              (t 'dedup))
(define (table-dedup!   t)                              (t 'dedup!))
(define (table-sort     t)                              (t 'sort))
(define (table-sort!    t)                              (t 'sort!))

(define ((column:const     c)                      _) c)
(define ((column:vector    rows)                   i) (vector-ref rows i))
(define ((column:table     columns)                i) (map (lambda (col) (col i)) columns))
(define ((column:indirect  column.pos column)      i) (column (column.pos i)))
(define ((column:interval  column.pos interval->x) i) (interval->x (column.pos i) (column.pos (+ i 1))))
(define ((column:bytes:nat bs size)                i) (bytes-nat-ref bs size (* i size)))

(define (column:port                     in type) (let ((size.type (sizeof type (void))))
                                                    (lambda (i)
                                                      (file-position in (* i size.type))
                                                      (decode        in type))))
(define (column:port-indirect column.pos in type)   (lambda (i)
                                                      (file-position in (column.pos i))
                                                      (decode        in type)))
(define (column:port-string   column.pos in)      (column:interval
                                                    column.pos
                                                    (lambda (pos.0 pos.1)
                                                      (file-position in pos.0)
                                                      (bytes->string/utf-8 (read-bytes (- pos.1 pos.0) in)))))

(define ((interval->dict:ordered i->key i->value) start end) (dict:ordered i->key i->value start end))

;; TODO: not needed?
;(define (dict:ordered:trie start end)
  ;)


(define dict.empty
  (method-lambda
    ((count)                   0)
    ((=/= _)                   dict.empty)
    ((==  _)                   dict.empty)
    ((<=  _)                   dict.empty)
    ((<   _)                   dict.empty)
    ((>=  _)                   dict.empty)
    ((>   _)                   dict.empty)
    ((bstr-prefix   _)         dict.empty)
    ((bstr-contains _)         dict.empty)
    ((has-key?      _)         #f)
    ((ref _ k.found k.missing) (k.missing))
    ((enumerator/2)            (lambda _ (void)))
    ((enumerator)              (lambda _ (void)))))

(define (dict:ordered i->key i->value start end)
  (let loop ((start start) (end end))
    (define self
      (if (<= end start)
        dict.empty
        (method-lambda
          ((pop)       (loop (+ start 1) end))
          ((count)     (- end start))
          ((top)       (i->value start))
          ((max)       (i->key   (- end 1)))
          ((min)       (i->key   start))
          ((>= key)    (loop (bisect-next start end (lambda (i) (any<?  (i->key i) key))) end))
          ((>  key)    (loop (bisect-next start end (lambda (i) (any<=? (i->key i) key))) end))
          ((<= key)    (loop start (bisect-prev start end (lambda (i) (any<?  key (i->key i))))))
          ((<  key)    (loop start (bisect-prev start end (lambda (i) (any<=? key (i->key i))))))
          ((== key)    ((self '>= key) '<= key))
          ((has-key? key)              (let ((self (self '>= key)))
                                         (and (< 0 (self 'count))
                                              (equal? (self 'min) key))))
          ((ref key k.found k.missing) (let ((self (self '>= key)))
                                         (if (or (= 0 (self 'count))
                                                 (not (equal? (self 'min) key)))
                                           (k.missing)
                                           (k.found (self 'top)))))
          ((enumerator/2)              (lambda (yield)
                                         (let loop ((i start))
                                           (when (< i end)
                                             (yield (i->key i) (i->value i))
                                             (loop (+ i 1))))))
          ((enumerator)                (lambda (yield)
                                         (let loop ((i start))
                                           (when (< i end)
                                             (yield (i->key i))
                                             (loop (+ i 1)))))))))
    self))

(define (dict:ordered:vector rows (t->key (lambda (t) t)) (start 0) (end (vector-length rows)))
  (define (i->value i) (vector-ref rows i))
  (define (i->key   i) (t->key (i->value i)))
  (dict:ordered i->key i->value start end))

(define (dict:hash k=>t)
  (let loop ((k=>t k=>t))
    (if (= (hash-count k=>t) 0)
      dict.empty
      (method-lambda
        ((count)                     (hash-count k=>t))
        ((=/= key)                   (loop (hash-remove k=>t key)))
        ((==  key)                   (if (hash-has-key? k=>t key)
                                       (loop (hash key (hash-ref k=>t key)))
                                       dict.empty))
        ((has-key? key)              (hash-has-key? k=>t key))
        ((ref key k.found k.missing) (if (hash-has-key? k=>t key)
                                       (k.found (hash-ref k=>t key))
                                       (k.missing)))
        ((enumerator/2)              (hash->enumerator/2 k=>t))
        ((enumerator)                (lambda (yield)
                                       (for ((k (in-hash-keys k=>t)))
                                         (yield k))))))))

(define ((merge-join A B) yield)
  (when (and (< 0 (A 'count))
             (< 0 (B 'count)))
    (let loop ((A   A)
               (k.A (A 'min))
               (B   B)
               (k.B (B 'min)))
      (case (compare-any k.A k.B)
        ((-1) (let ((A (A '>= k.B)))
                (when (< 0 (A 'count))
                  (loop A (A 'min) B k.B))))
        (( 1) (let ((B (B '>= k.A)))
                (when (< 0 (B 'count))
                  (loop A k.A B (B 'min)))))
        (else (let ((t.A (A 'top))
                    (t.B (B 'top))
                    (A   (A 'pop))
                    (B   (B 'pop)))
                (yield k.A t.A t.B)
                (when (and (< 0 (A 'count))
                           (< 0 (B 'count)))
                  (loop A (A 'min) B (B 'min)))))))))

(define ((merge-antijoin A B) yield)
  ((dict-antijoin-ordered (A 'enumerator/2) B) yield))

(define ((merge-key-union A B) yield)
  ((dict-key-union-ordered (A 'enumerator) B) yield))

(define (group-fold->hash en v.0 f)
  (define k=>v (hash))
  (en (lambda (k v) (set! k=>v (hash-update k=>v k
                                            (lambda (v.current) (f v v.current))
                                            v.0))))
  k=>v)

(define ((group-fold en v.0 f) yield)
  ((hash->enumerator/2 (group-fold->hash en v.0 f)) yield))

(define ((group-fold-ordered en v.0 f) yield)
  (let ((first?    #t)
        (k.current #f)
        (v.current v.0))
    (en (lambda (k v)
          (cond (first?               (set! first?    #f)
                                      (set! k.current k)
                                      (set! v.current (f v v.0)))
                ((equal? k k.current) (set! v.current (f v v.current)))
                (else                 (yield k.current v.current)
                                      (set! k.current k)
                                      (set! v.current (f v v.0))))))
    (unless first?
      (yield k.current v.current))))

(define ((enumerator-dedup en) yield)
  (define first?     #t)
  (define t.previous #f)
  (en (lambda (t)
        (cond (first?                      (set! first?     #f)
                                           (set! t.previous t)
                                           (yield t))
              ((not (equal? t t.previous)) (set! t.previous t)
                                           (yield t))))))

(define ((enumerator-project en f) yield)
  (en (lambda args (apply f yield args))))

(define ((enumerator-filter en ?) yield)
  (en (lambda (t) (when (? t) (yield t)))))

(define ((enumerator-sort en <?) yield)
  ((list->enumerator (sort (enumerator->rlist en) <?)) yield))

(define (enumerator->dict:ordered:vector-flat en (t->key (lambda (t) t)))
  (dict:ordered:vector
    (enumerator->vector (enumerator-dedup (enumerator-sort en any<?)))
    t->key))

(define (enumerator->dict:ordered:vector-group en t->key)
  (dict:ordered:vector
    (enumerator->vector
      (enumerator-project
        (group-fold-ordered
          (enumerator-project (enumerator-dedup (enumerator-sort en any<?))
                              (lambda (yield t) (yield (t->key t) t)))
          '() cons)
        (lambda (yield _ ts.reversed) (yield (reverse ts.reversed)))))
    (lambda (ts) (t->key (car ts)))))

(define ((hash-join en en.hash) yield)
  ((dict-join-unordered en (dict:hash (group-fold->hash en.hash '() cons)))
   (lambda (k t ts.hash)
     (for ((t.hash (in-list (reverse ts.hash))))  ; is this reversal necessary?
       (yield k t t.hash)))))

(define ((hash-antijoin en en.hash) yield)
  ((dict-antijoin-unordered en (dict:hash (group-fold->hash en.hash (void) (lambda _ (void)))))
   yield))

(define ((dict-join-unordered en d.index) yield)
  (when (< 0 (d.index 'count))
    (en (lambda (k v) (d.index 'ref k
                               (lambda (v.index) (yield k v v.index))
                               (lambda ()        (void)))))))

(define ((dict-join-ordered en.ordered d.index) yield)
  (when (< 0 (d.index 'count))
    (en.ordered (lambda (k v)
                  (set! d.index (d.index '>= k))
                  (d.index 'ref k
                           (lambda (v.index) (yield k v v.index))
                           (lambda ()        (void)))))))

(define ((dict-antijoin-unordered en d.index) yield)
  (en (if (= 0 (d.index 'count))
        yield
        (lambda (k v) (unless (d.index 'has-key? k)
                        (yield k v))))))

(define ((dict-antijoin-ordered en.ordered d.index) yield)
  (en.ordered (if (= 0 (d.index 'count))
                yield
                (lambda (k v)
                  (set! d.index (d.index '>= k))
                  (unless (d.index 'has-key? k)
                    (yield k v))))))

(define ((dict-subtract-unordered en d.index) yield)
  (en (if (= 0 (d.index 'count))
        yield
        (lambda (k) (unless (d.index 'has-key? k)
                      (yield k))))))

(define ((dict-subtract-ordered en.ordered d.index) yield)
  (en.ordered (if (= 0 (d.index 'count))
                yield
                (lambda (k)
                  (set! d.index (d.index '>= k))
                  (unless (d.index 'has-key? k)
                    (yield k))))))

(define ((hash-key-union en en.hash) yield)
  (define d.index (dict:hash (let ((k=> (hash)))
                               (en.hash (lambda (k) (set! k=> (hash-set k=> k (void)))))
                               k=>)))
  ((dict-key-union-unordered en d.index) yield))

(define ((dict-key-union-unordered en d.index) yield)
  ((dict-subtract-unordered en d.index) yield)
  ((d.index 'enumerator)                yield))

(define ((dict-key-union-ordered en.ordered d.index) yield)
  (en.ordered (if (= 0 (d.index 'count))
                yield
                (lambda (k)
                  (let loop ()
                    (if (= 0 (d.index 'count))
                      (yield k)
                      (let ((k.d (d.index 'min)))
                        (case (compare-any k k.d)
                          ((-1) (yield k))
                          (( 1) (set! d.index (d.index 'pop))
                                (yield k.d)
                                (loop))
                          (else (set! d.index (d.index 'pop))
                                (yield k))))))))))


;; TODO: computing fixed points?
;; - iteration: changed?
;; - relation/variable: stable, next, to-add

;; TODO: multiway-joins
;; - extend-with
;; - extend-anti
;; - filter-with
;; - filter-anti

(module+ test
  (require racket/pretty)

  (define (test.0 yield.0)
    (define (yield . args)
      (pretty-write `(yielding: . ,args))
      (apply yield.0 args))
    (yield 0 1)
    (yield 0 2)
    (yield 0 3)
    (yield 1 1)
    (yield 1 2)
    (yield 5 2)
    (yield 5 7))

  (define test.1 (enumerator->enumerator/2
                   (vector->enumerator '#((0 . 1)
                                          (0 . 2)
                                          (0 . 3)
                                          (1 . 1)
                                          (1 . 2)
                                          (5 . 2)
                                          (5 . 7)))))

  (displayln 'group-fold.0)
  ((group-fold test.0 0 +) (lambda (k v) (pretty-write (list k v))))

  (displayln 'group-fold-ordered.0)
  ((group-fold-ordered test.0 0 +) (lambda (k v) (pretty-write (list k v))))

  (displayln 'group-fold.1)
  ((group-fold test.1 0 +) (lambda (k v) (pretty-write (list k v))))

  (displayln 'group-fold-ordered.1)
  ((group-fold-ordered test.1 0 +) (lambda (k v) (pretty-write (list k v))))

  (displayln 'hash-join)
  ((hash-join
     (enumerator->enumerator/2 (list->enumerator '((5 . 6) (10 . 17) (8 . 33) (1 . 5) (0 . 7) (18 . 3))))
     (enumerator->enumerator/2 (list->enumerator '((7 . 61) (10 . 20) (18 . 33) (11 . 5) (0 . 77) (8 . 3)))))
   (lambda (k a b) (pretty-write (list k a b))))

  (displayln 'merge-join)
  ((merge-join
     (enumerator->dict:ordered:vector-group
       (list->enumerator '((5 . 6) (10 . 17) (8 . 33) (1 . 5) (0 . 7) (18 . 3)))
       car)
     (enumerator->dict:ordered:vector-group
       (list->enumerator '((7 . 61) (10 . 20) (18 . 33) (11 . 5) (0 . 77) (8 . 3)))
       car))
   (lambda (k a b) (pretty-write (list k a b))))

  (displayln 'hash-key-union)
  ((hash-key-union
     (list->enumerator (map car '((5 . 6) (10 . 17) (8 . 33) (1 . 5) (0 . 7) (18 . 3))))
     (list->enumerator (map car '((7 . 61) (10 . 20) (18 . 33) (11 . 5) (0 . 77) (8 . 3)))))
   pretty-write)

  (displayln 'merge-key-union)
  ((merge-key-union
     (enumerator->dict:ordered:vector-group
       (list->enumerator '((5 . 6) (10 . 17) (8 . 33) (1 . 5) (0 . 7) (18 . 3)))
       car)
     (enumerator->dict:ordered:vector-group
       (list->enumerator '((7 . 61) (10 . 20) (18 . 33) (11 . 5) (0 . 77) (8 . 3)))
       car))
   pretty-write)

  (displayln 'hash-antijoin)
  ((hash-antijoin
     (enumerator->enumerator/2 (list->enumerator '((5 . 6) (10 . 17) (8 . 33) (1 . 5) (0 . 7) (18 . 3))))
     (enumerator->enumerator/2 (list->enumerator '((7 . 61) (10 . 20) (18 . 33) (11 . 5) (0 . 77) (8 . 3)))))
   (lambda (k v) (pretty-write (list k v))))

  (displayln 'merge-antijoin)
  ((merge-antijoin
     (enumerator->dict:ordered:vector-group
       (list->enumerator '((5 . 6) (10 . 17) (8 . 33) (1 . 5) (0 . 7) (18 . 3)))
       car)
     (enumerator->dict:ordered:vector-group
       (list->enumerator '((7 . 61) (10 . 20) (18 . 33) (11 . 5) (0 . 77) (8 . 3)))
       car))
   (lambda (k v) (pretty-write (list k v))))

  (displayln 'table)
  (let ((t (table (vector (vector  0  0  0  0  1  1  1  2  2  3  3  3  3  3  4)
                          (vector 'a 'a 'a 'b 'a 'a 'b 'a 'a 'a 'b 'b 'c 'c 'a)
                          (vector  0  1  1  1  0  1  1  2  2  1  1  1  1  1  7)))))
    (for ((col (in-range (table-width t))))
      (for ((row (in-range (table-length t))))
        (printf "~s " (table-ref t col row)))
      (newline))

    (displayln 'table-dedup!)
    (table-dedup! t)

    (for ((col (in-range (table-width t))))
      (for ((row (in-range (table-length t))))
        (printf "~s " (table-ref t col row)))
      (newline)))
  )

;; TODO:
;join-many, need attribute order

;extend-with/anti
;filter-with/anti

;;; TODO: these are analogous to operators: a set of tuples flowing through a particular program point
;(struct idb-relation (done current new) #:prefab)

;;; exponential search aka one-sided binary search
;(define (join-tables t.0 t.1 k)
  ;;; (k key v.0 v.1)
;;key.0 key.1
  ;)

;;; NOTE: this semi-naive join does not correctly generalize to more than 2 relations
;(define (join-idb-relations r.0 r.1 logic)
  ;(append
    ;(map (lambda (t.1)
           ;(join-tables (idb-relation-current r.0)
                        ;t.1
                        ;;; TODO: no need to eta expand
                        ;(lambda (k v.0 v.1)
                          ;(logic k v.0 v.1))))
         ;(idb-relation-done r.1))
    ;(map (lambda (t.0)
           ;(join-tables t.0
                        ;(idb-relation-current r.1)
                        ;(lambda (k v.0 v.1)
                          ;(logic k v.0 v.1))))
         ;(idb-relation-done r.0))
    ;(join-tables (idb-relation-current r.0)
                 ;(idb-relation-current r.1)
                 ;(lambda (k v.0 v.1)
                   ;(logic k v.0 v.1)))))

;(define (idb-relation-step r)
  ;(match-define (idb-relation done current new) r)
  ;(let* ((done    (let loop ((done done) (current current))
                    ;(match done
                      ;('()             (list current))
                      ;((cons top done) (if (<= (vector-length top)
                                               ;(* 2 (vector-length current)))
                                         ;(loop done (table-union current top))
                                         ;(cons current (cons top done)))))))
         ;(current (foldl (lambda (t.done next)
                           ;(filter-not (lambda (tuple) (table-member? t.done)) next))
                         ;(vector->list (match new
                                         ;('()             (table '()))
                                         ;((cons next new) (foldl table-union next new))))
                         ;done)))
    ;(idb-relation done current '())))
