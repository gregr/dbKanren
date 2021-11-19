#lang racket/base
(require
  "../dbk.rkt"
  "../dbk/data.rkt"
  "../dbk/enumerator.rkt"
  racket/file
  racket/list
  racket/pretty
  racket/runtime-path
  racket/set)

(define-relation/table (cprop curie key value)   'path "rtx2/20210204/cprop")
(define-relation/table (edge  id subject object) 'path "rtx2/20210204/edge")
(define-relation/table (eprop id key value)      'path "rtx2/20210204/eprop")

(define-runtime-path path.here ".")
(define path.db      (path->string (build-path path.here "rtx-kg2_20210204.db")))
(define path.db.data (path->string (build-path path.db   "current")))
(define (data-path lpath) (path->string (build-path path.db.data lpath)))

(define metadata (call-with-input-file (build-path path.db "metadata.scm") read))
(define data     (hash-ref metadata 'data))

(define (lpath.table-index->dict lpath.ti)
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

(define (lpath.domain-text->id->string lpath.dt)
  (define desc.dt     (hash-ref data lpath.dt))
  (define size.pos    (hash-ref desc.dt 'size.position))
  (define apath.dt    (data-path lpath.dt))
  (define col.pos     (column:port (open-input-file (build-path apath.dt "position")) `#(nat ,size.pos)))
  ;; Optionally load positions into memory instead; does not seem to impact performance, though
  ;(define col.pos     (time (column:bytes:nat (file->bytes (build-path apath.dt "position")) size.pos)))
  (column:port-string col.pos (open-input-file (build-path apath.dt "value"))))

(define (lpath.domain-text->string->id lpath.dt)
  (define desc.dt     (hash-ref data lpath.dt))
  (define size.pos    (hash-ref desc.dt 'size.position))
  (define count       (hash-ref desc.dt 'count))
  (define apath.dt    (data-path lpath.dt))
  (define col.pos     (column:port (open-input-file (build-path apath.dt "position")) `#(nat ,size.pos)))
  (define i->key      (column:port-string col.pos (open-input-file (build-path apath.dt "value"))))
  (define dict        (dict:ordered i->key (lambda (i) i) 0 count))
  (lambda (str) (dict 'ref str
                      (lambda (id.found) id.found)
                      (lambda () (error "missing string" str)))))

(define lpath.index.eprop "table-index-1637181430-1") ; key value eid
;│   │   ├── [204M Nov 17 15:42]  column.0.indirect
;│   │   ├── [204M Nov 17 15:42]  column.0.key
;│   │   ├── [2.0G Nov 17 15:42]  column.1.key
;│   │   ├── [2.0G Nov 17 15:42]  column.2.key
(define lpath.index.edge  "table-index-1637195642-0") ; object eid subject
;│   │   ├── [ 21M Nov 17 19:35]  column.0.indirect
;│   │   ├── [ 21M Nov 17 19:35]  column.0.key
;│   │   ├── [204M Nov 17 19:35]  column.1.key
;│   │   ├── [204M Nov 17 19:35]  column.2.key
(define lpath.index.cprop "table-index-1637181128-0") ; curie key value
;│   │   ├── [ 40M Nov 17 15:33]  column.0.indirect
;│   │   ├── [ 40M Nov 17 15:33]  column.0.key
;│   │   ├── [375M Nov 17 15:33]  column.1.key
;│   │   ├── [375M Nov 17 15:33]  column.2.key
(define lpath.domain-text "domain-text-1637180795-0")
;│   │   ├── [309M Nov 17 15:28]  position
;│   │   └── [ 22G Nov 17 15:28]  value

(define id->string                   (lpath.domain-text->id->string lpath.domain-text))
(define string->id                   (lpath.domain-text->string->id lpath.domain-text))
(define dict.eprop.eid.value.key     (lpath.table-index->dict lpath.index.eprop))
(define dict.edge.subject.eid.object (lpath.table-index->dict lpath.index.edge))
(define dict.cprop.value.key.curie   (lpath.table-index->dict lpath.index.cprop))

(define (dict-select d key) (d 'ref key (lambda (v) v) (lambda () (error "dict ref failed" key))))

(define (benchmark-find-treatments curie.target)
  (define (run-query yield)
    (define curie.nausea.id       (string->id curie.target))
    (define ekey.predicate.id     (string->id "predicate"))
    (define evalue.treats.id      (string->id "biolink:treats"))
    (define ckey.category.id      (string->id "category"))
    (define ckey.name.id          (string->id "name"))
    (define dict.eprop.eid.value  (dict-select dict.eprop.eid.value.key     ekey.predicate.id))
    (define dict.eprop.eid        (dict-select dict.eprop.eid.value         evalue.treats.id))
    (define dict.edge.subject.eid (dict-select dict.edge.subject.eid.object curie.nausea.id))
    ((merge-join dict.eprop.eid dict.edge.subject.eid)
     (lambda (eid __ dict.edge.subject)
       ((merge-join dict.edge.subject dict.cprop.value.key.curie)
        (lambda (subject.id __ dict.cprop.value.key)
          (define subject (id->string subject.id))
          (define dict.cprop.category (dict-select dict.cprop.value.key ckey.category.id))
          (define dict.cprop.name     (dict-select dict.cprop.value.key ckey.name.id))
          ((dict.cprop.category 'enumerator)
           (lambda (category.id)
             (define category (id->string category.id))
             ((dict.cprop.name 'enumerator)
              (lambda (name.id)
                (define name (id->string name.id))
                (yield (list subject category name)))))))))))
  ;; Some nausea timings
  ;; cpu time: 1485 real time: 1610 gc time: 19
  ;; cpu time: 1539 real time: 1557 gc time: 15
  ;; cpu time: 1538 real time: 1556 gc time: 24
  (define results.old (time (run* (s cat name)
                              (fresh (eid)
                                (edge eid s curie.target)
                                (cprop s "category" cat)
                                (cprop s "name" name)
                                (eprop eid "predicate" "biolink:treats")))))
  ;; Some nausea timings
  ;; cpu time: 27 real time: 27 gc time: 0
  ;; cpu time: 31 real time: 31 gc time: 0
  ;; cpu time: 30 real time: 31 gc time: 0
  (define results.new (time (enumerator->rlist run-query)))
  ;; 149 results
  (pretty-write `(old:    ,(length results.old) ,results.old))
  (pretty-write `(new:    ,(length results.new) ,results.new))
  (pretty-write `(equal?: ,(equal? (list->set results.old) (list->set results.new)))))
