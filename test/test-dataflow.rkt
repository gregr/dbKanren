#lang racket/base
(require
  "../dbk.rkt"
  "../dbk/data.rkt"
  "../dbk/enumerator.rkt"
  racket/file
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

(define (lpath.domain-text->dict.id=>string lpath.dt)
  (define desc.dt     (hash-ref data lpath.dt))
  (define size.pos    (hash-ref desc.dt 'size.position))
  (define count       (hash-ref desc.dt 'count))
  (define apath.dt    (data-path lpath.dt))
  (define col.pos     (column:port (open-input-file (build-path apath.dt "position")) `#(nat ,size.pos)))
  ;; Optionally load positions into memory instead; does not seem to impact performance, though
  ;(define col.pos     (time (column:bytes:nat (file->bytes (build-path apath.dt "position")) size.pos)))
  (define id->str     (column:port-string col.pos (open-input-file (build-path apath.dt "value"))))
  (dict:ordered (lambda (id) id) id->str 0 count))

(define (lpath.domain-text->dict.string=>id lpath.dt)
  (define desc.dt     (hash-ref data lpath.dt))
  (define size.pos    (hash-ref desc.dt 'size.position))
  (define count       (hash-ref desc.dt 'count))
  (define apath.dt    (data-path lpath.dt))
  (define col.pos     (column:port (open-input-file (build-path apath.dt "position")) `#(nat ,size.pos)))
  (define i->key      (column:port-string col.pos (open-input-file (build-path apath.dt "value"))))
  (dict:ordered i->key (lambda (i) i) 0 count))

(define (dict-select d key) (d 'ref key (lambda (v) v) (lambda () (error "dict ref failed" key))))

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

(define db      (database path.db))
(define r.cprop (database-relation db '(rtx-kg2 cprop)))
(define r.edge  (database-relation db '(rtx-kg2 edge)))
(define r.eprop (database-relation db '(rtx-kg2 eprop)))

(define dict.eprop.eid.value.key     (relation-index-dict r.eprop '(key value eid)))
(define dict.edge.subject.eid.object (relation-index-dict r.edge  '(object eid subject)))
(define dict.cprop.value.key.curie   (relation-index-dict r.cprop '(curie key value)))
(define dict.id=>string              (lpath.domain-text->dict.id=>string lpath.domain-text))
(define dict.string=>id              (lpath.domain-text->dict.string=>id lpath.domain-text))

(define (string->id str) (dict-select dict.string=>id str))
(define (id->string id)  (dict-select dict.id=>string id))

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
             ((merge-join dict.cprop.name dict.id=>string)
              (lambda (name.id __ name)
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
