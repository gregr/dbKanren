#lang racket/base
(require "../dbk/io.rkt"
         "../dbk/data.rkt"
         "../dbk/stream.rkt"
         racket/runtime-path)

(define-runtime-path path.here ".")

(define db (database (path->string (simplify-path (build-path path.here "example-db")))))

(database-relation-add!
  db '(example cprop)
  'attributes '(curie  key    value)
  'type       '(string string string)
  'source     (in:file "example/example.nodeprop.tsv" 'header '(":ID" "propname" "value")))

(database-relation-add!
  db '(example edge)
  'attributes '(eid subject object)
  'type       '(nat string  string)
  'source     (s-map (lambda (row) (cons (string->number (car row)) (cdr row)))
                     (in:file "example/example.edge.tsv"     'header '(":ID" ":START" ":END"))))

(database-relation-add!
  db '(example eprop)
  'attributes '(eid key    value)
  'type       '(nat string string)
  'source     (s-map (lambda (row) (cons (string->number (car row)) (cdr row)))
                     (in:file "example/example.edgeprop.tsv" 'header '(":ID" "propname" "value"))))

(define cprop (database-relation '(example cprop)))
(define edge  (database-relation '(example edge)))
(define eprop (database-relation '(example eprop)))

(database-compact! db)

(relation-index-add! cprop
                     '(curie key)
                     '(key value))
(relation-index-add! edge
                     '(eid)
                     '(subject object)
                     '(object subject))
(relation-index-add! eprop
                     '(eid key)
                     '(key value))
