#lang racket/base
(require "../dbk/io.rkt"
         "../dbk/data.rkt"
         racket/match)

(define db (database "path/to/example/db"))

(define cprop (database-relation-add!
                db '(example cprop)
                'attributes '(curie  key    value)
                'type       '(string string string)
                'source     (in:file "example/example.nodeprop.tsv" 'header '(":ID" "propname" "value"))))
(define eprop (database-relation-add!
                db '(example eprop)
                'attributes '(eid key    value)
                'type       '(nat string string)
                'source     (in:file "example/example.edgeprop.tsv" 'header '(":ID" "propname" "value"))))
(define edge  (database-relation-add!
                db '(example edge)
                'attributes '(eid subject object)
                'type       '(nat string  string)
                'source     (in:file "example/example.edge.tsv"     'header '(":ID" ":START" ":END"))))

(database-compact! db)

(relation-index-add! cprop
                     '(curie key)
                     '(key value))
(relation-index-add! eprop
                     '(eid key)
                     '(key value))
(relation-index-add! edge
                     '(eid)
                     '(subject object)
                     '(object subject))