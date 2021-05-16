#lang racket/base
(provide path->format file-stats s-pop-header produce/pop-header
         port-produce port-consume
         (struct-out producer)
         out:transform out:procedure out:port out:file
         in:transform in:procedure in:port in:file
         in:stream in:pop-header
         json->scm scm->json jsexpr->scm scm->jsexpr
         jsonl:read jsonl:write json:read json:write
         tsv:read tsv:write)
(require "codec.rkt" "misc.rkt" "stream.rkt"
         json racket/list racket/match racket/port racket/string)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utilities
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (path->format path.0)
  (define path (if (path? path.0) (path->string path.0) path.0))
  (case (last (string-split path "." #:trim? #f))
    (("bscm")  'bscm)
    (("scm")   'scm)
    (("tsv")   'tsv)
    (("csv")   'csv)
    (("json")  'json)
    (("jsonl") 'jsonl)
    (else      #f)))

(define (file-stats path)
  (and (file-exists? path)
       (hash 'size          (file-size path)
             'time.modified (file-or-directory-modify-seconds path))))

(define (s-pop-header ? expected s.0)
  (define s (s-force s.0))
  (unless (pair? s)   (error "missing header:" 'expected: expected))
  (unless (? (car s)) (error "invalid header:" 'found: (car s) 'expected: expected))
  (cdr s))

(define (produce/pop-header produce header)
  (define ((produce/pop ? h)) (s-pop-header ? h (produce)))
  (match header
    (#f produce)
    (#t (produce/pop (lambda (x) #t)           #t))
    (h  (produce/pop (lambda (x) (equal? x h)) h))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Producers and consumers
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(struct producer (produce source-info) #:prefab)

(define (in:procedure proc) (producer proc          'procedure))
(define (in:stream s)       (producer (lambda () s) 'stream))

(define (in:transform p f)
  (cond (f (match-define (producer produce source-info) p)
           (producer (lambda () (f (produce))) source-info))
        (else p)))

(define (in:pop-header p header)
  (match-define (producer produce source-info) p)
  (producer (produce/pop-header produce header) source-info))

(define (in:port p . pargs)
  (define kwargs  (make-immutable-hash (plist->alist pargs)))
  (define (produce) (port-produce p
                                  (hash-ref kwargs 'close? #f)
                                  (hash-ref kwargs 'format)
                                  (hash-ref kwargs 'type #f)))
  (in:transform (producer (produce/pop-header produce
                                              (hash-ref kwargs 'header #f))
                          'port)
                (hash-ref kwargs 'transform #f)))

(define (in:file path.0 . pargs)
  (define path   (if (path? path.0) (path->string path.0) path.0))
  (define kwargs (make-immutable-hash (plist->alist pargs)))
  (define format (hash-ref kwargs 'format (path->format path)))
  (unless format (error "unknown format:" path))
  (define stats  (file-stats path))
  (unless stats  (error "missing input file:" 'path: path))
  (define (produce) (let ((in (open-input-file path)))
                      (port-produce in (lambda () (close-input-port in))
                                    format
                                    (hash-ref kwargs 'type #f))))
  (in:transform (producer (produce/pop-header produce
                                              (hash-ref kwargs 'header #f))
                          `(file ,stats))
                (hash-ref kwargs 'transform #f)))

(define (out:procedure proc) proc)
(define (out:transform c f)  (lambda (s) (c (f s))))

(define (out:port p . pargs)
  (define kwargs  (make-immutable-hash (plist->alist pargs)))
  (lambda (s) (port-consume p
                            (hash-ref kwargs 'close? #f)
                            (hash-ref kwargs 'format)
                            (hash-ref kwargs 'type #f)
                            s)))

(define (out:file path.0 . pargs)
  (define path   (if (path? path.0) (path->string path.0) path.0))
  (define kwargs (make-immutable-hash (plist->alist pargs)))
  (define exists (hash-ref kwargs 'exists 'error))
  (define format (hash-ref kwargs 'format (path->format path)))
  (unless format (error "unknown format:" path))
  (lambda (s) (let ((out (open-output-file path #:exists exists)))
                (port-consume out (lambda () (close-output-port out))
                              format
                              (hash-ref kwargs 'type #f)
                              s))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Port management
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (port-produce in close? format type)
  (case format
    ((json) (define data (vector->list (json:read in)))
            (when close? (close?))
            data)
    (else   (define get
              (case format
                ((bscm)  (lambda () (if (eof-object? (peek-byte in)) eof (decode in type))))
                ((scm)   (lambda () (read in)))
                ((tsv)   (lambda () (tsv:read in)))
                ;; TODO:
                ;((csv)   )
                ((jsonl) (lambda () (jsonl:read in)))
                (else    (error "unsupported input format:" format))))
            (let loop ()
              (lambda ()
                (define datum (get))
                (cond ((eof-object? datum) (when close? (close?))
                                           '())
                      (else                (cons datum (loop)))))))))

(define (port-consume out close? format type s)
  (case format
    ((json) (json:write out (list->vector (s-take #f s)))
            (when close? (close?)))
    (else   (define put
              (case format
                ((bscm)  (lambda (x) (encode out type x)))
                ((scm)   (lambda (x) (write x out) (write-char #\newline out)))
                ((tsv)   (lambda (x) (tsv:write out x)))
                ;; TODO:
                ;((csv)   )
                ((jsonl) (lambda (x) (jsonl:write out x)))
                (else    (error "unsupported output format:" format))))
            (let loop ((s s))
              (match (s-force s)
                ('()        (when close? (close?)))
                (`(,x . ,s) (put x) (loop s)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; JSON and JSONL formats
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (json->scm s) (jsexpr->scm    (string->jsexpr s)))
(define (scm->json x) (jsexpr->string (scm->jsexpr    x)))

(define (jsexpr->scm j)
  (cond ((hash?     j) (define kvs (hash->list j))
                       (make-immutable-hash
                         (map cons
                              (map symbol->string (map car kvs))
                              (map jsexpr->scm    (map cdr kvs)))))
        ((pair?     j) (list->vector (map jsexpr->scm j)))
        ((eq? 'null j) '())
        (else          j)))

(define (scm->jsexpr x)
  (cond ((hash?   x) (define kvs (hash->list x))
                     (make-immutable-hash
                       (map cons
                            (map string->symbol (map car kvs))
                            (map scm->jsexpr (map cdr kvs)))))
        ((vector? x) (map scm->jsexpr (vector->list x)))
        ((null?   x) 'null)
        (else        x)))

(define (jsonl:read in)
  (define s (read-line in 'any))
  (if (eof-object? s) s (json->scm s)))

(define (jsonl:write out x)
  (write-string (scm->json x) out)
  (write-char #\newline out))

(define (json:read in)
  (json->scm (port->string in)))

(define (json:write out x)
  (write-string (scm->json x) out))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TSV format
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Informal grammar for tab-separated values (no escapes supported)
;FIELD-SEPARATOR  ::= \t
;RECORD-SEPARATOR ::= \r\n | \n | \r
;record-stream    ::= EOF | record [RECORD-SEPARATOR] EOF | record RECORD-SEPARATOR record-stream
;record           ::= field | field FIELD-SEPARATOR record
;field            ::= CONTENT*
;CONTENT includes anything other than \t, \n, \r

(define (tsv:read in)
  (define l (read-line in 'any))
  (if (eof-object? l)
    l
    (let ((fields (string-split l "\t" #:trim? #f)))
      (if (null? fields)
        '("")
        fields))))

(define (tsv:write out x)
  (write-string (string-join x "\t" #:after-last "\n") out))
