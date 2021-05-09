#lang racket/base
(provide path->format file-stats s-pop-header produce/pop-header
         port-produce port-consume
         (struct-out producer)
         out:transform out:procedure out:port out:file
         in:transform in:procedure in:port in:file
         in:stream in:pop-header)
(require "codec.rkt" "misc.rkt" "stream.rkt"
         racket/list racket/match racket/string)

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
  (define get
    (case format
      ((bscm)  (lambda () (if (eof-object? (peek-byte in)) eof (decode in type))))
      ((scm)   (lambda () (read in)))
      ;; TODO:
      ;((tsv)   )
      ;((csv)   )
      ;((json)  )
      ;((jsonl) )
      (else    (error "unsupported input format:" format))))
  (let loop ()
    (lambda ()
      (define datum (get))
      (cond ((eof-object? datum) (when close? (close?))
                                 '())
            (else                (cons datum (loop)))))))

(define (port-consume out close? format type s)
  (define put
    (case format
      ((bscm)  (lambda (x) (encode out type x)))
      ((scm)   (lambda (x) (write x out) (write-char #\newline out)))
      ;; TODO:
      ;((tsv)   )
      ;((csv)   )
      ;((json)  )
      ;((jsonl) )
      (else    (error "unsupported output format:" format))))
  (let loop ((s s))
    (match (s-force s)
      ('()        (when close? (close?)))
      (`(,x . ,s) (put x) (loop s)))))
