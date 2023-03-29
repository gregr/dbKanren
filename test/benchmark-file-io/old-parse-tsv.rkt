#lang racket/base
(require
  racket/file
  racket/fixnum
  racket/match
  racket/string
  racket/unsafe/ops
  racket/vector
  )

;; 40GB
(define file-name "rtx_kg2.edgeprop.tsv")
;; 2GB
;(define file-name "rtx_kg2.edge.tsv")


;> time wc -l rtx_kg2.edgeprop.tsv
; 600183480 rtx_kg2.edgeprop.tsv
;
;real    0m21.825s
;user    0m18.612s
;sys     0m3.211s

;> time wc -l rtx_kg2.edge.tsv
; 53474163 rtx_kg2.edge.tsv
;
;real    0m1.369s
;user    0m1.045s
;sys     0m0.272s



;; rtx_kg2.edge.tsv
;> time racket parse-tsv.rkt
;cpu time: 14657 real time: 15454 gc time: 34
;53474163
;
;real    0m15.579s
;user    0m14.196s
;sys     0m0.581s
;(displayln
  ;(call-with-input-file
    ;file-name
    ;(lambda (in)
      ;(time
        ;(let loop ((line-count 0))
          ;(define l (read-line in 'any))
          ;(if (eof-object? l)
            ;line-count
            ;(loop (unsafe-fx+ line-count 1))))))))

;> time racket parse-tsv.rkt
;cpu time: 6382 real time: 6676 gc time: 24
;53474163

;real    0m6.819s
;user    0m6.038s
;sys     0m0.484s
;(displayln
;  (call-with-input-file
;    file-name
;    (lambda (in)
;      (time
;        (let loop ((line-count 0))
;          (define l (read-bytes-line in 'any))
;          (if (eof-object? l)
;            line-count
;            (loop (unsafe-fx+ line-count 1))))))))


;; rtx_kg2.edge.tsv
;> time racket parse-tsv.rkt
;cpu time: 18577 real time: 19373 gc time: 4
;53474163
;
;real    0m19.501s
;user    0m18.158s
;sys     0m0.542s
;(displayln
  ;(call-with-input-file
    ;file-name
    ;(lambda (in)
      ;(time
        ;(let loop ((line-count 0))
          ;(define b (read-byte in))
          ;(cond ((eof-object? b) line-count)
                ;((= b 10)        (loop (+ line-count 1)))
                ;(else            (loop    line-count))))))))

;; rtx_kg2.edge.tsv
;> time racket parse-tsv.rkt
;cpu time: 969 real time: 969 gc time: 0
;cpu time: 7851 real time: 8119 gc time: 976
;53474163

;real    0m9.280s
;user    0m7.882s
;sys     0m1.122s
;(let ((bs (time (file->bytes file-name))))
  ;(time
    ;(let loop ((i 0) (line-count 0))
      ;(if (< i (bytes-length bs))
        ;(if (= (bytes-ref bs i) 10)
          ;(loop (+ i 1) (+ line-count 1))
          ;(loop (+ i 1)    line-count   ))
        ;line-count))))

;; rtx_kg2.edge.tsv
;> time racket parse-tsv.rkt
;cpu time: 963 real time: 963 gc time: 0
;cpu time: 6430 real time: 6691 gc time: 975
;53474163

;real    0m7.855s
;user    0m6.459s
;sys     0m1.124s
;(let ((bs (time (file->bytes file-name))))
  ;(time
    ;(let loop ((i 0) (line-count 0))
      ;(if (< i (unsafe-bytes-length bs))
        ;(if (= (unsafe-bytes-ref bs i) 10)
          ;(loop (+ i 1) (+ line-count 1))
          ;(loop (+ i 1)    line-count   ))
        ;line-count))))


;; rtx_kg2.edge.tsv
;> time racket parse-tsv.rkt
;cpu time: 950 real time: 950 gc time: 0
;cpu time: 3837 real time: 4087 gc time: 968
;53474163

;real    0m5.225s
;user    0m3.856s
;sys     0m1.112s
;(define dispatch (make-fxvector 256 0))
;(fxvector-set! dispatch 10 1)
;(let ((bs (time (file->bytes file-name))))
  ;(time
    ;(let loop ((i 0) (line-count 0))
      ;(if (unsafe-fx< i (unsafe-bytes-length bs))
        ;(loop (unsafe-fx+ i 1)
              ;(unsafe-fx+ line-count
                          ;(unsafe-fxvector-ref dispatch
                                               ;(unsafe-bytes-ref bs i))))
        ;line-count))))


;> time racket parse-tsv.rkt
;cpu time: 955 real time: 955 gc time: 0
;cpu time: 3615 real time: 3864 gc time: 978
;53474163
;
;real    0m5.011s
;user    0m3.628s
;sys     0m1.130s
;(define dispatch (make-fxvector 256 0))
;(fxvector-set! dispatch 10 1)
;(let ((bs (time (file->bytes file-name))))
  ;(time
    ;(let loop ((i (- (unsafe-bytes-length bs) 1)) (line-count 0))
      ;(if (unsafe-fx<= 0 i)
        ;(loop (unsafe-fx- i 1)
              ;(unsafe-fx+ line-count
                          ;(unsafe-fxvector-ref dispatch
                                               ;(unsafe-bytes-ref bs i))))
        ;line-count))))


;> time racket parse-tsv.rkt
;cpu time: 1033 real time: 1033 gc time: 0
;cpu time: 4534 real time: 4788 gc time: 960
;53474163
;
;real    0m6.013s
;user    0m4.615s
;sys     0m1.132s
;(let ((bs (time (file->bytes file-name))))
  ;(time
    ;(let loop ((i 0) (line-count 0))
      ;(if (unsafe-fx< i (unsafe-bytes-length bs))
        ;(loop (unsafe-fx+ i 1)
              ;(unsafe-fx+ line-count
                          ;(if (unsafe-fx= (unsafe-bytes-ref bs i) 10)
                            ;1 0)))
        ;line-count))))


;> time racket parse-tsv.rkt
;cpu time: 956 real time: 956 gc time: 0
;cpu time: 3823 real time: 4073 gc time: 969
;53474163
;
;real    0m5.229s
;user    0m3.841s
;sys     0m1.127s
;(let ((bs (time (file->bytes file-name))))
  ;(time
    ;(let loop ((i (- (unsafe-bytes-length bs) 1)) (line-count 0))
      ;(if (unsafe-fx<= 0 i)
        ;(loop (unsafe-fx- i 1)
              ;(unsafe-fx+ line-count
                          ;(if (unsafe-fx= (unsafe-bytes-ref bs i) 10)
                            ;1 0)))
        ;line-count))))



;;;;;;;;;;;;;;;;;;;;;;
;;; PARSING FIELDS ;;;
;;;;;;;;;;;;;;;;;;;;;;

;> time racket parse-tsv.rkt
;cpu time: 70909 real time: 74126 gc time: 109
;53474163
;160422489
;
;real    1m14.266s
;user    1m9.962s
;sys     0m1.081s
;(call-with-input-file
  ;file-name
  ;(lambda (in)
    ;(time
      ;(let loop ((line-count 0) (field-count 0))
        ;(define l (read-line in 'any))
        ;(if (eof-object? l)
          ;(values line-count field-count)
          ;(let ((fields (string-split l "\t" #:trim? #f)))
            ;(loop (unsafe-fx+ line-count 1)
                  ;(unsafe-fx+ field-count (length fields)))))))))


;> time racket parse-tsv.rkt
;cpu time: 9484 real time: 10025 gc time: 21
;53474163
;160422489
;
;real    0m10.160s
;user    0m9.092s
;sys     0m0.521s
;(call-with-input-file
  ;file-name
  ;(lambda (in)
    ;(time
      ;(let loop ((line-count 0) (field-count 0))
        ;(define l (read-bytes-line in 'any))
        ;(if (eof-object? l)
          ;(values line-count field-count)
          ;(let field-loop ((i           (unsafe-fx- (unsafe-bytes-length l) 1))
                           ;(field-count (unsafe-fx+ field-count 1)))
            ;(if (unsafe-fx<= 0 i)
              ;(field-loop
                ;(unsafe-fx- i 1)
                ;(unsafe-fx+ field-count
                            ;(if (unsafe-fx= (unsafe-bytes-ref l i) 9)
                              ;1 0)))
              ;(loop (unsafe-fx+ line-count 1) field-count))))))))


;; rtx_kg2.edgeprop.tsv
;> time racket parse-tsv.rkt
;cpu time: 312868 real time: 327861 gc time: 471
;600183480
;1800550440
;
;real    5m28.003s
;user    5m1.955s
;sys     0m11.050s

;; rtx_kg2.edge.tsv
;> time racket parse-tsv.rkt
;cpu time: 11502 real time: 12081 gc time: 34
;53474163
;160422489
;
;real    0m12.224s
;user    0m11.093s
;sys     0m0.547s
;(call-with-input-file
  ;file-name
  ;(lambda (in)
    ;(time
      ;(let loop ((line-count 0) (field-count 0))
        ;(define l (read-bytes-line in 'any))
        ;(if (eof-object? l)
          ;(values line-count field-count)
          ;(let field-loop ((end    (unsafe-bytes-length l))
                           ;(i      (unsafe-fx- (unsafe-bytes-length l) 1))
                           ;(fields '()))
            ;(cond ((unsafe-fx< i 0)                      (loop (unsafe-fx+ line-count 1)
                                                               ;(unsafe-fx+ field-count
                                                                           ;(length (cons (subbytes l 0 end)
                                                                                         ;fields)))))
                  ;((unsafe-fx= (unsafe-bytes-ref l i) 9) (field-loop i
                                                                     ;(unsafe-fx- i 1)
                                                                     ;(cons (subbytes l i end) fields)))
                  ;(else                                  (field-loop end
                                                                     ;(unsafe-fx- i 1)
                                                                     ;fields)))))))))


;> time racket parse-tsv.rkt
;cpu time: 26126 real time: 27141 gc time: 47
;53474163
;160422489
;
;real    0m27.288s
;user    0m25.627s
;sys     0m0.641s
;(define (bytes-parse-base10-nat bs)
  ;(string->number (bytes->string/utf-8 bs)))

;> time racket parse-tsv.rkt
;cpu time: 11926 real time: 12556 gc time: 35
;53474163
;160422489
;
;real    0m12.698s
;user    0m11.502s
;sys     0m0.560s
;(define (unsafe-bytes-parse-base10-nat bs)
  ;(define len (unsafe-bytes-length bs))
  ;(let loop ((i 0) (n 0))
    ;(if (unsafe-fx< i len)
      ;(loop (unsafe-fx+ i 1)
            ;(unsafe-fx+ (unsafe-fx* n 10)
                        ;(unsafe-fx- (unsafe-bytes-ref bs i) 48)))
      ;n)))

;; rtx_kg2.edgeprop.tsv
;> time racket parse-tsv.rkt
;cpu time: 320177 real time: 335726 gc time: 473
;600183479
;1800550437
;
;real    5m35.873s
;user    5m9.320s
;sys     0m11.000s

;; rtx_kg2.edge.tsv
;> time racket parse-tsv.rkt
;cpu time: 12200 real time: 12827 gc time: 35
;53474162
;160422486
;
;real    0m12.968s
;user    0m11.781s
;sys     0m0.553s
;(define (bytes-parse-base10-nat bs)
  ;(define len (unsafe-bytes-length bs))
  ;(when (= len 0) (error "natural number must contain at least one digit" bs))
  ;(let loop ((i 0) (n 0))
    ;(if (unsafe-fx< i len)
      ;(let ((b (unsafe-bytes-ref bs i)))
        ;(unless (unsafe-fx<= 48 b 57)
          ;(error "natural number must contain only base10 digits" bs))
        ;(loop (unsafe-fx+ i 1)
              ;(unsafe-fx+ (unsafe-fx* n 10)
                          ;(unsafe-fx- b 48))))
      ;n)))

;; rtx_kg2.edgeprop.tsv
;> time racket parse-tsv.rkt
;cpu time: 331783 real time: 347511 gc time: 506
;600183479
;1800550437
;
;real    5m47.655s
;user    5m18.951s
;sys     0m12.969s

;; rtx_kg2.edge.tsv
;> time racket parse-tsv.rkt
;cpu time: 12456 real time: 13091 gc time: 35
;53474162
;160422486
;
;real    0m13.236s
;user    0m12.024s
;sys     0m0.571s
;(define (bytes-parse-base10-nat bs)
  ;(define len (bytes-length bs))
  ;(unless (< 0 len 19)
    ;(when (= len 0)  (error "natural number must contain at least one digit" bs))
    ;(when (< 18 len) (error "natural number must contain at most 18 digits (to safely fit in a fixnum)" bs)))
  ;(let loop ((i 0) (n 0))
    ;(if (unsafe-fx< i len)
      ;(let ((b (unsafe-bytes-ref bs i)))
        ;(unless (unsafe-fx<= 48 b 57)
          ;(error "natural number must contain only base10 digits" bs))
        ;(loop (unsafe-fx+ i 1)
              ;(unsafe-fx+ (unsafe-fx* n 10)
                          ;(unsafe-fx- b 48))))
      ;n)))

;(call-with-input-file
  ;file-name
  ;(lambda (in)
    ;(read-bytes-line in 'any)  ; drop header line
    ;(time
      ;(let loop ((line-count 0) (field-count 0))
        ;(define l (read-bytes-line in 'any))
        ;(if (eof-object? l)
          ;(values line-count field-count)
          ;(let field-loop ((end    (unsafe-bytes-length l))
                           ;(i      (unsafe-fx- (unsafe-bytes-length l) 1))
                           ;(fields '()))
            ;(cond ((unsafe-fx< i 0)                      (loop (unsafe-fx+ line-count 1)
                                                               ;(unsafe-fx+ field-count
                                                                           ;(length (cons (bytes-parse-base10-nat (subbytes l 0 end))
                                                                                         ;fields)))))
                  ;((unsafe-fx= (unsafe-bytes-ref l i) 9) (field-loop i
                                                                     ;(unsafe-fx- i 1)
                                                                     ;(cons (subbytes l i end) fields)))
                  ;(else                                  (field-loop end
                                                                     ;(unsafe-fx- i 1)
                                                                     ;fields)))))))))


;; rtx_kg2.edgeprop.tsv
;> time racket parse-tsv.rkt
;cpu time: 1877749 real time: 1943381 gc time: 15921
;600183479
;1800550437
;
;real    32m23.512s
;user    30m52.504s
;sys     0m25.371s

;; rtx_kg2.edge.tsv
;> time racket parse-tsv.rkt
;cpu time: 84243 real time: 87773 gc time: 110
;53474162
;160422486
;
;real    1m27.921s
;user    1m23.235s
;sys     0m1.150s
;(call-with-input-file
  ;file-name
  ;(lambda (in)
    ;(read-bytes-line in 'any)  ; drop header line
    ;(time
      ;(let loop ((line-count 0) (field-count 0))
        ;(define l (read-line in 'any))
        ;(if (eof-object? l)
          ;(values line-count field-count)
          ;(let ((fields (string-split l "\t" #:trim? #f)))
            ;(string->number (car fields))
            ;(loop (unsafe-fx+ line-count 1)
                  ;(unsafe-fx+ field-count (length fields)))))))))


;;;;;;;;;;;;;;;;;;;;;;;
;;; FULL PROCESSING ;;;
;;;;;;;;;;;;;;;;;;;;;;;

;(define (int-tuple<? a b)
  ;(let loop ((a a) (b b))
    ;(and (not (null? a))
         ;(or (< (car a) (car b))
             ;(and (= (car a) (car b))
                  ;(loop (cdr a) (cdr b)))))))

(define (int-tuple<? a b)
  (let loop ((a b) (b a))
    (and (not (null? a))
         (or (< (car a) (car b))
             (and (= (car a) (car b))
                  (loop (cdr a) (cdr b)))))))

(define (table-sort-and-dedup! count.tuples.initial vs.columns)
  (define table (make-vector count.tuples.initial))
  (time
    (let loop ((i (unsafe-fx- count.tuples.initial 1)))
      (when (<= 0 i)
        (vector-set! table i (map (lambda (vec.col) (unsafe-fxvector-ref vec.col i))
                                  vs.columns))
        (loop (unsafe-fx- i 1)))))
  (time (vector-sort! table int-tuple<?))
  (define (columns-set! j tuple) (for-each (lambda (vec.col value.col)
                                             (unsafe-fxvector-set! vec.col j value.col))
                                           vs.columns tuple))
  (define count.tuples.unique
    (time
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
              j))))))
  count.tuples.unique)

(define (tuple-inserter column-types max-count)
  (define (insert! tuple)
    (for-each (lambda (field proj vec)
                (fxvector-set! vec i.tuple (proj field)))
              tuple projections vs)
    (set! i.tuple (unsafe-fx+ i.tuple 1))
    ;(when (unsafe-fx= i.tuple max-count)
      ;;; TODO:
      ;;close! and restart
      ;(void)
      ;)
    )
  (define (close!)
    ;(define size.pos  (min-nat-bytes size.text))
    (define count.ids (hash-count text=>id))
    (define id=>id    (make-fxvector count.ids))
    ;(pretty-log `(ingested ,count.tuples.initial tuples))
    ;(pretty-log `(sorting ,(hash-count text=>id) strings -- ,size.text bytes total))

    ;(let ((text&id*.sorted (time/pretty-log (sort (hash->list text=>id)
    ;(lambda (a b) (bytes<? (car a) (car b)))))))
    (let ((text&id*.sorted (time (sort (hash->list text=>id)
                                       (lambda (a b) (bytes<? (car a) (car b)))))))

      ;(pretty-log '(writing sorted strings to) apath.domain.value
      ;'(writing positions to) apath.domain.pos)
      ;(let/files () ((out.text.value apath.domain.value)
      ;(out.text.pos   apath.domain.pos))
      ;(define (write-pos)
      ;(write-bytes (nat->bytes size.pos (file-position out.text.value)) out.text.pos))
      ;(write-pos)
      ;(time/pretty-log
      (time
        (let loop ((i 0) (t&id* text&id*.sorted))
          (unless (null? t&id*)
            (let* ((t&id (car t&id*))
                   ;(text (car t&id))
                   (id   (cdr t&id)))
              ;(write-bytes text out.text.value)
              ;(write-pos)
              (unsafe-fxvector-set! id=>id id i)
              (loop (+ i 1) (cdr t&id*))))))
      ;)
      )
    ;(define desc.domain-text
    ;(hash 'count         count.ids
    ;'size.position size.pos))
    ;(write-metadata (build-path apath.root lpath.domain-text fn.metadata.initial) desc.domain-text)

    (time
      (for-each (lambda (type vec)
                  (when (eqv? type 'text)
                    (let loop ((i (unsafe-fx- i.tuple 1)))
                      (when (<= 0 i)
                        (unsafe-fxvector-set! vec i (unsafe-fxvector-ref id=>id (unsafe-fxvector-ref vec i)))
                        (loop (unsafe-fx- i 1))))))
                column-types vs))

    (displayln 'table-sort-and-dedup!)
    (define count.tuples.unique (table-sort-and-dedup! i.tuple vs))

    ;; TODO: compute min/max etc.

    (values
      count.tuples.unique
      size.text
      text=>id
      vs))
  (define (text->id bs)
    (or (hash-ref text=>id bs #f)
        (let ((id (hash-count text=>id)))
          (hash-set! text=>id bs id)
          (set! size.text (+ size.text (bytes-length bs)))
          id)))
  (define (identity x) x)

  (define i.tuple     0)
  (define size.text   0)
  (define text=>id    (make-hash))
  (define vs          (map (lambda (_) (make-fxvector max-count)) column-types))
  (define projections (map (lambda (ctype) (if (eqv? ctype 'text)
                                             text->id
                                             identity))
                           column-types))
  (values insert! close!))

(define (unsafe-bytes-split-tab bs)
  (let loop ((end    (unsafe-bytes-length bs))
             (i      (unsafe-fx- (unsafe-bytes-length bs) 1))
             (fields '()))
    (cond ((unsafe-fx< i 0)                       (cons (subbytes bs 0 end) fields))
          ((unsafe-fx= (unsafe-bytes-ref bs i) 9) (loop i   (unsafe-fx- i 1) (cons (subbytes bs i end) fields)))
          (else                                   (loop end (unsafe-fx- i 1) fields)))))

(define (bytes-base10->fxnat bs)
  (define len (bytes-length bs))
  (unless (< 0 len 19)
    (when (= len 0)  (error "natural number must contain at least one digit" bs))
    (when (< 18 len) (error "natural number must contain at most 18 digits (to safely fit in a fixnum)" bs)))
  (let loop ((i 0) (n 0))
    (if (unsafe-fx< i len)
      (let ((b (unsafe-bytes-ref bs i)))
        (unless (unsafe-fx<= 48 b 57)
          (error "natural number must contain only base10 digits" bs))
        (loop (unsafe-fx+ i 1)
              (unsafe-fx+ (unsafe-fx* n 10)
                          (unsafe-fx- b 48))))
      n)))

(define (consume-tuples column-types max-count in)
  (define-values (insert! close!) (tuple-inserter column-types max-count))
  (time
    (let tuple-loop ((i.tuple 0))
      (when (unsafe-fx< i.tuple max-count)
        (let ((line (read-bytes-line in 'any)))
          (unless (eof-object? line)
            (insert! (let ((fields (unsafe-bytes-split-tab line)))
                       (cons (bytes-base10->fxnat (car fields)) (cdr fields))))
            (tuple-loop (unsafe-fx+ i.tuple 1)))))))
  (close!)


  ;(define (text->id bs)
    ;(or (hash-ref text=>id bs #f)
        ;(let ((id (hash-count text=>id)))
          ;(hash-set! text=>id bs id)
          ;(set! size.text (+ size.text (bytes-length bs)))
          ;id)))
  ;(define size.text   0)
  ;(define text=>id    (make-hash))
  ;(define vs          (map (lambda (_) (make-fxvector max-count)) column-types))
  ;(define projections (map (lambda (ctype)
                             ;(match ctype
                               ;('text text->id)
                               ;;; TODO: int
                               ;('nat  bytes-base10->fxnat)))
                           ;column-types))

  ;(define count.tuples.initial
    ;(time
      ;(let tuple-loop ((i.tuple 0))
        ;(if (unsafe-fx< i.tuple max-count)
          ;(let ((line (read-bytes-line in 'any)))
            ;(if (eof-object? line)
              ;i.tuple
              ;(begin
                ;(for-each (lambda (field proj vec)
                            ;(fxvector-set! vec i.tuple (proj field)))
                          ;(unsafe-bytes-split-tab line)
                          ;projections
                          ;vs)
                ;(tuple-loop (unsafe-fx+ i.tuple 1)))))
          ;max-count))))

  ;(define count.ids (hash-count text=>id))
  ;(define id=>id    (make-fxvector count.ids))
  ;;(pretty-log `(ingested ,count.tuples.initial tuples))
  ;;(pretty-log `(sorting ,(hash-count text=>id) strings -- ,size.text bytes total))

  ;;(let ((text&id*.sorted (time/pretty-log (sort (hash->list text=>id)
                                                ;;(lambda (a b) (bytes<? (car a) (car b)))))))
  ;(let ((text&id*.sorted (time (sort (hash->list text=>id)
                                     ;(lambda (a b) (bytes<? (car a) (car b)))))))

    ;;(pretty-log '(writing sorted strings to) apath.domain.value
                ;;'(writing positions to) apath.domain.pos)
    ;;(let/files () ((out.text.value apath.domain.value)
                   ;;(out.text.pos   apath.domain.pos))
      ;;(define (write-pos)
        ;;(write-bytes (nat->bytes size.pos (file-position out.text.value)) out.text.pos))
      ;;(write-pos)
      ;;(time/pretty-log
      ;(time
        ;(let loop ((i 0) (t&id* text&id*.sorted))
          ;(unless (null? t&id*)
            ;(let* ((t&id (car t&id*))
                   ;;(text (car t&id))
                   ;(id   (cdr t&id)))
              ;;(write-bytes text out.text.value)
              ;;(write-pos)
              ;(unsafe-fxvector-set! id=>id id i)
              ;(loop (+ i 1) (cdr t&id*))))))
      ;;)
    ;)
  ;;(define desc.domain-text
    ;;(hash 'count         count.ids
          ;;'size.position size.pos))
  ;;(write-metadata (build-path apath.root lpath.domain-text fn.metadata.initial) desc.domain-text)

  ;(time
    ;(for-each (lambda (type vec)
                ;(when (eqv? type 'text)
                  ;(let loop ((i (unsafe-fx- count.tuples.initial 1)))
                    ;(when (<= 0 i)
                      ;(unsafe-fxvector-set! vec i (unsafe-fxvector-ref id=>id (unsafe-fxvector-ref vec i)))
                      ;(loop (unsafe-fx- i 1))))))
              ;column-types vs))

  ;(displayln 'table-sort-and-dedup!)
  ;(define count.tuples.unique (table-sort-and-dedup! count.tuples.initial vs))

  ;;; TODO: compute min/max etc.

  ;(values
    ;count.tuples.unique
    ;size.text
    ;text=>id
    ;vs)
  )


;; rtx_kg2.edgeprop.tsv truncated to the size of rtx_kg2.edge.tsv
;> time racket parse-tsv.rkt
;cpu time: 59136 real time: 60700 gc time: 10574
;cpu time: 30208 real time: 30542 gc time: 8276
;cpu time: 699 real time: 701 gc time: 0
;cpu time: 193 real time: 205 gc time: 0
;table-sort-and-dedup!
;cpu time: 7997 real time: 8033 gc time: 6838
;cpu time: 6299 real time: 6425 gc time: 571
;cpu time: 4606 real time: 4738 gc time: 607
;cpu time: 109679 real time: 111888 gc time: 26868
;'(48612881 111598246 7368185)
;
;real    1m52.374s
;user    1m47.236s
;sys     0m2.925s

;; rtx_kg2.edge.tsv
;> time racket parse-tsv.rkt
;cpu time: 78208 real time: 79982 gc time: 10401
;cpu time: 43495 real time: 43902 gc time: 14499
;cpu time: 944 real time: 947 gc time: 0
;cpu time: 269 real time: 283 gc time: 0
;table-sort-and-dedup!
;cpu time: 8947 real time: 8982 gc time: 7855
;cpu time: 375 real time: 383 gc time: 0
;cpu time: 3725 real time: 3844 gc time: 283
;cpu time: 136590 real time: 138948 gc time: 33040
;'(53474162 133048704 8623104)
;
;real    2m19.439s
;user    2m13.396s
;sys     0m3.679s
(call-with-input-file
  file-name
  (lambda (in)
    (read-bytes-line in 'any)  ; drop header line
    (let-values (((tuple-count size.text text=>id vs)
                  (time (consume-tuples '(nat text text) 53474162 in))))
      (list tuple-count size.text (hash-count text=>id))
      )))
