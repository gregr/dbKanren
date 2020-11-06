#lang racket/base
(provide bisect bisect-next
         vector-table? vector-table-sort! vector-dedup
         table/metadata table/vector table/bytes table/port
         table/bytes/offsets table/port/offsets sorter tabulator encoder
         table-project table-intersect-start
         value-table-file-name offset-table-file-name
         call/files let/files s-encode s-decode)
(require "codec.rkt" "method.rkt" "order.rkt" "stream.rkt"
         racket/file racket/function racket/list racket/match racket/set
         racket/vector)

(define (s-encode out type s) (s-each (lambda (v) (encode out type v)) s))
(define (s-decode in type)
  (thunk (let loop () (if (eof-object? (peek-byte in)) '()
                        (cons (decode in type) (thunk (loop)))))))

(define (encoder out type) (method-lambda ((put v) (encode out type v))
                                          ((close) (flush-output out))))

(define (call/files fins fouts p)
  (let loop ((fins fins) (ins '()))
    (if (null? fins)
      (let loop ((fouts fouts) (outs '()))
        (if (null? fouts)
          (apply p (append (reverse ins) (reverse outs)))
          (call-with-output-file
            (car fouts) (lambda (out) (loop (cdr fouts) (cons out outs))))))
      (call-with-input-file
        (car fins) (lambda (in) (loop (cdr fins) (cons in ins)))))))

(define-syntax-rule (let/files ((in fin) ...) ((out fout) ...) body ...)
  (call/files (list fin ...) (list fout ...)
              (lambda (in ... out ...) body ...)))

;; TODO: multiple accessible columns, and order of revealing, expressed by dependency chains
;; TODO: support multiple sorted columns
;;       (wait until column-oriented tables are implemented for simplicity)

(define (table/vref vref key-col cols types mask start end)
  (define (ref i)          (vector-ref (vref i) mask))
  (define compare          (and (pair? types) (type->compare (car types))))
  (define <?               (and compare (compare-><?  compare)))
  (define <=?              (and compare (compare-><=? compare)))
  (define ((make-i<  v) i) (<?  (ref i) v))
  (define ((make-i<= v) i) (<=? (ref i) v))
  (define ((make-i>  v) i) (<?  v (ref i)))
  (define ((make-i>= v) i) (<=? v (ref i)))
  (define (trim start end) (table/vref vref key-col cols types mask start end))
  (define (skip<= v)       (bisect-next start end (make-i<= v)))
  (define (skip<  v)       (bisect-next start end (make-i<  v)))
  (method-lambda
    ((columns)       (cons key-col (s-take 1 cols)))
    ((types)         (cons 'nat    (s-take 1 types)))
    ((max-count col) (- end start))
    ((min col)       (if (equal? key-col col) start (ref start)))
    ((max col)       (if (equal? key-col col) end   (ref (- end 1))))
    ((>  col v) (trim (if (equal? key-col col) (min (max start (+ v 1)) end)
                        (skip<= v))
                      end))
    ((>= col v) (trim (if (equal? key-col col) (min (max start    v   ) end)
                        (skip<  v))
                      end))
    ((<  col v) (trim start
                      (if (equal? key-col col) (min (max start (- v 1)) end)
                        (bisect-prev start end (make-i>= v)))))
    ((<= col v) (trim start
                      (if (equal? key-col col) (min (max start    v   ) end)
                        (bisect-prev start end (make-i>  v)))))
    ((=  col v) (if (equal? col key-col)
                  (trim v (if (and (<= start v) (< v end)) (+ v 1) v))
                  (let* ((start.new (skip< v))
                         (end.new   (bisect-next start.new end (make-i<= v))))
                    (table/vref vref key-col (cdr cols) (cdr types) (+ mask 1)
                                start.new end.new))))))

;(define (table/indexes ts)
;  (define (col=>tids-add col=>tids tid t)
;    (foldl (lambda (col col=>tids)
;             ;; TODO: smart subsumption
;             (hash-update col=>tids col
;                          (lambda (tids) (remove-duplicates (cons tid tids)))
;                          '()))
;           col=>tids (t 'columns.fast)))
;  (define idts (map cons (range (length ts)) ts))
;  (let loop ((env    (hash))
;             (tid=>t (make-immutable-hash idts))
;             (col=>tids (foldl (lambda (idt col=>tids)
;                                 (match-define (cons tid t) idt)
;                                 (col=>tids-add col=>tids tid t))
;                               (hash) idts)))
;    (define (assign col v)
;      (define col=>tids+tid=>t
;        (foldl/and (lambda (tid col=>tids+tid=>t)
;                     (match-define (cons col=>tids tid=>t) col=>tids+tid=>t)
;                     (define t ((hash-ref tid=>t tid) '= col v))
;                     (and t (cons (col=>tids-add col=>tids tid t)
;                                  (hash-set tid=>t tid t))))
;                   (cons (hash-remove col=>tids col) tid=>t)
;                   (hash-ref col=>tids col '())))
;      (and col=>tids+tid=>t
;           (loop (hash-set env col v)
;                 (cdr col=>tids+tid=>t)
;                 (car col=>tids+tid=>t))))
;    (method-lambda
;      ((columns.bound) (hash-keys env))
;      ((columns.fast)  (hash-keys col=>tids))
;      ((columns)       (remove-duplicates
;                         (append* (map (lambda (t) (t 'columns))
;                                       (hash-values tid=>t)))))
;      ((max-count col) )
;      ((min col) )
;      ((max col) )
;      ;; TODO: avoid pathalogical (linear) "joining" behavior.
;      ;; Instead, take one step, and cooperatively compute fixed point
;      ;; alongside external constraints.  How do we indicate that the bounding
;      ;; is incomplete?  How do we resume?  This seems ugly.  Maybe this entire
;      ;; table/indexes idea is a bad.  We should just work with individual
;      ;; table constraints for each index, and teach them about smart
;      ;; subsumption.
;      ((>  col v) )
;      ((>= col v) )
;      ((<  col v) )
;      ((<= col v) )
;      ((=  col v) (assign col v)))))

(define (table vref cols types mask start end)
  (define (ref i j) (vector-ref  (vref i) (+ mask j)))
  (define (ref* i)  (vector-copy (vref i)    mask))
  (define compare (and (pair? types) (type->compare (car types))))
  (define <?      (and compare (compare-><? compare)))
  (define <=?     (and compare (compare-><=? compare)))
  (define (make-i<  v) (lambda (i) (<?  (ref i 0) v)))
  (define (make-i<= v) (lambda (i) (<=? (ref i 0) v)))
  (define (make-i>  v) (lambda (i) (<?  v (ref i 0))))
  (define (make-i>= v) (lambda (i) (<=? v (ref i 0))))
  (method-lambda
    ((columns) cols)
    ((types)   types)
    ((width)   (length cols))
    ((length)  (- end start))
    ((key)     start)
    ((ref* i)  (ref* (+ start i)))
    ((ref i j) (ref  (+ start i) j))
    ((mask j)  (table vref (s-drop j cols) (s-drop j types)
                      (+ mask j) start end))
    ((stream)  (let loop ((i 0))
                 (thunk (if (= i (- end start)) '()
                          (cons (vector->list (ref* (+ start i)))
                                (loop (+ i 1)))))))
    ((find<  v) (bisect start end (make-i< v)))
    ((find<= v) (bisect start end (make-i<= v)))
    ((drop<  v) (table vref cols types mask
                       (bisect-next start end (make-i< v)) end))
    ((drop<= v) (table vref cols types mask
                       (bisect-next start end (make-i<= v)) end))
    ((take<= v) (table vref cols types mask
                       start (bisect-next start end (make-i<= v))))
    ;; TODO: > >= variants: take>= drop> drop>=
    ;((drop> v)  (table vref cols types mask
                       ;start (bisect-prev start end (make-i> v))))
    ((drop-key< k) (table vref cols types mask (max start (min k end)) end))
    ((drop-key> k) (table vref cols types mask start (min end (max k start))))
    ((take count) (table vref cols types mask start           (+ count start)))
    ((drop count) (table vref cols types mask (+ count start) end))))

(define (table/port/offsets table.offsets key-col cols types in)
  (define type `#(tuple ,@types))
  (define (ref i)
    (file-position in (table.offsets 'ref i 0))
    (decode in type))
  (table ref cols types 0 0 (table.offsets 'length)))

(define (table/bytes/offsets table.offsets key-col cols types bs)
  (define in (open-input-bytes bs))
  (table/port/offsets table.offsets key-col cols types in))

;; TODO: table/file that does len calculation via file-size?
(define (table/port len key-col cols types in)
  (define type `#(tuple ,@types))
  (define width (sizeof type (void)))
  (define (ref i) (file-position in (* i width)) (decode in type))
  (table ref cols types 0 0 len))

(define (table/bytes key-col cols types bs)
  (define in (open-input-bytes bs))
  (table/port (quotient (bytes-length bs) (sizeof types (void)))
              key-col cols types in))

(define (table/vector key-col cols types v)
  (table (lambda (i) (vector-ref v i)) cols types 0 0 (vector-length v)))

(define (vector-table? types v)
  (define v< (compare-><? (type->compare types)))
  (define i (- (vector-length v) 1))
  (or (<= i 0) (let loop ((i (- i 1)) (next (vector-ref v i)))
                 (define current (vector-ref v i))
                 (and (v< current next) (or (= i 0) (loop (- i 1) current))))))
(define (vector-table-sort! types v)
  (vector-sort! v (compare-><? (type->compare `#(tuple ,@types)))))
(define (vector-dedup v) (list->vector (s-dedup (vector->list v))))

;; TODO: switch back to file->bytes once Racket IO bug is fixed
(define (file->bytes2 file-name)
  (define size (file-size file-name))
  (define bs (make-bytes size))
  (call-with-input-file
    file-name
    (lambda (in)
      (let loop ((i 0))
        (cond ((= i size) bs)
              (else (define end (+ i (min 1073741824
                                          (- size i))))
                    (read-bytes! bs in i end)
                    (loop end)))))))

(define (table/metadata retrieval-type directory-path info-alist)
  ;(define (warning . args) (printf "warning: ~s\n" args))
  (define (warning . args) (error "warning:" args))
  (define info         (make-immutable-hash info-alist))
  (define path-prefix
    (path->string (build-path directory-path (hash-ref info 'file-prefix))))
  (define fname.value  (value-table-file-name  path-prefix))
  (define fname.offset (offset-table-file-name path-prefix))
  (define offset-type  (hash-ref info 'offset-type))
  (define key-name     (hash-ref info 'key-name))
  (define column-names (hash-ref info 'column-names))
  (define column-types (hash-ref info 'column-types))
  (define len (hash-ref info 'length))
  (unless (equal? (file-size fname.value) (hash-ref info 'value-file-size))
    (error "file size does not match metadata:" fname.value
           (file-size fname.value) (hash-ref info 'value-file-size)))
  (unless (equal? (file-or-directory-modify-seconds fname.value)
                  (hash-ref info 'value-file-time))
    (warning "file modification time does not match metadata:" fname.value
             (file-or-directory-modify-seconds fname.value)
             (hash-ref info 'value-file-time)))
  (when offset-type
    (unless (equal? (file-size fname.offset) (hash-ref info 'offset-file-size))
      (error "file size does not match metadata:" fname.offset
             (file-size fname.offset) (hash-ref info 'offset-file-size)))
    (unless (equal? (file-or-directory-modify-seconds fname.offset)
                    (hash-ref info 'offset-file-time))
      (warning "file modification time does not match metadata:" fname.offset
               (file-or-directory-modify-seconds fname.offset)
               (hash-ref info 'offset-file-time))))
  (define t.value
    (case retrieval-type
      ((disk) (define in.value (open-input-file fname.value))
              (if offset-type
                (table/port/offsets
                  (table/port len #t '(offset) `(,offset-type)
                              (open-input-file fname.offset))
                  key-name column-names column-types in.value)
                (table/port len key-name column-names column-types in.value)))
      ((bytes) (define bs.value (file->bytes2 fname.value))
               (if offset-type
                 (table/bytes/offsets
                   (table/bytes #t '(offset) `(,offset-type)
                                (file->bytes2 fname.offset))
                   key-name column-names column-types bs.value)
                 (table/bytes key-name column-names column-types bs.value)))
      ((scm) (let/files ((in.value fname.value)) ()
               (table/vector
                 key-name column-names column-types
                 (list->vector
                   (s-take #f (s-decode in.value `#(tuple ,@column-types)))))))
      (else (error "unknown retrieval type:" retrieval-type))))
  t.value)

(define (bisect start end i<)
  (let loop ((start start) (end end))
    (if (<= end start) end
      (let ((i (+ start (quotient (- end start) 2))))
        (if (i< i) (loop (+ 1 i) end) (loop start i))))))

(define (bisect-next start end i<)
  (define i (- start 1))
  (let loop ((offset 1))
    (define next (+ i offset))
    (cond ((and (< next end) (i< next))
           (loop (arithmetic-shift offset 1)))
          (else (let loop ((i i) (o offset))
                  (let* ((o (arithmetic-shift o -1)) (next (+ i o)))
                    (cond ((= o 0)                      (+ i 1))
                          ((and (< next end) (i< next)) (loop next o))
                          (else                         (loop i    o)))))))))

;; TODO:
(define (bisect-prev start end i>)
  (error "bisect-prev not implemented yet:" start end i>))


(define (table-project t v) (((t 'drop< v) 'take<= v) 'mask 1))

;; TODO: table-intersect-end
(define (table-intersect-start ts)
  (define (next t) (and (< 0 (t 'length)) (list (t 'ref 0 0))))
  (define initial-max (and (not (null? ts)) (next (car ts))))
  (and initial-max
       (let loop ((max (car initial-max)) (ts ts) (finished '()))
         (if (null? ts)
           (let loop ((max max) (pending (reverse finished)) (finished '()))
             (if (null? pending) (loop max (reverse finished) '())
               (let ((t-next (caar pending)) (t (cdar pending)))
                 (if (equal? t-next max)
                   (cons max (map cdr (foldl cons pending finished)))
                   (let* ((t (t 'drop< max)) (new (next t)))
                     (and new (loop (car new) (cdr pending)
                                    (cons (cons (car new) t) finished))))))))
           (let* ((t ((car ts) 'drop< max)) (new (next t)))
             (and new (loop (car new) (cdr ts) (cons (cons (car new) t)
                                                     finished))))))))

(define (value-table-file-name  prefix) (string-append prefix ".value.table"))
(define (offset-table-file-name prefix) (string-append prefix ".offset.table"))

(define (tabulator buffer-size directory-path file-prefix
                   column-names column-types key-name sorted-columns)
  (define (unique?! as) (unless (= (length (remove-duplicates as)) (length as))
                          (error "duplicates:" as)))
  (unless (= (length column-names) (length column-types))
    (error "mismatching column names and types:" column-names column-types))
  (unique?! column-names)
  (when (member key-name column-names)
    (error "key name must be distinct:" key-name column-names))
  (unless (subset? sorted-columns column-names)
    (error "unknown sorted column names:" sorted-columns column-names))
  (define row-type column-types)  ;; TODO: possibly change this to tuple?
  (define row<     (compare-><? (type->compare row-type)))
  (define row-size (sizeof row-type (void)))
  (define path-prefix (path->string (build-path directory-path file-prefix)))
  (define value-file-name  (value-table-file-name path-prefix))
  (define offset-file-name (and (not row-size)
                                (offset-table-file-name path-prefix)))
  (define tsorter (sorter #t value-file-name offset-file-name buffer-size
                          row-type row<))
  (method-lambda
    ((put x) (tsorter 'put x))
    ((close) (match-define (cons offset-type item-count) (tsorter 'close))
             `((file-prefix       . ,file-prefix)
               (value-file-size   . ,(file-size value-file-name))
               (value-file-time   . ,(file-or-directory-modify-seconds
                                       value-file-name))
               (offset-file-size  . ,(and offset-file-name
                                          (file-size offset-file-name)))
               (offset-file-time  . ,(and offset-file-name
                                          (file-or-directory-modify-seconds
                                            offset-file-name)))
               (offset-type       . ,offset-type)
               (length            . ,item-count)
               (column-names      . ,column-names)
               (column-types      . ,column-types)
               (key-name          . ,key-name)
               (sorted-columns    . ,sorted-columns)))))

(define (sorter dedup? value-file-name offset-file-name? buffer-size
                type value<)
  (define fname-sort-value  (string-append value-file-name ".value.sort"))
  (define fname-sort-offset (string-append value-file-name ".offset.sort"))
  (define out-value         (open-output-file value-file-name))
  (define out-offset
    (and offset-file-name?  (open-output-file offset-file-name?)))
  (define out-sort-value    (open-output-file fname-sort-value))
  (define out-sort-offset   (open-output-file fname-sort-offset))
  (define ms (multi-sorter out-sort-value out-sort-offset buffer-size
                           type value<))
  (method-lambda
    ((put value) (ms 'put value))
    ((close)
     (match-define (vector initial-item-count chunk-count v?) (ms 'close))
     (close-output-port out-sort-value)
     (close-output-port out-sort-offset)
     (define omax (if v? (sizeof `#(array ,initial-item-count ,type) v?)
                    (file-size fname-sort-value)))
     (define otype (and out-offset (nat-type/max omax)))
     (define item-count
       (cond (v? (let loop ((prev #f) (i 0) (count 0))
                   (if (= i initial-item-count) count
                     (let ((x (vector-ref v? i)))
                       (cond ((not (and dedup? (< 0 i) (equal? x prev)))
                              (when out-offset
                                (encode out-offset otype (file-position
                                                           out-value)))
                              (encode out-value type x)
                              (loop x (+ i 1) (+ count 1)))
                             (else (loop x (+ i 1) count)))))))
             (else (let/files ((in fname-sort-value)
                               (in-offset fname-sort-offset)) ()
                     (multi-merge dedup? out-value out-offset type otype value<
                                  chunk-count in in-offset)))))
     (delete-file fname-sort-value)
     (delete-file fname-sort-offset)
     (close-output-port out-value)
     (when out-offset (close-output-port out-offset))
     (cons otype item-count))))

(define (multi-sorter out-chunk out-offset buffer-size type value<)
  (let ((v (make-vector buffer-size)) (chunk-count 0) (item-count 0) (i 0))
    (method-lambda
      ((put value) (vector-set! v i value)
                   (set! i (+ i 1))
                   (when (= i buffer-size)
                     (vector-sort! v value<)
                     (for ((x (in-vector v))) (encode out-chunk type x))
                     (encode out-offset 'nat (file-position out-chunk))
                     (set! item-count  (+ item-count buffer-size))
                     (set! chunk-count (+ chunk-count 1))
                     (set! i           0)))
      ((close) (vector-sort! v value< 0 i)
               (cond ((< 0 chunk-count)
                      (vector-sort! v value< 0 i)
                      (for ((i (in-range i)))
                        (encode out-chunk type (vector-ref v i)))
                      (encode out-offset 'nat (file-position out-chunk))
                      (vector (+ item-count i) (+ chunk-count 1) #f))
                     (else (vector i 0 v)))))))

;; TODO: separate chunk streaming from merging
(define (multi-merge
          dedup? out out-offset type otype v< chunk-count in in-offset)
  (define (s< sa sb) (v< (car sa) (car sb)))
  (define (s-chunk pos end)
    (cond ((<= end pos) '())
          (else (file-position in pos)
                (cons (decode in type) (let ((pos (file-position in)))
                                         (thunk (s-chunk pos end)))))))
  (define heap (make-vector chunk-count))
  (let loop ((hi 0) (start 0)) (when (< hi chunk-count)
                                 (define end (decode in-offset 'nat))
                                 (vector-set! heap hi (s-chunk start end))
                                 (loop (+ hi 1) end)))
  (heap! s< heap chunk-count)
  (let loop ((prev #f) (i 0) (hend chunk-count))
    (if (= hend 0) i
      (let* ((top (heap-top heap)) (x (car top)) (top (s-force (cdr top))))
        (loop x (cond ((not (and dedup? (< 0 i) (equal? x prev)))
                       (when out-offset (encode out-offset otype
                                                (file-position out)))
                       (encode out type x)
                       (+ i 1))
                      (else i))
              (cond ((null? top) (heap-remove!  s< heap hend)  (- hend 1))
                    (else        (heap-replace! s< heap hend top) hend)))))))

(define (heap-top h) (vector-ref h 0))
(define (heap! ? h end)
  (let loop ((i (- (quotient end 2) 1)))
    (when (<= 0 i) (heap-sink! ? h end i) (loop (- i 1)))))
(define (heap-remove! ? h end)
  (vector-set! h 0 (vector-ref h (- end 1))) (heap-sink! ? h (- end 1) 0))
(define (heap-replace! ? h end top)
  (vector-set! h 0 top)                      (heap-sink! ? h    end    0))
(define (heap-sink! ? h end i)
  (let loop ((i i))
    (let ((ileft (+ i i 1)) (iright (+ i i 2)))
      (cond ((<= end ileft))  ;; done
            ((<= end iright)
             (let ((p (vector-ref h i)) (l (vector-ref h ileft)))
               (when (? l p) (vector-set! h i l) (vector-set! h ileft p))))
            (else (let ((p (vector-ref h i))
                        (l (vector-ref h ileft)) (r (vector-ref h iright)))
                    (cond ((? l p) (cond ((? r l) (vector-set! h i r)
                                                  (vector-set! h iright p)
                                                  (loop iright))
                                         (else (vector-set! h i l)
                                               (vector-set! h ileft p)
                                               (loop ileft))))
                          ((? r p) (vector-set! h i r)
                                   (vector-set! h iright p)
                                   (loop iright)))))))))
(define (heap-add! ? h end v)
  (let loop ((i end))
    (if (= i 0) (vector-set! h i v)
      (let* ((iparent (- (quotient (+ i 1) 2) 1))
             (pv      (vector-ref h iparent)))
        (cond ((? v pv) (vector-set! h i pv) (loop iparent))
              (else     (vector-set! h i v)))))))
