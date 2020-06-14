#lang racket/base
(provide bisect bisect-next
         table/vector table/bytes table/port
         table/bytes/offsets table/port/offsets tabulator tabulate
         table-project table-intersect-start table-cross table-join
         call/files let/files s-encode s-decode)
(require "codec.rkt" "method.rkt" "order.rkt" "stream.rkt"
         racket/function racket/list racket/match racket/vector)

(define (s-encode out type s) (s-each s (lambda (v) (encode out type v))))
(define (s-decode in type)
  (thunk (let loop () (if (eof-object? (peek-byte in)) '()
                        (cons (decode in type) (thunk (loop)))))))

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

;; TODO: parameterize over column types
(define (table ref start end)
  ;; TODO: column-type-specific comparison operators instead of any
  (define (make-prefix<  prefix)
    (tuple<? (vector-map (lambda (_) compare-any) prefix)))
  (define (make-prefix<= prefix)
    (tuple<=? (vector-map (lambda (_) compare-any) prefix)))
  (method-lambda
    ((length) (- end start))
    ;; TODO: column-specific ref, i.e., ((ref i j) etc.)
    ((ref i)  (ref (+ start i)))
    ((mask j)
     ;; TODO: these copies will inefficiently cascade
     ;; this will be fixed by column-specific ref
     (table (lambda (i) (vector-copy (ref i) j)) start end))
    ((stream) (let loop ((i 0))
                (thunk (if (= i (- end start)) '()
                         (cons (vector->list (ref (+ start i)))
                               (loop (+ i 1)))))))
    ((find< prefix)  (define prefix< (make-prefix< prefix))
                     (define (i< i) (prefix< (ref i) prefix))
                     (bisect start end i<))
    ((find<= prefix) (define prefix<= (make-prefix<= prefix))
                     (define (i<= i) (prefix<= (ref i) prefix))
                     (bisect start end i<=))
    ((drop< prefix)  (define prefix< (make-prefix< prefix))
                     (define (i< i) (prefix< (ref i) prefix))
                     (table ref (bisect-next start end i<) end))
    ((drop<= prefix) (define prefix<= (make-prefix<= prefix))
                     (define (i<= i) (prefix<= (ref i) prefix))
                     (table ref (bisect-next start end i<=) end))
    ((take<= prefix) (define prefix<= (make-prefix<= prefix))
                     (define (i<= i) (prefix<= (ref i) prefix))
                     (table ref start (bisect-next start end i<=)))
    ;; TODO: > >= variants: take>= drop> drop>=
    ;((drop> prefix)  (define prefix< (make-prefix< prefix))
                     ;(define (i> i) (prefix< prefix (ref i)))
                     ;(table ref start (bisect-previous start end i>)))
    ((take count) (table ref start           (+ count start)))
    ((drop count) (table ref (+ count start) end))))

(define (table/port/offsets table:offsets type in)
  (define (ref i) (file-position in (table:offsets 'ref i)) (decode in type))
  (table ref 0 (table:offsets 'length)))

(define (table/bytes/offsets table:offsets type bs)
  (define in (open-input-bytes bs))
  (table/port/offsets table:offsets type in))

;; TODO: table/file that does len calculation via file-size?
(define (table/port type len in)
  (define width (sizeof type (void)))
  (define (ref i) (file-position in (* i width)) (decode in type))
  (table ref 0 len))

(define (table/bytes type bs)
  (define in (open-input-bytes bs))
  (table/port type (quotient (bytes-length bs) (sizeof type (void))) in))

(define (table/vector v)
  (table (lambda (i) (vector-ref v i)) 0 (vector-length v)))

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
;; TODO: bisect-previous

(define (table-project t prefix)
  (((t 'drop< prefix) 'take<= prefix) 'mask (vector-length prefix)))

;; TODO: table-intersect-end
(define (table-intersect-start ts prefix-size)
  (define (next t) (and (< 0 (t 'length))
                        (vector-copy (t 'ref 0) 0 prefix-size)))
  (define initial-max (and (not (null? ts)) (next (car ts))))
  (and initial-max
       (let loop ((max initial-max) (ts ts) (finished '()))
         (if (null? ts)
           (let loop ((max max) (pending (reverse finished)) (finished '()))
             (if (null? pending) (loop max (reverse finished) '())
               (let ((t-next (caar pending)) (t (cdar pending)))
                 (if (equal? t-next max)
                   (cons max (map cdr (foldl cons pending finished)))
                   (let* ((t (t 'drop< max)) (new (next t)))
                     (and new (loop new (cdr pending)
                                    (cons (cons new t) finished))))))))
           (let* ((t ((car ts) 'drop< max)) (new (next t)))
             (and new (loop new (cdr ts) (cons (cons new t) finished))))))))

;; TODO: this may only be useful as an example.
(define (table-cross ts prefix)
  (define psize (vector-length prefix))
  (define plist (vector->list prefix))
  (map (lambda (r) (list->vector (append plist r)))
       (let loop ((ts ts))
         (if (null? ts) '(())
           (let ((t (car ts)) (suffixes (loop (cdr ts))))
             (append*
               (map (lambda (i)
                      (map (lambda (suffix)
                             (append (vector->list
                                       (vector-copy (t 'ref i) psize))
                                     suffix))
                           suffixes))
                    (range (t 'length)))))))))

;; TODO: this may only be useful as an example.
(define (table-join ts prefix-size)
  (define (current prefix+ts)
    (define prefix (car prefix+ts))
    (table-cross (map (lambda (t) (t 'take<= prefix)) (cdr prefix+ts)) prefix))
  (define (next prefix+ts)
    (define prefix (car prefix+ts))
    (map (lambda (t) (t 'drop<= prefix)) (cdr prefix+ts)))
  (let loop ((ts ts))
    (let ((prefix+ts (table-intersect-start ts prefix-size)))
      (if (not prefix+ts) '()
        (append (current prefix+ts) (loop (next prefix+ts)))))))

(define (tabulate dedup? file-name offset-file-name? buffer-size type v< s)
  (define t (tabulator dedup? file-name offset-file-name? buffer-size type v<))
  (s-each s (lambda (x) (t 'put x)))
  (t 'close))

(define (tabulator dedup? data-file-name offset-file-name? buffer-size
                   type value<)
  (define fname-sort-data   (string-append data-file-name ".data.sort"))
  (define fname-sort-offset (string-append data-file-name ".offset.sort"))
  (define out-sort-data     (open-output-file fname-sort-data))
  (define out-sort-offset   (open-output-file fname-sort-offset))
  (define out-data          (open-output-file data-file-name))
  (define out-offset
    (and offset-file-name? (open-output-file offset-file-name?)))
  (define sorter (multi-sorter out-sort-data out-sort-offset buffer-size
                               type value<))
  (method-lambda
    ((put value) (sorter 'put value))
    ((close)
     (match-define (vector initial-item-count chunk-count v?) (sorter 'close))
     (close-output-port out-sort-data)
     (close-output-port out-sort-offset)
     (define omax (if v? (sizeof `#(array ,initial-item-count ,type) v?)
                    (file-size fname-sort-data)))
     (define otype (and out-offset `#(nat ,(- (sizeof 'nat omax) 1))))
     (define item-count
       (cond (v? (let loop ((prev #f) (i 0) (count 0))
                   (if (= i initial-item-count) count
                     (let ((x (vector-ref v? i)))
                       (cond ((not (and dedup? (< 0 i) (equal? x prev)))
                              (when out-offset
                                (encode out-offset otype (file-position
                                                           out-data)))
                              (encode out-data type x)
                              (loop x (+ i 1) (+ count 1)))
                             (else (loop x (+ i 1) count)))))))
             (else (let/files ((in fname-sort-data)
                               (in-offset fname-sort-offset)) ()
                     (multi-merge dedup? out-data out-offset type otype value<
                                  chunk-count in in-offset)))))
     (delete-file fname-sort-data)
     (delete-file fname-sort-offset)
     (close-output-port out-data)
     (when out-offset (close-output-port out-offset))
     item-count)))

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
