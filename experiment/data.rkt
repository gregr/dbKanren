#lang racket/base
(provide
  column-describe
  column-count
  column-ref
  column-ref*
  column-slice
  column-group
  column-group/key?
  column-group/key
  column-sorted-group/key
  column-group/key*
  column-sorted-group/key*
  column:encoding.int
  column:encoding.text
  column:ref/class-name
  column:ref.int
  column:ref.text
  column:vector/class-name
  column:vector.int
  column:vector.text
  group-map
  group-filter
  encode-int*
  encode-int*-baseline
  encode-text*
  encode-text*-baseline
  )
(require (for-syntax racket/base) racket/fixnum racket/list racket/set racket/vector)
;; WARNING: decoding corrupt or malicious data is currently a memory safety risk when using racket/unsafe/ops.
;; Data integrity can be validated by performing a full scan while using ops from safe-unsafe.rkt.
(require
  "../dbk/safe-unsafe.rkt"
  ;racket/unsafe/ops
  )

(define (column-describe          col) (col 'describe))
(define (column-count             col) (col 'count))
;; i -> x
(define (column-ref               col) (col 'ref))
;; start end -> (yield! x span.start span.end)
(define (column-group             col) (col 'group))
;; key* -> start end -> (yield! key span.start span.end)
(define (column-group/key*        col) (col 'group/key*))
;; key* -> start end -> (yield! key insertion-and-span.start span.end)
;; - with sorted-group/key*, if a key is not present, it will still be yielded, but its
;;   span.start and span.end will be equal
(define (column-sorted-group/key* col) (col 'sorted-group/key*))
;; key -> start end -> (yield! key span.start span.end)
(define (column-group/key col)
  (let ((group/key* (column-group/key* col)))
    (lambda (key) (group/key* (vector key)))))
;; key -> start end -> (yield! key insertion-and-span.start span.end)
(define (column-sorted-group/key col)
  (let ((sorted-group/key* (column-sorted-group/key* col)))
    (lambda (key) (sorted-group/key* (vector key)))))
;; key? -> start end -> (yield! key span.start span.end)
(define (column-group/key? col)
  (let ((group (column-group col)))
    (lambda (key?) (lambda (start end) (group-filter (group start end) key?)))))
;; i* -> x*
(define (column-ref* col)
  (let ((ref (column-ref col)))
    (lambda (i* count)
      (let ((x* (make-vector count)))
        (let loop ((i 0))
          (when (unsafe-fx< i count)
            (unsafe-vector*-set! x* i (ref (unsafe-fxvector-ref i* i)))
            (loop (unsafe-fx+ i 1))))
        x*))))
;; start end -> x*
(define (column-slice col)
  (let ((group (column-group col)))
    (lambda (start end)
      (let ((x* (make-vector (unsafe-fx- end start))) (i 0))
        ((group start end)
         (lambda (x span.start span.end)
           (let loop ((count (unsafe-fx- span.end span.start)))
             (when (unsafe-fx< 0 count)
               (unsafe-vector*-set! x* i x)
               (set! i (unsafe-fx+ i 1))
               (loop (unsafe-fx- count 1))))))
        x*))))

(define ((group-map    enum f) yield!) (enum (lambda (x start end) (yield! (f x) start end))))
(define ((group-filter enum ?) yield!) (enum (lambda (x start end)
                                               (when (? x) (yield! x start end)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Low-level representation ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (min-bits n)
  (let loop ((n n))
    (if (< 0 n) (+ 1 (loop (fxrshift n 1))) 0)))
(define (min-bytes n)
  (let ((bits (min-bits n)))
    (+ (quotient bits 8) (if (= 0 (remainder bits 8)) 0 1))))
(define (nat-min-byte-width nat.max) (max (min-bytes nat.max) 1))
(define (int-min-byte-width z)
  (nat-min-byte-width (if (unsafe-fx< z 0)
                          (unsafe-fx- (unsafe-fx+ (unsafe-fx+ z z) 1))
                          (unsafe-fx+ z z))))
(define (bit-width->int-min  bit-width)  (unsafe-fx- (unsafe-fxlshift 1 (unsafe-fx- bit-width 1))))
(define (byte-width->int-min byte-width) (bit-width->int-min (unsafe-fxlshift byte-width 3)))

;(define-syntax (define-inline stx)
;  (syntax-case stx ()
;    ((_ (name param ...) body ...)
;     (with-syntax (((E.param ...) (generate-temporaries #'(param ...))))
;       #'(define-syntax-rule (name E.param ...)
;           (let ((param E.param) ...) body ...))))))
(define 1-unrolled-unsafe-bytes-nat-ref unsafe-bytes-ref)
(define (2-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fxior (unsafe-fxlshift (unsafe-bytes-ref bs             i)     8)
                (unsafe-bytes-ref                  bs (unsafe-fx+ i 1))))
(define (3-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fxior (unsafe-fxlshift (unsafe-bytes-ref bs             i)    16)
                (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 1))  8)
                (unsafe-bytes-ref                  bs (unsafe-fx+ i 2))))
(define (4-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fxior (unsafe-fxlshift (unsafe-bytes-ref bs             i)    24)
                (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 1)) 16)
                (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 2))  8)
                (unsafe-bytes-ref                  bs (unsafe-fx+ i 3))))
(define (5-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fxior (unsafe-fxlshift (unsafe-bytes-ref bs             i)    32)
                (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 1)) 24)
                (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 2)) 16)
                (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 3))  8)
                (unsafe-bytes-ref                  bs (unsafe-fx+ i 4))))
(define (6-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fxior (unsafe-fxlshift (unsafe-bytes-ref bs             i)    40)
                (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 1)) 32)
                (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 2)) 24)
                (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 3)) 16)
                (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 4))  8)
                (unsafe-bytes-ref                  bs (unsafe-fx+ i 5))))
(define (7-unrolled-unsafe-bytes-nat-ref bs i)
  (unsafe-fxior (unsafe-fxlshift (unsafe-bytes-ref bs             i)    48)
                (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 1)) 40)
                (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 2)) 32)
                (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 3)) 24)
                (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 4)) 16)
                (unsafe-fxlshift (unsafe-bytes-ref bs (unsafe-fx+ i 5))  8)
                (unsafe-bytes-ref                  bs (unsafe-fx+ i 6))))
(define (unsafe-bytes-nat-ref/width width bs i)
  (case width
    ((1)  (1-unrolled-unsafe-bytes-nat-ref bs i))
    ((2)  (2-unrolled-unsafe-bytes-nat-ref bs i))
    ((3)  (3-unrolled-unsafe-bytes-nat-ref bs i))
    ((4)  (4-unrolled-unsafe-bytes-nat-ref bs i))
    ((5)  (5-unrolled-unsafe-bytes-nat-ref bs i))
    ((6)  (6-unrolled-unsafe-bytes-nat-ref bs i))
    (else (7-unrolled-unsafe-bytes-nat-ref bs i))))
(define 1-unrolled-unsafe-bytes-nat-set! unsafe-bytes-set!)
(define (2-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 n)))
(define (3-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 2) (unsafe-fxand 255 n)))
(define (4-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 24)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 2) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 3) (unsafe-fxand 255 n)))
(define (5-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 32)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 24)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 2) (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 3) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 4) (unsafe-fxand 255 n)))
(define (6-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 40)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 32)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 2) (unsafe-fxand 255 (unsafe-fxrshift n 24)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 3) (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 4) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 5) (unsafe-fxand 255 n)))
(define (7-unrolled-unsafe-bytes-nat-set! bs i n)
  (unsafe-bytes-set! bs i                (unsafe-fxand 255 (unsafe-fxrshift n 48)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 1) (unsafe-fxand 255 (unsafe-fxrshift n 40)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 2) (unsafe-fxand 255 (unsafe-fxrshift n 32)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 3) (unsafe-fxand 255 (unsafe-fxrshift n 24)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 4) (unsafe-fxand 255 (unsafe-fxrshift n 16)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 5) (unsafe-fxand 255 (unsafe-fxrshift n 8)))
  (unsafe-bytes-set! bs (unsafe-fx+ i 6) (unsafe-fxand 255 n)))
(define (unsafe-bytes-nat-set!/width width bs i n)
  (case width
    ((1)  (1-unrolled-unsafe-bytes-nat-set! bs i n))
    ((2)  (2-unrolled-unsafe-bytes-nat-set! bs i n))
    ((3)  (3-unrolled-unsafe-bytes-nat-set! bs i n))
    ((4)  (4-unrolled-unsafe-bytes-nat-set! bs i n))
    ((5)  (5-unrolled-unsafe-bytes-nat-set! bs i n))
    ((6)  (6-unrolled-unsafe-bytes-nat-set! bs i n))
    (else (7-unrolled-unsafe-bytes-nat-set! bs i n))))

(define (unsafe-bytes-int-ref/width width bs i)
  (unsafe-fx+ (unsafe-bytes-nat-ref/width width bs i) (byte-width->int-min width)))
(define (unsafe-bytes-int-set!/width width bs i z)
  (unsafe-bytes-nat-set!/width width bs i (unsafe-fx- z (byte-width->int-min width))))
(define (advance-unsafe-bytes-int-set!/width width bs i z)
  (unsafe-bytes-int-set!/width width bs i z)
  (unsafe-fx+ i width))
(define (advance-unsafe-bytes-nat-set!/width width bs i z)
  (unsafe-bytes-nat-set!/width width bs i z)
  (unsafe-fx+ i width))
(define (advance-unsafe-bytes-set! bs i x)
  (unsafe-bytes-set! bs i x)
  (unsafe-fx+ i 1))

(define (unsafe-bytes-compare a b)
  (let* ((len.a (unsafe-bytes-length a))
         (len.b (unsafe-bytes-length b))
         (end   (unsafe-fxmin len.a len.b)))
    (let loop ((i 0))
      (if (unsafe-fx= i end)
          (cond ((unsafe-fx< len.a len.b) -1)
                ((unsafe-fx< len.b len.a)  1)
                (else                      0))
          (let ((x.a (unsafe-bytes-ref a i)) (x.b (unsafe-bytes-ref b i)))
            (cond ((unsafe-fx< x.a x.b) -1)
                  ((unsafe-fx< x.b x.a)  1)
                  (else                 (loop (unsafe-fx+ i 1)))))))))
(define (unsafe-bytes=? a b)
  (let ((len (unsafe-bytes-length a)))
    (and (unsafe-fx= (unsafe-bytes-length b) len)
         (let loop ((i 0))
           (or (unsafe-fx= i len)
               (and (unsafe-fx= (unsafe-bytes-ref a i) (unsafe-bytes-ref b i))
                    (loop (unsafe-fx+ i 1))))))))
(define (unsafe-bytes<? a b)
  (let* ((len.a (unsafe-bytes-length a))
         (len.b (unsafe-bytes-length b))
         (end   (unsafe-fxmin len.a len.b)))
    (let loop ((i 0))
      (if (unsafe-fx= i end)
          (unsafe-fx< len.a len.b)
          (let ((x.a (unsafe-bytes-ref a i)) (x.b (unsafe-bytes-ref b i)))
            (or (unsafe-fx< x.a x.b)
                (and (unsafe-fx= x.a x.b) (loop (unsafe-fx+ i 1)))))))))
(define (unsafe-bytes-prefix? x prefix)
  (let ((len.prefix (unsafe-bytes-length prefix)))
    (and (unsafe-fx<= len.prefix (unsafe-bytes-length x))
         (let loop ((i 0))
           (or (unsafe-fx= i len.prefix)
               (and (unsafe-fx= (unsafe-bytes-ref x i) (unsafe-bytes-ref prefix i))
                    (loop (unsafe-fx+ i 1))))))))

(define (unsafe-bisect-skip start end <?)
  (let ((i (unsafe-fx- start 1)))
    (let loop ((offset 1))
      (let ((next (unsafe-fx+ i offset)))
        (if (and (unsafe-fx< next end) (<? next))
            (loop (unsafe-fxlshift offset 1))
            (let loop ((i i) (o offset))
              (let* ((o    (unsafe-fxrshift o 1))
                     (next (unsafe-fx+ i o)))
                (cond ((eq? o 0)                             (unsafe-fx+ i 1))
                      ((and (unsafe-fx< next end) (<? next)) (loop next o))
                      (else                                  (loop i    o))))))))))

;;;;;;;;;;;;;;;
;;; Sorting ;;;
;;;;;;;;;;;;;;;

(define (unsafe-fxvector-copy! vec.out out.start vec.in in.start in.end)
  (let loop ((in in.start) (out out.start))
    (when (unsafe-fx< in in.end)
      (unsafe-fxvector-set! vec.out out (unsafe-fxvector-ref vec.in in))
      (loop (unsafe-fx+ in 1) (unsafe-fx+ out 1)))))

(define (unsafe-fxvector-sort! z* start end) (unsafe-fxvector-sort!/<? unsafe-fx< z* start end))

(define (unsafe-fxvector-sort!/<? <?.orig z* start end)
  (let ((<? (lambda (i j) (<?.orig (unsafe-fxvector-ref z* i) (unsafe-fxvector-ref z* j)))))
    (define (find-unsorted start end)
      (let loop ((i start))
        (let ((i+1 (unsafe-fx+ i 1)))
          (if (or (unsafe-fx= i+1 end) (<? i+1 i)) i+1 (loop i+1)))))
    (when (unsafe-fx< start end)
      (let ((start.unsorted (find-unsorted start end)))
        (when (unsafe-fx< start.unsorted end)
          (unsafe-fxvector-sort!/buffer/<?/start.unsorted
            <?.orig (make-fxvector (unsafe-fx- end start)) 0 z* start start.unsorted end))))))

(define (unsafe-fxvector-sort!/buffer/<? <?.orig z*.buffer start.buffer z* start end)
  (let ((<? (lambda (i j) (<?.orig (unsafe-fxvector-ref z* i) (unsafe-fxvector-ref z* j)))))
    (define (find-unsorted start end)
      (let loop ((i start))
        (let ((i+1 (unsafe-fx+ i 1)))
          (if (or (unsafe-fx= i+1 end) (<? i+1 i)) i+1 (loop i+1)))))
    (when (unsafe-fx< start end)
      (let ((start.unsorted (find-unsorted start end)))
        (when (unsafe-fx< start.unsorted end)
          (unsafe-fxvector-sort!/buffer/<?/start.unsorted
            <?.orig z*.buffer start.buffer z* start start.unsorted end))))))

(define (unsafe-fxvector-sort!/buffer/<?/start.unsorted
          <? z*.buffer start.buffer z* start start.unsorted end)
  (let ((<buffer? (lambda (i i.buffer) (<? (unsafe-fxvector-ref z*        i)
                                           (unsafe-fxvector-ref z*.buffer i.buffer))))
        (<?       (lambda (i j) (<? (unsafe-fxvector-ref z* i) (unsafe-fxvector-ref z* j)))))
    (define (find-unsorted start end)
      (let loop ((i start))
        (let ((i+1 (unsafe-fx+ i 1)))
          (if (or (unsafe-fx= i+1 end) (<? i+1 i)) i+1 (loop i+1)))))
    (when (unsafe-fx< start.unsorted end)
      (let sort-range! ((start start) (start.unsorted start.unsorted) (end end))
        (let ((diff (unsafe-fx- end start)))
          (when (unsafe-fx< 1 diff)
            (let ((mid (unsafe-fx+ start (unsafe-fxrshift diff 1))))
              (if (unsafe-fx<= mid start.unsorted)
                  (sort-range! mid start.unsorted end)
                  (begin (sort-range! start start.unsorted mid)
                         (let ((start.unsorted (find-unsorted mid end)))
                           (when (unsafe-fx< start.unsorted end)
                             (sort-range! mid start.unsorted end)))))
              (let ((start (let loop ((start start) (end mid))
                             (if (unsafe-fx< start end)
                                 (let ((i (unsafe-fx+ (unsafe-fxrshift (unsafe-fx- end start) 1)
                                                      start)))
                                   (if (<? mid i)
                                       (loop start            i)
                                       (loop (unsafe-fx+ i 1) end)))
                                 end))))
                (when (unsafe-fx< start mid)
                  (unsafe-fxvector-copy! z*.buffer start.buffer z* start mid)
                  (let ((end.buffer (unsafe-fx+ start.buffer (unsafe-fx- mid start))))
                    (let merge! ((i.buffer start.buffer) (i mid) (out start))
                      (if (<buffer? i i.buffer)
                          (let ((i+1 (unsafe-fx+ i 1)) (out+1 (unsafe-fx+ out 1)))
                            (unsafe-fxvector-set! z* out (unsafe-fxvector-ref z* i))
                            (if (unsafe-fx< i+1 end)
                                (merge! i.buffer i+1 out+1)
                                (unsafe-fxvector-copy! z* out+1 z*.buffer i.buffer end.buffer)))
                          (let ((i+1.buffer (unsafe-fx+ i.buffer 1)))
                            (unsafe-fxvector-set! z* out (unsafe-fxvector-ref z*.buffer i.buffer))
                            (when (unsafe-fx< i+1.buffer end.buffer)
                              (merge! i+1.buffer i (unsafe-fx+ out 1))))))))))))))))

;;;;;;;;;;;;;;;;
;;; 2-3 tree ;;;
;;;;;;;;;;;;;;;;

;;; Public
(define (make-btree) (vector 0 #f))
(define (btree-count bt) (unsafe-vector*-ref bt 0))

(define (btree-enumerate bt yield)
  (let loop ((t (btree-root bt)))
    (when t
      (cond ((btree-2? t) (loop  (btree-2-left t))
                          (yield (btree-2-key t) (btree-2-leaf t))
                          (loop  (btree-2-right t)))
            (else (loop  (btree-3-left t))
                  (yield (btree-3-left-key t) (btree-3-left-leaf t))
                  (loop  (btree-3-middle t))
                  (yield (btree-3-right-key t) (btree-3-right-leaf t))
                  (loop  (btree-3-right t)))))))

(define (btree-ref-or-set! bt x)
  (let loop ((t        (btree-root bt))
             (replace! (lambda (t)            (btree-root-set! bt t)))
             (expand!  (lambda (key leaf l r) (btree-root-set! bt (make-btree-2 key leaf l r)))))
    (cond
      ((not t) (let ((count (btree-count bt)))
                 (btree-count-set! bt (unsafe-fx+ count 1))
                 (expand! x count #f #f)
                 count))
      ((btree-2? t) (case (unsafe-bytes-compare x (btree-2-key t))
                      ((-1) (loop (btree-2-left t)
                                  (lambda (u) (btree-2-left-set! t u))
                                  ;; 2(._ key/leaf .R) ==> 3(.l left-key/left-leaf .m key/leaf .R)
                                  (lambda (left-key left-leaf l m)
                                    (replace! (make-btree-3 left-key (btree-2-key t)
                                                            left-leaf (btree-2-leaf t)
                                                            l m (btree-2-right t))))))
                      (( 1) (loop (btree-2-right t)
                                  (lambda (u) (btree-2-right-set! t u))
                                  ;; 2(.L key/leaf ._) ==> 3(.L key/leaf .m right-key/right-leaf .r)
                                  (lambda (right-key right-leaf m r)
                                    (replace! (make-btree-3 (btree-2-key t) right-key
                                                            (btree-2-leaf t) right-leaf
                                                            (btree-2-left t) m r)))))
                      (else (btree-2-leaf t))))
      (else (case (unsafe-bytes-compare x (btree-3-left-key t))
              ((-1) (loop (btree-3-left t)
                          (lambda (u) (btree-3-left-set! t u))
                          ;; 3(._ left-key/left-leaf .M right-key/right-leaf .R)
                          ;; ==>
                          ;;           2(. left-key/left-leaf .)
                          ;;            /                      \
                          ;; 2(.l key/leaf .r)        2(.M right-key/right-leaf .R)
                          (lambda (key leaf l r)
                            (expand! (btree-3-left-key t)
                                     (btree-3-left-leaf t)
                                     (make-btree-2 key leaf l r)
                                     (make-btree-2 (btree-3-right-key t) (btree-3-right-leaf t)
                                                   (btree-3-middle t) (btree-3-right t))))))
              (( 1) (case (unsafe-bytes-compare x (btree-3-right-key t))
                      ((-1) (loop (btree-3-middle t)
                                  (lambda (u) (btree-3-middle-set! t u))
                                  ;; 3(.L left-key/left-leaf ._ right-key/right-leaf .R)
                                  ;; ==>
                                  ;;                       2(. key/leaf .)
                                  ;;                        /            \
                                  ;; 2(.L left-key/left-leaf .l)      2(.r right-key/right-leaf .R)
                                  (lambda (key leaf l r)
                                    (expand!
                                      key leaf
                                      (make-btree-2 (btree-3-left-key t) (btree-3-left-leaf t)
                                                    (btree-3-left t) l)
                                      (make-btree-2 (btree-3-right-key t) (btree-3-right-leaf t)
                                                    r (btree-3-right t))))))
                      (( 1) (loop (btree-3-right t)
                                  (lambda (u) (btree-3-right-set! t u))
                                  ;; 3(.L left-key/left-leaf .M right-key/right-leaf ._)
                                  ;; ==>
                                  ;;                 2(. right-key/right-leaf .)
                                  ;;                  /                        \
                                  ;; 2(.L left-key/left-leaf .M)        2(.l key/leaf .r)
                                  (lambda (key leaf l r)
                                    (expand!
                                      (btree-3-right-key t)
                                      (btree-3-right-leaf t)
                                      (make-btree-2 (btree-3-left-key t) (btree-3-left-leaf t)
                                                    (btree-3-left t) (btree-3-middle t))
                                      (make-btree-2 key leaf l r)))))
                      (else (btree-3-right-leaf t))))
              (else (btree-3-left-leaf t)))))))

;;; Private
(define (btree-count-set! bt count) (unsafe-vector*-set! bt 0 count))
(define (btree-root       bt)       (unsafe-vector*-ref  bt 1))
(define (btree-root-set!  bt t)     (unsafe-vector*-set! bt 1 t))

(define (make-btree-2 key                leaf                 l r)   (vector key                leaf                 l r))
(define (make-btree-3 left-key right-key left-leaf right-leaf l m r) (vector left-key right-key left-leaf right-leaf l m r))

(define (btree-2?      t) (unsafe-fx= (unsafe-vector*-length t) 4))
(define (btree-2-key   t) (unsafe-vector*-ref t 0))
(define (btree-2-leaf  t) (unsafe-vector*-ref t 1))
(define (btree-2-left  t) (unsafe-vector*-ref t 2))
(define (btree-2-right t) (unsafe-vector*-ref t 3))

(define (btree-2-left-set!  t u) (unsafe-vector*-set! t 2 u))
(define (btree-2-right-set! t u) (unsafe-vector*-set! t 3 u))

(define (btree-3-left-key   t) (unsafe-vector*-ref t 0))
(define (btree-3-right-key  t) (unsafe-vector*-ref t 1))
(define (btree-3-left-leaf  t) (unsafe-vector*-ref t 2))
(define (btree-3-right-leaf t) (unsafe-vector*-ref t 3))
(define (btree-3-left       t) (unsafe-vector*-ref t 4))
(define (btree-3-middle     t) (unsafe-vector*-ref t 5))
(define (btree-3-right      t) (unsafe-vector*-ref t 6))

(define (btree-3-left-set!   t u) (unsafe-vector*-set! t 4 u))
(define (btree-3-middle-set! t u) (unsafe-vector*-set! t 5 u))
(define (btree-3-right-set!  t u) (unsafe-vector*-set! t 6 u))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Column implementation utilities ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define ((sorted-group/key*/i<x?&i=x? i<x? i=x?) key*)
  (let ((len (unsafe-vector*-length key*)))
    (lambda (start end)
      (lambda (yield!)
        (let loop ((i 0) (start start))
          (when (unsafe-fx< i len)
            (let* ((key (unsafe-vector*-ref key* i)))
              (let* ((span.start (unsafe-bisect-skip start      end (lambda (j) (i<x? j key))))
                     (span.end   (unsafe-bisect-skip span.start end (lambda (j) (i=x? j key)))))
                (yield! key span.start span.end)
                (loop (unsafe-fx+ i 1) span.end)))))))))
(define ((group/single-value single-value) start end)
  (if (unsafe-fx< start end)
      (lambda (yield!) (yield! single-value start end))
      (lambda (yield!) (void))))
(define ((group/key*/single-value single-value x=x?) key*)
  (let ((len (unsafe-vector*-length key*)))
    (let loop ((i 0))
      (if (unsafe-fx< i len)
          (if (x=x? (unsafe-vector*-ref key* i) single-value)
              (lambda (start end)
                (if (unsafe-fx< start end)
                    (lambda (yield!) (yield! single-value start end))
                    (lambda (yield!) (void))))
              (loop (unsafe-fx+ i 1)))
          (lambda (start end) (lambda (yield!) (void)))))))
(define ((((sorted-group/key*/single-value single-value x<x? x=x?) key*) start end) yield!)
  (let ((len (unsafe-vector*-length key*)))
    (let loop ((i 0))
      (when (unsafe-fx< i len)
        (let ((key (unsafe-vector*-ref key* i)))
          (if (x<x? key single-value)
              (begin (yield! key start start) (loop (unsafe-fx+ i 1)))
              (begin (if (x=x? key single-value)
                         (yield! key start end)
                         (yield! key end end))
                     (let loop ((i (unsafe-fx+ i 1)))
                       (when (unsafe-fx< i len)
                         (yield! (unsafe-vector*-ref key* i) end end)
                         (loop (unsafe-fx+ i 1)))))))))))
(define ((group/key*/group group x=x?) key*)
  (let* ((len (unsafe-vector*-length key*))
         (?   (lambda (x)
                (let loop ((i 0))
                  (and (unsafe-fx< i len)
                       (or (x=x? (unsafe-vector*-ref key* i) x)
                           (loop (unsafe-fx+ i 1))))))))
    (lambda (start end) (group-filter (group start end) ?))))

(define ((controller:missing-method class-name) method) (error "unknown method" class-name method))
(define ((controller:column super count describe ref group group/key* sorted-group/key*) method)
  (case method
    ((ref)               ref)
    ((group)             group)
    ((group/key*)        group/key*)
    ((sorted-group/key*) sorted-group/key*)
    ((count)             count)
    ((describe)          (describe))
    (else                (super method))))

(define (controller:column/array ref x=x? i=x? i<x? class-name count describe)
  (define ((group start end) yield!)
    (when (unsafe-fx< start end)
      (let loop ((start start) (x.current (ref start)))
        (let group ((i (unsafe-fx+ start 1)))
          (if (unsafe-fx< i end)
              (if (i=x? i x.current)
                  (group (unsafe-fx+ i 1))
                  (begin (yield! x.current start i)
                         (loop i (ref i))))
              (yield! x.current start i))))))
  (define group/key* (group/key*/group group x=x?))
  (define sorted-group/key* (sorted-group/key*/i<x?&i=x? i<x? i=x?))
  (controller:column (controller:missing-method class-name) count describe ref group group/key*
                     sorted-group/key*))

;;;;;;;;;;;;;;;;;;;;;
;;; Encoding tags ;;;
;;;;;;;;;;;;;;;;;;;;;

(define-syntax-rule (define-enum name ...)
  (define-values (name ...) (apply values (range (length '(name ...))))))

;; 4 bits for the encoding tag
;; 4 bits to describe a bit width
(define-enum
  encoding.int:nat
  ;; - the embedded bit width describes the values
  encoding.int:int
  ;; - the embedded bit width describes the codes
  encoding.int:frame-of-reference
  ;; - the embedded bit width describes the codes, not the base
  ;; - another bit width is needed to describe the base
  encoding.int:dictionary
  ;; - the embedded bit width describes the dictionary count
  ;; - embeds an untagged encoding.int:nat for codes, with code width inferred from dictionary count
  encoding.int:run-length
  ;; - the embedded bit width describes the run count
  ;; - embeds an untagged encoding.int:nat for offsets, with offset width inferred from full count
  encoding.int:run-single-length
  ;; - the embedded bit width describes the run count
  ;; - offsets are a virtual encoding.int:delta-single-value with start=0, implicit in the single
  ;;   run-length inferred from the full count and run count (i.e., (quotient full-count run-count))
  ;; - the "delta" is assumed to be positive
  encoding.int:single-value
  ;; - the embedded bit width describes the single value
  encoding.int:delta-single-value
  ;; - the embedded bit width describes the starting value, not the delta value
  ;; - another bit width is needed to describe the delta value
  )

;; 4 bits for the encoding tag
;; 4 bits to describe a bit width
(define-enum
  encoding.text:raw
  ;; - embeds an untagged encoding.int:nat for offsets
  ;; - the embedded bit width describes the text value offsets
  encoding.text:raw-single-length
  ;; - embeds an untagged encoding.int:delta-single-value with start=0, for offsets
  ;; - the embedded bit width describes the delta value
  ;; - the "delta" is assumed to be positive
  encoding.text:dictionary
  ;; - the embedded bit width describes the dictionary count
  ;; - embeds an untagged encoding.int:nat for codes, with code width inferred from dictionary count
  encoding.text:run-length
  ;; - the embedded bit width describes the run count
  ;; - embeds an untagged encoding.int:nat for offsets, with offset width inferred from full count
  encoding.text:run-single-length
  ;; - the embedded bit width describes the run count
  ;; - offsets are a virtual encoding.int:delta-single-value with start=0, implicit in the single
  ;;   run-length inferred from the full count and run count (i.e., (quotient full-count run-count))
  ;; - the "delta" is assumed to be positive
  encoding.text:single-value
  ;; - the embedded bit width describes the length of the single value
  encoding.text:single-prefix
  ;; - the embedded bit width describes the length of the prefix
  encoding.text:multi-prefix
  ;; - the embedded bit width is ignored
  ;; TODO: redefine as a concatenation of element pairs from two columns
  ;; - note: the prefixes will always be either dictionary or run-length (or run-single-length) encoded,
  ;;   because if these encodings were not effective, multi-prefix would not have been chosen originally
  )

;;;;;;;;;;;;;;
;;; Column ;;;
;;;;;;;;;;;;;;

(define (column:encoding.int bv start.bv count)
  (let-values (((col end) (column:encoding.int&end bv start.bv count))) col))
(define (column:encoding.text bv start.bv count)
  (let-values (((col end) (column:encoding.text&end bv start.bv count))) col))

(define (column:encoding.int&end bv start.bv count)
  (let* ((pos.bv     start.bv)
         (encoding   (unsafe-bytes-ref bv pos.bv))
         (byte-width (unsafe-fxand encoding                     #b0111))
         (encoding   (unsafe-fxand (unsafe-fxrshift encoding 4) #b1111))
         (pos.bv     (unsafe-fx+ pos.bv 1)))
    (define (? x) (eq? encoding x))
    (cond
      ((? encoding.int:nat) (values (column:encoding.int:nat byte-width bv pos.bv count)
                                    (unsafe-fx+ (unsafe-fx* count byte-width) pos.bv)))
      ((? encoding.int:int) (values (column:encoding.int:int byte-width bv pos.bv count)
                                    (unsafe-fx+ (unsafe-fx* count byte-width) pos.bv)))
      ((? encoding.int:frame-of-reference)
       (let* ((byte-width.min (unsafe-bytes-ref bv pos.bv))
              (pos.bv         (unsafe-fx+ pos.bv 1))
              (z.min          (unsafe-bytes-int-ref/width byte-width.min bv pos.bv))
              (pos.bv         (unsafe-fx+ pos.bv byte-width.min)))
         (values (column:encoding.int:frame-of-reference z.min byte-width bv pos.bv count)
                 (unsafe-fx+ (unsafe-fx* count byte-width) pos.bv))))
      ((? encoding.int:dictionary)
       ;; Since count.dict can never be 0, we encode it with -1.  Undo it with +1.
       (let* ((count.dict (unsafe-fx+ (unsafe-bytes-nat-ref/width byte-width bv pos.bv) 1))
              (pos.bv     (unsafe-fx+ pos.bv byte-width))
              (bw.code    (nat-min-byte-width count.dict)))
         (let*-values (((col.dict pos.bv) (column:encoding.int&end bv pos.bv count.dict)))
           (values (column:encoding.int:dictionary
                     col.dict count.dict (column:encoding.int:nat bw.code bv pos.bv count) count)
                   (unsafe-fx+ (unsafe-fx* bw.code count) pos.bv)))))
      ((? encoding.int:run-length)
       ;; Since count.run can never be 0, we encode it with -1.  Undo it with +1.
       (let* ((count.run    (unsafe-fx+ (unsafe-bytes-nat-ref/width byte-width bv pos.bv) 1))
              (pos.bv       (unsafe-fx+ pos.bv byte-width))
              (count.offset (unsafe-fx+ count.run 1))
              (bw.offset    (nat-min-byte-width count))
              (col.offset   (column:encoding.int:nat bw.offset bv pos.bv count.offset))
              (pos.bv       (unsafe-fx+ (unsafe-fx* bw.offset count.offset) pos.bv)))
         (let*-values (((col.run pos.bv) (column:encoding.int&end bv pos.bv count.run)))
           (values (column:encoding.int:run-length col.offset col.run count.run count)
                   pos.bv))))
      ((? encoding.int:run-single-length)
       ;; Since count.run can never be 0, we encode it with -1.  Undo it with +1.
       (let* ((count.run    (unsafe-fx+ (unsafe-bytes-nat-ref/width byte-width bv pos.bv) 1))
              (pos.bv       (unsafe-fx+ pos.bv byte-width))
              (count.offset (unsafe-fx+ count.run 1))
              (len.run      (unsafe-fxquotient count count.run))
              (col.offset   (column:encoding.int:delta-single-value 0 len.run count.offset)))
         (let*-values (((col.run pos.bv) (column:encoding.int&end bv pos.bv count.run)))
           (values (column:encoding.int:run-length col.offset col.run count.run count)
                   pos.bv))))
      ((? encoding.int:single-value)
       (let ((single-value (unsafe-bytes-nat-ref/width byte-width bv pos.bv)))
         (values (column:encoding.int:single-value single-value count)
                 (unsafe-fx+ pos.bv byte-width))))
      ((? encoding.int:delta-single-value)
       (let* ((z.start          (unsafe-bytes-int-ref/width byte-width bv pos.bv))
              (pos.bv           (unsafe-fx+ pos.bv byte-width))
              (byte-width.delta (unsafe-bytes-ref bv pos.bv))
              (z.delta          (unsafe-bytes-int-ref/width byte-width.delta bv pos.bv)))
         (values (column:encoding.int:delta-single-value z.start z.delta count)
                 (unsafe-fx+ pos.bv byte-width.delta))))
      (else (error "unknown integer encoding" encoding)))))

(define (column:encoding.text&end bv start.bv count)
  (let* ((pos.bv     start.bv)
         (encoding   (unsafe-bytes-ref bv pos.bv))
         (byte-width (unsafe-fxand encoding                     #b0111))
         (encoding   (unsafe-fxand (unsafe-fxrshift encoding 4) #b1111))
         (pos.bv     (unsafe-fx+ pos.bv 1)))
    (define (? x) (eq? encoding x))
    (cond
      ((? encoding.text:raw)
       (let* ((count.offset (unsafe-fx+ count 1))
              (col.offset   (column:encoding.int:nat byte-width bv pos.bv count.offset))
              (pos.bv       (unsafe-fx+ (unsafe-fx* byte-width count.offset) pos.bv)))
         (values (column:encoding.text:raw col.offset bv pos.bv count)
                 (unsafe-fx+ ((column-ref col.offset) count) pos.bv))))
      ((? encoding.text:raw-single-length)
       (let* ((len        (unsafe-bytes-nat-ref/width byte-width bv pos.bv))
              (pos.bv     (unsafe-fx+ pos.bv byte-width))
              (col.offset (column:encoding.int:delta-single-value 0 len (unsafe-fx+ count 1))))
         (values (column:encoding.text:raw col.offset bv pos.bv count)
                 (unsafe-fx+ (unsafe-fx* len count) pos.bv))))
      ((? encoding.text:dictionary)
       ;; Since count.dict can never be 0, we encode it with -1.  Undo it with +1.
       (let* ((count.dict (unsafe-fx+ (unsafe-bytes-nat-ref/width byte-width bv pos.bv) 1))
              (pos.bv     (unsafe-fx+ pos.bv byte-width))
              (bw.code    (nat-min-byte-width count.dict)))
         (let*-values (((col.dict pos.bv) (column:encoding.text&end bv pos.bv count.dict)))
           (values (column:encoding.text:dictionary
                     col.dict count.dict (column:encoding.int:nat bw.code bv pos.bv count) count)
                   (unsafe-fx+ (unsafe-fx* bw.code count) pos.bv)))))
      ((? encoding.text:run-length)
       ;; Since count.run can never be 0, we encode it with -1.  Undo it with +1.
       (let* ((count.run    (unsafe-fx+ (unsafe-bytes-nat-ref/width byte-width bv pos.bv) 1))
              (pos.bv       (unsafe-fx+ pos.bv byte-width))
              (count.offset (unsafe-fx+ count.run 1))
              (bw.offset    (nat-min-byte-width count))
              (col.offset   (column:encoding.int:nat bw.offset bv pos.bv count.offset))
              (pos.bv       (unsafe-fx+ (unsafe-fx* bw.offset count.offset) pos.bv)))
         (let*-values (((col.run pos.bv) (column:encoding.text&end bv pos.bv count.run)))
           (values (column:encoding.text:run-length col.offset col.run count.run count)
                   pos.bv))))
      ((? encoding.text:run-single-length)
       ;; Since count.run can never be 0, we encode it with -1.  Undo it with +1.
       (let* ((count.run    (unsafe-fx+ (unsafe-bytes-nat-ref/width byte-width bv pos.bv) 1))
              (pos.bv       (unsafe-fx+ pos.bv byte-width))
              (count.offset (unsafe-fx+ count.run 1))
              (len.run      (unsafe-fxquotient count count.run))
              (col.offset   (column:encoding.int:delta-single-value 0 len.run count.offset)))
         (let*-values (((col.run pos.bv) (column:encoding.text&end bv pos.bv count.run)))
           (values (column:encoding.text:run-length col.offset col.run count.run count)
                   pos.bv))))
      ((? encoding.text:single-value)
       (let* ((len.single-value (unsafe-bytes-nat-ref/width byte-width bv pos.bv))
              (pos.bv           (unsafe-fx+ pos.bv byte-width))
              (single-value     (let ((t (make-bytes len.single-value)))
                                  (unsafe-bytes-copy! t 0 bv pos.bv
                                                      (unsafe-fx+ pos.bv len.single-value))
                                  t)))
         (values (column:encoding.text:single-value single-value count)
                 (unsafe-fx+ pos.bv len.single-value))))
      ((? encoding.text:single-prefix)
       ;; Since len.single-prefix can never be 0, we encode it with -1.  Undo it with +1.
       (let* ((len.single-prefix (unsafe-fx+ (unsafe-bytes-nat-ref/width byte-width bv pos.bv) 1))
              (pos.bv            (unsafe-fx+ pos.bv byte-width))
              (single-prefix     (let ((t (make-bytes len.single-prefix)))
                                   (unsafe-bytes-copy! t 0 bv pos.bv
                                                       (unsafe-fx+ pos.bv len.single-prefix))
                                   t))
              (pos.bv            (unsafe-fx+ pos.bv len.single-prefix)))
         (let-values (((col.suffix pos.bv) (column:encoding.text&end bv pos.bv count)))
           (values (column:encoding.text:single-prefix single-prefix col.suffix count)
                   pos.bv))))
      ;; TODO: redo this
      ((? encoding.text:multi-prefix)
       (let* ((count.prefix (unsafe-bytes-nat-ref/width byte-width bv pos.bv))
              (pos.bv       (unsafe-fx+ pos.bv byte-width)))
         (let*-values (((col.prefix pos.bv) (column:encoding.text&end bv pos.bv count.prefix))
                       ((col.suffix pos.bv) (column:encoding.text&end bv pos.bv count)))
           (values (column:encoding.text:multi-prefix col.prefix count.prefix col.suffix count)
                   pos.bv))))
      (else (error "unknown text encoding" encoding)))))

(define (column:encoding.int:nat byte-width bv start.bv count)
  (column:encoding.int:frame-of-reference 0 byte-width bv start.bv count))

(define (column:encoding.int:int byte-width bv start.bv count)
  (column:encoding.int:frame-of-reference (byte-width->int-min byte-width) byte-width bv start.bv count))

(define (column:encoding.int:frame-of-reference z.min byte-width bv start.bv count)
  (define (describe)
    (let ((size.bv (- (unsafe-fx+ (unsafe-fx* count byte-width) start.bv) start.bv)))
      `(column:encoding.int:frame-of-reference
         (count                     ,count)
         (byte-size                 ,size.bv)
         (average-bytes-per-element ,(exact->inexact (/ size.bv count)))
         (bit-width                 ,(* byte-width 8))
         (z.min                     ,z.min))))
  (define ref
    (let ((go (lambda (byte-width n-ref)
                (lambda (i)
                  (let ((offset (unsafe-fx* i byte-width)))
                    (unsafe-fx+ (n-ref bv (unsafe-fx+ offset start.bv))
                                z.min))))))
      (case byte-width
        ((1)  (go 1 1-unrolled-unsafe-bytes-nat-ref))
        ((2)  (go 2 2-unrolled-unsafe-bytes-nat-ref))
        ((3)  (go 3 3-unrolled-unsafe-bytes-nat-ref))
        ((4)  (go 4 4-unrolled-unsafe-bytes-nat-ref))
        ((5)  (go 5 5-unrolled-unsafe-bytes-nat-ref))
        ((6)  (go 6 6-unrolled-unsafe-bytes-nat-ref))
        (else (go 7 7-unrolled-unsafe-bytes-nat-ref)))))
  (define (i=x? i x) (unsafe-fx= (ref i) x))
  (define (i<x? i x) (unsafe-fx< (ref i) x))
  (controller:column/array ref unsafe-fx= i=x? i<x? 'column:encoding.int:frame-of-reference count
                           describe))

(define (column:encoding.text:raw col.pos bv start.bv count)
  (let ((ref.pos (column-ref col.pos)))
    (define (describe)
      (let ((size.bv (- (ref.pos count) start.bv)))
        `(column:encoding.text:raw
           (count                     ,count)
           (byte-size                 ,size.bv)
           (average-bytes-per-element ,(exact->inexact (/ size.bv count)))
           (column.position           ,(column-describe col.pos)))))
    (define (ref i)
      (let* ((start (ref.pos i))
             (end   (ref.pos (unsafe-fx+ i 1)))
             (count (unsafe-fx- end start))
             (t     (make-bytes count)))
        (unsafe-bytes-copy! t 0 bv start end)
        t))
    (define (i=x? i x)
      (let* ((start (ref.pos i))
             (end   (ref.pos (unsafe-fx+ i 1)))
             (len.i (unsafe-fx- end start)))
        (and (unsafe-fx= (unsafe-bytes-length x) len.i)
             (let loop ((i start) (j 0))
               (or (unsafe-fx= start end)
                   (and (unsafe-fx= (unsafe-bytes-ref bv i) (unsafe-bytes-ref x j))
                        (loop (unsafe-fx+ i 1) (unsafe-fx+ j 1))))))))
    (define (i<x? i x)
      (let* ((start (ref.pos i))
             (len.i (unsafe-fx- (ref.pos (unsafe-fx+ i 1)) start))
             (len.x (unsafe-bytes-length x))
             (count (unsafe-fxmin len.i len.x)))
        (let loop ((i 0))
          (if (unsafe-fx= i count)
              (unsafe-fx< len.i len.x)
              (let ((b.i (unsafe-bytes-ref bv (unsafe-fx+ start i))) (b.x (unsafe-bytes-ref x i)))
                (or (unsafe-fx< b.i b.x)
                    (and (unsafe-fx= b.i b.x) (loop (unsafe-fx+ i 1)))))))))
    (controller:column/array ref unsafe-bytes=? i=x? i<x? 'column:encoding.text:raw count
                             describe)))

(define (column:encoding.int:single-value single-value count)
  (define (describe)
    `(column:encoding.int:single-value
       (count ,count)
       (value ,single-value)))
  (define (ref i) single-value)
  (define group             (group/single-value             single-value))
  (define group/key*        (group/key*/single-value        single-value unsafe-fx=))
  (define sorted-group/key* (sorted-group/key*/single-value single-value unsafe-fx< unsafe-fx=))
  (controller:column (controller:missing-method 'column:encoding.int:single-value)
                     count describe ref group group/key* sorted-group/key*))

(define (column:encoding.text:single-value single-value count)
  (define (describe)
    `(column:encoding.text:single-value
       (count  ,count)
       (length ,(bytes-length single-value))
       (value  ,single-value)))
  (define (ref i) single-value)
  (define group      (group/single-value      single-value))
  (define group/key* (group/key*/single-value single-value unsafe-bytes=?))
  (define sorted-group/key*
    (sorted-group/key*/single-value single-value unsafe-bytes<? unsafe-bytes=?))
  (controller:column (controller:missing-method 'column:encoding.text:single-value)
                     count describe ref group group/key* sorted-group/key*))

(define (column:encoding.int:delta-single-value z.start z.delta count)
  (define (describe)
    `(column:encoding.int:delta-single-value
       (count ,count)
       (start ,z.start)
       (delta ,z.delta)))
  (define (ref i) (unsafe-fx+ (unsafe-fx* z.delta i) z.start))
  (define (group start end)
    (if (unsafe-fx< start end)
        (lambda (yield!)
          (let loop ((i start) (z (ref start)))
            (when (unsafe-fx< i end)
              (let ((i.next (unsafe-fx+ i 1)))
                (yield! z i i.next)
                (loop i.next (unsafe-fx+ z z.delta))))))
        (lambda (yield!) (void))))
  (define (((group/key* key*) start end) yield!)
    (when (unsafe-fx< start end)
      (let ((len (unsafe-vector*-length key*)))
        (let loop ((i 0))
          (when (unsafe-fx< i len)
            (let* ((key (unsafe-vector*-ref key* i))
                   (n   (unsafe-fx- key z.start)))
              (when (unsafe-fx= (unsafe-fxremainder n z.delta) 0)
                (let ((code (unsafe-fxquotient n z.delta)))
                  (when (and (unsafe-fx<= start code) (unsafe-fx< code end))
                    (yield! key code (unsafe-fx+ code 1)))))
              (loop (unsafe-fx+ i 1))))))))
  (define (((sorted-group/key* key*) start end) yield!)
    (let ((len (unsafe-vector*-length key*)))
      (if (unsafe-fx< start end)
          (let loop ((i 0))
            (when (unsafe-fx< i len)
              (let* ((key (unsafe-vector*-ref key* i))
                     (n   (unsafe-fx- key z.start)))
                (let ((code (unsafe-fxquotient n z.delta)))
                  (cond ((unsafe-fx< code start) (yield! key start start))
                        ((unsafe-fx<= end code) (yield! key end end))
                        (else (yield! key code (if (unsafe-fx= (unsafe-fxremainder n z.delta) 0)
                                                   (unsafe-fx+ code 1)
                                                   code)))))
                (loop (unsafe-fx+ i 1)))))
          (let loop ((i 0))
            (when (unsafe-fx< i len)
              (yield! (unsafe-vector*-ref key* i) start start)
              (loop (unsafe-fx+ i 1)))))))
  (controller:column (controller:missing-method 'column:encoding.text:delta-single-value)
                     count describe ref group group/key* sorted-group/key*))

(define ((column:encoding.X:dictionary/X x<x? x=x? column-type-id.description)
         col.dict count.dict col.code count)
  (define (describe)
    `(,column-type-id.description
       (count             ,count)
       (column.dictionary ,(column-describe col.dict))
       (column.code       ,(column-describe col.code))))
  (let ((ref.code               (column-ref               col.code))
        (group.code             (column-group             col.code))
        (group/key*.code        (column-group/key*        col.code))
        (sorted-group/key*.dict (column-sorted-group/key* col.dict))
        (ref.dict               (column-ref               col.dict)))
    (define (ref i) (ref.dict (ref.code i)))
    (define (group start end) (group-map (group.code start end) ref.dict))
    (define (group/key* key*)
      (let ((i 0) (key*.encoded (make-vector (unsafe-vector*-length key*))))
        ((sorted-group/key*.dict key* 0 count.dict)
         (lambda (key start end)
           (when (unsafe-fx< start end)
             (unsafe-vector*-set! key*.encoded i start)
             (set! i (unsafe-fx+ i 1)))))
        (if (unsafe-fx< 0 i)
            (let ((group (group/key*.code (vector-copy key*.encoded 0 i))))
              (lambda (start end) (group-map (group start end) ref.dict)))
            (lambda (start end) (lambda (yield!) (void))))))
    (define (i<x? j x) (x<x? (ref j) x))
    (define (i=x? j x) (x=x? (ref j) x))
    (define sorted-group/key* (sorted-group/key*/i<x?&i=x? i<x? i=x?))
    (controller:column (controller:missing-method column-type-id.description)
                       count describe ref group group/key* sorted-group/key*)))
(define column:encoding.int:dictionary
  (column:encoding.X:dictionary/X unsafe-fx< unsafe-fx= 'column:encoding.int:dictionary))
(define column:encoding.text:dictionary
  (column:encoding.X:dictionary/X unsafe-bytes<? unsafe-bytes=? 'column:encoding.text:dictionary))

(define ((column:encoding.X:run-length/X column-type-id.description)
         col.pos col.run count.run count)
  (define (describe)
    `(,column-type-id.description
       (count           ,count)
       (column.position ,(column-describe col.pos))
       (column.run      ,(column-describe col.run))))
  (let ((ref.pos               (column-ref               col.pos))
        (ref.run               (column-ref               col.run))
        (group.run             (column-group             col.run))
        (group/key*.run        (column-group/key*        col.run))
        (sorted-group/key*.run (column-sorted-group/key* col.run)))
    (define (ref i) (ref.run (unsafe-bisect-skip 0 (unsafe-fx+ count.run 1)
                                                 (lambda (j) (unsafe-fx< (ref.pos j) i)))))
    (define ((group/group.run group.run) start end)
      (let* ((i.start (if (unsafe-fx= start 0)
                          0
                          (unsafe-fx-
                            (unsafe-bisect-skip 0 (unsafe-fx+ count.run 1)
                                                (lambda (i) (unsafe-fx<= (ref.pos i) start)))
                            1)))
             (i.end   (if (unsafe-fx= end count)
                          count.run
                          (unsafe-bisect-skip i.start (unsafe-fx+ count.run 1)
                                              (lambda (i) (unsafe-fx< (ref.pos i) end)))))
             (enum    (group.run i.start i.end)))
        (lambda (yield!)
          (enum (lambda (x start.run end.run) (yield! x (unsafe-fxmax (ref.pos start.run) start)
                                                      (unsafe-fxmin (ref.pos end.run) end)))))))
    (define group                    (group/group.run group.run))
    (define (group/key*        key*) (group/group.run (group/key*.run        key*)))
    (define (sorted-group/key* key*) (group/group.run (sorted-group/key*.run key*)))
    (controller:column (controller:missing-method column-type-id.description)
                       count describe ref group group/key* sorted-group/key*)))
(define (column:encoding.int:run-length col.pos col.run count.run count)
  (column:encoding.X:run-length/X 'column:encoding.int:run-length))
(define (column:encoding.text:run-length col.pos col.run count.run count)
  (column:encoding.X:run-length/X 'column:encoding.text:run-length))

(define (column:encoding.text:single-prefix prefix col.suffix count)
  (define (describe)
    `(column:encoding.text:single-prefix
       (count         ,count)
       (prefix        ,prefix)
       (column.suffix ,(column-describe col.suffix))))
  (let ((len.prefix               (unsafe-bytes-length prefix))
        (ref.suffix               (column-ref col.suffix))
        (group.suffix             (column-group col.suffix))
        (group/key*.suffix        (column-group/key* col.suffix))
        (sorted-group/key*.suffix (column-sorted-group/key* col.suffix)))
    (define (decode suffix)
      (let* ((len.suffix (unsafe-bytes-length suffix))
             (t          (make-bytes (unsafe-fx+ len.prefix len.suffix))))
        (unsafe-bytes-copy! t 0 prefix 0 len.prefix)
        (unsafe-bytes-copy! t len.prefix suffix 0 len.suffix)
        t))
    (define (encode* key*)
      (let* ((len          (unsafe-vector*-length key*))
             (key*.encoded (make-vector len))
             (len          (let loop ((i 0) (j 0))
                             (if (unsafe-fx< i len)
                                 (let ((key (unsafe-vector*-ref key* i)))
                                   (if (unsafe-bytes-prefix? key prefix)
                                       (begin (unsafe-vector*-set!
                                                key*.encoded j (subbytes key len.prefix))
                                              (loop (unsafe-fx+ i 1) (unsafe-fx+ j 1)))
                                       (loop (unsafe-fx+ i 1) j)))
                                 j))))
        (vector-copy key*.encoded 0 len)))
    (define (ref i) (decode (ref.suffix i)))
    (define (group start end) (group-map (group.suffix start end) decode))
    (define (group/key* key*) (let ((group (group/key*.suffix (encode* key*))))
                                (lambda (start end) (group-map (group start end) decode))))
    (define (sorted-group/key* key*)
      (let* ((len          (unsafe-vector*-length key*))
             (key*.encoded (encode* key*))
             (group        (sorted-group/key*.suffix key*.encoded)))
        (lambda (start end)
          (let ((enum (group-map (group start end) decode)))
            (lambda (yield!)
              (let loop ((i 0))
                (when (unsafe-fx< i len)
                  (let ((key (unsafe-vector*-ref key* i)))
                    (when (unsafe-bytes<? key prefix)
                      (yield! key start start)
                      (loop (unsafe-fx+ i 1))))))
              (enum yield!)
              (let loop ((i (unsafe-fx- len 1)))
                (when (unsafe-fx<= 0 i)
                  (let ((key (unsafe-vector*-ref key* i)))
                    (when (and (unsafe-bytes<? prefix key)
                               (not (unsafe-bytes-prefix? key prefix)))
                      (yield! key end end)
                      (loop (unsafe-fx- i 1)))))))))))
    (controller:column (controller:missing-method 'column:encoding.text:single-prefix)
                       count describe ref group group/key* sorted-group/key*)))

(define (column:encoding.text:multi-prefix col.prefix count.prefix col.suffix count)
  (define (describe)
    `(column:encoding.text:multi-prefix
       (count         ,count)
       (column.prefix ,(column-describe col.prefix))
       (column.suffix ,(column-describe col.suffix))))
  (let ((byte-width.code (nat-min-byte-width count.prefix))
        (ref.prefix      (column-ref   col.prefix))
        (ref.suffix      (column-ref   col.suffix))
        (group.suffix    (column-group col.suffix)))
    (define decode
      (let ((go (lambda (byte-width.code n-ref)
                  (lambda (suffix)
                    (let* ((prefix     (ref.prefix (n-ref suffix 0)))
                           (len.prefix (unsafe-bytes-length prefix))
                           (len.suffix (unsafe-bytes-length suffix))
                           (t          (make-bytes
                                         (unsafe-fx+ (unsafe-fx- len.suffix byte-width.code)
                                                     len.prefix))))
                      (unsafe-bytes-copy! t 0          prefix 0               len.prefix)
                      (unsafe-bytes-copy! t len.prefix suffix byte-width.code len.suffix)
                      t)))))
        (case byte-width.code
          ((1)  (go 1 1-unrolled-unsafe-bytes-nat-ref))
          ((2)  (go 2 2-unrolled-unsafe-bytes-nat-ref))
          ((3)  (go 3 3-unrolled-unsafe-bytes-nat-ref))
          ((4)  (go 4 4-unrolled-unsafe-bytes-nat-ref))
          ((5)  (go 5 5-unrolled-unsafe-bytes-nat-ref))
          ((6)  (go 6 6-unrolled-unsafe-bytes-nat-ref))
          (else (go 7 7-unrolled-unsafe-bytes-nat-ref)))))
    (define (ref i) (decode (ref.suffix i)))
    (define (group start end) (group-map (group.suffix start end) decode))
    (define group/key* (group/key*/group group unsafe-bytes=?))
    (define (i<x? j x) (unsafe-bytes<? (ref j) x))
    (define (i=x? j x) (unsafe-bytes=? (ref j) x))
    (define sorted-group/key* (sorted-group/key*/i<x?&i=x? i<x? i=x?))
    (controller:column (controller:missing-method 'column:encoding.text:multi-prefix)
                       count describe ref group group/key* sorted-group/key*)))

(define ((column:ref/class-name class-name x<x? x=x?) ref count)
  (define (describe) `(,class-name (count ,count)))
  (define (i=x? i x) (x=x? (ref i) x))
  (define (i<x? i x) (x<x? (ref i) x))
  (controller:column/array ref x=x? i=x? i<x? class-name count describe))
(define column:ref.int  (column:ref/class-name 'column:ref.int unsafe-fx< unsafe-fx=))
(define column:ref.text (column:ref/class-name 'column:ref.text unsafe-bytes<? unsafe-bytes=?))
(define (column:vector/class-name class-name x<x? x=x?)
  (let ((make-col (column:ref/class-name class-name x<x? x=x?)))
    (lambda (x*) (make-col (lambda (i) (unsafe-vector*-ref x* i)) (unsafe-vector*-length x*)))))
(define column:vector.int  (column:vector/class-name 'column:vector.int unsafe-fx< unsafe-fx=))
(define column:vector.text (column:vector/class-name 'column:vector.text unsafe-bytes<? unsafe-bytes=?))

;;;;;;;;;;;;;;;;;;;;;;;
;;; Encode integers ;;;
;;;;;;;;;;;;;;;;;;;;;;;

;; NOTE: encoders always assume (< start end).  Don't try to encode an empty sequence.

(define (advance-unsafe-bytes-encoding&width-set! bv pos encoding width)
  (unsafe-bytes-set! bv pos (unsafe-fxior (unsafe-fxlshift encoding 4) width))
  (unsafe-fx+ pos 1))

(define (encode-X*-run-length
          count.run code* start end encoding.X:run-single-length encoding.X:run-length encode-X*)
  (let ((count.full (unsafe-fx- end start))
        (len*.run   (make-fxvector count.run)))
    (let loop ((i         (unsafe-fx+ start 1))
               (j         0)
               (code.prev (unsafe-fxvector-ref code* start))
               (len.run   1))
      (if (unsafe-fx< i end)
          (let ((code (unsafe-fxvector-ref code* i)))
            (if (unsafe-fx= code code.prev)
                (loop (unsafe-fx+ i 1) j code.prev (unsafe-fx+ len.run 1))
                (let ((j.next (unsafe-fx+ j 1)))
                  (unsafe-fxvector-set! len*.run j                         len.run)
                  (unsafe-fxvector-set! code*    (unsafe-fx+ start j.next) code)
                  (loop (unsafe-fx+ i 1) j.next code 1))))
          (let ((end (unsafe-fx+ start j 1)))
            (unsafe-fxvector-set! len*.run j len.run)
            (let-values (((size.run encode.run) (encode-X* code* start end)))
              ;; Since count.run can never be 0, we encode it with -1.
              (let ((bw.count.run (nat-min-byte-width (unsafe-fx- count.run 1)))
                    (len.run      (unsafe-fxvector-ref len*.run 0)))
                (if (let loop ((i 1))
                      (or (unsafe-fx= i count.run)
                          (and (unsafe-fx= (unsafe-fxvector-ref len*.run i) len.run)
                               (loop (unsafe-fx+ i 1)))))
                    (values
                      (unsafe-fx+ 1 bw.count.run size.run)
                      (lambda (bv pos)
                        (let* ((pos (advance-unsafe-bytes-encoding&width-set!
                                      bv pos encoding.X:run-single-length bw.count.run))
                               (pos (advance-unsafe-bytes-nat-set!/width bw.count.run bv pos
                                                                         (unsafe-fx- count.run 1))))
                          (encode.run bv pos))))
                    (let ((bw.offset (nat-min-byte-width count.full)))
                      (values
                        (unsafe-fx+ 1 bw.count.run (unsafe-fx* bw.offset (unsafe-fx+ count.run 1))
                                    size.run)
                        (lambda (bv pos)
                          (let* ((pos (advance-unsafe-bytes-encoding&width-set!
                                        bv pos encoding.X:run-length bw.count.run))
                                 (pos (advance-unsafe-bytes-nat-set!/width bw.count.run bv pos
                                                                           (unsafe-fx- count.run 1)))
                                 (pos (advance-unsafe-bytes-nat-set!/width bw.offset bv pos 0))
                                 (pos (let loop ((offset 0) (pos pos) (i 0))
                                        (if (unsafe-fx< i count.run)
                                            (let* ((len.run (unsafe-fxvector-ref len*.run i))
                                                   (offset  (unsafe-fx+ len.run offset)))
                                              (loop offset
                                                    (advance-unsafe-bytes-nat-set!/width
                                                      bw.offset bv pos offset)
                                                    (unsafe-fx+ i 1)))
                                            pos))))
                            (encode.run bv pos)))))))))))))

(define (encode-int*/frame-of-reference z.min z.max z* start end)
  (encode-int*/frame-of-reference/byte-width (nat-min-byte-width (unsafe-fx- z.max z.min))
                                             z.min z.max z* start end))

(define (encode-int*/frame-of-reference/byte-width bw.n.max z.min z.max z* start end)
  (define (use-frame-of-reference)
    (let ((bw.z.min (int-min-byte-width z.min)))
      (values
        (unsafe-fx+ 2 bw.z.min (* (unsafe-fx- end start) bw.n.max))
        (lambda (bv pos)
          (let* ((pos (advance-unsafe-bytes-encoding&width-set!
                        bv pos encoding.int:frame-of-reference bw.n.max))
                 (pos (advance-unsafe-bytes-set! bv pos bw.z.min))
                 (pos (advance-unsafe-bytes-int-set!/width bw.z.min bv pos z.min)))
            (let loop ((i start) (pos pos))
              (if (unsafe-fx< i end)
                  (let ((n (unsafe-fx- (unsafe-fxvector-ref z* i) z.min)))
                    (loop (unsafe-fx+ i 1)
                          (advance-unsafe-bytes-int-set!/width bw.n.max bv pos n)))
                  pos)))))))
  (define (use-nat)
    (values
      (unsafe-fx+ 1 (* (unsafe-fx- end start) bw.n.max))
      (lambda (bv pos)
        (let ((pos (advance-unsafe-bytes-encoding&width-set! bv pos encoding.int:nat bw.n.max)))
          (let loop ((i start) (pos pos))
            (if (unsafe-fx< i end)
                (let ((n (unsafe-fxvector-ref z* i)))
                  (loop (unsafe-fx+ i 1)
                        (advance-unsafe-bytes-nat-set!/width bw.n.max bv pos n)))
                pos))))))
  (define (use-int)
    (values
      (unsafe-fx+ 1 (* (unsafe-fx- end start) bw.n.max))
      (lambda (bv pos)
        (let ((pos (advance-unsafe-bytes-encoding&width-set! bv pos encoding.int:int bw.n.max)))
          (let loop ((i start) (pos pos))
            (if (unsafe-fx< i end)
                (let ((z (unsafe-fxvector-ref z* i)))
                  (loop (unsafe-fx+ i 1)
                        (advance-unsafe-bytes-int-set!/width bw.n.max bv pos z)))
                pos))))))
  (cond ((unsafe-fx= 0 z.min) (use-nat))
        ((unsafe-fx< 0 z.min) (if (unsafe-fx= (nat-min-byte-width z.max) bw.n.max)
                                  (use-nat)
                                  (use-frame-of-reference)))
        (else (let ((bw.z (unsafe-fxmax (int-min-byte-width z.min) (int-min-byte-width z.max))))
                (if (unsafe-fx= bw.z bw.n.max)
                    (use-int)
                    (use-frame-of-reference))))))

(define (encode-int*/single-value z)
  (let ((bw (int-min-byte-width z)))
    (values
      (unsafe-fx+ 1 bw)
      (lambda (bv pos)
        (let ((pos (advance-unsafe-bytes-encoding&width-set! bv pos encoding.int:single-value bw)))
          (advance-unsafe-bytes-int-set!/width bw bv pos z))))))

(define (encode-int*/try-delta-single-value fail z* start end)
  (let* ((z0    (unsafe-fxvector-ref z* start))
         (z1    (unsafe-fxvector-ref z* (unsafe-fx+ start 1)))
         (delta (unsafe-fx- z1 z0)))
    (let loop ((i (unsafe-fx+ start 2)) (z.prev z1))
      (let ((z (unsafe-fxvector-ref z* i)))
        (if (unsafe-fx= (unsafe-fx- z z.prev) delta)
            (let ((i (unsafe-fx+ i 1)))
              (if (unsafe-fx< i end)
                  (loop i z)
                  (let ((bw.start (int-min-byte-width z0))
                        (bw.delta (int-min-byte-width delta)))
                    (values
                      (unsafe-fx+ 2 bw.start bw.delta)
                      (lambda (bv pos)
                        (let* ((pos (advance-unsafe-bytes-encoding&width-set!
                                      bv pos encoding.int:delta-single-value bw.start))
                               (pos (advance-unsafe-bytes-int-set!/width bw.start bv pos z0))
                               (pos (advance-unsafe-bytes-set! bv pos bw.delta)))
                          (advance-unsafe-bytes-int-set!/width bw.delta bv pos delta)))))))
            (fail))))))

;; Assume (< 1 (- end start))
(define (encode-int*-ordered-distinct z.min z.max z* start end)
  (define (fail-dsv) (encode-int*/frame-of-reference z.min z.max z* start end))
  (encode-int*/try-delta-single-value fail-dsv z* start end))

(define (encode-int*/stats z.min z.max count.run z* start end)
  (define (try-dictionary)
    (let ((bw.n.max (nat-min-byte-width (unsafe-fx- z.max z.min))))
      (define (fail-dictionary)
        (encode-int*/frame-of-reference/byte-width bw.n.max z.min z.max z* start end))
      (if (unsafe-fx< 1 bw.n.max)
          ;; We aggressively limit count.dict.max to guarantee that code width will be 1, and that
          ;; the space savings will be worth the indirect access cost.
          (let ((count.dict.max (unsafe-fxmin (unsafe-fxrshift (unsafe-fx- end start) 2) 256)))
            (let loop ((i start) (z*.unique (set)))
              (if (unsafe-fx< i end)
                  (let* ((z         (unsafe-fxvector-ref z* i))
                         (z*.unique (set-add z*.unique z)))
                    (if (unsafe-fx<= (set-count z*.unique) count.dict.max)
                        (loop (unsafe-fx+ i 1) z*.unique)
                        (fail-dictionary)))
                  (let* ((count.dict (set-count z*.unique))
                         (z*.dict    (make-fxvector count.dict))
                         (i          0))
                    (set-for-each z*.unique (lambda (z)
                                              (unsafe-fxvector-set! z*.dict i z)
                                              (set! i (unsafe-fx+ i 1))))
                    (unsafe-fxvector-sort! z*.dict 0 count.dict)
                    (let ((z=>code (let loop ((z=>code (hash)) (i 0))
                                     (if (unsafe-fx< i count.dict)
                                         (loop (hash-set z=>code (unsafe-fxvector-ref z*.dict i) i)
                                               (unsafe-fx+ i 1))
                                         z=>code))))
                      (let loop ((i start))
                        (when (unsafe-fx< i end)
                          (unsafe-fxvector-set! z* i (hash-ref z=>code (unsafe-fxvector-ref z* i)))
                          (loop (unsafe-fx+ i 1)))))
                    (let-values (((size.dict encode.dict) (encode-int*-ordered-distinct
                                                            z.min z.max z*.dict 0 count.dict)))
                      (values
                        ;; NOTE: revise byte widths and calculation if we ever allow count.dict.max
                        ;; to be larger than 256.
                        (unsafe-fx+ 2 size.dict (unsafe-fx- end start))
                        (lambda (bv pos)
                          (let* ((pos (advance-unsafe-bytes-encoding&width-set!
                                        bv pos encoding.int:dictionary 1))
                                 (pos (advance-unsafe-bytes-set! bv pos (unsafe-fx- count.dict 1)))
                                 (pos (encode.dict bv pos)))
                            (let loop ((pos pos) (i start))
                              (if (unsafe-fx< i end)
                                  (loop (advance-unsafe-bytes-set! bv pos (unsafe-fxvector-ref z* i))
                                        (unsafe-fx+ i 1))
                                  pos))))))))))
          (fail-dictionary))))
  (define (fail-delta-single-value)
    (if (unsafe-fx<= (unsafe-fxlshift count.run 2) (unsafe-fx- end start))
        (let ((n*.pos (make-fxvector count.run)) (z*.run (make-fxvector count.run)))
          (encode-X*-run-length count.run z* start end
                                encoding.int:run-single-length
                                encoding.int:run-length
                                (lambda (z* start end)
                                  (encode-int*/stats z.min z.max count.run z* start end))))
        (try-dictionary)))
  (cond ((unsafe-fx= z.min z.max) (encode-int*/single-value z.min))
        ((and (unsafe-fx= (unsafe-fx- end start) count.run)
              (unsafe-fx= (unsafe-fxvector-ref z* start)              z.min)
              (unsafe-fx= (unsafe-fxvector-ref z* (unsafe-fx- end 1)) z.max)
              (unsafe-fx< 2 count.run))
         (encode-int*/try-delta-single-value fail-delta-single-value z* start end))
        (else (fail-delta-single-value))))

;; Assume z* may be modified.
(define (encode-int* z* start end)
  (let ((z0 (unsafe-fxvector-ref z* start)))
    (let loop ((i (unsafe-fx+ start 1)) (z.min z0) (z.max z0) (z.prev z0) (count.run 1))
      (if (unsafe-fx< i end)
          (let ((z (unsafe-fxvector-ref z* i)))
            (loop (unsafe-fx+ i 1)
                  (unsafe-fxmin z z.min)
                  (unsafe-fxmax z z.min)
                  z
                  (if (unsafe-fx= z z.prev) count.run (unsafe-fx+ count.run 1))))
          (encode-int*/stats z.min z.max count.run z* start end)))))

;; Assume z* may be modified.
(define (encode-int*-baseline z* start end)
  (let ((z0 (unsafe-fxvector-ref z* start)))
    (let loop ((i (unsafe-fx+ start 1)) (z.min z0) (z.max z0))
      (if (unsafe-fx< i end)
          (let ((z (unsafe-fxvector-ref z* i)))
            (loop (unsafe-fx+ i 1) (unsafe-fxmin z z.min) (unsafe-fxmax z z.min)))
          (encode-int*/frame-of-reference z.min z.max z* start end)))))

;;;;;;;;;;;;;;;;;;;
;;; Encode text ;;;
;;;;;;;;;;;;;;;;;;;

;; NOTE: encoders always assume (< start end).  Don't try to encode an empty sequence.

(define (advance-unsafe-bytes-copy!/len bv pos t len)
  (unsafe-bytes-copy! bv pos t 0 len)
  (unsafe-fx+ pos len))

(define (encode-text*-raw t*)
  (let ((len.t* (unsafe-vector*-length t*))
        (len.0  (unsafe-bytes-length (unsafe-vector*-ref t* 0))))
    (if (let loop ((i 1))
          (or (unsafe-fx= i len.t*)
              (and (unsafe-fx= (unsafe-bytes-length (unsafe-vector*-ref t* i)) len.0)
                   (loop (unsafe-fx+ i 1)))))
        (let ((bw.delta (nat-min-byte-width len.0)))
          (values
            (unsafe-fx+ 1 bw.delta (unsafe-fx* len.0 len.t*))
            (lambda (bv pos)
              (let* ((pos (advance-unsafe-bytes-encoding&width-set!
                            bv pos encoding.text:raw-single-length bw.delta))
                     (pos (advance-unsafe-bytes-nat-set!/width bw.delta bv pos len.0)))
                (let loop ((pos pos) (i 0))
                  (if (unsafe-fx< i len.t*)
                      (let ((t (unsafe-vector*-ref t* i)))
                        (loop (advance-unsafe-bytes-copy!/len bv pos t (unsafe-bytes-length t))
                              (unsafe-fx+ i 1)))
                      pos))))))
        (let* ((size (let loop ((size len.0) (i 1))
                       (if (unsafe-fx= i len.t*)
                           size
                           (loop (unsafe-fx+ (unsafe-bytes-length (unsafe-vector*-ref t* i)) size)
                                 (unsafe-fx+ i 1)))))
               (bw.off (nat-min-byte-width size)))
          (values
            (unsafe-fx+ 1 (unsafe-fx* bw.off (unsafe-fx+ len.t* 1)) size)
            (lambda (bv pos)
              (let* ((pos (advance-unsafe-bytes-encoding&width-set!
                            bv pos encoding.text:raw bw.off))
                     (pos (advance-unsafe-bytes-nat-set!/width bw.off bv pos 0)))
                (let loop ((offset     0)
                           (pos.offset pos)
                           (pos        (unsafe-fx+ (unsafe-fx* bw.off len.t*) pos))
                           (i          0))
                  (if (unsafe-fx< i len.t*)
                      (let* ((t      (unsafe-vector*-ref t* i))
                             (len    (unsafe-bytes-length t))
                             (offset (unsafe-fx+ len offset)))
                        (loop offset
                              (advance-unsafe-bytes-nat-set!/width bw.off bv pos.offset offset)
                              (advance-unsafe-bytes-copy!/len bv pos t len)
                              (unsafe-fx+ i 1)))
                      pos)))))))))

(define (encode-text*-single-value t)
  (let* ((len    (unsafe-bytes-length t))
         (bw.len (nat-min-byte-width len)))
    (values
      (unsafe-fx+ 1 bw.len len)
      (lambda (bv pos)
        (let* ((pos (advance-unsafe-bytes-encoding&width-set!
                      bv pos encoding.text:single-value bw.len))
               (pos (advance-unsafe-bytes-nat-set!/width bw.len bv pos len)))
          (advance-unsafe-bytes-copy!/len bv pos t len))))))

(define (encode-text*-dictionary code* start end t*)
  (let* ((count.dict    (unsafe-vector*-length t*))
         ;; Since count.dict can never be 0, we encode it with -1.
         (bw.count.dict (nat-min-byte-width (unsafe-fx- count.dict 1))))
    (let-values (((size.dict encode.dict) (encode-text*-ordered-distinct t*)))
      (values
        (unsafe-fx+ 1 bw.count.dict size.dict (* (unsafe-fx- end start) bw.count.dict))
        (lambda (bv pos)
          (let* ((pos (advance-unsafe-bytes-encoding&width-set!
                        bv pos encoding.text:dictionary bw.count.dict))
                 (pos (advance-unsafe-bytes-nat-set!/width bw.count.dict bv pos
                                                           (unsafe-fx- count.dict 1)))
                 (pos (encode.dict bv pos)))
            (let loop ((pos pos) (i start))
              (if (unsafe-fx< i end)
                  (loop (advance-unsafe-bytes-nat-set!/width bw.count.dict bv pos
                                                             (unsafe-fxvector-ref code* i))
                        (unsafe-fx+ i 1))
                  pos))))))))

;; Assume t* may be modified.
(define (encode-text*-baseline t*) (encode-text*-raw t*))

;; Assume t* may be modified.
(define (encode-text* t*)
  (let* ((len.code* (unsafe-vector*-length t*))
         (code*     (make-fxvector len.code*))
         (bt        (make-btree)))
    (let loop ((i 0))
      (when (unsafe-fx< i len.code*)
        (unsafe-fxvector-set! code* i (btree-ref-or-set! bt (unsafe-vector*-ref t* i)))
        (loop (unsafe-fx+ i 1))))
    (let* ((count.bt   (btree-count bt))
           (t*         (make-vector count.bt))
           (code=>code (make-fxvector count.bt))
           (code.final 0))
      (btree-enumerate
        bt
        (lambda (t code.initial)
          (unsafe-vector*-set! t* code.final t)
          (unsafe-fxvector-set! code=>code code.initial code.final)
          (set! code.final (unsafe-fx+ code.final 1))))
      (let loop ((i 0))
        (when (unsafe-fx< i len.code*)
          (unsafe-fxvector-set! code* i (unsafe-fxvector-ref code=>code
                                                             (unsafe-fxvector-ref code* i)))
          (loop (unsafe-fx+ i 1))))
      (encode-text*/code* #t code* 0 len.code* t*))))

;; Assume t* is sorted and deduplicated, and may also be modified.
(define (encode-text*/code* try-run-length? code* start end t*)
  (let ((len.t*    (unsafe-vector*-length t*))
        (len.code* (unsafe-fx- end start)))
    (define (fail-run-length)
      ;; We aggressively limit len.t* to ensure the space savings are worth the indirect access
      ;; cost of a dictionary encoding.
      (if (unsafe-fx<= (unsafe-fxlshift len.t* 1) len.code*)
          (encode-text*-dictionary code* start end t*)
          ;; TODO: try encoding.text:multi-prefix before falling back to encoding.text:raw
          ;; - multi-prefix
          ;;   - if omitted bytes due to common prefixes is 1/4 total byte size of t*
          ;;   - note: multi-prefix can be used without sorting the final text values
          ;;     - we can use the sorted t* to discover good prefixes, but still use the given
          ;;       text value order
          (let ((t*.raw (make-vector len.code*)))
            (let loop ((i start) (j 0))
              (if (unsafe-fx< i end)
                  (let ((t (unsafe-vector*-ref t* (unsafe-fxvector-ref code* i))))
                    (unsafe-vector*-set! t*.raw j t)
                    (loop (unsafe-fx+ i 1) (unsafe-fx+ j 1)))
                  (encode-text*-raw t*.raw))))))
    (cond ((unsafe-fx= len.t* 1) (encode-text*-single-value (unsafe-vector*-ref t* 0)))
          ;; TODO: check for encoding.text:single-prefix before run-length
          ;; - single-prefix
          ;;   - if length of common prefix of first and last, times (vector-length t*), is
          ;;     1/4 of the total estimated byte size of t*
          ((and try-run-length? (unsafe-fx<= (unsafe-fxlshift len.t* 2) len.code*))
           (let loop ((i         (unsafe-fx+ start 1))
                      (code.prev (unsafe-fxvector-ref code* start))
                      (count.run 1))
             (cond ((unsafe-fx< i end)
                    (let ((code (unsafe-fxvector-ref code* i)))
                      (loop (unsafe-fx+ i 1) code
                            (if (unsafe-fx= code code.prev) count.run (unsafe-fx+ count.run 1)))))
                   ((unsafe-fx<= (unsafe-fxlshift count.run 2) len.code*)
                    (encode-X*-run-length count.run code* start end
                                          encoding.text:run-single-length
                                          encoding.text:run-length
                                          (lambda (code* start end)
                                            (encode-text*/code* #f code* start end t*))))
                   (else (fail-run-length)))))
          (else (fail-run-length)))))

(define (encode-text*-ordered-distinct t*)
  ;; TODO: try encoding.text:multi-prefix before falling back to encoding.text:raw
  ;; - multi-prefix
  ;;   - if omitted bytes due to common prefixes is 1/4 total byte size of t*
  (encode-text*-raw t*))

;; TODO:
;; Input table processing should be able use a single btree, plus a large fxvector buffer that will be used for
;; multiple purposes:
;; - read file data in large chunks and scan for text values
;; - each text value is inserted into the single btree, which provides a corresponding integer code (or id)
;; - write a sequence of initial codes for each element, row by row
;; - build an id=>id remapping of initial codes to order-preserving codes
;;   - then we map id=>id over the full sequence of initial codes
;; - store a sequence of row ids, and some sorting buffer space, then sorting these row ids lexicographically
;;   - each row id, multiplied by the number of columns, points back into the fxvector buffer, plus
;;     column id to offset to access a particular row element
;; - divide our rows into a predetermined number of groups, so for each rows-per-group chunk of rows:
;;   - tranpsose row ids to contiguous columns of codes
;;   - then for each column
;;     - create a separate deduped (already sorted) sequence of the codes
;;     - this will then be used to form a column-specific id=>id remapping of global order-preserving codes to
;;       column-local order-preserving codes
;;     - we then remap the column codes using the new global-to-local id=>id
;;     - we also build an ordered vector t* of just the local text values
;;     - finally, encode the column
;;       - each column in the row group should be written contiguously so that a single read can access the
;;         data for the entire group
;;       - we will also need to store the sequence of positions of each column in the group
;; From this plan, we should be able to calculate the necessary fxvector buffer size:
;; - assume at most 65535 tuples per row group
;; - element-count = (* 65535 group-count column-count) elements
;;   - only (* row-count 2) to reserve space for a sorting buffer since it works on row ids
;;   - but (* element-count 2) worst case to reserve space for id=>id
;;     - this could be pretty large ... maybe we should reduce the number of row groups when the worst case tuple
;;       count applies
;;     - maybe it would be better to specify a worst-case row-count or even element-count directly
;;       - how about (* 65535 100) elements?
;; - only need (* 65535 column-count) for the tranposed contiguous column buffers, plus another 65535 for
;;   local id=>id
