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
  )
(require (for-syntax racket/base) racket/fixnum racket/list racket/set racket/vector)
;; NOTE: decoding corrupt or malicious data is currently a memory safety risk when using racket/unsafe/ops.
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
       (let* ((count.dict (unsafe-bytes-nat-ref/width byte-width bv pos.bv))
              (pos.bv     (unsafe-fx+ pos.bv byte-width)))
         (let*-values (((col.dict pos.bv) (column:encoding.int&end bv pos.bv count.dict))
                       ((col.code pos.bv) (column:encoding.int&end bv pos.bv count)))
           (values (column:encoding.int:dictionary col.dict count.dict col.code count)
                   pos.bv))))
      ((? encoding.int:run-length)
       (let* ((count.run (unsafe-bytes-nat-ref/width byte-width bv pos.bv))
              (pos.bv    (unsafe-fx+ pos.bv byte-width)))
         (let*-values (((col.pos pos.bv) (column:encoding.int&end bv pos.bv (unsafe-fx+ count.run 1)))
                       ((col.run pos.bv) (column:encoding.int&end bv pos.bv count.run)))
           (values (column:encoding.int:run-length col.pos col.run count.run count)
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
       (let-values (((col.pos pos.bv) (column:encoding.int&end bv pos.bv (unsafe-fx+ count 1))))
         (values (column:encoding.text:raw col.pos bv pos.bv count)
                 ((column-ref col.pos) count))))
      ((? encoding.text:dictionary)
       (let* ((count.dict (unsafe-bytes-nat-ref/width byte-width bv pos.bv))
              (pos.bv     (unsafe-fx+ pos.bv byte-width)))
         (let*-values (((col.dict pos.bv) (column:encoding.text&end bv pos.bv count.dict))
                       ((col.code pos.bv) (column:encoding.int&end  bv pos.bv count)))
           (values (column:encoding.text:dictionary col.dict count.dict col.code count)
                   pos.bv))))
      ((? encoding.text:run-length)
       (let* ((count.run (unsafe-bytes-nat-ref/width byte-width bv pos.bv))
              (pos.bv    (unsafe-fx+ pos.bv byte-width)))
         (let*-values (((col.pos pos.bv) (column:encoding.int&end bv pos.bv (unsafe-fx+ count.run 1)))
                       ((col.run pos.bv) (column:encoding.text&end bv pos.bv count.run)))
           (values (column:encoding.text:run-length col.pos col.run count.run count)
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
       (let* ((len.single-prefix (unsafe-bytes-nat-ref/width byte-width bv pos.bv))
              (pos.bv            (unsafe-fx+ pos.bv byte-width))
              (single-prefix     (let ((t (make-bytes len.single-prefix)))
                                   (unsafe-bytes-copy! t 0 bv pos.bv
                                                       (unsafe-fx+ pos.bv len.single-prefix))
                                   t))
              (pos.bv            (unsafe-fx+ pos.bv len.single-prefix)))
         (let-values (((col.suffix pos.bv) (column:encoding.text&end bv pos.bv count)))
           (values (column:encoding.text:single-prefix single-prefix col.suffix count)
                   pos.bv))))
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

;;;;;;;;;;;;;;
;;; Encode ;;;
;;;;;;;;;;;;;;

;; NOTE: encoders always assume (< start end).  Don't try to encode an empty sequence.

(define (advance-unsafe-bytes-encoding&width-set! bv pos encoding width)
  (unsafe-bytes-set! bv pos (unsafe-fxior (unsafe-fxlshift encoding 4) width))
  (unsafe-fx+ pos 1))

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
              (when (unsafe-fx< i end)
                (let ((n (unsafe-fx- (unsafe-vector*-ref z* i) z.min)))
                  (loop (unsafe-fx+ i 1)
                        (advance-unsafe-bytes-int-set!/width bw.n.max bv pos n))))))))))
  (define (use-nat)
    (values
      (unsafe-fx+ 1 (* (unsafe-fx- end start) bw.n.max))
      (lambda (bv pos)
        (let ((pos (advance-unsafe-bytes-encoding&width-set! bv pos encoding.int:nat bw.n.max)))
          (let loop ((i start) (pos pos))
            (when (unsafe-fx< i end)
              (let ((n (unsafe-vector*-ref z* i)))
                (loop (unsafe-fx+ i 1)
                      (advance-unsafe-bytes-nat-set!/width bw.n.max bv pos n)))))))))
  (define (use-int)
    (values
      (unsafe-fx+ 1 (* (unsafe-fx- end start) bw.n.max))
      (lambda (bv pos)
        (let ((pos (advance-unsafe-bytes-encoding&width-set! bv pos encoding.int:int bw.n.max)))
          (let loop ((i start) (pos pos))
            (when (unsafe-fx< i end)
              (let ((z (unsafe-vector*-ref z* i)))
                (loop (unsafe-fx+ i 1)
                      (advance-unsafe-bytes-int-set!/width bw.n.max bv pos z)))))))))
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
          (unsafe-bytes-int-set!/width bw bv pos z))))))

(define (encode-int*/try-delta-single-value fail z* start end)
  (let* ((z0    (unsafe-vector*-ref z* start))
         (z1    (unsafe-vector*-ref z* (unsafe-fx+ start 1)))
         (delta (unsafe-fx- z1 z0)))
    (let loop ((i (unsafe-fx+ start 2)) (z.prev z1))
      (let ((z (unsafe-vector*-ref z* i)))
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
                          (unsafe-bytes-int-set!/width bw.delta bv pos delta)))))))
            (fail))))))

(define (encode-int*/strictly-increasing z.min z.max z* start end)
  (define (fail-dsv) (encode-int*/frame-of-reference z.min z.max z* start end))
  (if (unsafe-fx= (unsafe-fx- end start) 1)
      (encode-int*/single-value z.min)
      (encode-int*/try-delta-single-value fail-dsv z* start end)))

(define (encode-int*/stats z.min z.max run-count z* start end)
  (define (try-dictionary)
    (let ((bw.n.max (nat-min-byte-width (unsafe-fx- z.max z.min))))
      (define (fail-dictionary)
        (encode-int*/frame-of-reference/byte-width bw.n.max z.min z.max z* start end))

      ;; TODO: only collect distinct elements up to minimum byte-width improvement, and if count is large enough
      ;; - which is only applicable if bw.n.max > 1
      (if (unsafe-fx< 1 bw.n.max)

          (let ((count.dict.max (unsafe-fxmin (unsafe-fxrshift (unsafe-fx- end start) 2) 255)))  ; guarantees that bw.code is 1

            (let loop ((i start) (z*.dict (set)))
              (if (unsafe-fx< i end)
                  (let* ((z       (unsafe-vector*-ref z* i))
                         (z*.dict (set-add z*.dict z)))
                    (if (unsafe-fx<= (set-count z*.dict) count.dict.max)
                        (loop (unsafe-fx+ i 1) z*.dict)
                        ;; TODO: otherwise, fail
                        (fail-dictionary)  ; do we really need these args?
                        ))
                  ;; TODO: otherwise, success
                  ;; - sort z*.dict, map z=>code, transform z* into code*, and encode both sorted z*.dict and code*
                  ;; - encode-int*/frame-of-reference for code*
                  ;; - encode-int*/strictly-increasing for z*.dict
                  (error "TODO")
                  )))
          (fail-dictionary))

      ;; TODO: dict column itself should never be worth encoding with:
      ;;   run-length, dictionary, single-value
      ;; but it could be nat, int, frame-of-reference, or delta-single-value
      ;; so can we use a faster analysis method? encode-int-dictionary*! ?

      ;; TODO: a dict codes column should always be encoded with nat
      ;; - otherwise a different encoding would have been chosen for the original column
      ;; - note, this is not necessarily true for multi-prefix codes, which could also
      ;;   benefit from run-length encoding
      ;;   - or we could not look at the prefixes as codes, and treat them as text values that
      ;;     are subject to encoding in the usual way, in which case the run-length encoding
      ;;     is really happening at the text value level, not the code level

      ;; TODO: multi-prefix encoding is really a concatenation of two text columns

      ))

  (define (fail-delta-single-value)
    (if (unsafe-fx<= (unsafe-fxlshift run-count 2) (unsafe-fx- end start))
        (let ((n*.pos (make-vector run-count)) (z*.run (make-vector run-count)))
          ;; TODO: position column should never be worth encoding with:
          ;;   run-length, dictionary, single-value, int, frame-of-reference
          ;; but it could be either nat or delta-single-value
          ;; so can we use a faster analysis method? encode-nat-increasing-sequence*! ?

          ;; TODO: run column should be encoded with:
          ;; (encode-int*/stats z.min z.max run-count z*.run start end)
          ;; where run-count is equal to (- end start), and z.min z.max are the same
          (error "TODO"))
        (try-dictionary)))
  (cond ((unsafe-fx= z.min z.max) (encode-int*/single-value z.min))
        ((and (unsafe-fx= (unsafe-fx- end start) run-count)
              (unsafe-fx= (unsafe-vector*-ref z* start)              z.min)
              (unsafe-fx= (unsafe-vector*-ref z* (unsafe-fx- end 1)) z.max)
              (unsafe-fx< 2 run-count))
         (encode-int*/try-delta-single-value fail-delta-single-value z* start end))
        (else (fail-delta-single-value))))

(define (encode-int* z* start end)
  (let ((z0 (unsafe-vector*-ref z* start)))
    (let loop ((i (unsafe-fx+ start 1)) (z.min z0) (z.max z0) (z.prev z0) (run-count 1))
      (if (unsafe-fx< i end)
          (let ((z (unsafe-vector*-ref z* i)))
            (loop (unsafe-fx+ i 1)
                  (unsafe-fxmin z z.min)
                  (unsafe-fxmax z z.min)
                  z
                  (if (unsafe-fx= z z.prev) run-count (unsafe-fx+ run-count 1))))
          (encode-int*/stats z.min z.max run-count z* start end)))))

(define (encode-int*-baseline z* start end)
  (let ((z0 (unsafe-vector*-ref z* start)))
    (let loop ((i (unsafe-fx+ start 1)) (z.min z0) (z.max z0))
      (if (unsafe-fx< i end)
          (let ((z (unsafe-vector*-ref z* i)))
            (loop (unsafe-fx+ i 1) (unsafe-fxmin z z.min) (unsafe-fxmax z z.min)))
          (encode-int*/frame-of-reference z.min z.max z* start end)))))

(define (encode-text*-baseline t*) (encode-text*-raw t*))

#;(define (encode-text* t*)
  ;- won't be commonly used since we'll likely build a btree / dictionary as input is processed
  ;- still could be useful for testing
  ;- dispatch to encode-text/code* by building a btree, and producing code* and ordered-distinct t*
  )

#;(define (encode-text*/code* code* start end t*)
  ;- general case for code* consuming encoders
  ;  - code* is not necessarily sorted, and possibly with duplicates
  ;  - t* is always sorted and deduplicated
  ;- try encodings in this order:
  ;  - single-value
  ;    - if (- end start) is 1
  ;  - single-prefix
  ;    - if length of common prefix of first and last, times (- end start), is 1/4 total byte size of t*
  ;  - run-length
  ;    - if run-count is 4 times smaller than count.code*
  ;  - dictionary
  ;    - if (- end start) is 2 times smaller than count.code*
  ;  - multi-prefix
  ;    - if omitted bytes due to common prefixes is 1/4 total byte size of t*
  ;    - note: multi-prefix can be used without sorting the final text values
  ;      - we can use the sorted t* to discover good prefixes, but still use the given text value order
  ;  - raw
  )

#;(define (encode-text*-ordered-distinct t*)
  ;- i.e., sorted and no duplicates, like a dictionary itself
  ;  - so dictionary and run-length encodings won't help
  ;- if (- end start) is 1, we have a single-value
  ;- otherwise, these encodings can make sense: raw, single-prefix, or multi-prefix
  ;- try encodings in this order:
  ;  - single-value
  ;  - single-prefix
  ;  - multi-prefix
  ;  - raw
  )

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
                        (unsafe-bytes-copy! bv pos t 0 (unsafe-bytes-length t))
                        (loop (unsafe-fx+ (unsafe-bytes-length t) pos) (unsafe-fx+ i 1)))
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
              (let* ((pos (advance-unsafe-bytes-encoding&width-set! bv pos encoding.text:raw bw.off))
                     (pos (advance-unsafe-bytes-nat-set!/width bw.off bv pos 0)))
                (let loop ((offset     0)
                           (pos.offset pos)
                           (pos        (unsafe-fx+ (unsafe-fx* bw.off len.t*) pos))
                           (i          0))
                  (if (unsafe-fx< i len.t*)
                      (let* ((t      (unsafe-vector*-ref t* i))
                             (len    (unsafe-bytes-length t))
                             (offset (unsafe-fx+ len offset)))
                        (unsafe-bytes-copy! bv pos t 0 len)
                        (loop offset
                              (advance-unsafe-bytes-nat-set!/width bw.off bv pos.offset offset)
                              (unsafe-fx+ pos len)
                              (unsafe-fx+ i 1)))
                      pos)))))))))
