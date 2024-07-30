(relation* +o *o edge path appendo)
(term* append + * some-constant)


;; Both terms and relations can use define.  They are syntactically distinguished:
;; - term definition:     (define ,identifier ,term ,formula ...)
;; - relation definition: (define (,identifier ,parameter ...) ,formula ...)

(define (appendo x* y x*y)
  (conde
    ((== x* '()) (== x*y* y))
    ((exist (w w* w*y)
       (== x*   (cons w w*))
       (== x*y (cons w w*y))
       (appendo w* y w*y)))))

(define some-constant '(this is a constant term))


quantification might be broken for bi-implication


(define append
  (case-lambda
    ((a)             a)
    ((a b)           c (appendo a b c))
    ((a b c . rest*) (append a (append b c . rest*)))))

(define +
  (case-lambda
    ((a)             a)
    ((a b)           c (+o a b c))
    ((a b c . rest*) (+ (+ a b) c . rest*))))

(define map
  (lambda (f x*)
    y*
    (local
      ((rule*
         ((mapo '() '()))
         ((mapo (cons a a*) (cons (f a) b*))
          (mapo a* b*))))
      (mapo x* y*))))

(define another-constant (map (lambda (x) (+ x 1)) '(6 7 8)))

(fact*
  (edge
    (a b)
    (a c)
    (a d)
    (b e)
    (c e)
    (d f)
    (e g)))

(rule*
  ((path a b) (edge a b))
  ((path a c) (path a b) (path b c))
  ;; another way to define appendo
  ((appendo '() y y))
  ((appendo (cons w w*) y (cons w w*y))
   (appendo w* y w*y))
  ;; we can also add more facts as rules
  ((edge 'g some-constant)))

;; NOTE: if implicit quantification seems unpleasant, but still want the convenient notation of
;; parameter patterns, we can instead add outer universal quantifiers.
;; For instance:
;
;(all (c y* a* b* w w* w*y)  ; These are all the local variables mentioned in any rule or definition.
;  (define append
;    (case-lambda
;      ((a)             a)
;      ((a b)           c (appendo a b c))
;      ((a b c . rest*) (append a (append b c . rest*)))))
;
;  (define +
;    (case-lambda
;      ((a)             a)
;      ((a b)           c (+o a b c))
;      ((a b c . rest*) (+ (+ a b) c . rest*))))
;
;  (define map
;    (lambda (f x*)
;      y*
;      (local
;        ((rule*
;           ((mapo '() '()))
;           ((mapo (cons a a*) (cons (f a) b*))
;            (mapo a* b*))))
;        (mapo x* y*))))
;
;  (rule*
;    ((path a b) (edge a b))
;    ((path a c) (path a b) (path b c))
;    ;; another way to define appendo
;    ((appendo '() y y))
;    ((appendo (cons w w*) y (cons w w*y))
;     (appendo w* y w*y))
;    ;; we can also add more facts as rules
;    ((edge 'g some-constant))
;    )
;
;  ;; etc.
;  )
