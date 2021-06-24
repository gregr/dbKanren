#lang racket/base
(provide
  (for-syntax current-vocabulary)
  with-no-vocabulary with-formula-vocabulary with-term-vocabulary
  conj disj imply negate all exist fresh conde query
  == =/= any<= any<
  dbk:term dbk:app dbk:apply dbk:cons dbk:list->vector dbk:append dbk:not
  dbk:map/merge dbk:map/append dbk:map dbk:filter dbk:filter-not
  dbk:begin dbk:let dbk:let* dbk:lambda dbk:if dbk:when dbk:unless dbk:cond dbk:and dbk:or
  define-relation define-relation/table define-relation/input)
(require "abstract-syntax.rkt" "relation.rkt"
         (for-syntax racket/base) racket/stxparam)

(define-syntax-parameter stxparam.vocabulary #f)
(begin-for-syntax (define-syntax-rule (current-vocabulary)
                    (syntax-parameter-value #'stxparam.vocabulary)))

(define-syntax-rule (with-no-vocabulary      body ...) (syntax-parameterize ((stxparam.vocabulary #f))       body ...))
(define-syntax-rule (with-formula-vocabulary body ...) (syntax-parameterize ((stxparam.vocabulary 'formula)) body ...))
(define-syntax-rule (with-term-vocabulary    body ...) (syntax-parameterize ((stxparam.vocabulary 'term))    body ...))

(define-syntax-rule (define-alias/relation name arity r.formula r.else)
  (... (define-syntax (name stx)
         (case (current-vocabulary)
           ((formula) (syntax-case stx ()
                        ((_ arg ...) (unless (= arity (length (syntax->list #'(arg ...))))
                                       (raise-syntax-error #f "relation called with invalid number of arguments" stx))
                                     #'(with-term-vocabulary (f:relate r.formula (list (scm->term arg) ...))))))
           (else      (syntax-case stx ()
                        ((_ . args) #'(r.else . args))
                        (_          #'r.else)))))))

(define-syntax-rule (define-primitive-relation name arity)
  (begin (define-syntax (retry stx)
           (syntax-case stx ()
             ((_ . args) #'(with-formula-vocabulary (name . args)))
             (_          #''(primitive name))))
         (define-alias/relation name arity 'name retry)))

(define-primitive-relation ==    2)
(define-primitive-relation =/=   2)
(define-primitive-relation any<= 2)
(define-primitive-relation any<  2)

(define-syntax conj*
  (syntax-rules ()
    ((_)          (== #t #t))
    ((_ f)        f)
    ((_ fs ... f) (f:and (conj* fs ...) f))))

(define-syntax disj*
  (syntax-rules ()
    ((_)          (== #t #f))
    ((_ f)        f)
    ((_ f fs ...) (f:or f (disj* fs ...)))))

(define-syntax-rule (conj   fs ...)  (with-formula-vocabulary (conj*     fs ...)))
(define-syntax-rule (disj   fs ...)  (with-formula-vocabulary (disj*     fs ...)))
(define-syntax-rule (imply  f.h f.c) (with-formula-vocabulary (f:implies f.h f.c)))
(define-syntax-rule (negate f)       (with-formula-vocabulary (f:not     f)))

(define-syntax-rule (define-quantifier-syntax name f:quantifier)
  (... (define-syntax (name stx)
         (syntax-case stx ()
           ((_ (x ...) body ...)
            (with-syntax (((name.x ...) (generate-temporaries #'(x ...))))
              #'(let ((name.x (fresh-name 'x)) ...)
                  (let ((x (t:var name.x)) ...)
                    (f:quantifier (list name.x ...) (conj body ...))))))))))

(define-quantifier-syntax exist f:exist)
(define-quantifier-syntax all   f:all)

(define-syntax-rule (fresh (x ...) body ...) (exist (x ...) body ...))

(define-syntax-rule (conde (f.0 fs.0 ...)
                           (f   fs   ...) ...)
  (disj (conj f.0 fs.0 ...)
        (conj f   fs   ...) ...))

(define-syntax query
  (syntax-rules ()
    ((_ (x ...) body ...) (query x.0 (exist (x ...)
                                       (== x.0 (list x ...))
                                       body ...)))
    ((_ x       body ...) (with-fresh-names
                            (let ((name.x (fresh-name 'x)))
                              (let ((x (t:var name.x)))
                                (t:query name.x (conj body ...))))))))

(define (dbk:term x)          (scm->term x))
(define (dbk:app  p . args)   (t:app (scm->term p) (map scm->term args)))
(define (dbk:cons a d)        (t:cons (scm->term a) (scm->term d)))
(define (dbk:list->vector xs) (t:list->vector (scm->term xs)))

(define-syntax (dbk:apply stx)
  (syntax-case stx ()
    ((_ . args) #'(t:app (t:prim 'apply) (map scm->term (list args))))
    (_          #'(t:prim 'apply))))

(define-syntax-rule (define-lambda (name params ...) body)
  (define-syntax (name stx)
    (syntax-case stx ()
      ((_ . args) #'(dbk:app (dbk:lambda (params ...) body) . args))
      (_          #'(dbk:lambda (params ...) body)))))

(define-lambda (dbk:not        x)                  (dbk:if x #f #t))
(define-lambda (dbk:append     xs ys)              (t:append (scm->term xs) (scm->term ys)))
(define-lambda (dbk:map/merge  f merge default xs) (apply t:map/merge (map scm->term (list f merge default xs))))
(define-lambda (dbk:map/append f               xs) (dbk:map/merge  f (dbk:lambda (a b) (dbk:append a b)) '()            xs))
(define-lambda (dbk:map        f               xs) (dbk:map/append (dbk:lambda (x) (list (dbk:app f x)))                xs))
(define-lambda (dbk:filter     p               xs) (dbk:map/append (dbk:lambda (x) (dbk:if (dbk:app p x) (list x) '())) xs))
(define-lambda (dbk:filter-not p               xs) (dbk:filter     (dbk:lambda (x) (dbk:not (dbk:app p x)))             xs))

(define-syntax dbk:begin
  (syntax-rules ()
    ((_)          (dbk:term (void)))
    ((_ e)        (dbk:term e))
    ((_ e es ...) (dbk:let ((temp.begin (dbk:term e))) es ...))))

(define-syntax dbk:let
  (syntax-rules ()
    ((_ ((x e) ...) body ...) (let ((name.x (fresh-name 'x)) ...)
                                (let ((x (t:var name.x)) ...)
                                  (with-term-vocabulary
                                    (t:let (list (cons name.x (dbk:term e)) ...)
                                           (dbk:begin body ...))))))))

(define-syntax dbk:let*
  (syntax-rules ()
    ((_ ()                  body ...) (dbk:let ()                              body ...))
    ((_ ((x e) (xs es) ...) body ...) (dbk:let ((x e)) (dbk:let* ((xs es) ...) body ...)))))

(define-syntax (dbk:lambda stx)
  (syntax-case stx ()
    ((_ (x ...)     body ...)
     (with-syntax (((name.x ...) (generate-temporaries #'(x ...))))
       #'(let ((name.x (fresh-name 'x)) ...)
           (let ((x (t:var name.x)) ...)
             (with-term-vocabulary
               (t:lambda (list  name.x ...)        (dbk:begin body ...)))))))
    ((_ (x ... . y) body ...)
     (with-syntax (((name.x ...) (generate-temporaries #'(x ...))))
       #'(let ((name.y (fresh-name 'y)) (name.x (fresh-name 'x)) ...)
           (let ((x (t:var name.x)) ...)
             (with-term-vocabulary
               (t:lambda (list* name.x ... name.y) (dbk:begin body ...)))))))))

(define-syntax-rule (dbk:if     c t f)      (t:if (dbk:term c) (dbk:term t) (dbk:term f)))
(define-syntax-rule (dbk:when   c body ...) (dbk:if c (dbk:begin body ...) (dbk:term (void))))
(define-syntax-rule (dbk:unless c body ...) (dbk:if c (dbk:term (void))    (dbk:begin body ...)))

(define-syntax (dbk:cond stx)
  (syntax-case stx (else =>)
    ((_)                      #'(dbk:term (void)))
    ((_ (else e ...))         #'(dbk:begin e ...))
    ((_ (else e ...) etc ...) (raise-syntax-error #f "misplaced else" stx))
    ((_ (c => p) cs ...)      #'(dbk:let ((x c)) (dbk:if x (dbk:app   p c)   (dbk:cond cs ...))))
    ((_ (c e ...) cs ...)     #'(                 dbk:if c (dbk:begin e ...) (dbk:cond cs ...)))
    ((_ c cs ...)             #'(dbk:let ((x c)) (dbk:if x x                 (dbk:cond cs ...))))))

(define-syntax dbk:and
  (syntax-rules ()
    ((_)          (dbk:term #t))
    ((_ e)        (dbk:term e))
    ((_ e es ...) (dbk:if e (dbk:and es ...) (dbk:term #f)))))

(define-syntax dbk:or
  (syntax-rules ()
    ((_)          (dbk:term #f))
    ((_ e)        (dbk:term e))
    ((_ e es ...) (dbk:let ((temp.or e)) (dbk:if temp.or temp.or (dbk:or es ...))))))

(define-syntax-rule (define-relation-syntax relation-name params r)
  (... (begin
         (define-alias/relation relation-name (length 'params) r r)
         (relation-properties-set! r 'name       'relation-name)
         (relation-properties-set! r 'attributes 'params))))

(define-syntax-rule (define-relation (name param ...) f ...)
  (begin (define name:relation (relation/rule (length '(param ...))
                                              (lambda (param ...) (with-formula-vocabulary (conj f ...)))))
         (define-relation-syntax name (param ...) name:relation)
         (relation-properties-set! name:relation 'rule '((name param ...) :- f ...))))

(define-syntax-rule (define-relation/table (name param ...) path)
  (begin (define name:relation (relation/table (length '(param ...)) path))
         (define-relation-syntax name (param ...) name:relation)))

(define-syntax-rule (define-relation/input (name param ...) produce)
  (begin (define name:relation (relation/input (length '(param ...)) produce))
         (define-relation-syntax name (param ...) name:relation)))

;; TODO: define-term
