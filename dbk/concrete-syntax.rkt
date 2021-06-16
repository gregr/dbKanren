#lang racket/base
(provide
  current-vocabulary with-no-vocabulary with-formula-vocabulary with-term-vocabulary
  conj disj imply negate all exist fresh conde query
  == =/= any<= any<
  dbk:term dbk:app dbk:cons dbk:list->vector
  dbk:if dbk:when dbk:unless dbk:cond dbk:begin dbk:let dbk:let* dbk:lambda dbk:quasiquote)
(require "abstract-syntax.rkt"
         (for-syntax racket/base) racket/stxparam)

(define-syntax-parameter current-vocabulary #f)
(define-syntax-rule (with-no-vocabulary      body ...) (syntax-parameterize ((current-vocabulary #f))       body ...))
(define-syntax-rule (with-formula-vocabulary body ...) (syntax-parameterize ((current-vocabulary 'formula)) body ...))
(define-syntax-rule (with-term-vocabulary    body ...) (syntax-parameterize ((current-vocabulary 'term))    body ...))

(define-syntax-rule (define-relation-syntax name relation)
  (... (define-syntax-rule (name arg ...) (f:relate relation (with-term-vocabulary
                                                               (list (scm->term arg) ...))))))

(define-relation-syntax ==    '(prim ==))
(define-relation-syntax =/=   '(prim =/=))
(define-relation-syntax any<= '(prim any<=))
(define-relation-syntax any<  '(prim any<))

(define (conj . fs)
  (if (null? fs)
    (== #t #t)
    (foldl (lambda (f2 f1) (f:and f1 f2)) (car fs) (cdr fs))))

(define (disj . fs)
  (if (null? fs)
    (== #t #f)
    (let loop ((f (car fs)) (fs (cdr fs)))
      (if (null? fs)
        f
        (f:or f (loop (car fs) (cdr fs)))))))

(define (imply  f.h f.c) (f:implies f.h f.c))
(define (negate f)       (f:not     f))

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

(define-syntax-rule (conde (f.0 fs.0 ...) (f fs ...) ...)
  (disj (conj f.0 fs.0 ...) (conj f fs ...) ...))

(define-syntax query
  (syntax-rules ()
    ((_ (x ...) body ...) (query x.0 (exist (x ...)
                                       (== x.0 (list x ...))
                                       body ...)))
    ((_ x       body ...) (with-fresh-names
                            (let ((name.x (fresh-name 'x)))
                              (let ((x (t:var name.x)))
                                (t:query name.x (with-formula-vocabulary
                                                  (conj body ...)))))))))

(define (dbk:term x)          (scm->term x))
(define (dbk:app  p . args)   (t:app  (scm->term p) (map scm->term args)))
(define (dbk:cons a d)        (t:cons (scm->term a) (scm->term d)))
(define (dbk:list->vector xs) (t:list->vector (scm->term xs)))

(define-syntax dbk:let
  (syntax-rules ()
    ((_ ((x e) ...) body ...) (let ((name.x (fresh-name 'x)) ...)
                                (let ((x (t:var name.x)) ...)
                                  (with-term-vocabulary
                                    (t:let (list (cons name.x (dbk:term e)) ...)
                                           (dbk:begin body ...))))))))

(define-syntax dbk:begin
  (syntax-rules ()
    ((_)          (dbk:term (void)))
    ((_ e)        (dbk:term e))
    ((_ e es ...) (dbk:let ((temp.begin (dbk:term e))) es ...))))

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

(define-syntax dbk:let*
  (syntax-rules ()
    ((_ ()                  body ...) (dbk:let ()                              body ...))
    ((_ ((x e) (xs es) ...) body ...) (dbk:let ((x e)) (dbk:let* ((xs es) ...) body ...)))))

(define-syntax (dbk:quasiquote/level stx)
  (syntax-case stx (quasiquote unquote unquote-splicing)
    ;; TODO: relational unquote-splicing
    ((_ level   (quasiquote q))   #'(dbk:term (list 'quasiquote (dbk:quasiquote/level (level) q))))
    ((_ ()      (unquote    e))   #'(dbk:term e))
    ((_ (level) (unquote    q))   #'(dbk:term (list 'unquote    (dbk:quasiquote/level level   q))))
    ((_ level   (q.a . q.d))      #'(dbk:cons                   (dbk:quasiquote/level level   q.a)
                                                                (dbk:quasiquote/level level   q.d)))
    ((_ level   #(q ...))         #'(dbk:list->vector           (dbk:quasiquote/level level   (q ...))))
    ((_ level   quasiquote)       (raise-syntax-error #f "misplaced quasiquote"       stx))
    ((_ level   unquote)          (raise-syntax-error #f "misplaced unquote"          stx))
    ((_ level   unquote-splicing) (raise-syntax-error #f "misplaced unquote-splicing" stx))
    ((_ level   q)                #'(dbk:term (quote q)))))

(define-syntax-rule (dbk:quasiquote q) (dbk:quasiquote/level () q))
