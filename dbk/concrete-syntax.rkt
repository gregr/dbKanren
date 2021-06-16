#lang racket/base
(provide
  current-vocabulary with-no-vocabulary with-formula-vocabulary with-term-vocabulary
  conj disj imply negate all exist fresh conde query
  == =/= any<= any<)
(require "abstract-syntax.rkt"
         (for-syntax racket/base) racket/stxparam)

(define-syntax-parameter current-vocabulary #f)
(define-syntax-rule (with-no-vocabulary      body ...) (syntax-parameterize ((current-vocabulary #f))       body ...))
(define-syntax-rule (with-formula-vocabulary body ...) (syntax-parameterize ((current-vocabulary 'formula)) body ...))
(define-syntax-rule (with-term-vocabulary    body ...) (syntax-parameterize ((current-vocabulary 'term))    body ...))

(define-syntax-rule (define-relation-syntax name relation)
  (define-syntax-rule (name . args) (f:relate relation (with-term-vocabulary
                                                         (map scm->term (list . args))))))

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
