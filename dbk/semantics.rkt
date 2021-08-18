#lang racket/base
(provide simplify-program factor-program)
(require racket/list racket/match racket/set)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Grammar
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Program parts
;(define (R Ns) Fs)
;(query  Ns     Fs)

;; Formulas F
;(relate R Ts)
;(not    F)
;(and    Fs)
;(or     Fs)
;(imply  F F)
;(iff    Fs)
;(exist  Ns F)
;(all    Ns F)

;; Terms T
;(quote C)
;(var   N)
;(app   Func Ts)

(define (quote?      t) (eq? (car t) 'quote))
(define (quote-value t) (cadr t))

(define (var      n) `(var ,n))
(define (var?     t) (eq? (car t) 'var))
(define (var-name t) (cadr t))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Simplification
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; - classical transformations:
;;   - `(not (not F))  ==> F`
;;   - `(iff P Q)      ==> (or (and P Q) (and (not P) (not Q)))`
;;   - `(imply P Q)    ==> (not (and P (not Q)))`
;;   - `(all X F)      ==> (not (exist X (not F)))`

;; - constructive/minimal transformations:
;;   - `(not (not (not F))) ==> (not F)`
;;   - `(not (or P Q))      ==> (and (not P) (not Q))`
;;   - `(iff P Q)           ==> (and (imply P Q) (imply Q P))`
;;   - `(imply P (not Q))   ==> (not (and P Q))`
;;   - `(all X (not F))     ==> (not (exist X F))`

;; - primitive transformations:
;;   - `(not (==    A B)) ==> (=/=   A B)`
;;   - `(not (=/=   A B)) ==> (==    A B)`
;;   - `(not (any<= A B)) ==> (any<  B A)`
;;   - `(not (any<  A B)) ==> (any<= B A)`

(define (simplify-program full? parts)
  (map (lambda (part)
         (match part
           (`(define (,r . ,params) ,f) `(define (,r . ,params) ,(simplify-formula full? f)))
           (`(query  ,params        ,f) `(query  ,params        ,(simplify-formula full? f)))))
       parts))

(define (simplify-formula full? formula)  ; currently applying classical transformations
  (define (loop/not f)
    (match f
      (`(relate      ,r ,@ts)    (let ((ts (map simplify-term ts)))
                                   (define (k r ts) `(relate ,r . ,ts))
                                   (match r
                                     ('==           (k '=/=            ts))
                                     ('=/=          (k '==             ts))
                                     ('any<=        (k 'any<  (reverse ts)))
                                     ('any<         (k 'any<= (reverse ts)))
                                     (_      `(not ,(k r               ts))))))
      (`(all         ,params ,f)      `(exist ,params ,(loop/not f)))
      (`(exist       ,params ,f) `(not (exist ,params ,(loop     f))))
      (`(not         ,f)         (loop f))
      (`(and         ,@fs)       `(not (and . ,(map loop fs))))
      (`(or          ,@fs)       `(and . ,(map loop/not fs)))
      (`(iff         ,@fs)       (if full? ; defer iff transformation until after factoring, for efficiency
                                   (loop/not `(or (and . ,fs)
                                                  (and . ,(map (lambda (f) `(not ,f)) fs))))
                                   `(not (iff . ,(map loop fs)))))
      (`(imply       ,p ,q)      (loop/not `(not (and ,p (not ,q)))))))
  (define (loop f)
    (match f
      (`(relate      ,r ,@ts)    `(relate ,r . ,(map simplify-term ts)))
      (`(all         ,params ,f) `(not (exist ,params ,(loop/not f))))
      (`(exist       ,params ,f)      `(exist ,params ,(loop     f)))
      (`(not         ,f)         (loop/not f))
      (`(and         ,@fs)       `(and . ,(map loop fs)))
      (`(or          ,@fs)       `(or  . ,(map loop fs)))
      (`(iff         ,@fs)       (if full? ; defer iff transformation until after factoring, for efficiency
                                   (loop `(or (and . ,fs)
                                              (and . ,(map (lambda (f) `(not ,f)) fs))))
                                   `(iff . ,(map loop fs))))
      (`(imply       ,p ,q)      (loop `(not (and ,p (not ,q)))))))
  (loop formula))

(define (simplify-term term)
  (match term
    (`(app ,func ,@ts)
      (match (cons func (map simplify-term ts))
        (`(cons         (quote ,a) (quote ,d))                                  `(quote ,(cons a d)))
        (`(vector       . ,(? (lambda (ts) (andmap quote? ts)) ts))             `(quote ,(list->vector (map quote-value ts))))
        (`(list->vector ,(? quote? (? (lambda (t) (list? (quote-value t))) t))) `(quote ,(list->vector (quote-value t))))
        (`(,_           . ,ts) `(app ,func . ,ts))))
    (_ term)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Program factoring via definition introduction
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (factor-program parts)
  (factor-locally
    (define factored (map (lambda (part)
                            (match part
                              (`(define (,r . ,params) ,f) `(define (,r . ,params) ,(factor-formula f)))
                              (`(query  ,params        ,f) `(query  ,params                   ,(factor-formula f)))))
                          parts))
    (define new      (map (lambda (f&r)
                            (match-define (cons f `(relate ,r . ,vs)) f&r)
                            `(define (,r . ,(map (lambda (v) (match-define `(var ,name) v) name) vs)) ,f))
                          (sort (hash->list (formula=>relate))
                                (lambda (kv.a kv.b)
                                  (define (kv-r kv) (cadr (caddr kv)))
                                  (< (kv-r kv.a) (kv-r kv.b))))))
    (append new factored)))

(define (factor-formula formula)
  (define (replace f) (rename-locally
                        (formula-unrename-variables (formula->relate (formula-rename-variables f)))))
  (match formula
    (`(relate                             ,r ,@ts)    (define f.new `(relate ,r . ,ts))
                                                      (if (foldl (lambda (t seen) (and seen (var? t) (not (set-member? seen (var-name t)))
                                                                                       (set-add seen (var-name t))))
                                                                 (set) ts)
                                                        f.new
                                                        (replace f.new)))
    (`(,(and (or 'exist 'all) quantifier) ,params ,f) (replace `(,quantifier ,params ,(factor-formula f))))
    (`(,connective                        ,@fs)       (replace `(,connective . ,(map factor-formula fs))))))

(define-syntax-rule (factor-locally body ...) (parameterize ((formula=>relate (hash))) body ...))

(define formula=>relate (make-parameter #f))

(define (formula->relate f)
  (define f=>r (formula=>relate))
  (or (hash-ref f=>r f #f)
      (let* ((count (hash-count f=>r))
             (r     `(relate (new ,count) . ,(map var (remove-duplicates (formula-free-names f))))))
        (formula=>relate (hash-set f=>r f r))
        r)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Variable manipulation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (term-free-names t)
  (match t
    (`(quote ,_)         '())
    (`(var   ,name)      (list name))
    (`(app   ,func ,@ts) (append* (map term-free-names ts)))))

(define (formula-free-names f)
  (match f
    (`(relate            ,r ,@ts)    (append* (map term-free-names ts)))
    (`(,(or 'exist 'all) ,params ,f) (define bound (list->set params))
                                     (filter-not (lambda (n) (set-member? bound n))
                                                 (formula-free-names f)))
    (`(,connective       ,@fs)       (append* (map formula-free-names fs)))))

(define (order-parameters params ordered-names)
  (let loop ((ordered-names ordered-names) (params (list->set params)))
    (match ordered-names
      ('()                       '())
      ((cons name ordered-names) (if (set-member? params name)
                                   (cons name (loop ordered-names (set-remove params name)))
                                   (loop ordered-names params))))))

(define-syntax-rule (rename-locally body ...) (parameterize ((name=>renamed (hash))
                                                             (renamed=>name (hash)))
                                                body ...))

(define name=>renamed (make-parameter #f))
(define renamed=>name (make-parameter #f))

(define (rename name)
  (define n=>n (name=>renamed))
  (or (hash-ref n=>n name #f)
      (let ((count (hash-count n=>n)))
        (name=>renamed (hash-set n=>n name count))
        (renamed=>name (hash-set (renamed=>name) count name))
        count)))

(define (unrename name) (hash-ref (renamed=>name) name))

(define (term-rename-variables term)
  (match term
    (`(quote ,_)       term)
    (`(var ,name)      `(var ,(rename name)))
    (`(app ,func ,@ts) `(app ,func . ,(map term-rename-variables ts)))))

(define (term-unrename-variables term)
  (match term
    (`(quote ,_)       term)
    (`(var ,name)      `(var ,(unrename name)))
    (`(app ,func ,@ts) `(app ,func . ,(map term-unrename-variables ts)))))

(define (formula-rename-variables formula)
  (match formula
    (`(relate                             ,r ,@ts)    `(relate      ,r . ,(map term-rename-variables ts)))
    (`(,(and (or 'exist 'all) quantifier) ,params ,f) (define free-names (formula-free-names f))
                                                      (for-each rename free-names)  ; allocate free names before bound names for readability
                                                      `(,quantifier ,(map rename (order-parameters params free-names))
                                                                    ,(formula-rename-variables f)))
    (`(,connective                        ,@fs)       `(,connective . ,(map formula-rename-variables fs)))))

(define (formula-unrename-variables formula)
  (match formula
    (`(relate                             ,r ,@ts)    `(relate      ,r . ,(map term-unrename-variables ts)))
    (`(,(and (or 'exist 'all) quantifier) ,params ,f) `(,quantifier ,(map unrename params)
                                                                    ,(formula-unrename-variables f)))
    (`(,connective                        ,@fs)       `(,connective . ,(map formula-unrename-variables fs)))))
