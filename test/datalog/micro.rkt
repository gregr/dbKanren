#lang racket/base
(provide run-datalog)
(require racket/match)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Terms and substitution ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(struct var (name) #:prefab)
(define subst.empty '())

(define (subst-extend S x t)
  (and (not (occurs? S (walk S x) t))
       (cons (cons (var-name x) t) S)))

(define (walk S t)
  (cond ((var? t) (let ((kv (assoc (var-name t) S)))
                    (if kv (walk S (cdr kv)) t)))
        (else     t)))

(define (walk* S t)
  (cond ((var? t)    (let ((kv (assoc (var-name t) S)))
                       (if kv (walk* S (cdr kv)) t)))
        ((pair? t)   (cons (walk* S (car t)) (walk* S (cdr t))))
        ((vector? t) (list->vector (walk* S (vector->list t))))
        (else        t)))

(define (occurs? S x t)
  (let ((t (walk S t)))
    (or (equal? x t)
        (and (pair? t) (or (occurs? S x (car t)) (occurs? S x (cdr t))))
        (and (vector? t) (occurs? S x (vector->list t))))))

(define (unify S u v)
  (let ((u (walk S u)) (v (walk S v)))
    (cond ((eqv? u v)  S)
          ((var? u)    (if (and (var? v) (equal? (var-name u) (var-name v)))
                         S
                         (subst-extend S u v)))
          ((var? v)    (subst-extend S v u))
          ((pair? u)   (and (pair? v)
                            (let ((S (unify S (car u) (car v))))
                              (and S
                                   (unify S (cdr u) (cdr v))))))
          ((vector? u) (and (vector? v)
                            (unify S (vector->list u) (vector->list v))))
          (else        (and (equal? u v) S)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Goals, ambitions, producers ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Goal     = S -> S*
;; Ambition = F* -> Goal
;; Producer = F* -> F*

(define (bind S* g) (if (null? S*) '() (append (g (car S*)) (bind (cdr S*) g))))

(define unit         (lambda (F*) (lambda (S) (list S))))
(define fail         (lambda (F*) (lambda (S) '())))
(define (== t0 t1)   (lambda (F*) (lambda (S) (let ((S (unify S t0 t1)))
                                                (if S (list S) '())))))
(define (conj a0 a1) (lambda (F*) (let ((g0 (a0 F*)) (g1 (a1 F*)))
                                    (lambda (S) (bind (g0 S) g1)))))
(define (disj a0 a1) (lambda (F*) (let ((g0 (a0 F*)) (g1 (a1 F*)))
                                    (lambda (S) (append (g0 S) (g1 S))))))
(define (conj+ a a*) (if (null? a*) a (conj a (conj+ (car a*) (cdr a*)))))
(define (disj+ a a*) (if (null? a*) a (disj a (disj+ (car a*) (cdr a*)))))
(define (conj* a*)   (if (null? a*) unit (conj+ (car a*) (cdr a*))))
(define (disj* a*)   (if (null? a*) fail (disj+ (car a*) (cdr a*))))

(define (relate atom)
  (lambda (F*)  ; This staging significantly improves performance.
    ((disj* (map (lambda (F) (== atom F))
                 (filter (lambda (F) (unify subst.empty atom F)) F*)))
     'ignored)))

(define (unique-cons x xs) (if (member x xs) xs (cons x xs)))
(define (unique-append xs ys)
  (if (null? xs)
    ys
    (unique-cons (car xs) (unique-append (cdr xs) ys))))

(define remember         (lambda (F*) F*))
(define (realize atom a) (lambda (F*) (map (lambda (S) (walk* S atom))
                                           ((a F*) subst.empty))))
(define (combine p0 p1)  (lambda (F*) (unique-append (p0 F*) (p1 F*))))
(define (combine* p*)    (if (null? p*)
                           remember
                           (combine (car p*) (combine* (cdr p*)))))
(define (exhaust p F*)   (let ((F*.new (p F*)))
                           (if (eq? F* F*.new) F* (exhaust p F*.new))))
(define (exhaust* p* F*) (exhaust (combine* p*) F*))

;;;;;;;;;;;;;;;;;;;;;;
;;; Example syntax ;;;
;;;;;;;;;;;;;;;;;;;;;;

;; This example syntax demonstrates how to use the core concepts.  This is only
;; one possible syntax.  For instance, you could also implement a Kanren-style
;; syntax that uses the same core concepts.

;; - Programs are made up of rules and facts:
;;   - Atom: a predicate constant followed by zero or more terms
;;   - Rule: a head atom followed by zero or more body atoms
;;   - Fact: a single atom

;; - Terms in rules may be variables or constants:
;;   - An unquoted symbol is treated as a variable.
;;   - Any quoted value is treated as a constant.
;;   - All other non-pair values are treated as constants.
;;   - Variables cannot appear nested in other terms.

;; - Terms in facts are always unquoted constants, including symbols and pairs.

;; - There are no queries.  There are only rules and facts.
;;   - e.g., (run-datalog rules facts) ==> more-facts

(define (atom-vars atom) (filter var? atom))

(define (rule-safe?! rule)
  (let ((vars.body (apply append (map atom-vars (cdr rule)))))
    (for-each (lambda (var.head) (or (member var.head vars.body)
                                     (error "unsafe rule" rule)))
              (atom-vars (car rule)))))

(define (parse-term expr)
  (match expr
    ((? symbol?) (var expr))
    (`(quote ,c) c)
    ((cons _ _)  (error "unsupported function call" expr))
    (_           expr)))

(define (parse-atom expr) (cons (car expr) (map parse-term (cdr expr))))
(define (parse-rule expr) (let ((rule (map parse-atom expr)))
                            (rule-safe?! rule)
                            rule))

(define (enforce rule) (realize (car rule) (conj* (map relate (cdr rule)))))

(define (run-datalog e*.rules F*)
  (exhaust* (map enforce (map parse-rule e*.rules)) F*))
