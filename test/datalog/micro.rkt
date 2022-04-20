#lang racket/base
(provide run-datalog)
(require racket/match)

;; Atom: a predicate constant followed by zero or more terms
;; Rule: a head atom followed by zero or more body atoms
;; Fact: a single atom

;; Terms in rules may be variables or constants:
;; - An unquoted symbol is treated as a variable.
;; - Any quoted value is treated as a constant.
;; - All other non-pair values are treated as constants.
;; - Variables cannot appear nested in other terms.

;; Terms in facts are always unquoted constants, even symbols or pairs.

;; There are no queries, just rules and facts.
;; (run-datalog rules facts) ==> more-facts

(define (unique-cons x xs) (if (member x xs) xs (cons x xs)))

(define (unique-append xs ys)
  (if (null? xs)
    ys
    (unique-cons (car xs) (unique-append (cdr xs) ys))))

(struct var (name) #:prefab)

(define subst.empty '())
(define (subst-extend subst x t) (cons (cons (var-name x) t) subst))

(define (walk t subst)
  (if (var? t)
    (let ((kv (assoc (var-name t) subst))) (if kv (cdr kv) t))
    t))

(define (atom-walk atom subst) (map (lambda (t) (walk t subst)) atom))

(define (unify x t subst)
  (let ((x (walk x subst)))
    (cond ((var? x)     (subst-extend subst x t))
          ((pair? x)    (let ((subst (unify (car x) (car t) subst)))
                          (and subst (unify (cdr x) (cdr t) subst))))
          ((equal? x t) subst)
          (else         #f))))

(define (bind substs g)
  (if (null? substs) '() (append (g (car substs)) (bind (cdr substs) g))))

(define (atom-goal atom facts)
  (lambda (subst) (filter (lambda (subst?) (not (not subst?)))
                          (map (lambda (fact) (unify atom fact subst)) facts))))

(define (body-goal atoms facts)
  (if (null? atoms)
    list
    (let ((g0 (atom-goal (car atoms) facts)) (g* (body-goal (cdr atoms) facts)))
      (lambda (subst) (bind (g0 subst) g*)))))

(define (rule-apply rule facts.old facts.new)
  (let ((head (car rule)) (g.body (body-goal (cdr rule) facts.old)))
    (unique-append (map (lambda (subst) (atom-walk head subst)) (g.body subst.empty))
                   facts.new)))

(define (rules-apply rules facts.old facts.new)
  (if (null? rules)
    facts.new
    (rules-apply (cdr rules) facts.old (rule-apply (car rules) facts.old facts.new))))

(define (rules-apply/fix rules facts.old)
  (let ((facts.new (rules-apply rules facts.old facts.old)))
    (if (eq? facts.new facts.old) facts.new (rules-apply/fix rules facts.new))))

(define (atom-vars atom) (filter var? atom))

(define (rule-safe?! rule)
  (let ((vars.body (apply append (map atom-vars (cdr rule)))))
    (for-each (lambda (var.head) (or (member var.head vars.body)
                                     (error "unsafe rule" rule)))
              (atom-vars (car rule)))))

(define (parse-term term)
  (match term
    ((? symbol?) (var term))
    (`(quote ,c) c)
    ((cons _ _)  (error "unsupported function call" term))
    (_           term)))

(define (parse-atom atom) (cons (car atom) (map parse-term (cdr atom))))
(define (parse-rule rule) (map parse-atom rule))

(define (run-datalog rules facts)
  (let ((rules (map parse-rule rules)))
    (for-each rule-safe?! rules)
    (rules-apply/fix rules (unique-append facts '()))))


;;; Alternative implementation of run-datalog using nested loops

;(define (bind* facts atoms)
  ;(let loop.atom ((atoms atoms) (subst subst.empty))
    ;(cond ((null? atoms) (list subst))
          ;(else (let ((atom (car atoms)) (atoms (cdr atoms)))
                  ;(let loop.fact ((facts facts))
                    ;(cond ((null? facts) '())
                          ;(else (let ((subst (unify atom (car facts) subst)))
                                  ;(append (if subst (loop.atom atoms subst) '())
                                          ;(loop.fact (cdr facts))))))))))))

;(define (rule-apply rule facts)
  ;(let ((head.rule (car rule)) (body.rule (cdr rule)))
    ;(map (lambda (subst) (atom-walk head.rule subst)) (bind* facts body.rule))))

;(define (run-datalog rules facts)
  ;(let ((rules (map parse-rule rules)))
    ;(for-each rule-safe?! rules)
    ;(let loop.fix ((facts.old (unique-append facts '())))
      ;(let ((facts (let loop.rule ((rules rules) (facts facts.old))
                     ;(match rules
                       ;('() facts)
                       ;((cons rule rules)
                        ;(loop.rule rules (unique-append (rule-apply rule facts.old) facts)))))))
        ;(if (eq? facts facts.old) facts (loop.fix facts))))))
