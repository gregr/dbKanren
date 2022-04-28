#lang racket/base
(provide
  (struct-out var)
  unit fail
  conj conj+ conj*
  disj disj+ disj*
  == relate compute
  realize exhaust*)
(require racket/set)

;; This version of the micro core supports fact merging for aggregation.

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

(define (compute proc args)
  (lambda (F*) (lambda (S) ((apply proc (walk* S args)) S))))

(define remember         (lambda (F*) F*))
(define (realize atom a) (lambda (F*) (map (lambda (S) (walk* S atom))
                                           ((a F*) subst.empty))))
(define (combine p0 p1)  (lambda (F*) (append (p0 F*) (p1 F*))))
(define (combine* p*)    (if (null? p*)
                           remember
                           (combine (car p*) (combine* (cdr p*)))))

(define (aggregate predicate=>merge F*)
  (let loop ((F*                    F*)
             (F*.skipped            '())
             (predicate=>key=>value (make-immutable-hash
                                      (map (lambda (key) (cons key (hash)))
                                           (hash-keys predicate=>merge)))))
    (if (null? F*)
      (apply append
             F*.skipped
             (map (lambda (p&k=>v)
                    (let ((predicate (car p&k=>v)))
                      (map (lambda (k&v)
                             (cons predicate
                                   (reverse (cons (cdr k&v) (car k&v)))))
                           (hash->list (cdr p&k=>v)))))
                  (hash->list predicate=>key=>value)))
      (let* ((F         (car F*))
             (predicate (car F))
             (merge     (hash-ref predicate=>merge predicate #f)))
        (if merge
          (loop (cdr F*) F*.skipped
                (hash-update
                  predicate=>key=>value
                  predicate
                  (lambda (key=>value)
                    (let* ((reversed (reverse (cdr F)))
                           (key      (cdr reversed))
                           (value    (car reversed)))
                      (hash-set key=>value key
                                (if (hash-has-key? key=>value key)
                                  (merge (hash-ref key=>value key) value)
                                  value))))))
          (loop (cdr F*) (cons F F*.skipped) predicate=>key=>value))))))

;; NOTE: motonicity filtering will not be necessary with delta-based evaluation
(define (exhaust p predicate=>merge non-monotonic-predicates F*)
  (define (monotonic? F) (not (set-member? non-monotonic-predicates (car F))))
  (define (filter-monotonic F*) (list->set (filter monotonic? (set->list F*))))
  (let ((F*.new (list->set (aggregate predicate=>merge (p (set->list F*))))))
    (if (set=? (filter-monotonic F*) (filter-monotonic F*.new))
      F*.new
      (exhaust p predicate=>merge non-monotonic-predicates F*.new))))

(define (exhaust* p* predicate=>merge non-monotonic-predicates F*)
  (set->list (exhaust (combine* p*) predicate=>merge non-monotonic-predicates
                      (list->set F*))))
