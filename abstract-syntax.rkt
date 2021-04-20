#lang racket/base
(provide
  )
(require racket/match racket/set)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TODO
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; modules
;; - module body syntax
;;   - imports from Racket
;;     - constants, functions
;;     - meta procedures (similar to macros, Racket procedures for building dbk computations)
;;   - definitions for relations, functions, values
;;     - closed definitions for all (for relations, these are bidirectional implications i.e., iff)
;;       - typical for Kanren languages
;;     - open definitions for relations via individual rules (which are unidirectional implications)
;;       - should be given arity/type signatures to mitigate typos and verify module linking compatibility
;;       - typical for Prolog or Datalog
;;       - maybe indicated with :- or <- or <==
;;       - incremental rule changes
;;       - linking modules extends rules by adding together all partial definitions
;;     - source data specifications for EDB relations
;;     - specifications for temporal relations
;;       - storage location, communication channel, sync or async, etc.
;;   - assertions: queries used for property checking or other validation
;; - module linking
;;   - by default, a module will export all definitions
;;   - apply visibility modifiers (such as except, only, rename, prefix) to change a module's exports
;;   - combine compatible modules to produce a new module
;;     - same module may be linked more than once, to produce different variations (mixin-style)
;;       - for instance, this may be used to swap in/out data/channels for EDB or temporal relations

;; ACILG hierarchy for analysis and optimization
;; - associative, commutative, idempotent, has-least-element, has-greatest-element
;; - comprehensions: map / fold / join / meet / fixed points
;;   - embodied by t:map/combine and t:fix
;; - an operator having all the properties in a prefix of this list may support more optimization
;; - associative: parallelism, but may need coordination for ordering
;; - commutative: parallelism, no ordering coordination needed
;; - idempotent:  fixed point computation without needing stratification; may need an initial value
;; - has-least-element: natural choice of initial value already known
;; - has-greatest-element: some computation may be stopped early, before all data is seen, if threshold is reached

;; order-by for converting sets/dicts to sequences

;; compact formulas as state component?
;; - state is a strategy-agnostic compact formula, plus a strategy-specific component (including a work scheduler)
;; - multi-pass strategy compilation: describe this state representation in the AST for the next pass's language

;; In some untyped systems, term evaluation may produce side-conditions (formulas)
;; - e.g., (car x) may introduce the formula (pair? x)

;; complex terms should be replaced with fresh variables to avoid redundant computations

;; more specific variable domain constraints:
;; - constant
;; - non-constant data construction (cons, vector, ...)
;;   - shape may not be fully known (vector with unspecified length)
;; - finite domain
;;   - could be represented as a join of constants; probably better to use a small table
;;   - could also include a representation for semi-joined table constraints
;; - interval domain
;;   - possibly with gaps/subtractions (negative finite domains)
;; - join of domain constraints

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Abstract syntax
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; formulas
(struct f:const   (value)         #:prefab)  ; can be thought of as a relation taking no arguments
(struct f:relate  (relation args) #:prefab)
(struct f:implies (if then)       #:prefab)
(struct f:iff     (f1 f2)         #:prefab)
(struct f:or      (f1 f2)         #:prefab)
(struct f:and     (f1 f2)         #:prefab)
(struct f:not     (f)             #:prefab)
(struct f:exist   (params body)   #:prefab)
(struct f:all     (params body)   #:prefab)

;; TODO: is this the right way to describe constraints?
(define (f:any<= u v) (f:relate 'any<= (list u v)))
(define (f:==    u v) (f:relate '==    (list u v)))
(define (f:=/=   u v) (f:not (f:== u v)))

;; terms (lambda calculus extended with constants (quote), logical queries,
;;        map/combine comprehensions, fixed points)
(struct t:query       (name formula)                  #:prefab)
(struct t:map/combine (proc.map proc.combine init xs) #:prefab)
(struct t:fix         (proc init threshold)           #:prefab)
(struct t:quote       (value)                         #:prefab)
(struct t:var         (name)                          #:prefab)
(struct t:app         (proc args)                     #:prefab)
(struct t:lambda      (params body)                   #:prefab)  ; omit for first order systems
;; possibly derived terms
(struct t:if          (c t f)                         #:prefab)
(struct t:let         (bindings body)                 #:prefab)
(struct t:letrec      (bindings body)                 #:prefab)
(struct t:match       (arg clauses)                   #:prefab)

;; possible derived term expansions, but these interpretations may vary per strategy/logic
;(define (t:let bindings body)  ; this expansion only works in higher order systems
;  (t:app (t:lambda (map car bindings) body) (map cadr bindings)))
;(define (t:letrec bindings body)
;  ;; Bind via single assignment of logic vars
;  (define lhss (map car  bindings))
;  (define rhss (map cadr bindings))
;  (define qresult (gensym "query.letrec"))
;  (t:apply (t:lambda lhss body)
;           (t:car (t:query qresult
;                           (f:exist lhss
;                                    (f:and (cons (f:== (t:var qresult)
;                                                       (foldr (lambda (param terms)
;                                                                (t:cons (t:var param) terms))
;                                                              (t:quote '()) lhss))
;                                                 (map (lambda (lhs rhs) (f:== (t:var lhs) rhs))
;                                                      lhss rhss))))))))
;(define (t:if c t f)  ; this may be unhelpfully indirect
;  (define qresult   (gensym "query.if"))
;  (define condition (gensym "condition"))
;  (t:car (t:query qresult
;                  (f:exist (list condition)
;                           (f:and (list (f:== condition c)
;                                        (f:implies (f:=/= condition (t:quote #f))
;                                                   (f:== qresult t))
;                                        (f:implies (f:==  condition (t:quote #f))
;                                                   (f:== qresult f))))))))

(define (t:apply f . args)  (t:app (t:quote apply        ) args))
(define (t:cons a d)        (t:app (t:quote cons         ) (list a d)))
(define (t:car p)           (t:app (t:quote car          ) (list p)))
(define (t:cdr p)           (t:app (t:quote cdr          ) (list p)))
(define (t:vector . args)   (t:app (t:quote vector       ) args))
(define (t:list->vector xs) (t:app (t:quote list->vector ) (list xs)))
(define (t:vector-ref v i)  (t:app (t:quote vector-ref   ) (list v i)))
(define (t:vector-length v) (t:app (t:quote vector-length) (list v)))

;; TODO: use CPS yielding to efficiently support partial-answer variations
(define (t-free-vars t)
  (match t
    ((t:query  name f)      (set-subtract (f-free-vars f) (set name)))
    ((t:quote  _)           (set))
    ((t:var    name)        (set name))
    ((t:app    func args)   (set-union (t-free-vars func) (t-free-vars* args)))
    ((t:lambda params body) (set-subtract (t-free-vars body) (list->set params)))
    ;((t:if     c t f)       (set-union (t-free-vars c) (t-free-vars t) (t-free-vars f)))
    ;; TODO: t:let t:letrec ?
    ))

(define (f-free-vars f)
  (match f
    ((f:const   _)             (set))
    ((f:or      f1 f2)         (set-union (f-free-vars f1) (f-free-vars f2)))
    ((f:and     f1 f2)         (set-union (f-free-vars f1) (f-free-vars f2)))
    ((f:implies if then)       (set-union (f-free-vars if) (f-free-vars then)))
    ((f:relate  relation args) (t-free-vars* args))
    ((f:exist   vnames body)   (set-subtract (f-free-vars body) (list->set vnames)))
    ((f:all     vnames body)   (set-subtract (f-free-vars body) (list->set vnames)))))

(define (t-free-vars* ts)
  (foldl (lambda (t vs) (set-union vs (t-free-vars t)))
         (set) ts))

(define (f-free-vars* fs)
  (foldl (lambda (f vs) (set-union vs (f-free-vars f)))
         (set) fs))

;; TODO: simplify within some context (which may bind/constrain variables)?
;(define (t-simplify t)
;  (match t
;    ((t:var   name)   t)
;    ((t:quote value)  t)
;    ((t:app   f args) (let ((args (map t-simplify args)))
;                        (if (andmap t:quote? args)
;                          (apply f args)
;                          (t:app f args))))))
