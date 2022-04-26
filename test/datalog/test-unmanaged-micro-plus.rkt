#lang racket/base
(require "unmanaged-notation-micro-plus.rkt"
         racket/list racket/pretty)
(print-as-expression #f)
;(pretty-print-abbreviate-read-macros #f)

(define-syntax-rule
  (pretty-results example ...)
  (begin (let ((result (time example)))
           (pretty-write 'example)
           (pretty-write '==>)
           (pretty-write result)
           (newline)) ...))

(define (run-stratified-queries predicate=>compute predicate=>merge rules.query rule** facts)
  (let ((facts (run-stratified predicate=>compute predicate=>merge
                               (cons rules.query rule**) facts)))
    (map (lambda (predicate.query)
           (filter (lambda (fact) (eq? (car fact) predicate.query)) facts))
         (map caar rules.query))))

(define (run-queries rules.query rules facts)
  (run-stratified-queries (hash) (hash) rules.query (list rules) facts))

;;;;;;;;;;;;;;;;;;;;;;;
;;; Graph traversal ;;;
;;;;;;;;;;;;;;;;;;;;;;;

(pretty-results
  (run-queries
    '(((q1 x)   (path 'a x))
      ((q2 x)   (path x 'f))
      ((q3 x y) (path x y))
      ((q4 x y) (edge x y)))
    '(((path x y) (edge x y))
      ((path x z) (edge x y) (path y z)))
    '((edge a b)
      (edge b c)
      (edge d e)
      (edge e f)
      (edge b f)
      (edge f a)  ; comment this edge for an acyclic graph
      )))

;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Finite arithmetic ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;

(define facts.+ (apply append (map (lambda (a)
                                     (map (lambda (b) `(+o ,a ,b ,(+ a b)))
                                          (range 50)))
                                   (range 50))))
(define facts.* (apply append (map (lambda (a)
                                     (map (lambda (b) `(*o ,a ,b ,(* a b)))
                                          (range 50)))
                                   (range 50))))
(define facts.< (apply append (map (lambda (a)
                                     (apply append (map (lambda (b) (if (< a b)
                                                                      `((<o ,a ,b))
                                                                      '()))
                                                        (range 50))))
                                   (range 50))))

(pretty-results
  (run-queries
    '(((q1 a b) (+o a b 7))
      ((q2 a b) (*o a b 7))
      ((q3 a b) (*o a b 18))
      ((q4 n)   (<o 0 n) (<o n 6)))
    '()
    (append facts.+ facts.* facts.<)))

;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Finite path length ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;

(pretty-results
  (run-stratified-queries
    (hash 'conso (lambda (a d ad)
                   (when (and (or (var? a) (var? d)) (var? ad))
                     (error "unsupported mode for conso" a d ad))
                   ((== (cons a d) ad) 'ignored)))
    (hash 'shortest-route-distance min)
    '(((q0 s t d)   (road  s t d))
      ((q1 s t d)   (shortest-route-distance s t d))
      ((q2 d)       (shortest-route-distance 'a 'd d))
      ((q3 s t d p) (shortest-route s t d p))
      ((q4 d p)     (shortest-route 'a 'd d p)))
    '((((shortest-route s t d p) (shortest-route-distance s t d)
                                 (road s t d)
                                 (conso t '() p.t)
                                 (conso s p.t p))
       ((shortest-route s t d p) (shortest-route-distance s t d)
                                 (road s mid d.0)
                                 (shortest-route mid t d.rest p.rest)
                                 (+o d.0 d.rest d)
                                 (conso s p.rest p)))
      (((shortest-route-distance s t d) (road s t d))
       ((shortest-route-distance s t d) (road s mid d.0)
                                        (shortest-route-distance mid t d.rest)
                                        (+o d.0 d.rest d))))
    (append facts.+ facts.<
            '((road a b 1)
              (road a c 7)
              (road b c 1)
              (road c d 1)
              (road d a 5)))))

;;;;;;;;;;;;;;;;;;;;;;;
;;; Mutable counter ;;;
;;;;;;;;;;;;;;;;;;;;;;;

(define count.current 0)
(define rules.count '(((next-count next) (+o current 1 next) (count current))))
(define (current-count-state)
  (run-stratified (hash) (hash) (list rules.count)
                  (append facts.+
                          `((count ,count.current)))))
(define (state-extract facts.count predicate)
  (cadar (filter (lambda (fact) (eq? (car fact) predicate)) facts.count)))

(define (now  st) (state-extract st 'count))
(define (next st) (state-extract st 'next-count))

(define (increment-count!)
  (let ((st (current-count-state)))
    (pretty-write `(current count: ,(now st)))
    (set! count.current (next st))
    (pretty-write `(next count: ,(next st)))))

(for-each (lambda (_) (increment-count!)) (range 10))
