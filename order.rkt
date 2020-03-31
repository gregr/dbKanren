#lang racket/base
(provide any<? null<? boolean<? inexact<? number<?
         pair<? list<? array<? tuple<?
         ;; TODO: suffix<?
         type-><?)
(require racket/match racket/math)

(define (any<? a b)
  (let loop ((i 0))
    (let ((type? (vector-ref <s i)) (type<? (vector-ref <s (+ i 1))))
      (cond ((type? a) (or (not (type? b)) (type<? a b)))
            ((type? b) #f)
            (else      (loop (+ i 2)))))))

(define (null<? a b)    #f)
(define (boolean<? a b) (and b (not a)))
(define (inexact<? a b) (or (< a b) (nan? b)))
(define (number<? a b)
  (if (inexact? a)
    (and (inexact? b) (inexact<? a b))
    (or  (inexact? b) (<         a b))))
(define ((pair<? a<? d<?) a b)
  (let ((aa (car a)) (ba (car b)))
    (or (a<? aa ba) (and (not (a<? ba aa)) (d<? (cdr a) (cdr b))))))
(define ((list<? element<?) a b)
  (define p<? (pair<? element<? <?))
  (define (<? a b) (and (not (null? b)) (or (null? a) (p<? a b))))
  (<? a b))
(define ((array<? element<?) a b)
  (let ((alen (vector-length a)) (blen (vector-length a)))
    (or (< alen blen)
        (and (= alen blen)
             (let loop ((i 0))
               (and (< i alen)
                    (let ((va (vector-ref a i)) (vb (vector-ref b i)))
                      (or (element<? va vb)
                          (and (not (element<? vb va)) (loop (+ i 1)))))))))))
(define ((tuple<? <?s) a b)
  (define len (vector-length <?s))
  (let loop ((i 0))
    (and (< i len)
         (let ((<? (vector-ref <?s i))
               (va (vector-ref a i)) (vb (vector-ref b i)))
           (or (<? va vb) (and (not (<? vb va)) (loop (+ i 1))))))))

;; TODO: string suffixes (need access to source strings)
;(define ((suffix<? source-text) a b)
  ;)

(define ((number?/? ?) x) (and (number? x) (? x)))

(define <s
  (vector null?                null<?
          boolean?             boolean<?
          (number?/?   exact?) <
          (number?/? inexact?) inexact<?
          symbol?              symbol<?
          string?              string<?
          bytes?               bytes<?
          pair?                (pair<? any<? any<?)
          vector?              (array<? any<?)))

(define (type-><? type)
  (match type
    (#f                         any<?)
    ((or 'nat    `#(nat    ,_)) <)
    ((or 'string `#(string ,_)) string<?)
    ((or 'symbol `#(symbol ,_)) symbol<?)
    ((or 'bytes  `#(bytes  ,_)) bytes<?)
    (`#(tuple ,@ts)             (tuple<? (map type-><? ts)))
    (`(,ta . ,td)               (pair<? (type-><? ta) (type-><? td)))
    ('array                     (array<? any<?))
    (`#(array ,_ ,t)            (array<? (type-><? t)))
    ('list                      (list<? any<?))
    (`#(list ,_ ,t)             (list<? (type-><? t)))
    ('number                    number<?)
    ((or 'true 'false '())      null<?)))
