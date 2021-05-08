#lang racket/base
(provide foldl/and let*/and define-variant
         plist->alist alist-ref alist-remove alist-update alist-set
         call/files let/files)
(require racket/match)

(define (foldl/and f acc xs . yss)
  (let loop ((acc acc) (xs xs) (yss yss))
    (if (null? xs)
      acc
      (and acc (loop (apply f (car xs) (append (map car yss) (list acc)))
                     (cdr xs)
                     (map cdr yss))))))

(define-syntax let*/and
  (syntax-rules ()
    ((_ () body ...)                   (let () body ...))
    ((_ ((lhs rhs) rest ...) body ...) (let ((lhs rhs))
                                         (and lhs (let*/and (rest ...)
                                                    body ...))))))

(define-syntax define-variant
  (syntax-rules ()
    ((_ type? (struct-name fields ...) ...)
     (begin (define (type? x)
              (match x
                ((struct-name fields ...) #t) ...
                (_                        #f)))
            (struct struct-name (fields ...) #:prefab) ...))))

(define (plist->alist kvs) (if (null? kvs) '()
                             (cons (cons (car kvs) (cadr kvs))
                                   (plist->alist (cddr kvs)))))

(define (alist-ref alist key (default (void)))
  (define kv (assoc key alist))
  (cond (kv              (cdr kv))
        ((void? default) (error "missing key in association list:" key alist))
        (else            default)))

(define (alist-remove alist key)
  (filter (lambda (kv) (not (equal? (car kv) key))) alist))

(define (alist-update alist key v->v (default (void)))
  (let loop ((kvs alist) (prev '()))
    (cond ((null?        kvs     )
           (when (void? default) (error "missing key in association list:" key alist))
           (cons (cons key (v->v default)) alist))
          ((equal? (caar kvs) key) (foldl cons (cons (cons key (v->v (cdar kvs))) (cdr kvs)) prev))
          (else                    (loop (cdr kvs) (cons (car kvs) prev))))))

(define (alist-set alist key value)
  (alist-update alist key (lambda (_) value) #f))

(define (call/files fins fouts p)
  (let loop ((fins fins) (ins '()))
    (if (null? fins)
      (let loop ((fouts fouts) (outs '()))
        (if (null? fouts)
          (apply p (append (reverse ins) (reverse outs)))
          (call-with-output-file
            (car fouts) (lambda (out) (loop (cdr fouts) (cons out outs))))))
      (call-with-input-file
        (car fins) (lambda (in) (loop (cdr fins) (cons in ins)))))))

(define-syntax-rule (let/files ((in fin) ...) ((out fout) ...) body ...)
  (call/files (list fin ...) (list fout ...)
              (lambda (in ... out ...) body ...)))
