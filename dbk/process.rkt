#lang racket/base
(provide
  )
(require "abstract-syntax.rkt" "misc.rkt" "parse.rkt"
         racket/list racket/match racket/set)

;; TODO: rethink support for disabled-submodules
(record pstate (dbms history data program disabled-submodules) #:prefab)

(define (pstate:new dbms prg) (pstate (dbms                dbms)
                                      (history             '())
                                      (data                'TODO)
                                      (program             prg)
                                      (disabled-submodules (set))))

;; TODO:
(define (pstate-query pst params formula)
  (set))

(define-variant pmod?
  (pmod:add     path module)
  (pmod:remove  path)
  (pmod:move    path.old path.new)
  (pmod:wrap    path)
  (pmod:unwrap  path)
  (pmod:enable  path)
  (pmod:disable path)
  (pmod:rename  vocab name.old name.new))

(define (pmod:hide vocab name) (pmod:rename vocab name #f))

(define (pstate-program-modify pst.0 pm)
  (define pst   (pstate:set pst.0 (history (cons pm (pstate-history pst.0)))))
  (define dsubs (pstate-disabled-submodules pst))
  (define prg   (pstate-program pst))
  (define m     (program-module prg))
  (match pm
    ((pmod:add     path module)
     ;; TODO: for safety, recreate all private names of both current and newly-added module?
     (pstate:set pst
                 (program (program:set prg (module (module-add    m path module))))))
    ((pmod:remove  path)
     (pstate:set pst
                 (program (program:set prg (module (module-remove m path))))
                 (disabled-modules
                   (list->set (filter-not (lambda (path.disabled) (list-prefix? path     path.disabled))
                                          (set->list dsubs))))))
    ((pmod:move    path.old path.new)
     (pstate:set pst
                 (program (program:set prg (module (module-add (module-remove m path.old)
                                                               path.new
                                                               (module-ref m path.old)))))
                 ;; It's not clear how to safely move these, so remove them
                 (disabled-modules
                   (list->set (filter-not (lambda (path.disabled) (list-prefix? path.old path.disabled))
                                          (set->list dsubs))))))
    ((pmod:wrap    path)
     (pstate:set pst
                 (program (program:set prg (module (module-wrap   m path))))
                 (disabled-modules
                   (list->set (set-map dsubs (lambda (path.disabled) (append path path.disabled)))))))
    ((pmod:unwrap  path)
     (pstate:set pst
                 (program (program:set prg (module (module-unwrap m path))))
                 (disabled-modules
                   (list->set (filter-not not (set-map dsubs (lambda (path.disabled)
                                                               (foldl (lambda (part path)
                                                                        (and (pair? path)
                                                                             (equal? (car path) part)
                                                                             (cdr path)))
                                                                      path.disabled path))))))))
    ((pmod:enable  path)
     (pstate:set pst (disabled-modules (set-remove dsubs path))))
    ((pmod:disable path)
     (pstate:set pst (disabled-modules (set-add    dsubs path))))
    ((pmod:rename  vocab name.old name.new)
     (define env.0   (program-env prg))
     (define env.1   (env-set env.0 vocab name.old #f))
     (define env.new (if name.new
                       (env-set env.1 vocab name.new (env-ref env.0 vocab name.old))
                       env.1))
     (pstate:set pst (program (program:set prg (env env.new)))))))

;; TODO: return #f if quiescent
(define (pstate-step pst)
  #f)

(record process (name state) #:prefab)

(define (process-dbms  p)        (pstate-dbms (process-state p)))
(define (process-query p . args) (apply pstate-query (process-state p) args))

(define (process-branch p name.new)
  (dbms-process-add! (process-dbms p) name.new (process-state p))
  (process:set p (name name.new)))

(define (process-move   p name.new)
  (dbms-process-move! (process-dbms p) (process-name p) name.new)
  (process:set p (name name.new)))

(define (process-program-modify! p pm)
  (define pst.new (pstate-program-modify (process-state p) pm))
  (dbms-process-set! (process-dbms p) (process-name p) pst.new)
  (process:set p (state pst.new)))

(define (process-step! p)
  (define pst.new (pstate-step (process-state p)))
  (cond ((not pst.new) p)
        (else          (dbms-process-set! (process-dbms p) (process-name p) pst.new)
                       (process:set p (state pst.new)))))

;; TODO:
(define (dbms-process-set!    dbms name pst)
  (void))

(define (dbms-process-add!    dbms name pst)
  (void))

(define (dbms-process-move!   dbms name.old name.new)
  (void))

(define (dbms-process-remove! dbms name pst.validation)
  (void))

(define (dbms-export!         dbms renamings path.out)
  (void))

(define (dbms-import!         dbms renamings path.in)
  (void))

(define (dbms-clean!          dbms)
  (void))
