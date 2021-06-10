#lang racket/base
(provide
  )
(require "abstract-syntax.rkt" "misc.rkt" "parse.rkt"
         racket/list racket/match racket/set)

(record pstate (dbms history data program) #:prefab)

(define (pstate:new dbms prg) (pstate (dbms                dbms)
                                      (history             '())
                                      (data                'TODO)
                                      (program             prg)))

;; TODO:
(define (pstate-query pst params formula)
  (set))

(define-variant pmod?
  (pmod:add     path module)
  (pmod:move    path.old path.new)
  (pmod:wrap    path)
  (pmod:unwrap  path)
  (pmod:rename  vocab name.old name.new))

(define (pmod:remove path)       (pmod:move   path #f))
(define (pmod:hide   vocab name) (pmod:rename vocab name #f))

(define (pstate-modify pst.0 pm)
  (define pst   (pstate:set pst.0 (history (cons pm (pstate-history pst.0)))))
  (define prg   (pstate-program pst))
  (define m     (program-module prg))
  (match pm
    ;; TODO: for safety, recreate all private names of both current and newly-added module?
    ((pmod:add     path module)       (pstate:set pst (program (program:set prg (module (module-add    m path module))))))
    ((pmod:move    path.old path.new) (define m.1 (module-remove m path.old))
                                      (define m.new (if path.new
                                                      (module-add m.1 path.new (module-ref m path.old))
                                                      m.1))
                                      (pstate:set pst (program (program:set prg (module m.new)))))
    ((pmod:wrap    path)              (pstate:set pst (program (program:set prg (module (module-wrap   m path))))))
    ((pmod:unwrap  path)              (pstate:set pst (program (program:set prg (module (module-unwrap m path))))))
    ((pmod:rename  vocab n.old n.new) (define env.0   (program-env prg))
                                      (define env.1   (env-set env.0 vocab n.old #f))
                                      (define env.new (if n.new
                                                        (env-set env.1 vocab n.new (env-ref env.0 vocab n.old))
                                                        env.1))
                                      (pstate:set pst (program (program:set prg (env env.new)))))))

;; TODO: return #f if quiescent
(define (pstate-step pst)
  #f)

(define (process name state)
  (define dbms (pstate-dbms state))
  (method-lambda
    ((branch name.new) (dbms-process-add!  dbms name.new state)
                       (process name.new state))
    ((move   name.new) (dbms-process-move! dbms name name.new)
                       (set! name name.new))
    ((modify pm)       (define state.new (pstate-modify state pm))
                       (dbms-process-set!  dbms name state.new)
                       (set! state state.new))
    ((step)            (define state.new (pstate-step state))
                       (and state.new (dbms-process-set! dbms name state.new)))
    ((query  . args)   (apply pstate-query state args))))

(define (process-query   p . args)   (apply p 'query args))
(define (process-branch  p name.new) (p 'branch name.new))
(define (process-move!   p name.new) (p 'move   name.new))
(define (process-step!   p)          (p 'step))
(define (process-modify! p pm)       (p 'modify pm))



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
