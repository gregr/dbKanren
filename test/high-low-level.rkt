#lang racket/base
(provide
  (struct-out storage-read-write-methods)
  storage:file-read-only
  storage:file-read-write
  storage:port-read-only
  storage:port-read-write
  storage:bytevector-read-only
  storage:bytevector-read-write
  )

(require
  "../dbk/safe-unsafe.rkt"
  ;racket/unsafe/ops
  racket/splicing
  )

bytevector
nonnegative-integer
  relative-address
  absolute-address


NOTE: good news, read-bytes! does not stop until it reads all the bytes requested, or hits eof

;;; TODO

move away from the accurate trie-based index, towards an approximate interval-based index of relevant blocks/chunks/sections

what shape should data vectors take?
- vector of tuples (where a tuple is a vector)
- tuple of vectors
- flat vector with stride positioning
  - column-major or row-major strides
  - this format might perform the best, but should confirm with benchmarks
  - can adapt old merge sort and dedup for row-major format

- can we read into every format for free with an enumerator interface? (yield i x)
- can we do similar for writing?
  - benchmark both of these to see how performance is affected by abstraction


database pointer at position 0:
- don't just point to the file endpoint where the most recently written header is
- instead, store the byte interval of the entire header: both start and end positions (6 bytes each)
  - could also store a history of start/end positions in the first 4k of the file, for recovery or versioning
  - and/or other sanity-checking metadata
- then we don't have to guess the header size, or insist on a fixed size, and can load it with a single read


dependently-typed low-level structures
- need both eager and lazy read/write modes
  - eager for headers
  - lazy for payloads
    - not really lazy so much as opt-in, to avoid reading irrelevant portions

table:
- header
  - 16-bit column-count
  - column-type array
    - a column-type might as well be stored in 8-bits for simplicity and possible future extension
    - 0 = int
    - 1 = text
  - 40-bit partition-count
    - a table is partitioned into one or more contiguous groups of column-count * contiguous segments
      - each of the segments describes, for the same range of rows, a partition of one of the table columns
  - 8-bit byte-position bit-width
  - segment start/end 16-bit-row-position-interval array
    - implicit initial start of 0
    - usual arrangement where each pair of adjacent positions defines an interval
      - each segment's end is the next segment's start
    - (- end start) is the number of rows in that segment
  - segment start/end byte-position-interval array (relative to base-address)
    - each segment ends where the next segment starts
      - e.g., the end of the last segment in each table partition is the start of the next partition's first segment
;  - IGNORE this, it won't be part of a table (it will be its own table):
;    - per-column index over segments indexes
;      - should probably keep this separate from the table
;        - in fact, this index data is itself naturally expressed as a table
;      - column value interval endpoint pairs
;        - an array of all segment endpoints for a column, with each min and max adjacent
;        - these may overlap, so unlike other interval formats, one segment's max is not the next segment's min
;          - each segment has its own two values just for itself
;      - min and max must bound all of a column's values in this segment, but do not have to be actual values in the column
;        - usually min/max will be actual values present in the column
;        - but since some text values may be too large, a short, conservative prefix may be substituted
- (* partition-count column-count) segments


REPLACE SECTIONS WITH MULTI-SEGMENTS
section (a table partition is column-count * contiguous sections):
- header
  - 16-bit segment-count
  - a single, segment start/end 16-bit-row-position-interval array (relative to base-row)
    - implicit initial start of 0
- segment-count segments

segment:
- header
  - compression method and its metadata
- compressed data codes
  - base-address starts at the beginning of the data codes

compression method and its metadata:
- 8-bit type
  - each of int and text has 4 possible methods, so we could name the method using 2-bits
    - table already knows the type of the columns
- type-specific metadata
  - int:single-value
    - a single int:none encoded value
      - includes 8-bit bit-width
  - text:single-value
    - a single text:none encoded value
  - int:none = int:frame-of-reference[minimum-value=(- (arithmetic-shift 1 (- bit-width 1)))]
  - text:none
    - 8-bit bit-width for byte-position
    - text value start/end byte-position-interval array (relative to base-address)
      - implicit start of 0
    - logically subsumed by previous-relative prefix method, but should be more efficient to decode
  - int:frame-of-reference
    - a single int:none encoded minimum-value
    - 8-bit bit-width
  - int:delta
    - a single int:none encoded start-value
    - the segment-header for delta-encoded values
      - this means multiple iterations of int:delta can be chained, if useful
      - int:single-value or int:frame-of-reference are most likely to follow
  - dictionary (int or text)
    - 16-bit count
    - 8-bit segment-size bit-width
    - segment-size
    - embedded segment for dictionary values
    - the segment-header for dictionary-encoded values
  - text:previous-relative-prefix
    - TODO


relative prefix compression for text:
- logical repr per-value:
  - length-of-prefix-shared-with-previous-value
  - length-of-full-value
  - sequence of non-shared-suffix bytes
- physical repr is a struct of 3 (actually 5) arrays, one for each of the logical fields (plus the 2 RLE arrays)
  - also, a byte count for each RLE array
    - once the bytes of either RLE array are decoded, we will know the full value count of this segment
    - also, after decoding the bytes for the RLE arrays, we will know the counts of the encoded length arrays
  - run-length encoding is applied to the arrays for both kinds of length
    - each logical RLE entry is either:
      - a count of repetitions of a single value
      - a count of non-repeated values
      - physically, the two cases could be distinguished by the presence of a sign bit in the count
        - but losing an entire bit in the 4-bit encoding is painful
      - alternatively, the non-repeat case could be distinguished by a 0 followed by a count
        - then we only lose one of 16 possible values in the 4-bit encoding case
  - a single bit-width is used for each kind of length (and a separate one for its run-length)
    - valid choices: 4, 8, 16, 24, 32, 40, 48, 56
      - maybe the added complication of decoding 4 bits isn't worth it after run-length encoding
      - though run-length is likely to be 4 bits itself
        - 4 bits for both might save another 10% of space overall for repetative, small text values



benchmark and compare ideas for prefix compression
- single shared prefix
- prefix relative to previous value
  - ; this compresses very well, and doesn't seem too slow, so let's stick with this for now

notice that prefix/length for each text value is often repeated, so we could run-length-encode compress these if we keep them separate from the text

how do we decide when to split a chunk into multiple segments with different compression kinds?
- s/chunk/section

maybe support storing arbitrary tables of data, rather than forcing them to be relations
- sorting is optional
- deduping is optional
- compression is optional
  - may want to avoid compression for faster materialization in the middle of a query
  - may also want to spend a larger than usual effort on compression for longer term storage?
- insertion order is preserved
- roughly random access streaming/scanning at the nth tuple
  - start streaming/scanning at the beginning of any section, where sections are uniformly distributed

also, store everything with tables, including table metadata itself
- db file commit pointer just points to a metadata table header
  - unless it's zero, which can be interpreted as an empty table
- for linear growth, need to support linked records rather than forced recopying of old records into new records
  - forced recopying to consolidate should still be an option, though, user should choose
  - what should the linked structure look like?
  - what should table metadata even look like?
    - would like actual data to be as contiguous as possible, so avoid putting too much into data sections themselves
      - this means that building multiple sections requires another source of space proportional to the number of sections
        - to hold the extra metadata until it can be written outside the section region, after all sections are written
    - section info:
      - location and size
        - size may be implicit in location of next section
        - or location may be implicit in end position of previous section
          - 0 start is implied for the first section, then for n section we store n end positions
            - these are all relative to some base address
      - number of tuples in this section
      - to support sequential chaining, we want enough info about the next section to grab it in a single disk read
        - but unless we write backwards, at the time of writing we don't know the details of the next section

      - min and max value in each column of this section? no
        - these depend on the indexing method, and should be stored separately
        - they can be tracked and held in memory (or another storage location) during construction, to be written later
        - or they can be computed in a post-construction traversal of the section
      - segments in this section
      - segment info:
        - number of values in this segment
        - compression method
        - data codes


how do we decide when a text segment should use:
  indexes into an ordered dictionary
  vs.
  ordered dictionary mapped to indexes  (aka inverse file)
  vs.
  inlined values?
- definitely inline values if they are already mostly sorted (no benefit from a separate dictionary)
- only use an ordered dictionary when a small number (how small?) of values are repeated
maybe never use a separate ordered dictionary, always inline text values
- unless the number of values is globally small, so we can share the same dictionary across segments?
  - but maybe this representation should be chosen explicitly by the user
- or maybe in extreme cases
  - such as fewer than 256 unique, not-already-sorted, values, in a segment of at least 1024 values?
- maybe just try all possibilities while scanning, and see which one will yield the best compression at the end of the scan



;;;; On-disk data structure formats

;; Write lower layers before upper layers until pending upper layers become too large and need to be flushed.
;; Then an initial, large contiguous read at the root of the trie can greatly reduce the number of subsequent disk accesses needed to reach a leaf.

;3-bit key-length byte-width
;key-length (length does not include shared-prefix, which may allow us to save on byte-width)
;key-content-up-to-max-inline-length
;key-pointer-to-remaining-content (remaining content does not include inlined content, which may allow us to save a little)

node can store most distant payload address that falls within the same page ?
then a single comparison can determine if we need another disk read to follow an address

;modularly-decomposed node batches
;if nodes are up to 256 bytes large, we can cluster up to 256 of them in 64k, and cluster 256 of those clusters in 16mb
;
;how long does a single 16mb read take compared to three 4k reads?
;
;> (define (go)
;    (time (let loop ((i 10000))
;            (unless (eq? 0 i)
;              (read-bytes! buffer in 0 size)
;              (loop (- i 1))))))
;> (define size (expt 2 16))  ;  64k: 11100ns
;> (go)
;cpu time: 111 real time: 247 gc time: 0
;> (define size (expt 2 14))  ;  16k:  4500ns
;> (go)
;cpu time: 45 real time: 74 gc time: 0
;> (define size (expt 2 12))  ;   4k:  1500ns
;> (go)
;cpu time: 15 real time: 21 gc time: 0
;> (define size (expt 2 8))   ;  256:   200ns
;> (go)
;cpu time: 2 real time: 2 gc time: 0
;>


NEW: up to 2^12 bytes per node within an IO-read-window of up to 2^16 bytes:

node types (3 logical, 4 physical):
- internal
- transition-leaf
- final-leaf
  - with typical and small-singleton physical type sub-varieties

typical node layout:
- header (2 (for final-leaf) or 8 bytes of overhead)
  - node type descriptor
  - shared-prefix-length bytecount (0-6)
  - key-length bytecount (1-6)
  - number-of-divisions (1-255 corresponding to 2-256 possible payloads)
    - each division corresponds to a divider (internal node) or key (leaf node)
    - internal nodes contain (+ 1 number-of-divisions) payload pointers, partitioned by comparison with the dividers
      - under the first payload pointer, every key is smaller than the first divider
      - under the last payload pointer, every key is greater than or equal to the last divider
  - payload-pointer bytecount (1-6)
    - not needed for final-leaf nodes, which have no payloads
  - full-sequence-positional-offset-of-last-leaf-reachable-from-this-node (1-1 trillion)
    - used for cardinality estimation
    - not needed for any leaf nodes, for which cardinality is equal to number-of-divisions
- metacontent
  - array[number-of-divisions] of shared-prefix-length
  - array[number-of-divisions] of key-length
  - in an internal-node: array[(+ 1 number-of-divisions)] of payload-pointer
  - in a transition-leaf-node: array[number-of-divisions] of payload-pointer
    - not needed for final-leaf nodes, which have no payloads
  - base-address of all pointers starts just after the end of metacontent
- content
  - blob[number-of-divisions] of keys/dividers

small-singleton final-leaf node layout:
- header (2 bits of overhead)
  - node type descriptor
- metacontent (6 bits of overhead)
  - key-length (0-63)
- content
  - key

;; TODO: don't define btt-node like this.
;; We can use a macro to embed struct-language descriptions in-context rather than having to save info at the syntax level:
;; - parse as a read-and-dispatch-handler
;; - parse as a write-handler
;; supercompile out all the branches

;; how does reader work?
;; - takes a current bytevector buffer, a valid start-end range, and a way to request that an out-of-range position is paged in
;; - if range is not large enough to include needed data, make a request for more
;; - handler code now has all necessary structure variables bound in its scope

;; how does writer work?
;; - handler code prepares a scope that binds all necessary structure variables
;; - takes a bytevector buffer, a valid start-end range, and a way to request that an out-of-range position be made available for writing
;; - bound variables are written to the buffer at its start position
;; - return number of bytes written

;; TODO: hold on, shouldn't we be reading BACKWARDS instead of FORWARD in the buffer?
;(define size.max.btt-node 4096)
;(define size.min.btt-node 4)
;(define-syntax-rule (handle-btt-read buffer start end E.fail.early E.fail.late E.succeed.0 E.succeed.1)
;  (if (unsafe-fx< (unsafe-fx- end start) size.min.btt-node)
;      E.fail.early
;      (let* ((b0                 (unsafe-bytes-ref buffer start))
;             (b1                 (unsafe-bytes-ref buffer (unsafe-fx+ start 1)))
;             (b2                 (unsafe-bytes-ref buffer (unsafe-fx+ start 2)))
;             (count.key*         (unsafe-bytes-ref buffer (unsafe-fx+ start 3)))
;             (count.key*         (unsafe-fx+ count.key* 1))
;             (size.body          (unsafe-fx+ (unsafe-fxlshift (unsafe-fxand #b1111 b1) 8) b2))
;             (address.body.start (unsafe-fx+ start 4))
;             (address.body.end   (unsafe-fx+ address.body.start size.body)))
;        (if (unsafe-fx< (unsafe-fx- end address.body.start) size.body)
;            E.fail.late
;            (let* ((bytecount.length.shared-prefix (unsafe-fxand #b111 (unsafe-fxrshift b0 3)))
;                   (bytecount.length.key           (unsafe-fxand #b111 b0))
;                   (bytecount.address.payload      (unsafe-fxand #b111 (unsafe-fxrshift b1 5)))
;                   (bytecount->n-ref
;                     (lambda (bytecount)
;                       (case bytecount
;                         ((0)  (lambda (pos) 0))
;                         ((1)  (lambda (pos) (1-unrolled-unsafe-bytes-nat-ref buffer pos)))
;                         ((2)  (lambda (pos) (2-unrolled-unsafe-bytes-nat-ref buffer pos)))
;                         ((3)  (lambda (pos) (3-unrolled-unsafe-bytes-nat-ref buffer pos)))
;                         ((4)  (lambda (pos) (4-unrolled-unsafe-bytes-nat-ref buffer pos)))
;                         ((5)  (lambda (pos) (5-unrolled-unsafe-bytes-nat-ref buffer pos)))
;                         ((6)  (lambda (pos) (6-unrolled-unsafe-bytes-nat-ref buffer pos))))))
;                   (pos address.body.start)
;                   (length*.shared-prefix
;                     (let ((n*    (make-fxvector count.key*))
;                           (n-ref (bytecount->n-ref bytecount.length.shared-prefix)))
;                       (let loop ((i 0) (pos pos))
;                         (when (unsafe-fx< i count.key*)
;                           (fxvector-set! n* i (n-ref pos))
;                           (loop (unsafe-fx+ i 1) (unsafe-fx+ pos bytecount.length.shared-prefix))))
;                       n*))
;                   (pos (unsafe-fx+ (unsafe-fx* bytecount.length.shared-prefix count.key*) pos))
;                   (length*.key
;                     (let ((n*    (make-fxvector count.key*))
;                           (n-ref (bytecount->n-ref bytecount.length.key)))
;                       (let loop ((i 0) (pos pos))
;                         (when (unsafe-fx< i count.key*)
;                           (fxvector-set! n* i (n-ref pos))
;                           (loop (unsafe-fx+ i 1) (unsafe-fx+ pos bytecount.length.key))))
;                       n*))
;                   (pos (unsafe-fx+ (unsafe-fx* bytecount.length.key count.key*) pos))
;                   (type.node (unsafe-fxrshift b0 7))
;                   (count.payload* (unsafe-fx+ type.node count.key*))
;                   (address*.payload
;                     (let ((n* (make-fxvector count.payload*))
;                           (n-ref (bytecount->n-ref bytecount.address.payload)))
;                       (let loop ((i 0) (pos pos))
;                         (when (unsafe-fx< i count.payload*)
;                           (fxvector-set! n* i (n-ref pos))
;                           (loop (unsafe-fx+ i 1) (unsafe-fx+ pos bytecount.address.payload))))
;                       n*))
;                   (pos (unsafe-fx+ (unsafe-fx* bytecount.address.payload count.payload*) pos)))
;              (case type.node
;                ((0) (let* ((max-reachable-cardinality-offset (6-unrolled-unsafe-bytes-nat-ref buffer pos))
;                            (address.key*                     (unsafe-fx+ pos 6)))
;                       E.succeed.0))
;                (else (let ((address.key* pos))
;                        E.succeed.1))))))))

(define-syntax handle-structure-read-etc
  (syntax-rules ()
    ((_ )
     )

    ))

(define btt-node  ; b-tree-trie
  (with-structure
    (struct
      (byte b0 b1 b2 count.key*)
      (let count.key* (unsafe-fx+ count.key* 1))
      (let size.body (unsafe-fx+ (unsafe-fxlshift (unsafe-fxand #b1111 b1) 8) b2))
      (size-= size.body)
      (here address.body.start)
      (let address.body.end (unsafe-fx+ address.body.start size.body))
      ;; TODO: unused bits
      ;(unsafe-fxand #b1 (unsafe-fxrshift b0 6))
      ;(unsafe-fxand #b1 (unsafe-fxrshift b1 4))
      (let bytecount.length.shared-prefix (unsafe-fxand #b111 (unsafe-fxrshift b0 3)))
      (let bytecount.length-key           (unsafe-fxand #b111 b0))
      (let bytecount.address.payload      (unsafe-fxand #b111 (unsafe-fxrshift b1 5)))
      ((array bytecount.length.shared-prefix count.key*) length*.shared-prefix)
      ((array bytecount.length.key           count.key*) length*.key)
      ;; type:
      ;; - 0: leaf
      ;; - 1: internal
      (let type.node (unsafe-fxrshift b0 7))
      ((array bytecount.address.payload (unsafe-fx+ type.node count.key*)) address*.payload)
      (discriminate type.node 0 1)
      (case type.node
        ((0) ((bytes 6) max-reachable-cardinality-offset))
        ((1) (struct)))
      (here address.key*))
    ;; TODO: handler code goes here
    ))

;(define btt-node  ; b-tree-trie
;  (with-structure
;    (struct
;      (size-min 3)
;      (size-max 4096)
;      (byte b0)
;      ((bytes 2) size.body)
;      (size-min size.body)
;      (size-max size.body)
;      ;; type:
;      ;; - 0: internal
;      ;; - 1: transition-leaf  (may no longer need a distinction between transition and final leaf)
;      ;; - 2: final-leaf
;      ;; - 3: small-singleton final-leaf  (no longer seems worth it)
;      (let type (unsafe-fxrshift b0 6))
;      (discriminate type (range 4))
;      (case type
;        ((3) (struct
;               (let length.key
;                 ;(unsafe-fxand #b111111 b0)
;                 size.body)
;               ((text length.key) key)))
;        (else
;          (struct
;            (here address.body.start)
;            (let address.body.end (unsafe-fx+ address.body.start size.body))
;            (let bytecount.length.shared-prefix (unsafe-fxand #b111 (unsafe-fxrshift b0 3)))
;            (let bytecount.length-key           (unsafe-fxand #b111 b0))
;            (byte count.key*)
;            ;(min count.key* 2)  ; probably not important
;            ((array (bytes bytecount.length.shared-prefix) count.key*) length*.shared-prefix)
;            ((array (bytes bytecount.length.key)           count.key*) length*.key)
;            (case type
;              ((2)  (struct))
;              (else (struct
;                      (byte bytecount.address.payload)
;                      ((array (bytes bytecount.address.payload) count.key*) address*.payload)
;                      (case type
;                        ((0) (struct
;                               ((bytes bytecount.address.payload) address.last-payload)
;                               ((bytes 6) max-reachable-cardinality-offset)))
;                        ((1) (struct))))))
;            (here address.key*)))))
;    ;; TODO: handler code goes here
;    ))

low-level read operations:
- parse a header from a bytevector w/ offset
- parse metacontent from a bytevector w/ offset
- find a key/divider in a bytevector w/ offset and predecessor
- enumerate keys/dividers from a bytevector w/ offset and predecessor
- locate a key pointer w/ base-address
- locate a payload pointer w/ base-address
  - can resolve a not-final-leaf node pointer locally if its header and metacontent fits and if the last payload pointer is <= end of buffer
  - can resolve a final-leaf node pointer locally if the previous bucket pointer is <= end of buffer

building an index by merging multiple indexes:
- divide and conquer using a heap (or tournament tree) for tracking the trie with the next tuple

building an index from a block of sorted tuples:
- for each non-final attribute
  - for each key of the attribute
    - identify subrange of the key
    - recurse with subrange and remaining attributes
    - collect keyed-subtries (at most 256 subtries in each 4096-byte node in a 64k window)
    - yield a keyed-subtrie for this key
- for final attribute collect keys into final-leaf nodes (at most 256 keys in each 4096-byte node in a 64k window)
- return index metadata
  - base address
  - total byte size
  - total number of tuples
  - cardinality of first attribute

(define window-max-size 65536)
(define node-max-size 4096)

;(define blob (make-bytes window-max-size))
(define blob (make-bytes node-max-size))
(define blob-write (storage-read-write-methods-write
                     (storage:bytevector-read-write blob 0)))

;; TODO: track previous value (initially empty string) and prefix-length to determine next shared-prefix
;; TODO: when starting new bucket, form divider using first-written bucket value to share its content (can be a prefix)
(define (build-index/tuple* write-block start.write tuple* count.attr)
  (if (= (vector-length tuple*) 0)
      (error "TODO: empty tuple*")
      (let* ((last.attr   (- count.attr 1))
             (pos         start.write)
             (write-block (lambda (count bv.source)
                            (write-block count pos bv.source 0)
                            (set! pos (unsafe-fx+ pos count)))))
        (let* ((sub*
                 (let loop.attr ((i.attr       0)
                                 (start.tuple* 0)
                                 (end.tuple*   (vector-length tuple*)))
                   (if (unsafe-fx< i.attr last.attr)
                       (let loop.key ((start.tuple* 0)
                                      (end.tuple*   (vector-length tuple*))
                                      (sub*         '()))
                         (if (unsafe-fx< start.tuple* end.tuple*)
                             (let* ((t       (vector-ref tuple* start.tuple*))
                                    (key     (vector-ref t i.attr))
                                    (end.key
                                      ;; TODO: binary search
                                      (let loop ((i start.tuple*))
                                        (cond ((= i end.tuple*)                                       end.tuple*)
                                              ;; TODO: use faster bytevector equality check
                                              ((equal? (vector-ref (vector-ref tuple* i) i.attr) key) (loop (+ i 1)))
                                              (else                                                   i)))))
                               (loop.key end.key end.tuple*
                                         ;; TODO: real accumulation
                                         (cons
                                           (let* ((sub*       (loop.attr (unsafe-fx+ i.attr 1) start.tuple* end.key))
                                                  (count.sub* (length sub*))
                                                  (count.bv   (+ 2 (bytes-length key) (* 2 count.sub*)))
                                                  (bv.sub*    (make-bytes count.bv)))
                                             (for-each (lambda (i sub)
                                                         (bytes-set! bv.sub* (* i 2)       (unsafe-fxand 255 sub))
                                                         (bytes-set! bv.sub* (+ (* i 2) 1) (unsafe-fxrshift sub 8))
                                                         )
                                                       (range count.sub*)
                                                       sub*)
                                             (bytes-set! bv.sub* count.sub* count.sub*)
                                             (bytes-set! bv.sub* (+ count.sub* 1) (bytes-length key))
                                             (bytes-copy! bv.sub* (+ count.sub* 2) key)
                                             (write-block count.bv bv.sub*)
                                             pos)
                                           sub*)))
                             ;; TODO: real return
                             (reverse sub*)))
                       ;; TODO: real final leaf building
                       (map (lambda (i)
                              (let* ((val   (vector-ref (vector-ref tuple* i) i.attr))
                                     (count (bytes-length val))
                                     )
                                (write-block count val)
                                (write-block 1 (bytes count))
                                pos))
                            (range start.tuple* end.tuple*)))))
               (count.sub* (length sub*))
               (count.bv   (+ 1 (* 2 count.sub*)))
               (bv.sub*    (make-bytes count.bv)))
          (for-each (lambda (i sub)
                      (bytes-set! bv.sub* (* i 2)       (unsafe-fxand 255 sub))
                      (bytes-set! bv.sub* (+ (* i 2) 1) (unsafe-fxrshift sub 8)))
                    (range count.sub*)
                    sub*)
          (bytes-set! bv.sub* count.sub* count.sub*)
          (write-block count.bv bv.sub*)
          pos))))

(define example-tuple*
  '#(
     #(#"00" #"abc" #"0123")
     #(#"00" #"abc" #"0124")
     #(#"00" #"abc" #"0245")
     #(#"00" #"abc" #"0246")
     #(#"00" #"abc" #"0247")
     #(#"00" #"abc" #"0250")
     #(#"00" #"abc" #"0251")
     #(#"00" #"abd" #"0123")
     #(#"00" #"abd" #"0124")
     #(#"00" #"abd" #"0245")
     #(#"00" #"abd" #"0246")
     #(#"00" #"abd" #"0247")
     #(#"00" #"abd" #"0250")
     #(#"00" #"abd" #"0251")
     #(#"00" #"bbb" #"0245")
     #(#"00" #"bbb" #"0246")
     #(#"00" #"bbb" #"0247")
     #(#"00" #"bbb" #"0250")

     #(#"01" #"abc" #"0123")
     #(#"01" #"abc" #"0124")
     #(#"01" #"abc" #"0245")
     #(#"01" #"abc" #"0246")
     #(#"01" #"abc" #"0247")
     #(#"01" #"abc" #"0250")
     #(#"01" #"abc" #"0251")
     #(#"01" #"abd" #"0246")
     #(#"01" #"abd" #"0247")
     #(#"01" #"abd" #"0250")
     #(#"01" #"abd" #"0251")
     #(#"01" #"bbb" #"0245")
     #(#"01" #"bbb" #"0246")
     #(#"01" #"bbb" #"0247")
     #(#"01" #"bbb" #"0250")

     #(#"02" #"abc" #"0123")
     #(#"02" #"abc" #"0124")
     #(#"02" #"abc" #"0245")
     #(#"02" #"abc" #"0246")
     #(#"02" #"abc" #"0247")
     #(#"02" #"abc" #"0250")
     #(#"02" #"abc" #"0251")
     #(#"02" #"abd" #"0123")
     #(#"02" #"abd" #"0124")
     #(#"02" #"abd" #"0245")
     #(#"02" #"abd" #"0246")
     #(#"02" #"abd" #"0247")
     #(#"02" #"abd" #"0250")
     #(#"02" #"abd" #"0251")
     #(#"02" #"bbb" #"0245")
     #(#"02" #"bbb" #"0246")
     ))

4096 bytes per node, with indirect overflow for oversized keys

shared across all slots:

- 12 bits for node size (I don't think we need this since node records face backwards on disk!)
  - but maybe we do need a 12-bit base-address offset in order to locate "inline sub-tries" properly

- 1 bit for node type (internal vs. leaf)
  - should we have another type for singleton value? may not be necessary since node overhead would only be about 8 bytes
- 7 (or 8?) bits for key count

3-bit key-cardinality byte-width
key-cardinality (number of keys encompassed by this sub-trie)

;; base-offset can just be the address of this trie-node, with pointers interpreted as negative distances relative to base
;; - negative distances because all addresses should be to content written to disk before this node
;3-bit pointer base-offset byte-width
;pointer base-offset

3-bit pointer byte-width
- pointer is 1 to 8 bytes wide

3-bit key-max-inline-length byte-width
key-max-inline-length
- if a key is longer, it will only be partially inlined, and include a pointer to the remaining content

per value slot: (except in final leaf layer)
value-pointer (which may point into inlined value content blob (this is the case if the pointer is less than 4096))


per key slot: (keys in internal nodes only need to be dividers, not complete actual keys)

3-bit key-shared-prefix-length byte-width
3-bit key-length byte-width
key-shared-prefix-length (shared with previous key in this node, if any)
key-length (length does not include shared-prefix, which may allow us to save on byte-width)
key-content-up-to-max-inline-length
key-pointer-to-remaining-content (remaining content does not include inlined content, which may allow us to save a little)


inlined value content blob:
shape of this blob varies


;; - for internal nodes:

;; - width of

;; - for leaf nodes:


;; What are the building blocks for on-disk record structures?
;; - arrays of fixed-width integers
;;   - learn the array length, and element width, from out-of-band info
;; - blobs of variable-width bytevectors
;;   - individual bytevectors are only accessible after learning their position and length

;; - fixed or variable-width bytevectors
;;   - variable-width values only become accessible after learning their size
;;     - i.e., they are manipulated like "fixed"-width once you learn the width
;;   - fixed-width integers
;;     - base-relative pointers

;;; Metadata and type descriptors
;; - Database
;; - Relation

;;; Trie
;; - Using skip-list or b-tree-like index layers
;; - (multiple levels of) prefix sharing
;; - LZ compressed suffixes

;;; Column
;; - a value's position in the column is implicit
;; - are all columns actually represented as tries?
;;   - logical vs. physical (representing an index layer) columns


;;; Physical column
;; - used to represent key-value mappings, such as index and data layers


refer to sequentially accessed disk data as a list, or (chunked) stream (aka tape)


alphabet compression (e.g., 16 possible values can fit into 4 bits instead of needing 8)

one-to-many value prefixes

one-to-one value prefixes
- force truncation at a fixed width for more efficient storage and retrieval
  - if a full value is shorter than the prefix width, would need to incorporate length information, or suffix with zeroes if they are not in alphabet
  - or just heterogeneously stack blocks of different kinds, and note this in metadata for a slab / chunk
    - i.e., if one value is not compatible with a fixed width format, then separate it into a different type of block, even if it is awkwardly located
      - let a layout optimizer worry about whether doing this is too inefficient to be worth using fixed width blocks around this thorn value

consider representation and meaning to be orthogonal
- e.g., a sequence of values may or may not be ordered, but this should not matter to the representation of the sequence
  - it only matters to metadata, i.e., the data describing such a sequence, and logically grouping it with other sequences

so what are the operations on a representation of a sequence of values?
- get a bytevector or integer (and a simple, special case for a single byte)
  - special case for integer since it is common and avoids allocation
  - both need: position, bytecount
    - bytevector also needs alphabet remapping
    - integer needs signedness and endianness (maybe just endianness, actually)
- compare a value with a bytevector without allocation
  - need: position, bytecount

So these low-level-representation operations are not very interesting.  The interesting part will be
the metadata and higher-level structures built on top.  What are these?

Atom ::= Text | Integer
;; specific atom type in each case should always be homogeneous
D ::= UnorderedList of Atom
    | OrderedSet of Atom
    | OrderedDict of Atom to D  ; hierarchical type structure of D should be statically known and homogeneous

Even higher level structures can be defined in terms of these.
For instance:

Table could be a tuple of unordered lists, each of the same length
- although maybe this is not appropriate at this level
  - in fact, this idea may be more appriopriate at an even lower level, to implement dicts, sets, lists out of tuples of raw sequences
Trie (used for an index) could be a Dict of Dicts of Dicts ... of a Set


Atom ::= Text | Integer
Sequence of Atom
Table is a tuple of Sequences of the same length

OrderedDict is built from Tables with this structure:

or are they built from a pair of lists, which may be built from tables?


sequences themselves may have hierarchical representation with prefix tries etc.


maybe we should start by enumerating representation ideas, then work bottom up


atom ::= text | integer
represented as a string of bytes

fixed-width-seq: (width, count) -> [atom]
varying-width-seq: count -> (width-of-width, width*:fixed-width-seq, [atom])

prefix-sharing-seq: prefix-bytes ->

seq-of-prefix-sharing-seq: count-of-prefixes -> (x-seq-of-prefixes, x-seq-of-spans-that-address-more-seqs)


trie:


EVERYTHING SHOULD BE LOCAL, EVEN WITH COMPRESSION (like prefixes)


block format:
- type (compression and subformat (such as widths) info, keys only, keys and values)
  - if this block fits in less than one page, indicate whether the next block also fits entirely in the same page, allowing for immediate continuance
- key prefix, if any
- n: item count (number of keys or key value pairs)
- n * fixed-width values (if non-leaf; these are always addresses to the next index block)
- n * fixed-width key lengths
- n * keys

leaf vs. non-leaf blocks

non-leaf has keys that are always short, and includes "values" (addresses to next index block)
leaf has full keys, but no "values"

what kind of compression and subformat info?
- whether prefix, its length and content
- whether delta encoding and offset
- whether sparse byte-column contents, with column-specific offsets and byte values
  - like a column-oriented run-length encoding
  - probably common when used with delta (or delta of delta) encoding
  - does this subsume prefix compression? yes, with multi-column repetition
  - would this be better done as a byte-keyed trie or radix tree?
    - maybe each block header initiates the next (optional) radix tree branch or bucket of leaves?
      - this would subsume sparse representation if used with delta (of delta)
  - when a prefix is introduced, it persists into later levels of an index
    - but disappears at the final (leaf) layer, which has to function independently of the index (or does it?)
      - maybe independent leaf layer doesn't make sense, because it would only be iterable for the first column?
      - well, other columns would still be iterable at their leaf layers...
        - but we wouldn't have a way to correlate table positions across columns unless we include a repeat-count
- whether alphabet encoding, and its mapping
  - is this subsumed by delta (of delta) ?  no

we should include cardinality subcounts so that we can reconstruct the cardinality of a subrange of keys, right?
- we need this for query planning
- seems like we only need to include cardinality offsets at each leaf key, since any range restriction will
  identify a min/max leaf key
  - no cardnality info needed at internal b-tree nodes
  - cardinality for current range is: (- max-leaf-key-cardinality-offset min-leaf-key-cardinality-offset)

how do we construct a page-friendly radix tree from sorted values?
- write shared-prefix entries backwards on disk as they are discovered?
- also write string contents backwards?
  - because we want the beginnings of long strings to be close enough to the header to be on the same page

is this disk layout and writing style compatible with leveled memory usage for indexes?

column-oriented ordered streaming format vs. row-trie-oriented layered index format
- streaming format can only make decisions using local information
- layered index can be built with multiple passes (roughly one per layer?) over the streams
  - intermediate radix/prefix nodes need to be inserted between layers when shared prefixes are so
    large that you would need more than a handful of bytes per separator to keep them distinct


what are the operations on a sequence of values in general?

what are the parameters of representations for a sequence of values?
- each parameter may have subparameters that need to be learned from out-of-band metadata
- fixed vs. varying width
  - for fixed width, need the width
  - for varying width, need the length encoding
    - length encoding could itself be fixed or varying width
      - two options for representing varying width lengths:
        - utf8-style byte chains with continuation identified by highest bit
        - leading byte count
      - but it may be good to avoid varying width length encodings entirely in value sequences by
        changing the block format at boundaries where the bytes-needed-for-length change
        - so text of length < 256 can be grouped together, < 255 and < 65536 grouped together, etc.
    - MAYBE BETTER: it could make sense to store varying lengths out-of-band, together in a separate block
      - these lengths then become amenable to compression techniques (can think of fixed length as a pathologically good case of compression)
      - for instance, if all lengths are < 255 and < 512, we can still represent each with one byte, sharing knowledge of the constant upper byte
- alphabet remap
  - need the target bitvector width and the byte-to-bitvector mapping
  - for clean byte boundaries, could limit valid bitvector widths to 1, 2, 4, 8 (where 8 involves no remapping)


maybe no maps at all, just columns, and a map is a pair of columns (or maybe we can map to zero, or multiple columns, so more like a keyed table)
- better for cache if we just search one column and track implicit position, then find same position in second column?
- also better for compression
- we can concatenate the pair of columns together and use a single continguous block read to obtain both at once
  - for alignment, may want to put the fixed-width value sequence first, even if it is logically the thing being mapped to, rather than the keys

typical keyed table co-located blocks
- keys (which may be prefixes, not necessarily complete values)
- counts of values appearing before each key, to support lookup by logical position
- locations in and sizes of blocks in another column (e.g., next trie layer)


offset map
- one to one
  - but could be indexed with one-to-many layers, right?

value map
- one to many, used in tries


;;;

;;; Sequence of values

;;; Varying-length value
;; - a length-prefixed bytevector

;;; Fixed-length value
;; - a bytevector of known length


;;; codec

;; primary type tags
immediate: payload differentiates between these: () #f #t
integer: payload is small bit-length
ratio: payload is shared small bit-length

text: payload differentiates between these: symbol string bytevector, followed by small length
pair
vector: payload depends on tuple vs. array subformat
- tuple: small length followed by length * subtype tags
  - maybe this is not useful enough to justify special treatment
- array: single subtype tag followed by length (or should it be the other way around for disambiguating lengths?)

;; most common will be text, so make that the least expensive to encode
;; - bytevector will be most common subtype, symbol will be least common

;; next most common will be integer, or maybe this is more common than text
;; - would be good to have 6 bits free for describing the small bit-length
;;   - can have two bit-length modes, decided using one more bit: bit-length vs. byte-length mode
;; then maybe ratio, maybe followed by immediate, followed by vector, followed by pair


;; type tag by bit:
;; 0
;; - 0
;;   - 0
;;   - 1
;; - 1
;;   - 0
;;   - 1
;; 1
;; - 0
;;   - 0
;;   - 1
;; - 1
;;   - 0
;;   - 1



;; A storage instance backs a single, mutable database, with immutable relations

storage:file-read-only
storage:file-read-write
storage:file-read-write-new

storage-reader
storage-append!
storage-commit!
storage-abort!  ; should truncate the file


(compressed) block storage vs database

database metadata represented with a table found at the end of storage
- this table maps names to tables (identified by their position in storage)

a btree-like index is layered, where each layer is a table mapping keys to blocks in the next layer
topmost layer is ideally a single block, with subsequent layers branching out by some factor
final layer maps keys to blocks in the table being indexed


numerically-columned tables instead of relations?  what should we assume about table substructure? (skip-lists, btrees, etc.)
also row-based slicing and access to row numbers
what table operations should be automatically planned, and which need to be explicit?

main table data is a sequence of blocks of tuples, with basic statistics such as tuple
count, per-column min/max, maybe also full-tuple min/max? and
maybe a list of fixed-width offsets to tuples if we don't inline tuple lengths

indexes don't necessarily have to precisely map column values to individual rows, they could instead point a sorted range of column values to blocks
- should often reduce index size significantly
- precise rows don't help much anyway since entire blocks are decoded as a whole
But what is the interface to then access an arbitrary block of a table?
- Rely on knowing the block size and narrow the table via the delimiting row ids?
  - But block size is based on a total size in bytes, not a tuple count
  - Also, it would be inefficient to locate a block indirectly by an id, rather than directly by its byte position (both start and end positions)


;; need something like a buffer-size (separate from backing store, even for in-memory storage)
;; but how is it divided over complex operations?
;; if we consider it to be a block-size for the final transfer step, maybe we can infer what the other sub-operation block-sizes would need to be
;; ?
(define preferred-block-size-for-storage                (expt 2 14))
(define preferred-block-size-for-incremental-processing (expt 2 12))
(define default-buffer-size                             (expt 2 9))  ; TODO: assert this is large enough to determine metadata record size

;;; Assume at most one storage object has been created with a given path or fixed-memory buffer.

;(splicing-local
;  ;; TODO: for performance, is it necessary to cache the port with thread-local storage?
;  ((define (file->reader path)
;     (let ((in (open-input-file path)))
;       (file-stream-buffer-mode in 'none)
;       (lambda (pos count bv start)
;         (file-position in pos)
;         (read-bytes! bv in start (+ start count)))))
;   (define (file->writer path)
;     (let ((out (open-output-file path)))
;       (file-stream-buffer-mode out 'none)
;       (lambda (pos count bv start)
;         (file-position out pos)
;         (write-bytes bv out start (+ start count))
;         ;; TODO: is flush-output necessary when we use 'none buffer-mode?
;         (flush-output out)))))
;  (define (storage:file-read-only path)
;    (unless (file-exists? path) (error "storage does not exist" path))
;    (lambda (method)
;      (case method
;        ((reader) (file->reader path))
;        (else     (error "not a method of storage:file-read-only" method)))))
;  (define (storage:file-read-write path new?)
;    (define ((end-writer pos) out)
;      (file-position out 0)
;      (write-bytes (integer->integer-bytes pos 8 #f #t) out)
;      (flush-output out))
;    (let ((exists? (file-exists? path)))
;      (and (or new? exists?)
;           (not (and new? exists?))
;           (begin
;             (when new? (call-with-output-file path (end-writer 0)))
;             (let ((pos.end (call-with-input-file path (lambda (in) (integer-bytes->integer
;                                                                      (read-bytes 8 in) #f #t)))))
;               (lambda (method)
;                 (case method
;                   ((reader)   (file->reader path))
;                   ((writer)   (file->writer path))
;                   ((end-ref)  pos.end)
;                   ((end-set!) (lambda (pos)
;                                 (call-with-output-file path (end-writer pos) #:exists 'update)
;                                 (set! pos.end pos)))
;                   (else       (error "not a method of storage:file-read-write" method))))))))))

;(splicing-local
;  ((define (bytevector->reader bv.memory start end)
;     (lambda (pos count bv start)
;       (let ((start.memory (+ pos start)))
;         (bytes-copy! bv start bv.memory start.memory (+ start.memory count)))))
;   (define (bytevector->writer bv.memory start end)
;     (lambda (pos count bv start)
;       (bytes-copy! bv.memory (+ pos start) bv start (+ start count)))))
;  (define (storage:fixed-memory-read-only bv.memory start end)
;    (case method
;      ((reader) (bytevector->reader bv.memory start end))
;      (else     (error "not a method of storage:fixed-memory-read-only" method))))
;  (define (storage:fixed-memory-read-write bv.memory start end new?)
;    (case method
;      ((reader) (bytevector->reader bv.memory start end))
;      ((writer) (bytevector->writer bv.memory start end))
;      (else     (error "not a method of storage:fixed-memory-read-write" method)))))
;
;(define (storage-reader   s)     (s 'reader))
;(define (storage-writer   s)     (s 'writer))
;(define (storage-end-ref  s)     (s 'end-ref))
;(define (storage-end-set! s pos) ((s 'end-set!) pos))

(splicing-local
  ((define (metadata-buffer-and-size sread! pos.end)
     (let* ((buffer-size preferred-block-size-for-storage)
            (buffer      (make-bytes buffer-size 0)))
       (sread! (- pos.end buffer-size) buffer-size buffer 0)
       (let ((size (integer-bytes->integer buffer #f #t (- buffer-size 8))))
         (if (< buffer-size size)
             (let ((buffer.new (make-bytes size 0)))
               (bytes-copy! buffer.new 0 buffer 0 buffer-size)
               (sread! (- pos.end size) (- size buffer-size) buffer.new buffer-size)
               (values size buffer.new))
             (values buffer-size buffer)))))
   ;; Reverse storage format for a database metadata record:
   ;; - 8 bytes describing the size of this record
   ;; - points to a previous database, or the empty database
   ;;   - 8 bytes
   ;; - points to a parent   database, or the empty database
   ;;   - 8 bytes
   ;; - maps relation names to relations
   ;;   - 8 bytes describing the number, N, of name-to-relation mappings
   ;;   - repeated N times:
   ;;     - 8 bytes offset to a relation metadata record
   ;;     - 8 bytes number of bytes, K, representing the name
   ;;     - K bytes representing the name
   (define (database:metadata-pos s pos)
     (if (= pos 0)
         (letrec ((db.empty (database s (lambda () db.empty) (lambda () db.empty) (hash))))
           db.empty)
         (let*-values (((sread!)             (storage-reader s))
                       ((buffer-size buffer) (metadata-buffer-and-size sread! pos.end)))
           (let ((pos.previous (integer-bytes->integer buffer #f #t (- buffer-size 16)))
                 (pos.parent   (integer-bytes->integer buffer #f #t (- buffer-size 24))))
             (let loop ((count.relation* (integer-bytes->integer buffer #f #t (- buffer-size 32)))
                        (end             (- buffer-size 32))
                        (name=>relation  (hash)))
               (cond
                 ((= count.relation* 0)

                  ;; TODO: a downside of this interface is that a database may be instantiated multiple
                  ;; times, due to being reachable from multiple children and next.  We could avoid this
                  ;; by instantiating databases eagerly with a memo table keyed by pos.end.
                  ;; This same downside will also affect relations, because each lazilyl loaded database
                  ;; wouldn't know what relations have already been loaded.  So we probably do want to
                  ;; eagerly load everything.  If not, we will need to maintain a memo table indefinitely.

                  (database s
                            (let ((previous pos.previous))
                              (lambda ()
                                (when (integer? previous)
                                  (set! previous (database:metadata-pos s previous)))
                                previous))
                            (let ((parent pos.parent))
                              (lambda ()
                                (when (integer? parent)
                                  (set! parent (database:metadata-pos s parent)))
                                parent))
                            name=>relation))
                 (else
                   (let* ((size.name (integer-bytes->integer buffer #f #t (- end 16)))
                          (end.next  (- end (+ 16 size.name))))
                     (loop (- count.relation* 1)
                           end.next
                           (hash-set name=>relation
                                     (let ((bv.name (make-bytes size.name 0)))
                                       (sread! end.next size.name bv.name 0)

                                       ;; TODO: decode
                                       bv.name

                                       )
                                     (relation:metadata-pos s (integer-bytes->integer
                                                                buffer #f #t (- end 8)))))))))))))

   (define (database s ^previous ^parent name=>relation)
     (lambda (method)
       (case method
         ((previous) (^previous))
         ((parent)   (^parent))
         ((names)    (hash-keys name=>relation))
         ((ref)      (lambda (name) (hash-ref name=>relation name #f)))
         ((set)      (lambda (arg*)
                       (let* ((name* (let loop ((name (car arg*)) (arg* (cddr arg*)))
                                       (cons name (cond ((null? arg*) '())
                                                        (else (loop (car arg*) (cddr arg*)))))))
                              (r*    (let loop ((r    (cadr arg*)) (arg* (cddr arg*)))
                                       (cons r    (cond ((null? arg*) '())
                                                        (else (loop (cadr arg*) (cddr arg*)))))))
                              (name=>relation (foldl (lambda (name r n=>r) (hash-set n=>r name r))
                                                     name=>relation name* r*)))
                         ;; TODO: eagerly materialize relations to storage if they're not already present
                         ;; TODO: write new db to storage and return it

                         )))
         ((remove)   (lambda (name*)
                       (let ((name=>relation (foldl (lambda (name n=>r) (hash-remove n=>r name))
                                                    name=>relation name*)))
                         ;; TODO: write new db to storage and return it

                         )))
         ((become-latest!)

          ;; TODO: write new db to storage and return it
          )
         (else (error "not a database method" method)))))

   ;; TODO: does this need to be a reverse storage format?
   ;; Reverse storage format for a relation metadata record:
   ;; - 8 bytes describing the size of this record
   ;; - tuple blocks?
   ;;   - tuple count
   ;;   - full size (also acts as offset to index blocks)
   ;; - index blocks?
   ;;   - count
   (define (relation:metadata-pos s pos)
     )

   )
  (define (database:latest s) (database:metadata-pos s (storage-end-ref storage))))

;; Cached data will be shared across threads.
;; The cache will prioritize top-level indexes, and gradually load lower levels as space permits.
;; If the data is small enough, it may be loaded entirely into the cache.
(relation-cache! r level)  ; level = 0 through #f

;; Force db to be the most recent update in storage, so that it is returned by database:latest
(define (database-become-latest! db)               (db 'become-latest!))
(define (database-previous       db)               (db 'previous))
(define (database-parent         db)               (db 'parent))
(define (database-names          db)               (db 'names))
(define (database-ref            db name)          ((db 'ref) name))
(define (database-set            db name r . arg*) ((db 'set) (cons name (cons r arg*))))
(define (database-remove         db name . name*)  ((db 'remove) (cons name name*)))

;; stream blocks of tuples rather than individual tuples for better data locality
;; each block should be encoded with the dbk binary format
(relation:block-stream ^block-stream attribute-count assume-sorted? assume-deduped?)
;; TODO: define helpers for forming block-streams from ordinary data streams

;; functional interface for "immutable" relations, each backed by a database (may be the explicit null database to avoid using disk)

;; TODO: high-level relation operators (this is a functional interface: relations are not modified, only new ones are produced, if any)

(relation-attribute-rename r name.old name.new)


;; helpers derived from relation-reduce
(define (relation-attribute*-keep r name*) (relation-reduce r name* #f #f))
(define (relation-attribute*-drop r name*)
  (relation-attribute*-keep r (set->list (set-subtract (list->set (relation-attribute* r))
                                                       (list->set name*)))))
(define (relation-attribute*-reorder r name*)
  ;; TODO: assert that name* is a complete list
  (relation-attribute*-keep r name*))

;; all attributes must have the same name
(relation-subtract r1 r2)  ; (- r1 r2)
(relation-union r1 r2)

(relation-join r1 r2)  ; natural join over same-named attributes, possibly using Generic Join if multiple joins are chained

(relation-min-find r inclusive? tuple)
(relation-max-find r inclusive? tuple)
;; TODO: we may not want these
;(relation-attribute=  r attribute-name value)
;(relation-attribute<  r attribute-name value)
;(relation-attribute<= r attribute-name value)
;(relation-attribute>  r attribute-name value)
;(relation-attribute>= r attribute-name value)

;; Only attribute*.key and attribute*.value.new will be preserved
;; reducer should be associative and commutative
;; This can also be used for dropping attributes by not naming a value attribute or :+:
;; It can also be used for reordering attributes.
(relation-reduce r attribute*.key attribute.value :+:)

;; TODO: can we do without mappend, map, filter entirely, by just processing in Racket?
;; (still need reduce for efficient accumulation)
;(relation-mappend r f   attribute*.arg)
;
;(define (relation-map r f attribute*.arg)
;  (relation-mappend r (lambda arg* (list (apply f arg*))) attribute*.arg))
;(define (relation-filter r ? attribute*.arg)
;  (relation-mappend r (lambda arg* (if (apply ? arg*) (list arg*) '())) attribute*.arg))

;; This is probably not the right way to present statistics
;(relation-count r)

;; This does not extract values, it just truncates the relation.
;(relation-take r count)
;(relation-drop r count)

;; Should this be how we get values out of a relation?
;(relation->block-producer r)
;; This is not symmetric if we have a single-block interface for inserting into (rather, creating) a relation.
;; We would want something more like take and drop, but by block size.
;; Or maybe a stream-like thing is fine.  Instead of just thunks though, it might be better to have a lambda that takes a block-size parameter for each block request.
;; block-producer : block-size -> (or bytevector-of-size-less-than-block-size #f minimum-block-size-for-next-request)

;; How about this interface instead:
(relation-count      r)
(relation-min*-count r block-size)  ; may return 0
(relation-min*       r count)
(relation-min*-pop   r count)
(relation-min*-find  r inclusive? tuple.min)
(relation-max*-count r block-size)  ; may return 0
(relation-max*       r count)
(relation-max*-pop   r count)
(relation-max*-find  r inclusive? tuple.min)
;; Will this work for a non-materialized relation?  We could forbid it.  But then how do we use them?

;; Maybe more reasonable to combine operations and return multiple values
(relation-count      r)
(relation-min*-find  r inclusive? tuple.min)
(relation-min*-pop   r block-size)  => (values block r.remaining)
etc. for max

;; But the block-producer interface might be the most appropriate for non-materialized relations.


;; For now, this is intended only for human consumption.
(relation-describe r)

(relation-index*        r)
(relation-index*-add    r attribute-name*)
(relation-index*-remove r attribute-name*)


what is the structure of a block-stream ?

a stream is a () (value . stream) or (lambda () stream)

a block is a bytevector encoded with the dbk binary format

each block represents, and can be decoded into, a list or vector (or even stream) of tuples, or tuples can be enumerated via yield



should block-based relation insertion be managed explicitly via union instead?  then we don't even need block-streams, just blocks

how do we ensure in-memory aggregation via reduce?
use a database backed by an in-memory bytevector-port
dump it each round after replacing it with a new one containing a merged result
if a db gets too large, replace it with a disk-backed db


;;; Intermediate representation ideas

((2-hop a b)
 (edge a x)
 (edge x b))
==>
(for R edge
 (for S edge
  (fresh (a b x)
    (== (vector a x) R)
    (== (vector x b) S)
    (emit-vector 2-hop a b))))
==>
(for R edge
 (for S edge
  (fresh (a b x)
    (== a (vector-ref R 0))
    (== b (vector-ref S 1))
    (== x (vector-ref R 1))
    (== x (vector-ref S 0))
    (emit-vector 2-hop a b))))
==>
(for R edge
 (for S edge
  (== (vector-ref R 1) (vector-ref S 0))
  (emit-vector 2-hop (vector-ref R 0) (vector-ref S 1))))
==>
(join1 edge 1 edge 0   ; join1 single index vs. join multiple tuple indexes
 (lambda (R S)
  (emit-vector 2-hop (vector-ref R 0) (vector-ref S 1))))
