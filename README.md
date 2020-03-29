# dbKanren

This implementation of Kanren supports defining, and efficiently querying,
large-scale relations.


## TODO

### Data processing

* types
  * string/vector prefixes, suffixes, and slices
    * represented as pairs or vectors of nats
      * ID, start, end are all fixed-size nats
    * prefix: `(ID . end)`
    * suffix: `(ID . start)`
    * slice:  `#(ID start end)`
  * binary/bytevector serialization format for compact storage and fast loading
    * more efficient numbers: polymorphic, neg, int, float

* table transformations
  * file formats
    * internal binary encoding
    * text, such as csv, tsv, nq, json, s-expression
      * column-specific types: string (default), number, json, s-expression
  * flattening of tuple/array (pair, vector) fields, increasing record arity
    * supports compact columnarization of scalar-only fields
  * positional ID generation and substitution
    * replace strings/structures with unique IDs
    * IDs given by logical or file position
      * original value obtained by dereferencing ID appropriately
    * leads to compact and uniform record representations

* ingestion
  * parsing: nq (n-quads)
  * gathering statistics
    * total cardinality
    * reservoir sampling
    * optional column type inference
    * per-type statistics for polymorphic columns
    * count, min, max, sum, min-length, max-length
    * histogram up to distinct element threshold
    * range bucketing
  * transformations specified relationally
    * possibly involving embedded Racket computation

* relational source data transformation
  * all records are considered ordered
    * though order may need to be implicitly given by a virtual `position` column
  * nonuniform data width, without a position table for random access
    * typical for text formats, which will be common for input sources
  * able to process non-binary (text, such as csv, tsv) directly, given format
  * ability to attach metadata to relations, describing:
    * representation (text (csv, tsv, json, s-expression, etc.) or binary)
    * column types
    * column semantics
      * immediate value
      * indirect value: logical or file position, or string suffix
    * ephemeral or persistent status
    * memory usage budget

* general comparison operators for ordering relations
  * must provide total order on s-expressions
  * consistent normalization: careful comparison of exact and inexact numbers
  * semantically order indirect references
    * string suffixes must be dereferenced to be ordered semantically
    * positional IDs from same source are orderable without dereferencing
      * dereferenced source values guaranteed to be be in same order


### Database representation

* namespaces of user-level (extensional and intensional) relations
  * namespaces/relations are (un)loadable at runtime
    * causes dynamic extension/retraction of dependent intensional relations
  * incremental definition, modification, and persistence
  * metadata and storage
    * subnamespaces (as subdirectories)
    * relation definitions and metadata
    * table files
    * external links (names and paths to namespace/relation dependencies)
    * namespace directory structure:
      * metadata.scm
      * sub
        * any subnamespace directories
      * data
        * persistent
        * ephemeral
        * temporal
        * io

* intensional relations (user-level)
  * search strategy: backward or forward chaining
    * could be inferred
    * try automatic goal reordering based on cardinality/statistics
  * forward-chaining supports stratified negation and aggregation
  * materialization (caching/tabling)
    * if materialized, may be populated on-demand or up-front
    * partial materialization supported, specified per-rule
  * optionally temporal
    * each time-step, elements may be inserted or removed
      * update is remove and insert
      * some insertions may happen at non-deterministic times (async)

* extensional relations (user-level)
  * backed by one or more tables and indices
  * mapping between user-level and low-level values
    * value (dis)assembly for dispatching to low-level tables
    * described using pattern matching?
      * tuple/array (un)flattening and reordering
      * ID substitution
  * metadata
    * field/column names and types
      * optional remapping of lexicographical order
    * backing tables: record, any indices, vw-columns, and/or text-suffixes
    * uniqueness or other constraints
    * statistics (derived from table statistics)

* low-level tables with optional keys/indices (not user-level)
  * disk/memory residence and memory structure reconfigurable at runtime
  * bulk lookup and joining via binary search (with optional galloping)
  * types of tables:
    * source records: lexicographically sorted fixed-width tuples
      * reference variable-width columns by position
    * variable-width columns: sorted variable-width elements (like text)
      * source record columns map to variable-width elements by position
    * variable-width column offsets: map logical position (ID) to file position
    * indices: map an indexed source column to source records by position
    * suffix arrays: given order of the array describes sorted text
  * high-level semantic types
    * variable-width: logical position (ID) determined by offset table
      * #f:          `#f`
      * text:        `string`
        * also possible to map text to a smaller alphabet for faster search
    * fixed-width: logical position (ID) = file-position / width
      * offset:      file-pos
      * text suffix: `(ID . start-pos)`
      * record:      `#(tuple fw-value ...)`
      * index:       `(fw-value . ID)`
  * metadata
    * integrity/consistency checking
      * file-size, file-or-directory-modify-seconds
      * source files (csvs or otherwise) with their size/modification-time
        * element type/transformations, maybe statistics about their content
    * files/types and table dependencies
      * variable-width columns
        * vw-values file
          * `#f` or `string`
        * offsets file
          * `#(nat ,size)`
      * text suffix:
        * suffix file
          * `(#(nat ,text-ID-size) . #(nat ,pos-size))`
        * text column table
      * record:
        * fw-values file
          * `#(tuple ,fw-type ...)`
        * vw-value `(#f or text)` column tables
          * these column tables may be foreign/shared
      * index:
        * fw-values file
          * fw-type: probably known-width `nat` (ID), `int`, or `float`
        * record table
    * statistics


### Relational language for rules and queries

* functional term sublanguage
  * to what extent can this be done as embedded Racket evaluation?
    * without jeopardizing dependency analysis and scheduling?
  * atoms, constructors
  * support first-order (and maybe limited higher-order) functions
    * whitelist opaque Racket procedures
    * optional constraint satisfaction rules and backward modes
      * functions must only be used for calls
      * logic variables must not flow to function positions
    * arithmetic, aggregation
      * track monotonicity for efficient incremental update
    * note: partial/anywhere text searches are functional, not relational
  * queries, which may be embedded for aggregation
    * usual mk operators `run` and `run*`
      * finite, grounded results will be returned non-duplicated and in order
      * infinite or ungrounded results will be returned as in typical mk
    * `query` streams results without order or duplication guarantees
      * but forward/bottom-up computation will implicitly order and deduplicate

* relations
  * `(define-relation (name param ...) goal ...) => (define name (relation (param ...) goal ...))`
  * `(relation (param-name ...) goal ...)`
  * `(use (relation-or-var-name ...) goal-that-may-involve-computation ...)`
    * force vars to be grounded so that embedded Racket computation succeeds
    * force dependency on given relations to ensure stratification
  * usual mk operators for forming goals:
    * `fresh`, `conde`, and constraints such as `==`, `=/=`, `symbolo`, etc.
    * `(r arg+ ...)` where `r` is a relation and `arg+ ...` are terms
  * if `r` is a relation:
    * `(r arg+ ...)` relates `arg+ ...` by `r`
    * `(r)` accesses a metaprogramming control structure
      * configure persistence, representation, uniqueness and type constraints
      * indicate evaluation preferences or hints, such as indexing
      * dynamically disable/enable
      * view source, statistics, configuration, preferences, other metadata

* misc conveniences
  * underscore "don't care" logic variables
  * query results as lazy stream to enable non-materializing aggregation
  * tuple-column/record-field keywords
    * optional keyword argument calling convention for relations
    * optional keyword projection of query results
  * relation extension?

* evaluation
  * relational computation is performed in the context of a logic environment
    * logic environments are introduced by query, relation, and fresh bodies
  * query and other term computation is performed in the context of Racket
    * Racket environments are embedded in logic environments by `use`
    * can only occur during forward/bottom-up computation (ground variables)
  * begin query top-down for non-bulk pruning while non-branching/terminating
    * mode inference with simple termination checking?
      * up front or just in time?
  * continue query bottom-up for bulk-efficient joins if safe/terminating
  * finish any remaining unsafe or branching portion of a query top-down
    * may return constrained, unground results
    * like typical miniKanren search
  * maintain constraint satisfiability during query
    * cheap first-pass via domain consistency, then arc consistency
    * then global satisfiability via search, interleaving with cheap methods

* lazy population of text/non-atomic fields
  * equality within the same shared-id column can be done by id
    * also possible with foreign keys
    * analogous to pointer address equality
  * equality within the same nonshared-id column must be done by value
    * equal if ids are the same, but may still be equal with different ids
  * equality between incomparable columns must be done by value
    * address spaces are different
