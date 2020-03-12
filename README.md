# dbKanren

This implementation of Kanren supports defining, and efficiently querying,
large-scale relations.


## TODO

### Data processing

* types
  * string/vector prefixes, suffixes, and slices
    * represented as pairs or vectors of nats
      * ID, start, end are all fixed-size nats
    * prefix: (ID . end)
    * suffix: (ID . start)
    * slice:  #(ID start end)

* record/field transformations
  * type conversion: string (default), number, json, s-expression
  * flattening of tuple/array (pair, vector) fields, increasing record arity
    * supports compact columnarization of scalar-only fields
  * unique ID generation and substitution
    * replace strings/structures with unique IDs
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
  * transformations specified as logical passes
    * may be interleaved to minimize physical passes

* binary/bytevector serialization format for compact storage and fast loading
  * more efficient numbers: polymorphic, neg, int, float


### Database representation

* namespaces of user-level (extensional and intensional) relations
  * may be parameterized by other namespaces and/or relations
    * i.e., lambdas for wiring relations together
  * high-level description of keys and indices
    * text indices: whole-string or suffix-based
      * configurable alphabet approximation (for smaller index)
  * (un)loadable at runtime
    * causes dynamic extension/retraction of affected intensional relations
  * incremental definition, modification, and persistence

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
      * #f:          #f
      * text:        string
    * fixed-width: logical position (ID) = file-position / width
      * offset:      file-pos
      * text suffix: (ID . start-pos)
      * record:      #(tuple fw-value ...)
      * index:       (fw-value . ID)
  * metadata
    * integrity/consistency checking
      * file-size, file-or-directory-modify-seconds
      * source files (csvs or otherwise) with their size/modification-time
        * element type/transformations, maybe statistics about their content
    * files/types and table dependencies
      * variable-width columns
        * vw-values file
          * #f or string
        * offsets file
          * #(nat ,size)
      * text suffix:
        * suffix file
          * (#(nat ,text-ID-size) . #(nat ,pos-size))
        * text column table
      * record:
        * fw-values file
          * #(tuple ,fw-type ...)
        * vw-value (#f or text) column tables
          * these column tables may be foreign/shared
      * index:
        * fw-values file
          * fw-type: probably known-width nat (ID), int, or float
        * record table
    * statistics


### Relational language for rules and queries

* lazy population of text/non-atomic fields
  * equality within the same shared-id column can be done by id
    * also possible with foreign keys
    * analogous to pointer address equality
  * equality within the same nonshared-id column must be done by value
    * equal if ids are the same, but may still be equal with different ids
  * equality between incomparable columns must be done by value
    * address spaces are different

* support first-order (and maybe limited higher-order) functions
  * arithmetic, aggregation
    * track monotonicity for efficient incremental update
  * partial/anywhere text searches are functional, not relational
