# dbKanren

This implementation of Kanren supports defining, and efficiently querying,
large-scale relations.


## TODO

### Data processing

* types
  * typical s-expression
    * boolean, null, number, symbol, string, pair
  * vectors as tuples (may be heterogeneous) or arrays (must be homogeneous)
  * fixed-size nat (number-of-bytes, content)
    * used to represent IDs, offsets, lengths
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
  * parsing
    * csv, nq (n-quads), json, s-expressions
    * simpler notation for implementing state machines
  * one-shot streams with eager buffering
  * gathering statistics
    * reservoir sampling
    * optional column type inference
    * per-type statistics for polymorphic columns
    * count, min, max, sum, min-length, max-length
    * histogram up to distinct element threshold
    * range bucketing
  * transformations specified as logical passes
    * may be interleaved to minimize physical passes

* binary/bytevector serialization format for compact storage and fast loading
  * type descriptions: T
    * #f for polymorphic LENGTH, SIZE, or element T
    * boolean, null, number
    * #(nat SIZE)
    * #(string LENGTH)
    * #(symbol LENGTH)
    * (T . T)
    * #(tuple T ...) or #(array LENGTH T)
  * encode and decode: polymorphic over all types
    * scalar data
      * encode,decode-string: polymorphic over length
        * encode,decode-string/length
      * encode,decode-nat: polymorphic over size
        * encode,decode-nat/size
      * encode,decode for boolean, null, symbol (similar to string), any number
    * aggregate data
      * element type descriptions: heterogeneous or homogeneous/uniform
      * encode,decode-pair
      * encode,decode-vector
        * encode,decode-vector/length
  * track and store serialization offsets of elements that vary in size/length
    * these can be used for random access to these elements


### Database representation

* namespaces of user-level (extensional and intensional) relations
  * may be parameterized by other namespaces and/or relations
    * i.e., lambdas for wiring relations together
  * high-level description of keys and indices
    * text indices: whole-string or suffix-based
      * configurable alphabet approximation (for smaller index)
  * (un)loadable at runtime
    * causes dynamic extension/retraction of affected intensional relations

* intensional relations (user-level)
  * search strategy: backward or forward chaining
    * could be inferred
    * try automatic goal reordering based on cardinality statistics
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

* low-level tables with optional keys/indices (not user-level)
  * backed by files/ports, bytes, hash tables, or decoded vectors
    * disk/memory residence and memory structure reconfigurable at runtime
  * key-less tables
    * made up of one copy of the data which is not reordered in any way
    * supports random access by offset
      * by position when elements are of fixed size
      * or when offset is otherwise known (supported by random access table)
    * examples include tables used as indices, which must not be reordered:
      * random access tables: arrays of size-varying element offsets
        * these happen to be sorted, but the elements are not user data
      * suffix arrays: given order of the array describes sorted text
  * keyed tables
    * made up of one or more lexicographically sorted copies of the data
    * columns ordered so that all keys can be expressed as prefixes
      * when keys would conflict in required column order, we make a copy
        of the data, but using a different column order to handle those keys
        * if full copying is too expensive, we can build an index relation
          * maps indexed columns to record offsets in original relation
      * bulk lookup and joining via binary search
        * records must be flattened, ID-substituted, and may be columnarized
    * keys are mapped to their supporting copy of the data
    * a key can, but doesn't need to, imply uniqueness

* incremental definition, modification, and persistence of namespaces
