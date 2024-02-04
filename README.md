# Canter: A triplestore inspired by Datomic

Canter aims to be a simple-to-use general-purpose database for managing data
that changes over time. It is heavily inspired by
[Datomic](https://www.datomic.com/), but it is currently focused on single-node
operation and does not implement sharding or any distributed features.

## Goals

[ ] Provide an embedded triplestore as a Go library
[ ] Provide a SPARQL endpoint for querying data externally
[ ] Focus on performance over flexibility. For example, all predicates/attributes must be declared with a specific type ahead of time.
[ ] Allow time travel queries
