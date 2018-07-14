# kineo-compiler

A prototype for compiling SPARQL algebra to pseudocode using the approach
presented in "Efficiently Compiling Efficient Query Plans for Modern Hardware,"
Thomas Neumann, Proceedings of the VLDB Endowment, 2011.

```
% swift build
% cat test.rq
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?s (GROUP_CONCAT(?name) AS ?names) WHERE {
    ?s a ?type ;
        foaf:name ?name ;
        foaf:nick ?nick
}
GROUP BY ?s
LIMIT 2

% ./.build/debug/kineo-compiler -g http://example.org/default-graph/ test.rq
var rowCount1 = 0
var results2 = []
var groups2 = [:]
for result in match_bgp([?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type <http://example.org/default-graph/>., ?s <http://xmlns.com/foaf/0.1/name> ?name <http://example.org/default-graph/>., ?s <http://xmlns.com/foaf/0.1/nick> ?nick <http://example.org/default-graph/>.]) {
    let group2 = result.project([?s])
    groups2[group2].append(result)
}
for group2 in groups2 {
    var result = group2.copy()
    result["names"] = aggregate(groups[group2], GROUP_CONCAT(?name))
    result = result.project(["names", "s"])
    rowCount1 += 1
    if rowCount1 > 2 { break }
    GENERATE_RESULT(result)
}
```

## Issues

Known issues include:

* Does not currently support subqueries, query forms other than SELECT, variable named graphs (`GRAPH ?g { ... }`)
