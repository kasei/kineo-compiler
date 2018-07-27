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
var rowCount_1 = 0
let groups_3 = GroupBy([?s])
var groupsData_3 = [TermResult:[TermResult]]()
let bgp_4 = bgp_pattern([?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type ., ?s <http://xmlns.com/foaf/0.1/name> ?name ., ?s <http://xmlns.com/foaf/0.1/nick> ?nick .])
for result_4 in match(bgp: bgp_4, in: <http://example.org/>) {
    let group_3 = result_4.project(groups_3)
    groupsData_3[group_3].append(result_4)
}
for group_3 in groupsData_3 {
    var result_3 = group_3.copy()
    let agg0_3 = Aggregation(GROUP_CONCAT(?name))
    result_3["names"] = aggregate(groupsData_3[group_3], agg0_3)
    let result_2 = result_3.project(["names", "s"])
    rowCount_1 += 1
    if rowCount_1 > 2 { break }
    GENERATE_RESULT(result_2)
}
```

## Issues

Known issues include:

* Does not currently support the DESCRIBE or CONSTRUCT query forms
