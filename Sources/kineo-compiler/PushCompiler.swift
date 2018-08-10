//
//  PushCompiler.swift
//  kineo-push
//
//  Created by Gregory Todd Williams on 7/11/18.
//

import Foundation
import Kineo
import SPARQLSyntax

public class QueryCompiler {
    var depth: Int
    var nextVariable: Int
    var uniqueVariables: [Ancestors:Int]
    public init() {
        nextVariable = 0
        depth = 0
        uniqueVariables = [:]
    }
    
    func evaluateTable(columns names: [Node], rows: [[Term?]]) throws -> AnyIterator<TermResult> {
        var results = [TermResult]()
        for row in rows {
            var bindings = [String:Term]()
            for (node, term) in zip(names, row) {
                guard case .variable(let name, _) = node else {
                    Logger.shared.error("Unexpected variable generated during table evaluation")
                    throw QueryError.evaluationError("Unexpected variable generated during table evaluation")
                }
                if let term = term {
                    bindings[name] = term
                }
            }
            let result = TermResult(bindings: bindings)
            results.append(result)
        }
        return AnyIterator(results.makeIterator())
    }
    
    struct Ancestors: Hashable {
        enum ChildIdentifier: Hashable {
            case lhs
            case rhs
            case custom(Int)
        }
        
        struct AncestorItem: Hashable {
            var plan: Plan
            var child: ChildIdentifier
        }
        
        unowned var compiler: QueryCompiler
        var ancestors: [AncestorItem]
        
        init(ancestors: [AncestorItem], compiler: QueryCompiler) {
            self.ancestors = ancestors
            self.compiler = compiler
        }
        
        public static func == (lhs: Ancestors, rhs: Ancestors) -> Bool {
            return lhs.ancestors == rhs.ancestors
        }
        
        func hash(into hasher: inout Hasher) {
            hasher.combine(ancestors)
        }
        
        func consume(state: PartialResultState, resultName: String, depth: Int) throws {
            if let parent = ancestors.last {
                try compiler.consume(plan: parent.plan, parents: self.droppingLast(), resultName: resultName, inPosition: parent.child)
            } else {
                compiler.emit(instruction: .generateResult(resultName))
            }
        }
        
        func adding(_ plan: Plan, inPosition child: ChildIdentifier = .lhs) -> Ancestors {
            let a = ancestors + [AncestorItem(plan: plan, child: child)]
            return Ancestors(ancestors: a, compiler: compiler)
        }
        
        func droppingLast() -> Ancestors {
            let a = ancestors.dropLast()
            return Ancestors(ancestors: Array(a), compiler: compiler)
        }
    }
    
    struct PartialResultState: Hashable {
        var distinct: Bool
        var necessarilyBound: Set<String>
        var potentiallyBound: Set<String>
        
        init() {
            distinct = false
            necessarilyBound = []
            potentiallyBound = []
        }
        
        init<S: Sequence>(_ vars: S) where S.Element == String {
            distinct = false
            necessarilyBound = Set(vars)
            potentiallyBound = Set(vars)
        }
        
        init<S: Sequence, T: Sequence>(distinct: Bool, necessarilyBound: S, potentiallyBound: T) where S.Element == String, T.Element == String {
            self.distinct = distinct
            self.necessarilyBound = Set(necessarilyBound)
            self.potentiallyBound = Set(potentiallyBound)
        }
        
        func addingPotentiallyBound(_ name: String) -> PartialResultState {
            let pb = potentiallyBound.union([name])
            return PartialResultState(distinct: false, necessarilyBound: necessarilyBound, potentiallyBound: pb)
        }
        
        func addingNecessarilyBound(_ name: String) -> PartialResultState {
            let nb = necessarilyBound.union([name])
            let pb = potentiallyBound.union([name])
            return PartialResultState(distinct: false, necessarilyBound: nb, potentiallyBound: pb)
        }
        
        func projecting<S: Sequence>(_ vars: S) -> PartialResultState where S.Element == String {
            let nb = necessarilyBound.intersection(vars)
            let pb = potentiallyBound.intersection(vars)
            return PartialResultState(distinct: false, necessarilyBound: nb, potentiallyBound: pb)
        }
    }
    
    indirect enum Plan: Hashable {
        case ask(Plan, analysis: PartialResultState)
        case construct(Plan, [TriplePattern], analysis: PartialResultState)
        case describe(Plan, [Node], analysis: PartialResultState)
        case exists(Plan, Plan, String, analysis: PartialResultState)
        
        case empty(analysis: PartialResultState)
        case joinIdentity(analysis: PartialResultState)
        case table([TermResult], analysis: PartialResultState)
        case quad(QuadPattern, analysis: PartialResultState)
        case bgp(Node, [TriplePattern], analysis: PartialResultState)
        case path(Node, Node, PropertyPath, Node, analysis: PartialResultState)
        case innerHashJoin(Plan, Plan, Set<String>, analysis: PartialResultState)
        case leftOuterHashJoin(Plan, Plan, Expression, Set<String>, analysis: PartialResultState)
        case filter(Plan, Expression, analysis: PartialResultState)
        case union(Plan, Plan, analysis: PartialResultState)
        case namedGraph(Plan, String, analysis: PartialResultState)
        case extend(Plan, Expression, String, analysis: PartialResultState)
        case minus(Plan, Plan, analysis: PartialResultState)
        case project(Plan, Set<String>, analysis: PartialResultState)
        case setDistinct(Plan, analysis: PartialResultState)
        case service(URL, Algebra, Bool, analysis: PartialResultState)
        case slice(Plan, Int?, Int?, analysis: PartialResultState)
        case order(Plan, [Algebra.SortComparator], analysis: PartialResultState)
        case aggregate(Plan, [Expression], Set<Algebra.AggregationMapping>, analysis: PartialResultState)
        case window(Plan, [Expression], [Algebra.WindowFunctionMapping], analysis: PartialResultState)
        case subquery(Plan, analysis: PartialResultState)
    }

    enum CompilerInstruction {
        case forVariableIn(String, String)
        case ifCondition(String)
        case elseCondition
        case open(String)
        case close
        case setInsert(String, String)
        case assign(String, String)
        case listAppend(String, String)
        case increment(String)
        case constant(String, String)
        case variable(String, String)
        case generateResult(String)
        case literal(String)
        case commentBlock(String)
        case importModule(String)
    }
    
    func emit(instruction: CompilerInstruction) {
        let indent = String(repeating: " ", count: 4*depth)
        switch instruction {
        case .commentBlock(let c):
            print("\n\(indent)/**")
            print("\(c)")
            print("\(indent)**/\n")
        case .assign(let name, let value):
            print("\(indent)\(name) = \(value)")
        case .forVariableIn(let name, let s):
            print("\(indent)for \(name) in \(s) {")
            depth += 1
        case .listAppend(let list, let value):
            print("\(indent)\(list).append(\(value))")
        case .setInsert(let set, let value):
            print("\(indent)\(set).insert(\(value))")
        case .ifCondition(let s):
            print("\(indent)if \(s) {")
            depth += 1
        case .elseCondition:
            let indent = String(repeating: " ", count: 4*(depth-1))
            print("\(indent)} else {")
        case .open(let s):
            print("\(indent)\(s) {")
            depth += 1
        case .close:
            depth -= 1
            let indent = String(repeating: " ", count: 4*depth)
            print("\(indent)}")
        case .constant(let s, let val):
            print("\(indent)let \(s) = \(val)")
        case .variable(let s, let val):
            print("\(indent)var \(s) = \(val)")
        case .increment(let s):
            print("\(indent)\(s) += 1")
        case .generateResult(let s):
            print("\(indent)GENERATE_RESULT(\(s))")
        case .importModule(let m):
            print("import \(m)")
        case .literal(let s):
            print("\(indent)\(s)")
        }
    }
    
    func uniqueVariable(_ name: String, parents: Ancestors) -> String {
        if let id = uniqueVariables[parents] {
            return "\(name)_\(id)"
        } else {
            nextVariable += 1
            let id = nextVariable
            uniqueVariables[parents] = id
            return "\(name)_\(id)"
        }
    }

    func queryPlan(for query: Query, activeGraph: Node) throws -> (Plan, PartialResultState) {
        let algebra = query.algebra
        var (plan, state) = try queryPlan(for: algebra, activeGraph: activeGraph)
        plan = wrap(plan: plan, for: query, state: state)
        return (plan, state)
    }
    
    func queryPlan(for algebra: Algebra, activeGraph: Node) throws -> (Plan, PartialResultState) {
        switch algebra {
        case .unionIdentity:
            let state = PartialResultState(distinct: true, necessarilyBound: [], potentiallyBound: [])
            return (.empty(analysis: state), state)
        case .joinIdentity:
            let state = PartialResultState(distinct: true, necessarilyBound: [], potentiallyBound: [])
            return (.joinIdentity(analysis: state), state)
        case let .table(cols, rows):
            let state = PartialResultState(distinct: false, necessarilyBound: [], potentiallyBound: algebra.inscope)
            let results = try Array(evaluateTable(columns: cols, rows: rows))
            return (.table(results, analysis: state), state)
        case let .quad(qp):
            let state = PartialResultState(distinct: false, necessarilyBound:algebra.necessarilyBound, potentiallyBound: algebra.inscope)
            return (.quad(qp, analysis: state), state)
        case let .triple(tp):
            let state = PartialResultState(distinct: false, necessarilyBound:algebra.necessarilyBound, potentiallyBound: algebra.inscope)
            let qp = QuadPattern(triplePattern: tp, graph: activeGraph)
            return (.quad(qp, analysis: state), state)
        case let .bgp(tps):
            let state = PartialResultState(distinct: false, necessarilyBound: algebra.necessarilyBound, potentiallyBound: algebra.inscope)
            return (.bgp(activeGraph, tps, analysis: state), state)
        case let .path(subject, pp, object):
            let state = PartialResultState(distinct: false, necessarilyBound: algebra.necessarilyBound, potentiallyBound: algebra.inscope)
            return (.path(activeGraph, subject, pp, object, analysis: state), state)
        case let .service(endpoint, child, silent):
            let state = PartialResultState(distinct: false, necessarilyBound: algebra.necessarilyBound, potentiallyBound: algebra.inscope)
            return (.service(endpoint, child, silent, analysis: state), state)
        case let .innerJoin(lhs, rhs):
            let pb = lhs.inscope.union(rhs.inscope)
            let nb = lhs.necessarilyBound.intersection(rhs.necessarilyBound)
            let state = PartialResultState(distinct: false, necessarilyBound: nb, potentiallyBound: pb)
            let (l, _) = try queryPlan(for: lhs, activeGraph: activeGraph)
            let (r, _) = try queryPlan(for: rhs, activeGraph: activeGraph)
            return (.innerHashJoin(l, r, nb, analysis: state), state)
        case let .leftOuterJoin(lhs, rhs, expr):
            let pb = lhs.inscope.union(rhs.inscope)
            let nb = lhs.necessarilyBound.intersection(rhs.necessarilyBound)
            let state = PartialResultState(distinct: false, necessarilyBound: nb, potentiallyBound: pb)
            let (l, _) = try queryPlan(for: lhs, activeGraph: activeGraph)
            let (r, _) = try queryPlan(for: rhs, activeGraph: activeGraph)
            return (.leftOuterHashJoin(l, r, expr, nb, analysis: state), state)
        case let .filter(child, expr):
            let (c, state) = try queryPlan(for: child, activeGraph: activeGraph)

            var exists = [String: Algebra]()
            let counter = AnyIterator(sequence(first: 1) { $0 + 1 })
            let e = expr.removeExists(counter, mapping: &exists)

            if exists.count > 0 {
                var plan = c
                for (name, algebra) in exists {
                    let (existsPlan, _) = try queryPlan(for: algebra, activeGraph: activeGraph)
                    plan = .exists(plan, existsPlan, name, analysis: state.addingNecessarilyBound(name))
                }
                return (.filter(plan, e, analysis: state), state)
            } else {
                return (.filter(c, expr, analysis: state), state)
            }
        case let .union(lhs, rhs):
            let pb = lhs.inscope.union(rhs.inscope)
            let nb = lhs.necessarilyBound.intersection(rhs.necessarilyBound)
            let state = PartialResultState(distinct: false, necessarilyBound: nb, potentiallyBound: pb)
            let (l, _) = try queryPlan(for: lhs, activeGraph: activeGraph)
            let (r, _) = try queryPlan(for: rhs, activeGraph: activeGraph)
            return (.union(l, r, analysis: state), state)
        case let .namedGraph(child, .bound(term)):
            return try queryPlan(for: child, activeGraph: .bound(term))
        case let .namedGraph(child, .variable(graphVariable, binding: binding)):
            let pb = child.inscope.union([graphVariable])
            let nb = child.necessarilyBound.union([graphVariable])
            let (c, s) = try queryPlan(for: child, activeGraph: .variable(graphVariable, binding: binding))
            let state = PartialResultState(distinct: s.distinct, necessarilyBound: nb, potentiallyBound: pb)
            return (.namedGraph(c, graphVariable, analysis: state), state)
        case let .extend(child, expr, name):
            let (c, state) = try queryPlan(for: child, activeGraph: activeGraph)
            return (.extend(c, expr, name, analysis: state), state)
        case let .minus(lhs, rhs):
            let (l, state) = try queryPlan(for: lhs, activeGraph: activeGraph)
            let (r, _) = try queryPlan(for: rhs, activeGraph: activeGraph)
            return (.minus(l, r, analysis: state), state)
        case let .project(child, vars):
            let (c, s) = try queryPlan(for: child, activeGraph: activeGraph)
            let state = s.projecting(vars)
            return (.project(c, vars, analysis: state), state)
        case let .distinct(child):
            let (c, s) = try queryPlan(for: child, activeGraph: activeGraph)
            var state = s
            state.distinct = true
            return (.setDistinct(c, analysis: state), state)
        case let .slice(child, offset, limit):
            let (c, state) = try queryPlan(for: child, activeGraph: activeGraph)
            return (.slice(c, offset, limit, analysis: state), state)
        case let .order(child, cmps):
            let (c, state) = try queryPlan(for: child, activeGraph: activeGraph)
            return (.order(c, cmps, analysis: state), state)
        case let .aggregate(child, groups, aggs):
            let (c, _) = try queryPlan(for: child, activeGraph: activeGraph)
            let state = PartialResultState(distinct: false, necessarilyBound:algebra.necessarilyBound, potentiallyBound: algebra.inscope)
            return (.aggregate(c, groups, aggs, analysis: state), state)
        case let .window(child, groups, windows):
            fatalError("TODO: implement queryPlan(for: .window(\(child), \(groups), \(windows)))")
        case let .subquery(q):
            return try queryPlan(for: q, activeGraph: activeGraph)
        }
    }

    func wrap(plan: Plan, for query: Query, state: PartialResultState) -> Plan {
        switch query.form {
        case .select(_):
            return plan
        case .ask:
            return .ask(plan, analysis: state.projecting([]))
        case .construct(_):
            fatalError("TODO: implement CONSTRUCT")
        case .describe(_):
            fatalError("TODO: implement DESCRIBE")
        }
    }
    
    private func emitPreamble() {
        emit(instruction: .importModule("Kineo"))
        emit(instruction: .literal("func GENERATE_RESULT(_ result : TermResult) { print(result) }"))
        emit(instruction: .literal("func service(_ endpoint : String, _ sparql : String, _ silent : Bool) -> [TermResult] { fatalError(\"SERVICE not implemented\") }"))
    }
    
    public func compile(query: Query, activeGraph: Term) throws {
        emitPreamble()
        emit(instruction: .commentBlock(query.serialize()))
        let (plan, _) = try queryPlan(for: query, activeGraph: .bound(activeGraph))
        let parents = QueryCompiler.Ancestors(ancestors: [], compiler: self)
        return try produce(plan: plan, parents: parents)
    }
    
    func emitExpressionRewritting(variable eVar: String, expr: String, result replacementResultVariable: String?) {
        if let replacement = replacementResultVariable {
            emit(instruction: .variable(eVar, expr))
            emit(instruction: .assign(eVar, "\(eVar).replaceVariables(with: \(replacement))"))
        } else {
            emit(instruction: .constant(eVar, expr))
        }
    }
    
    func produce(plan: Plan, parents: Ancestors, replacementResultVariable: String? = nil) throws {
        let result = uniqueVariable("result", parents: parents)
        switch plan {
        case .empty:
            break
        case .joinIdentity:
            let state = PartialResultState(distinct: true, necessarilyBound: [], potentiallyBound: [])
            emit(instruction: .constant(result, "TermResult()"))
            try parents.consume(state: state, resultName: result, depth: depth)
        case let .table(results, state):
            for r in results {
                emit(instruction: .constant(result, "\(r)"))
                try parents.consume(state: state, resultName: result, depth: depth)
            }
        case let .quad(qp, state):
            var qpVar = uniqueVariable("qp", parents: parents)
            emit(instruction: .constant(qpVar, "quad_pattern(\(qp))"))
            if let replacement = replacementResultVariable {
                let qpVar2 = uniqueVariable("qp_replaced", parents: parents)
                emit(instruction: .constant(qpVar2, "\(qpVar).replaceVariables(with: \(replacement))"))
                qpVar = qpVar2
            }
            emit(instruction: .forVariableIn(result, "match_quad(\(qpVar))"))
            try parents.consume(state: state, resultName: result, depth: depth)
            emit(instruction: .close)
        case let .bgp(graph, tps, state):
            var bgpVar = uniqueVariable("bgp", parents: parents)
            emit(instruction: .constant(bgpVar, "bgp_pattern(\(tps))"))
            if let replacement = replacementResultVariable {
                let bgpVar2 = uniqueVariable("bgp_replaced", parents: parents)
                emit(instruction: .constant(bgpVar2, "\(bgpVar).replaceVariables(with: \(replacement))"))
                bgpVar = bgpVar2
            }
            switch graph {
            case .bound(let t):
                emit(instruction: .forVariableIn(result, "match(bgp: \(bgpVar), in: \(t))"))
            case .variable(let name, binding: _):
                emit(instruction: .forVariableIn(result, "match(bgp: \(bgpVar), in: \(name))"))
            }
            try parents.consume(state: state, resultName: result, depth: depth)
            emit(instruction: .close)
        case let .path(_, subject, pp, object, state):
            var pathVar = uniqueVariable("path", parents: parents)
            emit(instruction: .constant(pathVar, "path_pattern(\(subject), \(pp), \(object))"))
            if let replacement = replacementResultVariable {
                let pathVar2 = uniqueVariable("path_replaced", parents: parents)
                emit(instruction: .constant(pathVar2, "\(pathVar).replaceVariables(with: \(replacement))"))
                pathVar = pathVar2
            }
            emit(instruction: .forVariableIn(result, "match_path(\(pathVar))"))
            try parents.consume(state: state, resultName: result, depth: depth)
            emit(instruction: .close)
        case let .service(endpoint, child, silent, state):
            let s = SPARQLSerializer(prettyPrint: false)
            guard let q = try? Query(form: .select(.star), algebra: child) else {
                throw QueryError.evaluationError("Failed to serialize SERVICE algebra into SPARQL string")
            }
            let sparql = try s.serialize(q.sparqlTokens())

            emit(instruction: .forVariableIn(result, "service(\"\(endpoint)\", \"\(sparql)\", \(silent))"))
            try parents.consume(state: state, resultName: result, depth: depth)
            emit(instruction: .close)
            
        case let .exists(child, _, _, _):
            try produce(plan: child, parents: parents.adding(plan, inPosition: .lhs), replacementResultVariable: replacementResultVariable)
        case let .innerHashJoin(lhs, rhs, _, _):
            let ht = uniqueVariable("hashTable", parents: parents)
            emit(instruction: .variable(ht, "[TermResult:[TermResult]]()"))
            try produce(plan: lhs, parents: parents.adding(plan, inPosition: .lhs), replacementResultVariable: replacementResultVariable)
            try produce(plan: rhs, parents: parents.adding(plan, inPosition: .rhs), replacementResultVariable: replacementResultVariable)
        case let .leftOuterHashJoin(lhs, rhs, expr, _, _):
            let exprVar = uniqueVariable("expr", parents: parents)
            emitExpressionRewritting(variable: exprVar, expr: "Expression(\(expr))", result: replacementResultVariable)
            let ht = uniqueVariable("hashTable", parents: parents)
            emit(instruction: .variable(ht, "[TermResult:[TermResult]]()"))
            try produce(plan: rhs, parents: parents.adding(plan, inPosition: .rhs), replacementResultVariable: replacementResultVariable)
            try produce(plan: lhs, parents: parents.adding(plan, inPosition: .lhs), replacementResultVariable: replacementResultVariable)
        case let .filter(child, expr, _):
            let exprVar = uniqueVariable("expr", parents: parents)
            emitExpressionRewritting(variable: exprVar, expr: "Expression(\(expr))", result: replacementResultVariable)
            try produce(plan: child, parents: parents.adding(plan), replacementResultVariable: replacementResultVariable)
        case let .union(lhs, rhs, _):
            try produce(plan: lhs, parents: parents.adding(plan, inPosition: .lhs), replacementResultVariable: replacementResultVariable)
            try produce(plan: rhs, parents: parents.adding(plan, inPosition: .rhs), replacementResultVariable: replacementResultVariable)
        case let .namedGraph(child, graphVariable, _):
            if let replacement = replacementResultVariable {
                emit(instruction: .ifCondition("let \(graphVariable) = \(replacement)[\"\(graphVariable)\"]"))
                try produce(plan: child, parents: parents.adding(plan, inPosition: .lhs), replacementResultVariable: replacementResultVariable)
                emit(instruction: .elseCondition)
                emit(instruction: .forVariableIn(graphVariable, "graphs()"))
                try produce(plan: child, parents: parents.adding(plan, inPosition: .lhs), replacementResultVariable: replacementResultVariable)
                emit(instruction: .close)
                emit(instruction: .close)
            } else {
                emit(instruction: .forVariableIn(graphVariable, "graphs()"))
                try produce(plan: child, parents: parents.adding(plan, inPosition: .lhs), replacementResultVariable: replacementResultVariable)
                emit(instruction: .close)
            }
        case let .extend(child, expr, _, _):
            let exprVar = uniqueVariable("expr", parents: parents)
            emitExpressionRewritting(variable: exprVar, expr: "Expression(\(expr))", result: replacementResultVariable)
            try produce(plan: child, parents: parents.adding(plan), replacementResultVariable: replacementResultVariable)
        case let .minus(lhs, rhs, _):
            let ht = uniqueVariable("hashTable", parents: parents)
            emit(instruction: .variable(ht, "[TermResult:[TermResult]]()"))
            try produce(plan: rhs, parents: parents.adding(plan, inPosition: .rhs), replacementResultVariable: replacementResultVariable)
            try produce(plan: lhs, parents: parents.adding(plan, inPosition: .lhs), replacementResultVariable: replacementResultVariable)
        case let .project(child, _, _):
            try produce(plan: child, parents: parents.adding(plan), replacementResultVariable: replacementResultVariable)
        case let .setDistinct(child, _):
            let set = uniqueVariable("set", parents: parents)
            emit(instruction: .variable(set, "Set()"))
            try produce(plan: child, parents: parents.adding(plan), replacementResultVariable: replacementResultVariable)
        case let .slice(child, _, _, _):
            let rowCount = uniqueVariable("rowCount", parents: parents)
            emit(instruction: .variable(rowCount, "0"))
            try produce(plan: child, parents: parents.adding(plan), replacementResultVariable: replacementResultVariable)
        case let .order(child, cmps, state):
            let cmpsVar = uniqueVariable("comparators", parents: parents)
            emitExpressionRewritting(variable: cmpsVar, expr: "Comparators(\(cmps))", result: replacementResultVariable)
            let results = uniqueVariable("results", parents: parents)
            emit(instruction: .variable(results, "[TermResult]()"))
            try produce(plan: child, parents: parents.adding(plan), replacementResultVariable: replacementResultVariable)
            emit(instruction: .assign(results, "sort(\(results), with: \(cmpsVar))"))
            emit(instruction: .forVariableIn(result, results))
            try parents.consume(state: state, resultName: result, depth: depth)
            emit(instruction: .close)
        case let .aggregate(child, groups, aggs, state):
            let groupsVar = uniqueVariable("groups", parents: parents)
            var groupVars = [String]()
            for (i, g) in groups.enumerated() {
                switch g {
                case .node(.variable(let name, _)):
                    groupVars.append(name)
                default:
                    let exprVar = uniqueVariable("expr\(i)", parents: parents)
                    let name = uniqueVariable("result\(i)", parents: parents)
                    emitExpressionRewritting(variable: exprVar, expr: "Expression(\(g))", result: replacementResultVariable)
                    groupVars.append(name)
                }
            }
            
            emitExpressionRewritting(variable: groupsVar, expr: "\(groupVars)", result: replacementResultVariable)
            let gs = uniqueVariable("groupsData", parents: parents)
            emit(instruction: .variable(gs, "[TermResult:[TermResult]]()"))
            try produce(plan: child, parents: parents.adding(plan), replacementResultVariable: replacementResultVariable)
            let g = uniqueVariable("group", parents: parents)
            emit(instruction: .forVariableIn(g, gs))
            emit(instruction: .variable(result, "\(g).copy()"))
            for (i, a) in aggs.enumerated() {
                let aggVar = uniqueVariable("agg\(i)", parents: parents)
                emitExpressionRewritting(variable: aggVar, expr: "Aggregation(\(a.aggregation))", result: replacementResultVariable)
                emit(instruction: .assign("\(result)[\"\(a.variableName)\"]", "aggregate(\(gs)[\(g)], \(aggVar))"))
            }
            try parents.consume(state: state, resultName: result, depth: depth)
            emit(instruction: .close)
        case let .window(child, groups, windows, _):
            try produce(plan: child, parents: parents.adding(plan), replacementResultVariable: replacementResultVariable)
            fatalError("TODO: implement produce(.window(\(child), \(groups), \(windows)))")
        case let .subquery(child, _):
            try produce(plan: child, parents: parents.adding(plan), replacementResultVariable: replacementResultVariable)
            
        case let .ask(child, state):
            let rowCount = uniqueVariable("rowCount", parents: parents)
            emit(instruction: .variable(rowCount, "0"))
            try produce(plan: child, parents: parents.adding(plan), replacementResultVariable: replacementResultVariable)
            emit(instruction: .assign(result, "(\(rowCount) > 0) ? true : false"))
            try parents.consume(state: state, resultName: result, depth: depth)
        case let .describe(child, nodes, _):
            let describeNodes = uniqueVariable("describeNodes", parents: parents)
            emitExpressionRewritting(variable: describeNodes, expr: "\(nodes)", result: replacementResultVariable)

            try produce(plan: child, parents: parents.adding(plan), replacementResultVariable: replacementResultVariable)
            fatalError("TODO: implement DESCRIBE")
        case let .construct(child, _, _):
            try produce(plan: child, parents: parents.adding(plan), replacementResultVariable: replacementResultVariable)
            fatalError("TODO: implement CONSTRUCT")
        }
    }
    
    func consume(plan: Plan, parents: Ancestors, resultName: String, inPosition position: Ancestors.ChildIdentifier = .lhs) throws {
        switch plan {
        case .empty, .joinIdentity, .quad(_), .bgp(_), .service(_), .path(_):
            fatalError("Plan cannot consume results: \(plan)")
        case let .exists(_, exists, name, state):
            if case .rhs = position {
                // exists branch
                let rowCount = uniqueVariable("rowCount", parents: parents)
                emit(instruction: .increment(rowCount))
                emit(instruction: .literal("break"))
            } else {
                // main branch
                let rowCount = uniqueVariable("rowCount", parents: parents)
                emit(instruction: .variable(rowCount, "0"))
                try produce(plan: exists, parents: parents.adding(plan, inPosition: .rhs), replacementResultVariable: resultName)
                emit(instruction: .assign(name, "(\(rowCount) > 0) ? true : false"))
                try parents.consume(state: state, resultName: resultName, depth: depth)
            }
        case let .innerHashJoin(_, _, _, state):
            let nb = state.necessarilyBound
            let ht = uniqueVariable("hashTable", parents: parents)
            if case .lhs = position {
                emit(instruction: .listAppend("\(ht)[\(resultName).project(\(nb))]", resultName))
            } else {
                emit(instruction: .forVariableIn(resultName, "compatible(\(resultName), \(ht)[\(resultName).project(\(nb))])"))
                try parents.consume(state: state, resultName: resultName, depth: depth)
                emit(instruction: .close)
            }
        case let .leftOuterHashJoin(_, _, _, _, state):
            let expr = uniqueVariable("expr", parents: parents)
            let ht = uniqueVariable("hashTable", parents: parents)
            let nb = state.necessarilyBound
            if case .rhs = position {
                emit(instruction: .listAppend("\(ht)[\(resultName).project(\(nb))]", resultName))
            } else {
                emit(instruction: .variable("leftJoinCount", "0"))
                let m = uniqueVariable("matching", parents: parents)
                emit(instruction: .constant(m, "compatible(\(resultName), \(ht)[\(resultName).project(\(nb))])"))
                emit(instruction: .forVariableIn(resultName, m))
                emit(instruction: .ifCondition("eval(\(resultName), \(expr))"))
                emit(instruction: .increment("leftJoinCount"))
                try parents.consume(state: state, resultName: resultName, depth: depth)
                emit(instruction: .close)
                emit(instruction: .close)
                emit(instruction: .ifCondition("leftJoinCount == 0"))
                try parents.consume(state: state, resultName: resultName, depth: depth)
                emit(instruction: .close)
            }
        case let .filter(_, _, state):
            let exprVar = uniqueVariable("expr", parents: parents)
            emit(instruction: .ifCondition("eval(\(resultName), \(exprVar))"))
            try parents.consume(state: state, resultName: resultName, depth: depth)
            emit(instruction: .close)
        case let .union(_, _, state):
            try parents.consume(state: state, resultName: resultName, depth: depth)
        case let .namedGraph(_, _, state):
            try parents.consume(state: state, resultName: resultName, depth: depth)
        case let .extend(_, _, name, state):
            let exprVar = uniqueVariable("expr", parents: parents)
            emit(instruction: .constant(resultName, "\(resultName).extend(?\(name), \(exprVar))"))
            try parents.consume(state: state.addingPotentiallyBound(name), resultName: resultName, depth: depth)
        case let .minus(_, _, state):
            let ht = uniqueVariable("hashTable", parents: parents)
            let nb = state.necessarilyBound
            if case .rhs = position {
                emit(instruction: .listAppend("\(ht)[\(resultName).project(\(nb))]", resultName))
            } else {
                let m = uniqueVariable("matching", parents: parents)
                emit(instruction: .constant(m, "compatible(\(resultName), \(ht)[\(resultName).project(\(nb))])"))
                emit(instruction: .ifCondition("matching.count == 0"))
                try parents.consume(state: state, resultName: resultName, depth: depth)
                emit(instruction: .close)
                let candidate = uniqueVariable("candidate", parents: parents)
                emit(instruction: .forVariableIn(candidate, "matching"))
                emit(instruction: .ifCondition("dom(\(resultName)).disjointWith(dom(\(candidate)))"))
                try parents.consume(state: state, resultName: resultName, depth: depth)
                emit(instruction: .close)
                emit(instruction: .close)
            }
        case let .project(_, vars, state):
            let result = uniqueVariable("result", parents: parents)
            emit(instruction: .constant(result, "\(resultName).project(\(vars))"))
            try parents.consume(state: state.projecting(vars), resultName: result, depth: depth)
        case let .setDistinct(_, state):
            let set = uniqueVariable("set", parents: parents)
            emit(instruction: .ifCondition("!set.contains(\(resultName))"))
            emit(instruction: .setInsert(set, "insert(\(resultName))"))
            try parents.consume(state: state, resultName: resultName, depth: depth)
            emit(instruction: .close)
        case let .slice(_, .some(offset), .some(limit), state):
            let rowCount = uniqueVariable("rowCount", parents: parents)
            emit(instruction: .increment(rowCount))
            emit(instruction: .literal("if \(rowCount) <= \(offset) { continue }"))
            emit(instruction: .literal("if \(rowCount) > \(limit+offset) { break }")) // TODO: does this break out far enough?
            try parents.consume(state: state, resultName: resultName, depth: depth)
        case let .slice(_, 0, .some(limit), state), let .slice(_, nil, .some(limit), state):
            let rowCount = uniqueVariable("rowCount", parents: parents)
            emit(instruction: .increment(rowCount))
            emit(instruction: .literal("if \(rowCount) > \(limit) { break }")) // TODO: does this break out far enough?
            try parents.consume(state: state, resultName: resultName, depth: depth)
        case let .slice(_, .some(offset), 0, state), let .slice(_, .some(offset), nil, state):
            let rowCount = uniqueVariable("rowCount", parents: parents)
            emit(instruction: .increment(rowCount))
            emit(instruction: .literal("if \(rowCount) <= \(offset) { continue }"))
            try parents.consume(state: state, resultName: resultName, depth: depth)
        case .order(_, _, _):
            let results = uniqueVariable("results", parents: parents)
            emit(instruction: .listAppend(results, resultName))
        case let .aggregate(_, groups, _, _):
            for (i, g) in groups.enumerated() {
                switch g {
                case .node(.variable(_, _)):
                    break
                default:
                    let exprVar = uniqueVariable("expr\(i)", parents: parents)
                    let name = uniqueVariable("result\(i)", parents: parents)
                    emit(instruction: .constant(resultName, "\(resultName).extend(?\(name), \(exprVar))"))
                }
            }
            let groupsVar = uniqueVariable("groups", parents: parents)
            let g = uniqueVariable("group", parents: parents)
            let gs = uniqueVariable("groupsData", parents: parents)
            emit(instruction: .constant(g, "\(resultName).project(\(groupsVar))")) // TODO: grouping should be projecting on variables, not expressions
            emit(instruction: .listAppend("\(gs)[\(g)]", resultName))
        case let .window(child, groups, windows, _):
            fatalError("TODO: implement consume(.window(\(child), \(groups), \(windows)))")
        case let .subquery(_, state):
            try parents.consume(state: state, resultName: resultName, depth: depth)

        case .ask(_, _):
            let rowCount = uniqueVariable("rowCount", parents: parents)
            emit(instruction: .increment(rowCount))
            emit(instruction: .literal("break"))
        case .describe(_, _, _):
            fatalError("TODO: implement DESCRIBE")
        case .construct(_, _, _):
            fatalError("TODO: implement CONSTRUCT")
        }
    }
}
