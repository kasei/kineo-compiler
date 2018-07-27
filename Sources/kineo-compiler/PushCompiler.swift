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
        
        func consume(state: PartialResultState, depth: Int) throws {
            if let parent = ancestors.last {
                try compiler.consume(plan: parent.plan, parents: self.droppingLast(), inPosition: parent.child)
            } else {
                compiler.emit(instruction: .generateResult("result"))
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
        case describe(Plan, Node, analysis: PartialResultState)
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
    }
    
    func emit(instruction: CompilerInstruction) {
        let indent = String(repeating: " ", count: 4*depth)
        switch instruction {
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
        case .literal(let s):
            print("\(indent)\(s)")
        }
    }
    
    func uniqueVariable(_ name: String, parents: Ancestors) -> String {
        if let id = uniqueVariables[parents] {
            return "\(name)\(id)"
        } else {
            nextVariable += 1
            let id = nextVariable
            uniqueVariables[parents] = id
            return "\(name)\(id)"
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
            fatalError()
        case .describe(_):
            fatalError()
        }
    }
    
    public func compile(query: Query, activeGraph: Term) throws {
        let (plan, _) = try queryPlan(for: query, activeGraph: .bound(activeGraph))
        let parents = QueryCompiler.Ancestors(ancestors: [], compiler: self)
        return try produce(plan: plan, parents: parents)
    }
    
    func produce(plan: Plan, parents: Ancestors) throws {
        switch plan {
        case .empty:
            break
        case .joinIdentity:
            let state = PartialResultState(distinct: true, necessarilyBound: [], potentiallyBound: [])
            emit(instruction: .constant("result", "TermResult()"))
            try parents.consume(state: state, depth: depth)
        case let .table(results, state):
            for r in results {
                emit(instruction: .constant("result", "\(r)"))
                try parents.consume(state: state, depth: depth)
            }
        case let .quad(qp, state):
            let qpVar = uniqueVariable("qp", parents: parents)
            emit(instruction: .constant(qpVar, "quad_pattern(\(qp))"))
            emit(instruction: .forVariableIn("result", "match_quad(\(qpVar))"))
            try parents.consume(state: state, depth: depth)
            emit(instruction: .close)
        case let .bgp(graph, tps, state):
            let bgpVar = uniqueVariable("bgp", parents: parents)
            emit(instruction: .constant(bgpVar, "bgp_pattern(\(tps))"))
            switch graph {
            case .bound(let t):
                emit(instruction: .forVariableIn("result", "match(bgp: \(bgpVar), in: \(t))"))
            case .variable(let name, binding: _):
                emit(instruction: .forVariableIn("result", "match(bgp: \(bgpVar), in: \(name))"))
            }
            try parents.consume(state: state, depth: depth)
            emit(instruction: .close)
        case let .path(_, subject, pp, object, state):
            let pathVar = uniqueVariable("path", parents: parents)
            emit(instruction: .constant(pathVar, "path_pattern(\(subject), \(pp), \(object))"))
            emit(instruction: .forVariableIn("result", "match_path(\(pathVar))"))
            try parents.consume(state: state, depth: depth)
            emit(instruction: .close)
        case let .service(endpoint, child, silent, state):
            emit(instruction: .forVariableIn("result", "service(\(endpoint), \(child), \(silent))"))
            try parents.consume(state: state, depth: depth)
            emit(instruction: .close)
            
        case let .exists(child, _, _, _):
            try produce(plan: child, parents: parents.adding(plan, inPosition: .lhs))
        case let .innerHashJoin(lhs, rhs, _, _):
            let ht = uniqueVariable("hashTable", parents: parents)
            emit(instruction: .variable(ht, "[:]"))
            try produce(plan: lhs, parents: parents.adding(plan, inPosition: .lhs))
            try produce(plan: rhs, parents: parents.adding(plan, inPosition: .rhs))
        case let .leftOuterHashJoin(lhs, rhs, _, _, _):
            let ht = uniqueVariable("hashTable", parents: parents)
            emit(instruction: .variable(ht, "[:]"))
            try produce(plan: rhs, parents: parents.adding(plan, inPosition: .rhs))
            try produce(plan: lhs, parents: parents.adding(plan, inPosition: .lhs))
        case let .filter(child, _, _):
            try produce(plan: child, parents: parents.adding(plan))
        case let .union(lhs, rhs, _):
            try produce(plan: lhs, parents: parents.adding(plan, inPosition: .lhs))
            try produce(plan: rhs, parents: parents.adding(plan, inPosition: .rhs))
        case let .namedGraph(child, graphVariable, _):
            emit(instruction: .forVariableIn(graphVariable, "graphs()"))
            try produce(plan: child, parents: parents.adding(plan, inPosition: .lhs))
            emit(instruction: .close)
        case let .extend(child, _, _, _):
            try produce(plan: child, parents: parents.adding(plan))
        case let .minus(lhs, rhs, _):
            let ht = uniqueVariable("hashTable", parents: parents)
            emit(instruction: .variable(ht, "[:]"))
            try produce(plan: rhs, parents: parents.adding(plan, inPosition: .rhs))
            try produce(plan: lhs, parents: parents.adding(plan, inPosition: .lhs))
        case let .project(child, _, _):
            try produce(plan: child, parents: parents.adding(plan))
        case let .setDistinct(child, _):
            let set = uniqueVariable("set", parents: parents)
            emit(instruction: .variable(set, "Set()"))
            try produce(plan: child, parents: parents.adding(plan))
        case let .slice(child, _, _, _):
            let rowCount = uniqueVariable("rowCount", parents: parents)
            emit(instruction: .variable(rowCount, "0"))
            try produce(plan: child, parents: parents.adding(plan))
        case let .order(child, cmps, state):
            let results = uniqueVariable("results", parents: parents)
            emit(instruction: .variable(results, "[]"))
            try produce(plan: child, parents: parents.adding(plan))
            emit(instruction: .assign(results, "sort(\(results), with: \(cmps))"))
            emit(instruction: .forVariableIn("result", results))
            try parents.consume(state: state, depth: depth)
            emit(instruction: .close)
        case let .aggregate(child, _, aggs, state):
            let results = uniqueVariable("results", parents: parents)
            emit(instruction: .variable(results, "[]"))
            let gs = uniqueVariable("groups", parents: parents)
            emit(instruction: .variable(gs, "[:]"))
            try produce(plan: child, parents: parents.adding(plan))
            let g = uniqueVariable("group", parents: parents)
            emit(instruction: .forVariableIn(g, gs))
            emit(instruction: .variable("result", "\(g).copy()"))
            for a in aggs {
                emit(instruction: .assign("result[\"\(a.variableName)\"]", "aggregate(groups[\(g)], \(a.aggregation))"))
            }
            try parents.consume(state: state, depth: depth)
            emit(instruction: .close)
        case let .window(child, groups, windows, _):
            try produce(plan: child, parents: parents.adding(plan))
            fatalError("TODO: implement produce(.window(\(child), \(groups), \(windows)))")
        case let .subquery(child, _):
            try produce(plan: child, parents: parents.adding(plan))
            
        case let .ask(child, state):
            let rowCount = uniqueVariable("rowCount", parents: parents)
            emit(instruction: .variable(rowCount, "0"))
            try produce(plan: child, parents: parents.adding(plan))
            emit(instruction: .assign("result", "(\(rowCount) > 0) ? true : false"))
            try parents.consume(state: state, depth: depth)
        case let .describe(child, _, _), let .construct(child, _, _):
            try produce(plan: child, parents: parents.adding(plan))
            fatalError()
        }
    }
    
    func consume(plan: Plan, parents: Ancestors, inPosition position: Ancestors.ChildIdentifier = .lhs) throws {
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
                emit(instruction: .literal("// TODO: substitute variables from result into exists pattern:"))
                try produce(plan: exists, parents: parents.adding(plan, inPosition: .rhs))
                emit(instruction: .assign(name, "result.extend(?\(name), (\(rowCount) > 0) ? true : false)"))
                try parents.consume(state: state, depth: depth)
            }
        case let .innerHashJoin(_, _, _, state):
            let nb = state.necessarilyBound
            let ht = uniqueVariable("hashTable", parents: parents)
            if case .lhs = position {
                emit(instruction: .listAppend("\(ht)[result.project(\(nb))]", "result"))
            } else {
                emit(instruction: .forVariableIn("result", "compatible(result, \(ht)[result.project(\(nb))])"))
                try parents.consume(state: state, depth: depth)
                emit(instruction: .close)
            }
        case let .leftOuterHashJoin(_, _, expr, _, state):
            let ht = uniqueVariable("hashTable", parents: parents)
            let nb = state.necessarilyBound
            if case .rhs = position {
                emit(instruction: .listAppend("\(ht)[result.project(\(nb))]", "result"))
            } else {
                emit(instruction: .variable("leftJoinCount", "0"))
                let m = uniqueVariable("matching", parents: parents)
                emit(instruction: .constant(m, "compatible(result, \(ht)[result.project(\(nb))])"))
                emit(instruction: .forVariableIn("result", m))
                emit(instruction: .ifCondition("eval(result, \(expr))"))
                emit(instruction: .increment("leftJoinCount"))
                try parents.consume(state: state, depth: depth)
                emit(instruction: .close)
                emit(instruction: .close)
                emit(instruction: .ifCondition("leftJoinCount == 0"))
                try parents.consume(state: state, depth: depth)
                emit(instruction: .close)
            }
        case let .filter(_, expr, state):
            emit(instruction: .ifCondition("eval(result, \(expr))"))
            try parents.consume(state: state, depth: depth)
            emit(instruction: .close)
        case let .union(_, _, state):
            try parents.consume(state: state, depth: depth)
        case let .namedGraph(_, _, state):
            try parents.consume(state: state, depth: depth)
        case let .extend(_, expr, name, state):
            emit(instruction: .constant("result", "result.extend(?\(name), \(expr))"))
            try parents.consume(state: state.addingPotentiallyBound(name), depth: depth)
        case let .minus(_, _, state):
            let ht = uniqueVariable("hashTable", parents: parents)
            let nb = state.necessarilyBound
            if case .rhs = position {
                emit(instruction: .listAppend("\(ht)[result.project(\(nb))]", "result"))
            } else {
                let m = uniqueVariable("matching", parents: parents)
                emit(instruction: .constant(m, "compatible(result, \(ht)[result.project(\(nb))])"))
                emit(instruction: .ifCondition("matching.count == 0"))
                try parents.consume(state: state, depth: depth)
                emit(instruction: .close)
                let candidate = uniqueVariable("candidate", parents: parents)
                emit(instruction: .forVariableIn(candidate, "matching"))
                emit(instruction: .ifCondition("dom(result).disjointWith(dom(\(candidate)))"))
                try parents.consume(state: state, depth: depth)
                emit(instruction: .close)
                emit(instruction: .close)
            }
        case let .project(_, vars, state):
            emit(instruction: .assign("result", "result.project(\(vars))"))
            try parents.consume(state: state.projecting(vars), depth: depth)
        case let .setDistinct(_, state):
            let set = uniqueVariable("set", parents: parents)
            emit(instruction: .ifCondition("!set.contains(result)"))
            emit(instruction: .setInsert(set, "insert(result)"))
            try parents.consume(state: state, depth: depth)
            emit(instruction: .close)
        case let .slice(_, .some(offset), .some(limit), state):
            let rowCount = uniqueVariable("rowCount", parents: parents)
            emit(instruction: .increment(rowCount))
            emit(instruction: .literal("if \(rowCount) <= \(offset) { continue }"))
            emit(instruction: .literal("if \(rowCount) > \(limit+offset) { break }")) // TODO: does this break out far enough?
            try parents.consume(state: state, depth: depth)
        case let .slice(_, 0, .some(limit), state), let .slice(_, nil, .some(limit), state):
            let rowCount = uniqueVariable("rowCount", parents: parents)
            emit(instruction: .increment(rowCount))
            emit(instruction: .literal("if \(rowCount) > \(limit) { break }")) // TODO: does this break out far enough?
            try parents.consume(state: state, depth: depth)
        case let .slice(_, .some(offset), 0, state), let .slice(_, .some(offset), nil, state):
            let rowCount = uniqueVariable("rowCount", parents: parents)
            emit(instruction: .increment(rowCount))
            emit(instruction: .literal("if \(rowCount) <= \(offset) { continue }"))
            try parents.consume(state: state, depth: depth)
        case .order(_, _, _):
            let results = uniqueVariable("results", parents: parents)
            emit(instruction: .listAppend(results, "result"))
        case let .aggregate(_, groups, _, _):
            let g = uniqueVariable("group", parents: parents)
            let gs = uniqueVariable("groups", parents: parents)
            emit(instruction: .constant(g, "result.project(\(groups))"))
            emit(instruction: .listAppend("\(gs)[\(g)]", "result"))
        case let .window(child, groups, windows, _):
            fatalError("TODO: implement consume(.window(\(child), \(groups), \(windows)))")
        case let .subquery(_, state):
            try parents.consume(state: state, depth: depth)

        case .ask(_, _):
            let rowCount = uniqueVariable("rowCount", parents: parents)
            emit(instruction: .increment(rowCount))
            emit(instruction: .literal("break"))
        case .describe(_, _, _):
            fatalError()
        case .construct(_, _, _):
            fatalError()
        }
    }
}
