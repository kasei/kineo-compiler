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
            var algebra: Algebra
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
        
        func consume(state: PartialResultState, depth: Int) {
            if let parent = ancestors.last {
                compiler.consume(algebra: parent.algebra, state: state, parents: self.droppingLast(), inPosition: parent.child)
            } else {
                let indent = String(repeating: " ", count: 4*depth)
                compiler.emit(instruction: .generateResult("result"))
            }
        }
        
        func adding(_ algebra: Algebra, inPosition child: ChildIdentifier = .lhs) -> Ancestors {
            let a = ancestors + [AncestorItem(algebra: algebra, child: child)]
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
    
    public func compile(algebra: Algebra, activeGraph: Node) throws {
        let parents = QueryCompiler.Ancestors(ancestors: [], compiler: self)
        return try produce(algebra: algebra, parents: parents, activeGraph: activeGraph)
    }
    
    func produce(algebra: Algebra, parents: Ancestors, activeGraph: Node) throws {
        switch algebra {
        case .unionIdentity:
            break
        case .joinIdentity:
            let state = PartialResultState(distinct: true, necessarilyBound: [], potentiallyBound: [])
            emit(instruction: .constant("result", "TermResult()"))
            parents.consume(state: state, depth: depth)
        case let .table(cols, rows):
            let state = PartialResultState(distinct: false, necessarilyBound: [], potentiallyBound: algebra.inscope)
            let results = try evaluateTable(columns: cols, rows: rows)
            for r in results {
                emit(instruction: .constant("result", "\(r)"))
                parents.consume(state: state, depth: depth)
            }
        case let .quad(qp):
            let state = PartialResultState(distinct: false, necessarilyBound:algebra.necessarilyBound, potentiallyBound: algebra.inscope)
            emit(instruction: .forVariableIn("result", "match_quad(\(qp))"))
            parents.consume(state: state, depth: depth)
            emit(instruction: .close)
        case let .triple(tp):
            let state = PartialResultState(distinct: false, necessarilyBound: algebra.necessarilyBound, potentiallyBound: algebra.inscope)
            let qp = QuadPattern(triplePattern: tp, graph: activeGraph)
            emit(instruction: .forVariableIn("result", "match_quad(\(qp))"))
            parents.consume(state: state, depth: depth)
            emit(instruction: .close)
        case let .bgp(tps):
            let state = PartialResultState(distinct: false, necessarilyBound: algebra.necessarilyBound, potentiallyBound: algebra.inscope)
            let qps = tps.map { QuadPattern(triplePattern: $0, graph: activeGraph) }
            emit(instruction: .forVariableIn("result", "match_bgp(\(qps))"))
            parents.consume(state: state, depth: depth)
            emit(instruction: .close)
        case let .path(subject, pp, object):
            let state = PartialResultState(distinct: false, necessarilyBound: algebra.necessarilyBound, potentiallyBound: algebra.inscope)
            emit(instruction: .forVariableIn("result", "match_path(\(subject), \(pp), \(object))"))
            parents.consume(state: state, depth: depth)
            emit(instruction: .close)
        case let .service(endpoint, child, silent):
            let state = PartialResultState(distinct: false, necessarilyBound: algebra.necessarilyBound, potentiallyBound: algebra.inscope)
            emit(instruction: .forVariableIn("result", "service(\(endpoint), \(child), \(silent))"))
            parents.consume(state: state, depth: depth)
            emit(instruction: .close)
            
        case let .innerJoin(lhs, rhs):
            let ht = uniqueVariable("hashTable", parents: parents)
            emit(instruction: .variable(ht, "[:]"))
            try produce(algebra: lhs, parents: parents.adding(algebra, inPosition: .lhs), activeGraph: activeGraph)
            try produce(algebra: rhs, parents: parents.adding(algebra, inPosition: .rhs), activeGraph: activeGraph)
        case let .leftOuterJoin(lhs, rhs, _):
            let ht = uniqueVariable("hashTable", parents: parents)
            emit(instruction: .variable(ht, "[:]"))
            try produce(algebra: rhs, parents: parents.adding(algebra, inPosition: .rhs), activeGraph: activeGraph)
            try produce(algebra: lhs, parents: parents.adding(algebra, inPosition: .lhs), activeGraph: activeGraph)
        case let .filter(child, _):
            try produce(algebra: child, parents: parents.adding(algebra), activeGraph: activeGraph)
        case let .union(lhs, rhs):
            try produce(algebra: lhs, parents: parents.adding(algebra, inPosition: .lhs), activeGraph: activeGraph)
            try produce(algebra: rhs, parents: parents.adding(algebra, inPosition: .rhs), activeGraph: activeGraph)
        case let .namedGraph(child, .bound(term)):
            try produce(algebra: child, parents: parents.adding(algebra, inPosition: .lhs), activeGraph: .bound(term))
        case let .namedGraph(child, .variable(graphVariable)):
            fatalError("TODO: implement produce(.namedGraph(\(child), \(graphVariable)))")
        case let .extend(child, _, _):
            try produce(algebra: child, parents: parents.adding(algebra), activeGraph: activeGraph)
        case let .minus(lhs, rhs):
            let ht = uniqueVariable("hashTable", parents: parents)
            emit(instruction: .variable(ht, "[:]"))
            try produce(algebra: rhs, parents: parents.adding(algebra, inPosition: .rhs), activeGraph: activeGraph)
            try produce(algebra: lhs, parents: parents.adding(algebra, inPosition: .lhs), activeGraph: activeGraph)
        case let .project(child, _):
            try produce(algebra: child, parents: parents.adding(algebra), activeGraph: activeGraph)
        case let .distinct(child):
            let set = uniqueVariable("set", parents: parents)
            emit(instruction: .variable(set, "Set()"))
            try produce(algebra: child, parents: parents.adding(algebra), activeGraph: activeGraph)
        case let .slice(child, _, _):
            let rowCount = uniqueVariable("rowCount", parents: parents)
            emit(instruction: .variable(rowCount, "0"))
            try produce(algebra: child, parents: parents.adding(algebra), activeGraph: activeGraph)
        case let .order(child, cmps):
            let results = uniqueVariable("results", parents: parents)
            emit(instruction: .variable(results, "[]"))
            try produce(algebra: child, parents: parents.adding(algebra), activeGraph: activeGraph)
            emit(instruction: .assign(results, "sort(\(results), with: \(cmps))"))
            let state = PartialResultState(distinct: false, necessarilyBound:algebra.necessarilyBound, potentiallyBound: algebra.inscope)
            emit(instruction: .forVariableIn("result", results))
            parents.consume(state: state, depth: depth)
            emit(instruction: .close)
        case let .aggregate(child, _, aggs):
            let results = uniqueVariable("results", parents: parents)
            emit(instruction: .variable(results, "[]"))
            let gs = uniqueVariable("groups", parents: parents)
            emit(instruction: .variable(gs, "[:]"))
            try produce(algebra: child, parents: parents.adding(algebra), activeGraph: activeGraph)
            let g = uniqueVariable("group", parents: parents)
            emit(instruction: .forVariableIn(g, gs))
            emit(instruction: .variable("result", "\(g).copy()"))
            for a in aggs {
                emit(instruction: .assign("result[\"\(a.variableName)\"]", "aggregate(groups[\(g)], \(a.aggregation))"))
            }
            let state = PartialResultState(distinct: false, necessarilyBound:algebra.necessarilyBound, potentiallyBound: algebra.inscope)
            parents.consume(state: state, depth: depth)
            emit(instruction: .close)
        case let .window(child, groups, windows):
            try produce(algebra: child, parents: parents.adding(algebra), activeGraph: activeGraph)
            fatalError("TODO: implement produce(.window(\(child), \(groups), \(windows)))")
        case let .subquery(q):
            fatalError("TODO: implement produce(.subquery(\(q)))")
        }
    }
    
    func consume(algebra: Algebra, state: PartialResultState, parents: Ancestors, inPosition position: Ancestors.ChildIdentifier = .lhs) {
        switch algebra {
        case .unionIdentity, .joinIdentity, .triple(_), .quad(_), .bgp(_), .service(_), .path(_):
            fatalError("Algebra cannot consume results: \(algebra)")
        case let .innerJoin(lhs, rhs):
            let ht = uniqueVariable("hashTable", parents: parents)
            let pb = lhs.necessarilyBound.union(rhs.necessarilyBound)
            let nb = lhs.necessarilyBound.intersection(rhs.necessarilyBound)
            let state = PartialResultState(distinct: false, necessarilyBound: nb, potentiallyBound: pb)
            if case .lhs = position {
                emit(instruction: .listAppend("\(ht)[result.project(\(nb))]", "result"))
            } else {
                emit(instruction: .forVariableIn("result", "compatible(result, \(ht)[result.project(\(nb))])"))
                parents.consume(state: state, depth: depth)
                emit(instruction: .close)
            }
        case let .leftOuterJoin(lhs, rhs, expr):
            let ht = uniqueVariable("hashTable", parents: parents)
            let pb = lhs.necessarilyBound.union(rhs.necessarilyBound)
            let nb = lhs.necessarilyBound.intersection(rhs.necessarilyBound)
            let state = PartialResultState(distinct: false, necessarilyBound: nb, potentiallyBound: pb)
            if case .rhs = position {
                emit(instruction: .listAppend("\(ht)[result.project(\(nb))]", "result"))
            } else {
                emit(instruction: .variable("leftJoinCount", "0"))
                let m = uniqueVariable("matching", parents: parents)
                emit(instruction: .constant(m, "compatible(result, \(ht)[result.project(\(nb))])"))
                emit(instruction: .forVariableIn("result", m))
                emit(instruction: .ifCondition("eval(result, \(expr))"))
                emit(instruction: .increment("leftJoinCount"))
                parents.consume(state: state, depth: depth)
                emit(instruction: .close)
                emit(instruction: .close)
                emit(instruction: .ifCondition("leftJoinCount == 0"))
                parents.consume(state: state, depth: depth)
                emit(instruction: .close)
            }
        case let .filter(_, expr):
            emit(instruction: .ifCondition("eval(result, \(expr))"))
            parents.consume(state: state, depth: depth)
            emit(instruction: .close)
        case .union(_, _):
            parents.consume(state: state, depth: depth)
        case .namedGraph(_, .bound(_)):
            parents.consume(state: state, depth: depth)
        case let .namedGraph(child, .variable(graphVariable)):
            fatalError("TODO: implement consume(.namedGraph)")
        case let .extend(_, expr, name):
            emit(instruction: .constant("result", "result.extend(?\(name), \(expr))"))
            parents.consume(state: state.addingPotentiallyBound(name), depth: depth)
        case let .minus(lhs, rhs):
            let ht = uniqueVariable("hashTable", parents: parents)
            let pb = lhs.necessarilyBound.union(rhs.necessarilyBound)
            let nb = lhs.necessarilyBound.intersection(rhs.necessarilyBound)
            let state = PartialResultState(distinct: false, necessarilyBound: nb, potentiallyBound: pb)
            if case .rhs = position {
                emit(instruction: .listAppend("\(ht)[result.project(\(nb))]", "result"))
            } else {
                let m = uniqueVariable("matching", parents: parents)
                emit(instruction: .constant(m, "compatible(result, \(ht)[result.project(\(nb))])"))
                emit(instruction: .ifCondition("matching.count == 0"))
                parents.consume(state: state, depth: depth)
                emit(instruction: .close)
                let candidate = uniqueVariable("candidate", parents: parents)
                emit(instruction: .forVariableIn(candidate, "matching"))
                emit(instruction: .ifCondition("dom(result).disjointWith(dom(\(candidate)))"))
                parents.consume(state: state, depth: depth)
                emit(instruction: .close)
                emit(instruction: .close)
            }
        case let .project(_, vars):
            emit(instruction: .assign("result", "result.project(\(vars))"))
            parents.consume(state: state.projecting(vars), depth: depth)
        case .distinct(_):
            let set = uniqueVariable("set", parents: parents)
            var s = state
            s.distinct = true
            emit(instruction: .ifCondition("!set.contains(result)"))
            emit(instruction: .setInsert(set, "insert(result)"))
            parents.consume(state: state, depth: depth)
            emit(instruction: .close)
        case let .slice(_, .some(offset), .some(limit)):
            let rowCount = uniqueVariable("rowCount", parents: parents)
            emit(instruction: .increment(rowCount))
            emit(instruction: .literal("if \(rowCount) <= \(offset) { continue }"))
            emit(instruction: .literal("if \(rowCount) > \(limit+offset) { break }")) // TODO: does this break out far enough?
            parents.consume(state: state, depth: depth)
        case let .slice(_, 0, .some(limit)), let .slice(_, nil, .some(limit)):
            let rowCount = uniqueVariable("rowCount", parents: parents)
            emit(instruction: .increment(rowCount))
            emit(instruction: .literal("if \(rowCount) > \(limit) { break }")) // TODO: does this break out far enough?
            parents.consume(state: state, depth: depth)
        case let .slice(_, .some(offset), 0), let .slice(_, .some(offset), nil):
            let rowCount = uniqueVariable("rowCount", parents: parents)
            emit(instruction: .increment(rowCount))
            emit(instruction: .literal("if \(rowCount) <= \(offset) { continue }"))
            parents.consume(state: state, depth: depth)
        case .order(_, _):
            let results = uniqueVariable("results", parents: parents)
            emit(instruction: .listAppend(results, "result"))
        case let .aggregate(_, groups, _):
            let g = uniqueVariable("group", parents: parents)
            let gs = uniqueVariable("groups", parents: parents)
            emit(instruction: .constant(g, "result.project(\(groups))"))
            emit(instruction: .listAppend("\(gs)[\(g)]", "result"))
        case let .window(child, groups, windows):
            fatalError("TODO: implement consume(.window(\(child), \(groups), \(windows)))")
        case let .subquery(q):
            fatalError("TODO: implement consume(.subquery(\(q)))")
        }
    }
}
