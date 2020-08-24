//
//  Expression.swift
//  kineo-compiler
//
//  Created by Gregory Todd Williams on 7/26/18.
//

import Foundation
import SPARQLSyntax

extension Expression {
    func removeExists(_ counter: AnyIterator<Int>, mapping: inout [String:Algebra]) -> Expression {
        switch self {
        case .node(_), .aggregate(_):
            return self
        case .neg(let expr):
            return .neg(expr.removeExists(counter, mapping: &mapping))
        case .not(let expr):
            return .not(expr.removeExists(counter, mapping: &mapping))
        case .isiri(let expr):
            return .isiri(expr.removeExists(counter, mapping: &mapping))
        case .isblank(let expr):
            return .isblank(expr.removeExists(counter, mapping: &mapping))
        case .isliteral(let expr):
            return .isliteral(expr.removeExists(counter, mapping: &mapping))
        case .isnumeric(let expr):
            return .isnumeric(expr.removeExists(counter, mapping: &mapping))
        case .lang(let expr):
            return .lang(expr.removeExists(counter, mapping: &mapping))
        case .langmatches(let expr, let pattern):
            return .langmatches(expr.removeExists(counter, mapping: &mapping), pattern.removeExists(counter, mapping: &mapping))
        case .sameterm(let expr, let pattern):
            return .sameterm(expr.removeExists(counter, mapping: &mapping), pattern.removeExists(counter, mapping: &mapping))
        case .datatype(let expr):
            return .datatype(expr.removeExists(counter, mapping: &mapping))
        case .bound(let expr):
            return .bound(expr.removeExists(counter, mapping: &mapping))
        case .boolCast(let expr):
            return .boolCast(expr.removeExists(counter, mapping: &mapping))
        case .intCast(let expr):
            return .intCast(expr.removeExists(counter, mapping: &mapping))
        case .floatCast(let expr):
            return .floatCast(expr.removeExists(counter, mapping: &mapping))
        case .doubleCast(let expr):
            return .doubleCast(expr.removeExists(counter, mapping: &mapping))
        case .decimalCast(let expr):
            return .decimalCast(expr.removeExists(counter, mapping: &mapping))
        case .dateTimeCast(let expr):
            return .dateTimeCast(expr.removeExists(counter, mapping: &mapping))
        case .dateCast(let expr):
            return .dateCast(expr.removeExists(counter, mapping: &mapping))
        case .stringCast(let expr):
            return .stringCast(expr.removeExists(counter, mapping: &mapping))
        case .call(let f, let exprs):
            return .call(f, exprs.map { $0.removeExists(counter, mapping: &mapping) })
        case .eq(let lhs, let rhs):
            return .eq(lhs.removeExists(counter, mapping: &mapping), rhs.removeExists(counter, mapping: &mapping))
        case .ne(let lhs, let rhs):
            return .ne(lhs.removeExists(counter, mapping: &mapping), rhs.removeExists(counter, mapping: &mapping))
        case .lt(let lhs, let rhs):
            return .lt(lhs.removeExists(counter, mapping: &mapping), rhs.removeExists(counter, mapping: &mapping))
        case .le(let lhs, let rhs):
            return .le(lhs.removeExists(counter, mapping: &mapping), rhs.removeExists(counter, mapping: &mapping))
        case .gt(let lhs, let rhs):
            return .gt(lhs.removeExists(counter, mapping: &mapping), rhs.removeExists(counter, mapping: &mapping))
        case .ge(let lhs, let rhs):
            return .ge(lhs.removeExists(counter, mapping: &mapping), rhs.removeExists(counter, mapping: &mapping))
        case .add(let lhs, let rhs):
            return .add(lhs.removeExists(counter, mapping: &mapping), rhs.removeExists(counter, mapping: &mapping))
        case .sub(let lhs, let rhs):
            return .sub(lhs.removeExists(counter, mapping: &mapping), rhs.removeExists(counter, mapping: &mapping))
        case .div(let lhs, let rhs):
            return .div(lhs.removeExists(counter, mapping: &mapping), rhs.removeExists(counter, mapping: &mapping))
        case .mul(let lhs, let rhs):
            return .mul(lhs.removeExists(counter, mapping: &mapping), rhs.removeExists(counter, mapping: &mapping))
        case .and(let lhs, let rhs):
            return .and(lhs.removeExists(counter, mapping: &mapping), rhs.removeExists(counter, mapping: &mapping))
        case .or(let lhs, let rhs):
            return .or(lhs.removeExists(counter, mapping: &mapping), rhs.removeExists(counter, mapping: &mapping))
        case .between(let a, let b, let c):
            return .between(a.removeExists(counter, mapping: &mapping), b.removeExists(counter, mapping: &mapping), c.removeExists(counter, mapping: &mapping))
        case .valuein(let expr, let exprs):
            return .valuein(expr.removeExists(counter, mapping: &mapping), exprs.map { $0.removeExists(counter, mapping: &mapping) })
        case .exists(let child):
            guard let c = counter.next() else { fatalError("No fresh variable available") }
            let name = ".exists-\(c)"
            mapping[name] = child
            let node: Node = .variable(name, binding: true)
            return .node(node)
        case .window(_):
            fatalError("removeExists(:mapping:) unimplemented for window functions")
        }
    }

}
