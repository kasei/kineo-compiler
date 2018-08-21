//
//  IR.swift
//  kineo-compiler
//
//  Created by Gregory Todd Williams on 8/20/18.
//

import Foundation

public indirect enum CompilerExpression {
    case constantBool(Bool)
    case dictionaryMember(CompilerExpression, CompilerExpression)
    case listMember(CompilerExpression, CompilerExpression)
    case not(CompilerExpression)
    case emptyList(String)
    case emptySet(String)
    case emptyDictionary(String)
    case emptyStruct(String)
    case setContains(CompilerExpression, CompilerExpression)
    case identifier(String)
    case constantInteger(Int)
    case lt(CompilerExpression, CompilerExpression)
    case le(CompilerExpression, CompilerExpression)
    case gt(CompilerExpression, CompilerExpression)
    case eq(CompilerExpression, CompilerExpression)
    case ternary(CompilerExpression, CompilerExpression, CompilerExpression)

    /**
     
     case add
     case sub
     case mul
     case div
     case land
     case lor
     
     **/
    case literal_expr_TODO(String)

    var value : String {
        switch self {
        case .identifier(let s):
            return s
        default:
            return "???"
        }
    }
}

public enum CompilerStatement {
    case importModule(CompilerExpression)
    case forVariableIn(CompilerExpression, CompilerExpression)
    case ifCondition(CompilerExpression)
    case elseCondition
//    case open(String)
    case close
    case _break
    case _continue
    case setInsert(CompilerExpression, CompilerExpression)
    case assign(CompilerExpression, CompilerExpression)
    case listAppend(CompilerExpression, CompilerExpression)
    case increment(CompilerExpression)
    case constant(CompilerExpression, CompilerExpression)
    case variable(CompilerExpression, CompilerExpression)
    case generateResult(CompilerExpression)
    case commentBlock(String)
    case literal_TODO(String)
}

public protocol InstructionWriter {
    mutating func emitPreamble()
    mutating func emit(instruction: CompilerStatement)
}

public struct SwiftInstructionWriter : InstructionWriter {
    var depth: Int
    
    public init() {
        depth = 0
    }
    
    mutating public func emitPreamble() {
        emit(instruction: .importModule(.identifier("Kineo")))
        print("func GENERATE_RESULT(_ result : TermResult) { print(result) }")
        print("func service(_ endpoint : String, _ sparql : String, _ silent : Bool) -> [TermResult] { fatalError(\"SERVICE not implemented\") }")
    }
    
    mutating public func emit(instruction: CompilerStatement) {
        let indent = String(repeating: " ", count: 4*depth)
        switch instruction {
        case .commentBlock(let c):
            print("\n\(indent)/**")
            print("\(c)")
            print("\(indent)**/\n")
        case ._break:
            print("\(indent)break")
        case ._continue:
            print("\(indent)continue")
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
//        case .open(let s):
//            print("\(indent)\(s) {")
//            depth += 1
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
        case .literal_TODO(let s):
            print("# TODO: *** remove use of .literal_TODO case for code generation")
            print("\(indent)\(s)")
        }
    }
    
}

