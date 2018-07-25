import Foundation
import Kineo
import SPARQLSyntax

func data(fromFileOrString qfile: String) throws -> Data {
    let url = URL(fileURLWithPath: qfile)
    let data: Data
    if case .some(true) = try? url.checkResourceIsReachable() {
        data = try Data(contentsOf: url)
    } else {
        guard let s = qfile.data(using: .utf8) else {
            fatalError("Could not interpret SPARQL query string as UTF-8")
        }
        data = s
    }
    return data
}







var verbose = false
let argscount = CommandLine.arguments.count
var args = PeekableIterator(generator: CommandLine.arguments.makeIterator())
guard let pname = args.next() else { fatalError("Missing command name") }
guard argscount >= 1 else {
    print("Usage: \(pname) [-v] QUERY")
    print("")
    exit(1)
}

var graph = Term(iri: "http://example.org/")
while true {
    if let next = args.peek() {
        if next.hasPrefix("-") {
            _ = args.next()
            if next == "-v" {
                verbose = true
                continue
            } else if next == "-g" {
                guard let iri = args.next() else { fatalError("No IRI value given after -g") }
                graph = Term(iri: iri)
                continue
            }
        }
    }
    break
}

guard let qfile = args.next() else { fatalError("No query file given") }


let startTime = getCurrentTime()
let startSecond = getCurrentDateSeconds()

do {
    let sparql = try data(fromFileOrString: qfile)
    guard var p = SPARQLParser(data: sparql) else { fatalError("Failed to construct SPARQL parser") }
    let q = try p.parseQuery()
    let c = QueryCompiler()
    try c.compile(query: q, activeGraph: graph)
} catch let e {
    warn("*** Failed to evaluate query:")
    warn("*** - \(e)")
}

let endTime = getCurrentTime()
let elapsed = Double(endTime - startTime)
if verbose {
    warn("elapsed time: \(elapsed)s")
}
