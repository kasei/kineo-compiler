// swift-tools-version:5.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "KineoCompiler",
    platforms: [.macOS(.v10_15)],
    dependencies: [
		.package(name: "SPARQLSyntax", url: "https://github.com/kasei/swift-sparql-syntax.git", .branch("update")),
		.package(name: "Cserd", url: "https://github.com/kasei/swift-serd.git", .upToNextMinor(from: "0.0.4")),
		.package(name: "Kineo", url: "https://github.com/kasei/kineo.git", from: "0.0.16"),
		.package(name: "KineoFederation", url: "https://github.com/kasei/kineo-federation.git", from: "0.0.4"),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages which this package depends on.
        .target(
            name: "kineo-compiler",
            dependencies: ["Kineo", "SPARQLSyntax", "KineoFederation"]),
    ]
)
