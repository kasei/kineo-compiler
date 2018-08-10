// swift-tools-version:4.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "kineo-compiler",
    dependencies: [
		.package(url: "https://github.com/kasei/swift-sparql-syntax.git", from: "0.0.41"),
		.package(url: "https://github.com/kasei/swift-serd.git", from: "0.0.0"),
		.package(url: "https://github.com/kasei/kineo.git", from: "0.0.16"),
		.package(url: "https://github.com/kasei/kineo-federation.git", from: "0.0.2"),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages which this package depends on.
        .target(
            name: "kineo-compiler",
            dependencies: ["Kineo", "SPARQLSyntax", "KineoFederation"]),
    ]
)
