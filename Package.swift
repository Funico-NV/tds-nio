// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "swift-tds",
    platforms: [
       .macOS(.v10_15)
    ],
    products: [
        .library(name: "TDS", targets: ["TDS"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", .upToNextMinor(from: "2.92.0")),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", .upToNextMinor(from: "2.36.0")),
        .package(url: "https://github.com/apple/swift-metrics.git", .upToNextMinor(from: "2.7.0")),
        .package(url: "https://github.com/apple/swift-log.git", .upToNextMajor(from: "1.8.0")),
    ],
    targets: [
        .target(name: "TDS", dependencies: [
            .product(name: "Logging", package: "swift-log"),
            .product(name: "Metrics", package: "swift-metrics"),
            .product(name: "NIO", package: "swift-nio"),
            .product(name: "NIOSSL", package: "swift-nio-ssl"),
        ]),
        .testTarget(name: "TDSTests", dependencies: [
            .target(name: "TDS"),
            .product(name: "NIOTestUtils", package: "swift-nio"),
        ]),
    ]
)
