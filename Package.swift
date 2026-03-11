// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "swift-tds",
    platforms: [
       .macOS(.v13)
    ],
    products: [
        .library(name: "TDS", targets: ["TDS"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: Version(2,0,0)),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: Version(2,0,0)),
        .package(url: "https://github.com/apple/swift-log.git", from: Version(1,0,0)),
    ],
    targets: [
        .target(
            name: "TDS",
            dependencies: [
                .product(name: "Logging", package: "swift-log"),
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
            ]
        ),
        .testTarget(
            name: "TDSTests",
            dependencies: [
                .target(name: "TDS"),
                .product(name: "NIOTestUtils", package: "swift-nio"),
            ]
        ),
    ]
)
