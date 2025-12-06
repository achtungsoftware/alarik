// swift-tools-version:6.2
import PackageDescription

let package = Package(
    name: "Alarik",
    platforms: [
        .macOS(.v26)
    ],
    dependencies: [
        .package(url: "https://github.com/juliangerhards/vapor.git", branch: "main"),
        .package(url: "https://github.com/vapor/fluent.git", from: "4.9.0"),
        .package(url: "https://github.com/vapor/fluent-sqlite-driver.git", from: "4.6.0"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.65.0"),
        .package(url: "https://github.com/CoreOffice/XMLCoder.git", from: "0.17.1"),
        .package(url: "https://github.com/vapor/jwt.git", from: "5.0.0"),
        .package(url: "https://github.com/soto-project/soto.git", from: "7.10.0"),
        .package(url: "https://github.com/weichsel/ZIPFoundation.git", from: "0.9.0"),
    ],
    targets: [
        .executableTarget(
            name: "Alarik",
            dependencies: [
                .product(name: "Fluent", package: "fluent"),
                .product(name: "FluentSQLiteDriver", package: "fluent-sqlite-driver"),
                .product(name: "Vapor", package: "vapor"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "XMLCoder", package: "xmlcoder"),
                .product(name: "JWT", package: "jwt"),
                .product(name: "SotoS3", package: "soto"),
                .product(name: "SotoSES", package: "soto"),
                .product(name: "SotoIAM", package: "soto"),
                .product(name: "ZIPFoundation", package: "ZIPFoundation"),
            ],
            swiftSettings: swiftSettings
        ),
        .testTarget(
            name: "AlarikTests",
            dependencies: [
                .target(name: "Alarik"),
                .product(name: "VaporTesting", package: "vapor"),
            ],
            resources: [
                .process("Files")
            ],
            swiftSettings: swiftSettings
        ),
    ]
)

var swiftSettings: [SwiftSetting] {
    [
        .enableUpcomingFeature("ExistentialAny")
    ]
}
