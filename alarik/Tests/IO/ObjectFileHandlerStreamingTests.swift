/*
Copyright 2025-present Julian Gerhards

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import Crypto
import Foundation
import Testing

@testable import Alarik

@Suite("ObjectFileHandler streaming write Tests", .serialized)
struct ObjectFileHandlerStreamingTests {

    private func createTempDir() -> String {
        let dir = "\(NSTemporaryDirectory())streaming-test-\(UUID().uuidString)/"
        try? FileManager.default.createDirectory(atPath: dir, withIntermediateDirectories: true)
        return dir
    }

    private func cleanup(dir: String) {
        try? FileManager.default.removeItem(atPath: dir)
    }

    private func makeMeta(size: Int, etag: String) -> ObjectMeta {
        ObjectMeta(
            bucketName: "stream-bucket",
            key: "stream-key.bin",
            size: size,
            contentType: "application/octet-stream",
            etag: etag,
            updatedAt: Date()
        )
    }

    /// Deterministic pseudo-random payload, large enough to span several copy windows
    private func makePayload(size: Int, seed: UInt8 = 7) -> Data {
        var data = Data(capacity: size)
        var state: UInt8 = seed
        for _ in 0..<size {
            state = state &* 31 &+ 17
            data.append(state)
        }
        return data
    }

    @Test("writeStreamed single source round-trips byte-identically")
    func writeStreamedSingleSource() throws {
        let dir = createTempDir()
        defer { cleanup(dir: dir) }

        // 3 MiB + odd tail so the copy loop crosses window boundaries unevenly
        let payload = makePayload(size: 3 * 1024 * 1024 + 12345)
        let spoolPath = dir + "spool.raw"
        try payload.write(to: URL(fileURLWithPath: spoolPath))

        let objPath = dir + "object.obj"
        let meta = makeMeta(size: payload.count, etag: "streamed-etag")
        try ObjectFileHandler.writeStreamed(
            metadata: meta, payloadFile: spoolPath, payloadOffset: 0,
            payloadSize: payload.count, to: objPath)

        let (readMeta, readData) = try ObjectFileHandler.read(from: objPath)
        #expect(readMeta.etag == "streamed-etag")
        #expect(readData == payload)

        let location = try ObjectFileHandler.payloadLocation(path: objPath)
        #expect(location.payloadSize == payload.count)
        #expect(location.meta.etag == "streamed-etag")
    }

    @Test("writeStreamed with offset copies exactly the selected region")
    func writeStreamedWithOffset() throws {
        let dir = createTempDir()
        defer { cleanup(dir: dir) }

        // Source is a full .obj file - the region to copy is its payload, not its header
        let payload = makePayload(size: 256 * 1024)
        let sourceObj = dir + "source.obj"
        try ObjectFileHandler.write(
            metadata: makeMeta(size: payload.count, etag: "src"), data: payload, to: sourceObj)
        let location = try ObjectFileHandler.payloadLocation(path: sourceObj)
        #expect(location.payloadOffset > 4)

        let destObj = dir + "dest.obj"
        try ObjectFileHandler.writeStreamed(
            metadata: makeMeta(size: payload.count, etag: "dst"),
            payloadFile: sourceObj, payloadOffset: location.payloadOffset,
            payloadSize: location.payloadSize, to: destObj)

        let (_, copied) = try ObjectFileHandler.read(from: destObj)
        #expect(copied == payload)
    }

    @Test("writeStreamed multi-source concatenates parts in order")
    func writeStreamedMultiSource() throws {
        let dir = createTempDir()
        defer { cleanup(dir: dir) }

        let part1 = makePayload(size: 1_500_000, seed: 1)
        let part2 = makePayload(size: 900_000, seed: 2)
        let part3 = makePayload(size: 42, seed: 3)
        var sources: [(path: String, offset: Int, size: Int)] = []
        for (index, part) in [part1, part2, part3].enumerated() {
            let path = dir + "part-\(index + 1)"
            try part.write(to: URL(fileURLWithPath: path))
            sources.append((path: path, offset: 0, size: part.count))
        }

        let expected = part1 + part2 + part3
        let objPath = dir + "assembled.obj"
        try ObjectFileHandler.writeStreamed(
            metadata: makeMeta(size: expected.count, etag: "assembled"),
            payloadSources: sources, to: objPath)

        let (readMeta, readData) = try ObjectFileHandler.read(from: objPath)
        #expect(readMeta.size == expected.count)
        #expect(readData == expected)
    }

    @Test("writeStreamed aborts cleanly when a source is shorter than declared")
    func writeStreamedShortSourceAborts() throws {
        let dir = createTempDir()
        defer { cleanup(dir: dir) }

        let payload = makePayload(size: 1000)
        let spoolPath = dir + "short.raw"
        try payload.write(to: URL(fileURLWithPath: spoolPath))

        let objPath = dir + "never.obj"
        #expect(throws: (any Error).self) {
            try ObjectFileHandler.writeStreamed(
                metadata: makeMeta(size: 2000, etag: "x"),
                payloadFile: spoolPath, payloadOffset: 0, payloadSize: 2000, to: objPath)
        }
        // No final file, and no leftover temp file in the directory
        #expect(!FileManager.default.fileExists(atPath: objPath))
        let leftovers = try FileManager.default.contentsOfDirectory(atPath: dir)
            .filter { $0.hasPrefix(".tmp-") }
        #expect(leftovers.isEmpty)
    }

    @Test("md5HexOfFileRegion matches Data-based MD5 for a payload region")
    func md5OfFileRegion() throws {
        let dir = createTempDir()
        defer { cleanup(dir: dir) }

        let payload = makePayload(size: 2 * 1024 * 1024 + 7)
        let objPath = dir + "hashme.obj"
        try ObjectFileHandler.write(
            metadata: makeMeta(size: payload.count, etag: "h"), data: payload, to: objPath)
        let location = try ObjectFileHandler.payloadLocation(path: objPath)

        let streamed = try ObjectFileHandler.md5HexOfFileRegion(
            path: objPath, offset: location.payloadOffset, size: location.payloadSize)
        let expected = Insecure.MD5.hash(data: payload).hex
        #expect(streamed == expected)
    }

    @Test("fsync-off mode (ALARIK_FSYNC=false) still writes atomically and correctly")
    func fsyncOffModeWrites() throws {
        let original = ProcessInfo.processInfo.environment["ALARIK_FSYNC"]
        setenv("ALARIK_FSYNC", "false", 1)
        defer {
            if let original {
                setenv("ALARIK_FSYNC", original, 1)
            } else {
                unsetenv("ALARIK_FSYNC")
            }
        }
        #expect(Durability.fsyncEnabled == false)

        let dir = createTempDir()
        defer { cleanup(dir: dir) }

        let payload = makePayload(size: 128 * 1024)
        let objPath = dir + "nofsync.obj"
        try ObjectFileHandler.write(
            metadata: makeMeta(size: payload.count, etag: "nf"), data: payload, to: objPath)
        let (_, readData) = try ObjectFileHandler.read(from: objPath)
        #expect(readData == payload)
    }

    @Test("AtomicObjectWriter creates missing parent directories on first write (cold-start path)")
    func writerCreatesMissingNestedDirectory() throws {
        let dir = createTempDir()
        defer { cleanup(dir: dir) }

        // Several levels deep, none of which exist yet - AtomicObjectWriter must create the
        // whole chain via the ENOENT fallback, not just the immediate parent.
        let objPath = dir + "a/b/c/object.obj"
        #expect(!FileManager.default.fileExists(atPath: dir + "a"))

        let payload = makePayload(size: 4096)
        try ObjectFileHandler.write(
            metadata: makeMeta(size: payload.count, etag: "nested"), data: payload, to: objPath)

        let (_, readData) = try ObjectFileHandler.read(from: objPath)
        #expect(readData == payload)
    }

    @Test("AtomicObjectWriter reuses an already-existing parent directory (warm path)")
    func writerReusesExistingDirectory() throws {
        let dir = createTempDir()
        defer { cleanup(dir: dir) }

        // First write establishes the directory; subsequent writes into the same directory
        // must succeed without needing to (re)create it - this is the common-case path the
        // optimistic-open optimization targets.
        for i in 0..<3 {
            let payload = makePayload(size: 512, seed: UInt8(i + 1))
            let objPath = dir + "object-\(i).obj"
            try ObjectFileHandler.write(
                metadata: makeMeta(size: payload.count, etag: "warm-\(i)"), data: payload,
                to: objPath)
            let (_, readData) = try ObjectFileHandler.read(from: objPath)
            #expect(readData == payload)
        }
    }

    @Test("fsync is enabled by default")
    func fsyncDefaultOn() throws {
        let original = ProcessInfo.processInfo.environment["ALARIK_FSYNC"]
        unsetenv("ALARIK_FSYNC")
        defer {
            if let original { setenv("ALARIK_FSYNC", original, 1) }
        }
        #expect(Durability.fsyncEnabled == true)
    }

    @Test("only the literal value \"false\" disables fsync - everything else (including typos) stays safe")
    func fsyncParsingFailsSafe() throws {
        let original = ProcessInfo.processInfo.environment["ALARIK_FSYNC"]
        defer {
            if let original {
                setenv("ALARIK_FSYNC", original, 1)
            } else {
                unsetenv("ALARIK_FSYNC")
            }
        }

        for (value, expectedEnabled) in
            [
                ("false", false),
                ("FALSE", false),
                ("true", true),
                ("on", true),
                ("off", true),  // only "false" disables - "off" is not recognized
                ("0", true),
                ("garbage", true),
            ] as [(String, Bool)]
        {
            setenv("ALARIK_FSYNC", value, 1)
            #expect(Durability.fsyncEnabled == expectedEnabled, "ALARIK_FSYNC=\(value)")
        }
    }
}
