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

#if canImport(Darwin)
    import Darwin
#elseif canImport(Glibc)
    import Glibc
#elseif canImport(Musl)
    import Musl
#endif

/// Cross-platform POSIX file operations.
/// These wrappers avoid naming conflicts with Swift methods named `read`, `open`, etc.
enum POSIXFile {
    static func open(_ path: String, _ flags: Int32) -> Int32 {
        path.withCString { cPath in
            #if canImport(Darwin)
                Darwin.open(cPath, flags)
            #elseif canImport(Glibc)
                Glibc.open(cPath, flags)
            #elseif canImport(Musl)
                Musl.open(cPath, flags)
            #endif
        }
    }

    static func read(_ fd: Int32, _ buf: UnsafeMutableRawPointer, _ count: Int) -> Int {
        #if canImport(Darwin)
            Darwin.read(fd, buf, count)
        #elseif canImport(Glibc)
            Glibc.read(fd, buf, count)
        #elseif canImport(Musl)
            Musl.read(fd, buf, count)
        #endif
    }

    static func close(_ fd: Int32) -> Int32 {
        #if canImport(Darwin)
            Darwin.close(fd)
        #elseif canImport(Glibc)
            Glibc.close(fd)
        #elseif canImport(Musl)
            Musl.close(fd)
        #endif
    }

    static func lseek(_ fd: Int32, _ offset: off_t, _ whence: Int32) -> off_t {
        #if canImport(Darwin)
            Darwin.lseek(fd, offset, whence)
        #elseif canImport(Glibc)
            Glibc.lseek(fd, offset, whence)
        #elseif canImport(Musl)
            Musl.lseek(fd, offset, whence)
        #endif
    }

    static func fstat(_ fd: Int32, _ buf: UnsafeMutablePointer<stat>) -> Int32 {
        #if canImport(Darwin)
            Darwin.fstat(fd, buf)
        #elseif canImport(Glibc)
            Glibc.fstat(fd, buf)
        #elseif canImport(Musl)
            Musl.fstat(fd, buf)
        #endif
    }
}
