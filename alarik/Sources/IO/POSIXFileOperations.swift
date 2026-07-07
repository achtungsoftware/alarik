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

    /// Positioned read: like `read` but at an explicit offset, without touching the file
    /// descriptor's seek pointer - safe for concurrent windowed reads from one shared fd.
    static func pread(_ fd: Int32, _ buf: UnsafeMutableRawPointer, _ count: Int, _ offset: off_t)
        -> Int
    {
        #if canImport(Darwin)
            Darwin.pread(fd, buf, count, offset)
        #elseif canImport(Glibc)
            Glibc.pread(fd, buf, count, offset)
        #elseif canImport(Musl)
            Musl.pread(fd, buf, count, offset)
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

    static func openWrite(_ path: String, _ flags: Int32, _ mode: mode_t) -> Int32 {
        path.withCString { cPath in
            #if canImport(Darwin)
                Darwin.open(cPath, flags, mode)
            #elseif canImport(Glibc)
                Glibc.open(cPath, flags, mode)
            #elseif canImport(Musl)
                Musl.open(cPath, flags, mode)
            #endif
        }
    }

    static func write(_ fd: Int32, _ buf: UnsafeRawPointer, _ count: Int) -> Int {
        #if canImport(Darwin)
            Darwin.write(fd, buf, count)
        #elseif canImport(Glibc)
            Glibc.write(fd, buf, count)
        #elseif canImport(Musl)
            Musl.write(fd, buf, count)
        #endif
    }

    /// Flushes a file's data to stable storage
    static func fsyncData(_ fd: Int32) -> Int32 {
        #if canImport(Darwin)
            // F_FULLFSYNC can fail on filesystems that don't support it (e.g. some network
            // mounts) - fall back to plain fsync rather than failing the write, same fallback
            // Go's runtime uses.
            if fcntl(fd, F_FULLFSYNC) == 0 {
                return 0
            }
            return Darwin.fsync(fd)
        #elseif canImport(Glibc)
            Glibc.fdatasync(fd)
        #elseif canImport(Musl)
            Musl.fdatasync(fd)
        #endif
    }

    static func rename(_ oldPath: String, _ newPath: String) -> Int32 {
        oldPath.withCString { cOld in
            newPath.withCString { cNew in
                #if canImport(Darwin)
                    Darwin.rename(cOld, cNew)
                #elseif canImport(Glibc)
                    Glibc.rename(cOld, cNew)
                #elseif canImport(Musl)
                    Musl.rename(cOld, cNew)
                #endif
            }
        }
    }

    static func unlink(_ path: String) -> Int32 {
        path.withCString { cPath in
            #if canImport(Darwin)
                Darwin.unlink(cPath)
            #elseif canImport(Glibc)
                Glibc.unlink(cPath)
            #elseif canImport(Musl)
                Musl.unlink(cPath)
            #endif
        }
    }
}
