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

/// Faults of the plain-file storage layer, typed so callers match on cases instead of `NSError`
/// domain strings. Read failures of a file that exists are damage, never "not found".
enum StorageError: Error, CustomStringConvertible {
    case openFailed(path: String, errno: Int32)
    case statFailed(path: String, errno: Int32)
    /// The file ended before the bytes its header or caller promised.
    case truncatedFile(path: String)
    /// The `.obj` header (length prefix + metadata JSON) is not one this layer wrote.
    case corruptHeader(path: String, reason: String)

    var description: String {
        switch self {
        case .openFailed(let path, let code): "Could not open '\(path)' (errno \(code))"
        case .statFailed(let path, let code): "Could not stat '\(path)' (errno \(code))"
        case .truncatedFile(let path): "File '\(path)' ended early"
        case .corruptHeader(let path, let reason): "Corrupt object header in '\(path)': \(reason)"
        }
    }
}
