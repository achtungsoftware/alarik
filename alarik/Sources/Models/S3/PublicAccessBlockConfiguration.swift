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

import Foundation

/// AWS's bucket-level "Block Public Access" safety toggle (`?publicAccessBlock`). `blockPublicAcls`
/// and `ignorePublicAcls` are accepted/stored for client compatibility (many tools, e.g.
/// Terraform, set all 4 fields unconditionally) but have no effect, since this system has no ACL
/// concept at all - only `blockPublicPolicy` and `restrictPublicBuckets` are actually enforced
/// (see `BucketPolicy` and `S3Service.authenticateOrAuthorizePublic`).
struct PublicAccessBlockConfiguration: Equatable {
    var blockPublicAcls: Bool
    var ignorePublicAcls: Bool
    var blockPublicPolicy: Bool
    var restrictPublicBuckets: Bool

    /// Parses a `PutPublicAccessBlock` request body. Matches the simple substring-based style
    /// already used for VersioningConfiguration - a missing element defaults to false, matching
    /// real S3 behavior.
    static func parse(xml: String) -> PublicAccessBlockConfiguration {
        func flag(_ tag: String) -> Bool {
            xml.contains("<\(tag)>true</\(tag)>")
        }
        return PublicAccessBlockConfiguration(
            blockPublicAcls: flag("BlockPublicAcls"),
            ignorePublicAcls: flag("IgnorePublicAcls"),
            blockPublicPolicy: flag("BlockPublicPolicy"),
            restrictPublicBuckets: flag("RestrictPublicBuckets")
        )
    }

    /// Builds the `GetPublicAccessBlock` response XML.
    func toXML() -> String {
        """
        <?xml version="1.0" encoding="UTF-8"?><PublicAccessBlockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><BlockPublicAcls>\(blockPublicAcls)</BlockPublicAcls><IgnorePublicAcls>\(ignorePublicAcls)</IgnorePublicAcls><BlockPublicPolicy>\(blockPublicPolicy)</BlockPublicPolicy><RestrictPublicBuckets>\(restrictPublicBuckets)</RestrictPublicBuckets></PublicAccessBlockConfiguration>
        """
    }
}
