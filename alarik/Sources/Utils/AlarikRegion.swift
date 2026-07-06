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

import Vapor

/// Alarik's single-region identity - the region SigV4 requests must be signed for, and the
/// value `GetBucketLocation` reports. A self-hosted deployment has exactly one storage backend,
/// not a fleet of independent regional datacenters like AWS, so one configurable value for the
/// whole process is enough. Contrast a replication *target*'s region
/// (`ReplicationTarget.region`), which describes wherever that remote endpoint happens to be,
/// not this deployment.
enum AlarikRegion {
    /// Matches the default every S3 client already assumes when talking to a "generic"
    /// S3-compatible endpoint (AWS CLI, Soto, rclone, boto3 all default here), so an
    /// unconfigured deployment keeps working with zero client-side changes - this was Alarik's
    /// only behavior before `ALARIK_REGION` existed.
    static let `default` = "us-east-1"

    /// Reads the configured region fresh each call (a single `Environment.get`, effectively
    /// free) - it never changes at runtime, so there's nothing to gain from caching it, and
    /// resolving on demand keeps this a plain value lookup usable from anywhere (SigV4
    /// validation, `GetBucketLocation`, event payloads) without threading `Application` through
    /// call sites that don't otherwise need it.
    static func resolve() -> String {
        Environment.sanitizedGet("ALARIK_REGION") ?? `default`
    }
}
