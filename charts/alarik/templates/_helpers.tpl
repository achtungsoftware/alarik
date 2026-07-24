{{/* Common naming helpers */}}

{{- define "alarik.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "alarik.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "alarik.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{ include "alarik.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "alarik.selectorLabels" -}}
app.kubernetes.io/name: {{ include "alarik.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: storage
{{- end -}}

{{- define "alarik.console.selectorLabels" -}}
app.kubernetes.io/name: {{ include "alarik.name" . }}-console
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: console
{{- end -}}

{{- define "alarik.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "alarik.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Headless Service backing the StatefulSet's stable per-pod DNS. */}}
{{- define "alarik.headlessServiceName" -}}
{{- printf "%s-headless" (include "alarik.fullname" .) -}}
{{- end -}}

{{- define "alarik.secretName" -}}
{{- if .Values.auth.existingSecret -}}
{{- .Values.auth.existingSecret -}}
{{- else -}}
{{- printf "%s-auth" (include "alarik.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Seed addresses for cluster bootstrap.

Two entries, both stable across scaling so changing replicaCount never rewrites the pod spec (which
would force a rolling restart just to add a node):

  - Pod 0's stable DNS name. It always exists and is always the first pod up under
    OrderedReady, so it is the reliable founding member.
  - The headless Service name, whose DNS resolves to every pod. This is what lets two halves of a
    cluster that never met find each other - the periodic membership exchange always keeps a seed
    in the peers it talks to, precisely for that case.
*/}}
{{- define "alarik.seedNodes" -}}
{{- $full := include "alarik.fullname" . -}}
{{- $svc := include "alarik.headlessServiceName" . -}}
{{- printf "http://%s-0.%s.%s.svc.cluster.local:8080,http://%s.%s.svc.cluster.local:8080" $full $svc .Release.Namespace $svc .Release.Namespace -}}
{{- end -}}

{{/*
Refuse to render a cluster that cannot accept a single write.

Erasure coding needs one node per shard, so a cluster smaller than k+m has nowhere to place a
stripe and rejects every upload. Failing here turns that into an install-time error with a clear
message, rather than a healthy-looking deployment that 503s the first PUT.
*/}}
{{- define "alarik.validateReplicas" -}}
{{- $required := add (int .Values.erasureCoding.dataShards) (int .Values.erasureCoding.parityShards) -}}
{{- if lt (int .Values.replicaCount) $required -}}
{{- fail (printf "replicaCount (%d) is below erasureCoding.dataShards + parityShards (%d+%d=%d). A cluster smaller than k+m cannot place a full stripe and would refuse every write. Either raise replicaCount to %d, or lower the shard counts." (int .Values.replicaCount) (int .Values.erasureCoding.dataShards) (int .Values.erasureCoding.parityShards) $required $required) -}}
{{- end -}}
{{- if lt (int .Values.erasureCoding.dataShards) 1 -}}
{{- fail "erasureCoding.dataShards must be at least 1." -}}
{{- end -}}
{{- if ne (int .Values.metadataErasureCoding.dataShards) 1 -}}
{{- fail "metadataErasureCoding.dataShards must be 1. Control-plane metadata is replicated, not striped: with k>1 no single node holds a whole record, so routine credential and bucket lookups become distributed gathers that fail whenever a peer is slow or restarting." -}}
{{- end -}}
{{- end -}}

{{/*
Default PodDisruptionBudget minAvailable.

Reads need `dataShards` nodes, so voluntary disruption must never take the cluster below that.
One spare above the read floor keeps a single additional involuntary failure from breaking reads
mid-drain.
*/}}
{{- define "alarik.pdbMinAvailable" -}}
{{- if .Values.podDisruptionBudget.minAvailable -}}
{{- .Values.podDisruptionBudget.minAvailable -}}
{{- else -}}
{{- $floor := add (int .Values.erasureCoding.dataShards) 1 -}}
{{- if gt $floor (int .Values.replicaCount) -}}
{{- .Values.replicaCount -}}
{{- else -}}
{{- $floor -}}
{{- end -}}
{{- end -}}
{{- end -}}
