{{- define "tb.name" -}}{{ .Release.Name }}{{- end -}}

{{- define "tb.labels" -}}
app.kubernetes.io/part-of: tributary
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/* image ref: tb.image (dict "root" . "pkg" "receivers") */}}
{{- define "tb.image" -}}
{{- $d := index .root.Values.image.digests .pkg -}}
{{- if $d -}}{{ .root.Values.image.registry }}/tributary-{{ .pkg }}@{{ $d }}
{{- else -}}{{ .root.Values.image.registry }}/tributary-{{ .pkg }}:{{ .root.Values.image.tag }}{{- end -}}
{{- end -}}

{{- define "tb.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: {{ .Values.securityContext.runAsUser }}
runAsGroup: {{ .Values.securityContext.runAsGroup }}
fsGroup: {{ .Values.securityContext.fsGroup }}
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "tb.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
runAsNonRoot: true
capabilities:
  drop: [ALL]
{{- end -}}

{{/* Downward-API environment shared by every data-plane container. */}}
{{- define "tb.downwardEnv" -}}
- name: POD_NAME
  valueFrom: {fieldRef: {fieldPath: metadata.name}}
- name: POD_IP
  valueFrom: {fieldRef: {fieldPath: status.podIP}}
- name: TB_INSTANCE_ID
  valueFrom: {fieldRef: {fieldPath: metadata.name}}
{{- end -}}

{{/* tb.kafkaVolumes (dict "root" . "role" "receiver") : mounts */}}
{{- define "tb.kafkaMounts" -}}
- {name: engine-kafka, mountPath: /etc/tributary, readOnly: true}
- {name: kafka-role, mountPath: /var/run/secrets/tributary/{{ index .root.Values.kafka.roleSecretRefs .role }}, readOnly: true}
- {name: kafka-ca, mountPath: /var/run/secrets/tributary/{{ .root.Values.kafka.caSecretRef }}, readOnly: true}
- {name: contracts, mountPath: /contracts, readOnly: true}
- {name: tmp, mountPath: /tmp}
{{- end -}}

{{- define "tb.kafkaVolumes" -}}
- name: engine-kafka
  configMap: {name: {{ include "tb.name" .root }}-engine-kafka-{{ .role }}}
- name: kafka-role
  secret: {secretName: {{ index .root.Values.kafka.roleSecretRefs .role }}}
- name: kafka-ca
  secret: {secretName: {{ .root.Values.kafka.caSecretRef }}}
- name: contracts
  configMap: {name: {{ .root.Values.contractsConfigMap }}}
- name: tmp
  emptyDir: {sizeLimit: 64Mi}
{{- end -}}

{{- define "tb.commonEnv" -}}
- {name: TB_ENGINE_KAFKA_FILE, value: /etc/tributary/engine-kafka.json}
- {name: TB_CONTRACTS_DIR, value: /contracts}
- {name: TB_PROFILE, value: production}
{{- end -}}

{{- define "tb.archiverEnv" -}}
- {name: TB_ENGINE_KAFKA_FILE, value: /etc/tributary/engine-kafka.json}
- {name: TB_CONTRACTS_DIR, value: /contracts}
- {name: TB_PROFILE, value: production}
- {name: TB_ARCHIVE_CLICKHOUSE_HOST, value: {{ .Values.archiver.clickhouse.host | quote }}}
- {name: TB_ARCHIVE_CLICKHOUSE_PORT, value: {{ .Values.archiver.clickhouse.port | quote }}}
- {name: TB_ARCHIVE_CLICKHOUSE_DATABASE, value: {{ .Values.archiver.clickhouse.database | quote }}}
- {name: TB_ARCHIVE_CLICKHOUSE_SECURE, value: {{ ternary "1" "0" .Values.archiver.clickhouse.secure | quote }}}
- {name: TB_ARCHIVE_TTL_DAYS, value: {{ .Values.archiver.ttlDays | quote }}}
{{- end }}
