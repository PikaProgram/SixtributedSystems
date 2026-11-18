{{- define "sixtributed.labels" -}}
app.kubernetes.io/name: sixtributed
app.kubernetes.io/instance: {{ $.Release.Name }}
{{- end -}}
