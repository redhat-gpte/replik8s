{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "replik8s.name" -}}
{{-   default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "replik8s.chart" -}}
{{-   printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "replik8s.labels" -}}
helm.sh/chart: {{ include "replik8s.chart" . }}
{{ include "replik8s.selectorLabels" . }}
{{-   if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{-   end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "replik8s.selectorLabels" -}}
app.kubernetes.io/name: {{ include "replik8s.name" . }}
{{-   if (ne (lower .Release.Name) "release-name") }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{-   end -}}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "replik8s.serviceAccountName" -}}
{{-   if .Values.serviceAccount.create -}}
{{      default (include "replik8s.name" .) .Values.serviceAccount.name }}
{{-   else -}}
{{      default "default" .Values.serviceAccount.name }}
{{-   end -}}
{{- end -}}

{{/*
Define the image to deploy
*/}}
{{- define "replik8s.image" -}}
  {{- if .Values.image.override -}}
    {{- .Values.image.override -}}
  {{- else if .Values.image.tag -}}
    {{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
  {{- else -}}
    {{- printf "%s:v%s" .Values.image.repository .Chart.AppVersion -}}
  {{- end -}}
{{- end -}}
