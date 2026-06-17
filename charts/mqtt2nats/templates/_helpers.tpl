{{- define "mqtt2nats.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "mqtt2nats.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "mqtt2nats.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "mqtt2nats.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "mqtt2nats.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "mqtt2nats.selectorLabels" -}}
app.kubernetes.io/name: {{ include "mqtt2nats.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "mqtt2nats.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "mqtt2nats.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "mqtt2nats.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{- define "mqtt2nats.httpPort" -}}
{{- last (splitList ":" (.Values.config.http_addr | default ":8080")) -}}
{{- end -}}

{{- /* Which single NATS auth method (if any) is configured. */ -}}
{{- define "mqtt2nats.natsMethod" -}}
{{- if .Values.secrets.nats.creds.secret.name -}}creds
{{- else if .Values.secrets.nats.token.secret.name -}}token
{{- else if .Values.secrets.nats.user.secret.name -}}user
{{- end -}}
{{- end -}}

{{- /* Whether any secret volume is needed. */ -}}
{{- define "mqtt2nats.hasSecrets" -}}
{{- if or .Values.secrets.mqtt.password.secret.name (include "mqtt2nats.natsMethod" . | trim) -}}true{{- end -}}
{{- end -}}

{{- /* Render config.json, injecting *_file paths for configured secrets. */ -}}
{{- define "mqtt2nats.configJson" -}}
{{- $cfg := deepCopy .Values.config -}}
{{- if .Values.secrets.mqtt.password.secret.name -}}
{{- $_ := set $cfg.mqtt "password_file" "/etc/mqtt2nats/secrets/mqtt.password" -}}
{{- end -}}
{{- $method := include "mqtt2nats.natsMethod" . | trim -}}
{{- if eq $method "creds" -}}
{{- $_ := set $cfg.nats "creds_file" "/etc/mqtt2nats/secrets/nats.creds" -}}
{{- else if eq $method "token" -}}
{{- $_ := set $cfg.nats "token_file" "/etc/mqtt2nats/secrets/nats.token" -}}
{{- else if eq $method "user" -}}
{{- $_ := set $cfg.nats "user_file" "/etc/mqtt2nats/secrets/nats.user" -}}
{{- $_ := set $cfg.nats "password_file" "/etc/mqtt2nats/secrets/nats.password" -}}
{{- end -}}
{{ toPrettyJson $cfg }}
{{- end -}}
