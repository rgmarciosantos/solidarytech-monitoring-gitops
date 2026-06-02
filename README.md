# 📊 solidarytech-monitoring-gitops — SolidaryTech Observability Stack

Stack completa de observabilidade (**Metrics + Logs + Traces + Alerting + Self-Healing**) da plataforma **SolidaryTech**, entregue via **GitOps** com ArgoCD no padrão **App-of-Apps**.

## Visão geral

Um único `kubectl apply` no `monitoring-app-of-apps.yaml` cria a Application raiz `solidarytech-root`, que registra automaticamente tudo o que está em `apps/`:

- **`apps/00-monitoring.yaml`** (sync-wave −10) → sobe primeiro e instala o stack de observabilidade (varre `argocd-apps/`):
  - `kube-prometheus-stack` — Prometheus + Grafana + Alertmanager
  - `loki-stack` — Loki + Promtail (logs)
  - `otel-collector` — OpenTelemetry Collector (hub central: OTLP → Prometheus/Loki/New Relic)
  - `monitoring-manifests` — ServiceMonitors, dashboard, alertas, External Secrets e self-healing (varre `manifests/`)
- **`apps/10-*.yaml`** (sync-wave 10) → só depois sobem os microsserviços, cada um apontando para seu repo de deploy:
  - `donation-service` → `deploy-donation-service` (namespace `solidarytech-donation`)
  - `ngo-service` → `deploy-ngo-service` (namespace `solidarytech-ngo`)
  - `volunteer-service` → `deploy-volunteer-service` (namespace `solidarytech-volunteer`)

## Estrutura

```
solidarytech-monitoring-gitops/
├── monitoring-app-of-apps.yaml          # ÚNICO apply manual (cria solidarytech-root)
├── apps/                                # Apps gerenciados pelo solidarytech-root
│   ├── 00-monitoring.yaml               # wave -10: stack de observabilidade
│   ├── 10-donation-service.yaml         # wave 10: aponta p/ deploy-donation-service
│   ├── 10-ngo-service.yaml              # wave 10: aponta p/ deploy-ngo-service
│   └── 10-volunteer-service.yaml        # wave 10: aponta p/ deploy-volunteer-service
├── argocd-apps/                         # Applications do stack de observabilidade
│   ├── 01-kube-prometheus-stack.yaml
│   ├── 02-loki-stack.yaml
│   ├── 03-otel-collector.yaml
│   └── 04-monitoring-manifests.yaml
└── manifests/                           # Recursos aplicados no namespace monitoring
    ├── prometheus/service-monitors.yaml
    ├── alerting/prometheus-rules.yaml
    ├── grafana/dashboard-configmap.yaml
    ├── external-secrets/{secretstore,externalsecrets}.yaml
    └── self-healing/{rbac,webhook-receiver}.yaml
```

## Fluxo de métricas (importante)

Os 3 serviços são instrumentados com **OpenTelemetry** e enviam métricas/traces/logs via **OTLP** para o `otel-collector`, que faz `prometheusremotewrite` para o Prometheus (e exporta para Loki e New Relic). Logo, as métricas HTTP (`http_server_request_duration_seconds_*`, com label `service_namespace="solidarytech"`) de **todos** os serviços chegam ao Prometheus pelo collector — é isso que o dashboard e os alertas consomem.

Há **um** `ServiceMonitor` extra, só para o **donation-service** (Go), que expõe `/metrics` via promhttp. O `ngo` e o `volunteer` (Python) **não** têm `/metrics` — suas métricas vêm pelo OTLP — então não têm ServiceMonitor (evita falso-positivo de `TargetDown`).

## Pré-requisitos (uma vez)

1. **Bucket S3 do Loki:**
   ```bash
   aws s3 mb s3://solidarytech-loki-$(aws sts get-caller-identity --query Account --output text) --region us-east-1
   ```
   Se o ID da sua conta for diferente do placeholder, ajuste `s3: s3://us-east-1/solidarytech-loki-ACCOUNT_ID` em `argocd-apps/02-loki-stack.yaml` (e o nome do bucket nos demais lugares).

2. **Secret `solidarytech/monitoring`** no AWS Secrets Manager, com as chaves:
   - `DISCORD_WEBHOOK_URL`
   - `PAGERDUTY_SERVICE_KEY`
   - `GRAFANA_ADMIN_USER`
   - `GRAFANA_ADMIN_PASSWORD`
   - `NEW_RELIC_API_KEY`

3. **Secret `aws-credentials`** no namespace `monitoring` (criado pelo Terraform — é o mesmo bootstrap que o SecretStore e o Loki usam).

## Deploy

```bash
kubectl apply -f monitoring-app-of-apps.yaml
```

O ArgoCD cuida do resto. Mudanças futuras em `argocd-apps/` ou `manifests/` sincronizam automaticamente após `git push`.

## Acesso

Expostos via Ingress NGINX no host `solidarytech.pt`:

| Componente | URL | Credenciais |
|---|---|---|
| Grafana | `http://solidarytech.pt/grafana` | Secret `grafana-admin-credentials` (de `solidarytech/monitoring`) |
| Prometheus | `http://solidarytech.pt/prometheus` | — |
| Alertmanager | `http://solidarytech.pt/alertmanager` | — |

Aponte o DNS de `solidarytech.pt` para o Load Balancer do ingress-nginx (ou, em teste local, adicione `<EKS-LB>  solidarytech.pt` no `/etc/hosts`).

> Se você criar este repositório com **outro nome**, ajuste o `repoURL` nos 3 arquivos auto-referenciados: `monitoring-app-of-apps.yaml`, `apps/00-monitoring.yaml` e `argocd-apps/04-monitoring-manifests.yaml`.
