# 📊 solidarytech-monitoring-gitops

> Stack completa de observabilidade da plataforma **SolidaryTech** — **métricas, logs, traces, alertas e self-healing** — entregue por **GitOps** com Argo CD no padrão **App-of-Apps**.

---


## Sumário

- [Arquitetura GitOps (App-of-Apps)](#arquitetura-gitops-app-of-apps)
- [Componentes do stack](#componentes-do-stack)
- [Fluxo de telemetria](#fluxo-de-telemetria)
- [Alertas e roteamento](#alertas-e-roteamento)
- [Self-healing](#self-healing)
- [Segredos (External Secrets)](#segredos-external-secrets)
- [Estrutura do repositório](#estrutura-do-repositório)
- [Pré-requisitos (uma vez)](#pré-requisitos-uma-vez)
- [Deploy](#deploy)
- [Verificação](#verificação)
- [Acesso](#acesso)
- [Notas de ambiente e troubleshooting](#notas-de-ambiente-e-troubleshooting)
- [Customização](#customização)

---

## Arquitetura GitOps (App-of-Apps)

`monitoring-app-of-apps.yaml` é o **único manifesto aplicado à mão**. Ele cria a Application raiz, que varre a pasta `apps/` e registra tudo o que estiver lá. As *sync-waves* garantem que a observabilidade sobe **antes** dos microsserviços:

```mermaid
flowchart TD
    apply["kubectl apply<br/>monitoring-app-of-apps.yaml"] --> root["solidarytech-root<br/><i>Application raiz</i>"]

    root -->|varre apps/| mon["monitoring-app-of-apps<br/><b>wave −10</b> · sobe primeiro"]
    root -->|varre apps/| s1["donation-service<br/>wave 10"]
    root -->|varre apps/| s2["ngo-service<br/>wave 10"]
    root -->|varre apps/| s3["volunteer-service<br/>wave 10"]

    mon -->|varre argocd-apps/| kps["kube-prometheus-stack"]
    mon --> loki["loki-stack"]
    mon --> otel["otel-collector"]
    mon --> mm["monitoring-manifests<br/><i>varre manifests/</i>"]

    s1 -.-> dr1["repo deploy-donation-service<br/>ns solidarytech-donation"]
    s2 -.-> dr2["repo deploy-ngo-service<br/>ns solidarytech-ngo"]
    s3 -.-> dr3["repo deploy-volunteer-service<br/>ns solidarytech-volunteer"]

    classDef wave fill:#EF7B4D,stroke:#333,color:#fff;
    class mon,s1,s2,s3 wave;
```

A wave `−10` só libera a wave `10` quando o `monitoring-app-of-apps` estiver **Synced + Healthy** — ou seja, quando as quatro Applications filhas (Prometheus stack, Loki, OTel Collector e os manifests) já estiverem de pé. Assim os microsserviços nunca sobem antes da observabilidade que vai monitorá-los.

---

## Componentes do stack

| Application | Origem | Versão | Entrega |
|---|---|---|---|
| **kube-prometheus-stack** | Helm `prometheus-community` | `83.4.2` | Prometheus, Grafana, Alertmanager, node-exporter, kube-state-metrics |
| **loki-stack** | Helm `grafana` | chart `2.10.3` (Loki `2.9.10`) | Loki + Promtail (DaemonSet), backend **S3** |
| **otel-collector** | Helm `open-telemetry` (imagem *contrib*) | `0.146.1` | OpenTelemetry Collector em modo *deployment* — hub central |
| **monitoring-manifests** | Este repositório (`manifests/`) | `main` | ServiceMonitors, dashboard, alertas, External Secrets e self-healing |

**Retenção:** Prometheus `7d`, Loki `168h` (7 dias). Sem PVC — ver [notas de ambiente](#notas-de-ambiente-e-troubleshooting).

---

## Fluxo de telemetria

Os três serviços são instrumentados com **OpenTelemetry** e enviam tudo via **OTLP** para o Collector, que atua como hub central e distribui cada sinal para o destino certo. Logs de pods também chegam ao Loki por um caminho independente (Promtail).

```mermaid
flowchart LR
    d["donation-service · Go"]
    n["ngo-service · Python"]
    v["volunteer-service · Python"]

    d & n & v -- OTLP --> col["OTel Collector"]
    d -. "/metrics<br/>(ServiceMonitor)" .-> prom["Prometheus"]

    col -- "remote_write" --> prom
    col -- "OTLP (metrics+logs+traces)" --> nr["New Relic"]

    pt["Promtail · DaemonSet"] -- "push logs" --> loki["Loki"]

    prom --> graf["Grafana"]
    loki --> graf
    prom --> am["Alertmanager"]

    am --> disc["Discord"]
    am --> pd["PagerDuty"]
    am -- "self_healing=true" --> wh["Self-Healing Webhook"]
    wh -- "rollout restart" --> d & n & v
```

Pontos que costumam confundir e que valem destacar:

- **Métricas de todos os serviços chegam ao Prometheus pelo Collector** (`prometheusremotewrite`), com o label `service_namespace="solidarytech"`. É esse conjunto que o dashboard e os alertas consomem (`http_server_request_duration_seconds_*`).
- **Só o `donation-service` (Go) tem `ServiceMonitor`**, porque é o único que expõe `/metrics` via `promhttp`. O `ngo` e o `volunteer` (Python) **não** expõem `/metrics` — suas métricas entram só por OTLP. Eles não têm ServiceMonitor de propósito, para evitar falso-positivo de `TargetDown`.
- **Logs vão para o Loki via Promtail** (DaemonSet que raspa os logs dos pods) e para o **New Relic** via Collector. Traces vão só para o New Relic.

---

## Alertas e roteamento

A `PrometheusRule` `solidarytech-alerts` define cinco regras:

| Alerta | Disparo | Severidade | Self-healing |
|---|---|---|---|
| `HighErrorRate5xx` | erros 5xx > 5% por 2 min | 🔴 critical | ✅ |
| `ServiceDown` | deployment com 0 réplicas por 1 min | 🔴 critical | ✅ |
| `HighLatencyP95` | latência P95 > 2s por 3 min | 🟠 warning | — |
| `PodCrashLooping` | > 3 restarts em 15 min | 🟠 warning | — |
| `HighMemoryUsage` | > 85% do limite de memória por 5 min | 🟠 warning | — |

O Alertmanager (config renderizada por External Secrets) roteia por prioridade:

```mermaid
flowchart TD
    a[Alerta] --> w{Watchdog /<br/>InfoInhibitor?}
    w -- sim --> nul["🔇 descarta (ruído do chart)"]
    w -- não --> c{critical +<br/>self_healing?}
    c -- sim --> r1["PagerDuty + Discord + 🔧 Self-Healing"]
    c -- não --> c2{critical?}
    c2 -- sim --> r2["PagerDuty + Discord"]
    c2 -- não --> c3{warning?}
    c3 -- sim --> r3["💬 só Discord (ChatOps)"]
```

> A rota que descarta `Watchdog`/`InfoInhibitor` precisa ser a **primeira** — no Alertmanager a ordem das rotas importa.

---

## Self-healing

Um pequeno servidor HTTP em Python (`self-healing-webhook`, imagem `alpine/k8s` com `kubectl`, porta `9095`) recebe os webhooks do Alertmanager e age sozinho. Ele só executa quando **as três condições** são verdadeiras:

1. o alerta está `firing` (não `resolved`);
2. o label `self_healing="true"` está presente;
3. o `service_name` é conhecido (mapeado para seu namespace).

A ação é um `kubectl rollout restart` no deployment afetado, seguido de uma notificação no Discord (✅ sucesso / ❌ falha). As permissões vêm de um `ServiceAccount` + `ClusterRole` dedicados (`deployments: get/list/patch`, `pods: get/list/delete`, `events: get/list`).

---

## Disaster Recovery (Velero)

O **Velero** (`apps/20-velero.yaml`, sync-wave 20) implementa a **Opção A** do desafio: backup do estado do cluster para um bucket S3. O Argo CD instala o Velero via Helm e cria **duas** `Schedule`s (uma de hora em hora e uma diária, no padrão avô-pai-filho) — sem `kubectl` manual.

| Item | Valor |
|---|---|
| Chart / versão | Helm `vmware-tanzu` · `velero 12.1.0` (Velero `1.18.1`) |
| Plugin | `velero-plugin-for-aws:v1.14.2` (linha compatível com Velero 1.18.x) |
| Destino | Bucket `solidarytech-velero-<ACCOUNT_ID>` · `us-east-1` (mesma conta) |
| Agenda · nível 1 | `hourly-cluster-backup` · `0 * * * *` (de hora em hora) · ttl `72h` (3 dias) → **RPO de topologia ≈ 1h** |
| Agenda · nível 2 | `daily-cluster-backup` · `0 5 * * *` (05:00 UTC = 02:00 BRT) · ttl `168h` (7 dias) → semana de pontos diários |
| Escopo | Cluster inteiro (`includedNamespaces: '*'`) |

**O que é (e não é) protegido.** Como o cluster de lab não tem EBS CSI Driver e roda tudo em `emptyDir`, **não há PersistentVolume** para snapshot — por isso `snapshotsEnabled: false`. O backup captura o **estado do cluster** (namespaces, deployments, services, configmaps, secrets, ingress, RBAC, CRs). Os **dados transacionais** das doações continuam protegidos pelo **PITR do RDS/DynamoDB** (RPO ~15 min) — camada separada, como descreve o PCN. O backup roda em **dois níveis**: o **horário** (retenção de 3 dias) garante o RPO da topologia do cluster em **≈ 1h**, exatamente o que o dossiê define para a "Topologia da aplicação" (≈ 1 h, configurável); o **diário** (retenção de 7 dias) mantém uma semana de pontos de restauração sem acumular ~168 backups horários. O RPO de **~15 min dos dados** transacionais é de outra camada (PITR do RDS/DynamoDB) e permanece inalterado.

**CRDs.** Instalados pelo Argo CD a partir da pasta `crds/` do chart (o `helm template` do Argo já roda com `--include-crds`). O job `upgradeCRDs` fica **desligado** para evitar dependência de RBAC/imagem `kubectl` no lab.

**Credenciais.** Via `credentials.existingSecret: velero-aws-credentials` (chave `cloud`, formato INI), criado pelo bootstrap Terraform — ver [Pré-requisitos](#pré-requisitos-uma-vez). O plugin usa `aws-sdk-go-v2`, que lê `aws_session_token` do INI (funciona com as credenciais temporárias do Academy).

**Métricas.** `metrics.serviceMonitor.enabled: true` → o Prometheus faz scrape do Velero, então o sucesso/falha de cada backup aparece no Prometheus/Grafana (evidência do "backup monitorado" do PCN).

**Demonstrar (evidência para a banca):**
```bash
# instalar o CLI do velero e apontar para o namespace velero
velero version
velero backup-location get                       # default · Available
velero schedule get                              # hourly-cluster-backup + daily-cluster-backup
velero backup create demo --from-schedule hourly-cluster-backup   # dispara na hora
velero backup describe demo --details
velero backup logs demo
aws s3 ls "s3://solidarytech-velero-<ACCOUNT_ID>/backups/"        # objetos no bucket
```

Restauração (num cluster novo/limpo): `velero restore create --from-backup <nome>`.

---

## Segredos (External Secrets)

Nada de segredo no Git. Um `SecretStore` aponta para o **AWS Secrets Manager** (`us-east-1`) e quatro `ExternalSecret` materializam os Secrets do cluster a partir do segredo `solidarytech/monitoring`:

| ExternalSecret | Secret gerado | Consumido por |
|---|---|---|
| `alertmanager-config` | `alertmanager-config` | renderiza o `alertmanager.yaml` inteiro (Discord + PagerDuty) |
| `grafana-admin-credentials` | `grafana-admin-credentials` | login admin do Grafana |
| `otel-collector-secrets` | `otel-collector-secrets` | `NEW_RELIC_API_KEY` do Collector |
| `self-healing-secrets` | `self-healing-secrets` | `DISCORD_WEBHOOK_URL` do webhook |

---

## Estrutura do repositório

```
solidarytech-monitoring-gitops/
├── monitoring-app-of-apps.yaml          # ÚNICO apply manual (cria solidarytech-root)
├── apps/                                # Apps gerenciados pelo solidarytech-root
│   ├── 00-monitoring.yaml               # wave −10: stack de observabilidade
│   ├── 10-donation-service.yaml         # wave 10 → repo deploy-donation-service
│   ├── 10-ngo-service.yaml              # wave 10 → repo deploy-ngo-service
│   ├── 10-volunteer-service.yaml        # wave 10 → repo deploy-volunteer-service
│   └── 20-velero.yaml                   # wave 20 → Velero (DR): instala + backup horário + diário
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

---

## Pré-requisitos (uma vez)

1. **Bucket S3 para o Loki:**
   ```bash
   aws s3 mb s3://solidarytech-loki-$(aws sts get-caller-identity --query Account --output text) --region us-east-1
   ```
   Se o ID da sua conta for diferente do placeholder, ajuste `s3: s3://us-east-1/solidarytech-loki-ACCOUNT_ID` em `argocd-apps/02-loki-stack.yaml`.

2. **Segredo `solidarytech/monitoring`** no AWS Secrets Manager, com as chaves:
   `DISCORD_WEBHOOK_URL`, `PAGERDUTY_SERVICE_KEY`, `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD`, `NEW_RELIC_API_KEY`.

3. **Secret `aws-credentials`** no namespace `monitoring` (criado pelo bootstrap Terraform), com `access-key`, `secret-access-key` e `session-token` — é o mesmo que o `SecretStore` e o Loki usam.

4. **Bucket S3 para o Velero (DR)** — na **mesma conta e região** do restante:
   ```bash
   ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
   aws s3 mb "s3://solidarytech-velero-$ACCOUNT_ID" --region us-east-1

   # Tags FinOps (o `aws s3 mb` não taggeia sozinho):
   aws s3api put-bucket-tagging \
     --bucket "solidarytech-velero-$ACCOUNT_ID" \
     --tagging 'TagSet=[{Key=Project,Value=SolidaryTech},{Key=Environment,Value=Production},{Key=CostCenter,Value=NGO-Core},{Key=Owner,Value=Plataforma},{Key=ManagedBy,Value=Manual}]'
   ```
   Se o ID da sua conta for diferente do placeholder `042702827117`, ajuste o `bucket:` em `apps/20-velero.yaml`.

5. **Secret `velero-aws-credentials`** no namespace `velero`, com a chave `cloud`
   (arquivo de credenciais AWS em formato INI). É criado pelo **bootstrap Terraform**
   (bastion) junto com o namespace `velero`. Consumido pelo Velero via
   `credentials.existingSecret`.

   > **AWS Academy:** essas credenciais são temporárias (`session-token`) e expiram.
   > Quando expiram, o backup agendado do Velero falha até o Secret ser renovado —
   > exatamente a mesma limitação do Loki. Em conta real, isso seria IRSA.

-
---

## Acesso

Expostos via **Ingress NGINX** no host `solidarytech.pt`:

| Componente | URL | Credenciais |
|---|---|---|
| Grafana | `http://solidarytech.pt/grafana` | Secret `grafana-admin-credentials` (de `solidarytech/monitoring`) |
| Prometheus | `http://solidarytech.pt/prometheus` | — |
| Alertmanager | `http://solidarytech.pt/alertmanager` | — |

O dashboard **SolidaryTech – Visão Geral** (pasta `SolidaryTech` no Grafana) traz CPU/memória por namespace, taxa de requisições e de erros 5xx por serviço, P95 de latência, pods em restart e logs em tempo real do Loki.

Aponte o DNS de `solidarytech.pt` para o Load Balancer do `ingress-nginx` — ou, em teste local, adicione `<EKS-LB>  solidarytech.pt` ao seu `/etc/hosts`.

---

## Notas de ambiente e troubleshooting

Este stack foi afinado para **AWS Academy / EKS**, onde algumas premissas comuns não valem. Os ajustes abaixo já estão aplicados e evitam armadilhas conhecidas:

| Decisão | Por quê |
|---|---|
| `prometheusOperator.admissionWebhooks` desabilitado | em lab o certificado TLS do webhook não sobe direito e o StatefulSet do Prometheus nunca é criado |
| Sem PVC (`emptyDir`) no Prometheus/Loki | a conta não tem o EBS CSI Driver — os dados **não persistem** entre reinícios dos pods |
| `kubeControllerManager`, `kubeScheduler` e `kubeEtcd` desabilitados | o control plane do EKS é gerenciado e **não** expõe essas métricas; deixá-los ligados gera falso-positivo de "Down" |
| Endpoint do `remote_write` com prefixo `/prometheus` | o Prometheus roda com `routePrefix: /prometheus`; sem o prefixo o Collector recebe 404 |
| `RespectIgnoreDifferences=true` no `monitoring-manifests` | sem ele o `selfHeal` ignora o `ignoreDifferences` e fica sobrescrevendo os campos que o External Secrets injeta, deixando o app **OutOfSync** eterno |
| Service do Loki chama-se `loki-stack` (não `loki`) | o chart `loki-stack` nomeia o Service como `{{ .Release.Name }}-stack` |

---

## Customização

Se você **forkar ou renomear** este repositório, ajuste o `repoURL` nos três arquivos auto-referenciados:

- `monitoring-app-of-apps.yaml`
- `apps/00-monitoring.yaml`
- `argocd-apps/04-monitoring-manifests.yaml`