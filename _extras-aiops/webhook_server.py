#!/usr/bin/env python3
"""
SolidaryTech — Self-Healing + AIOps Incident Brain (webhook receiver)
=====================================================================
Recebe grupos de alertas do Alertmanager e trata cada grupo como UM incidente:

  1) ENRIQUECE   o contexto ANTES de chamar o Claude — logs do pod
     (kubectl logs --tail), Events recentes do namespace, uma consulta ao
     Prometheus (taxa de erro 5xx + latência p95 no momento) e correlação com o
     último rollout ("começou logo após o deploy X?").
  2) CORRELACIONA (anti-tempestade + FinOps) — colapsa os N alertas do grupo em
     UMA análise, UMA notificação e UMA issue; dedupe por janela de tempo.
  3) REMEDIA      conforme o tipo de alerta (catálogo):
        • ServiceDown / HighErrorRate5xx  -> rollout restart (self_healing)
        • OOMKilled                       -> recomendação/PR de rightsizing de memória
        • ImagePullBackOff/ErrImagePull   -> se a imagem não existe no ECR,
          dispara o pipeline de CI do microsserviço (workflow_dispatch) para
          (re)construir e publicar a imagem; senão, diagnóstico
        • CrashLoopBackOff                -> logs + explicação do Claude
     Guardas de segurança: circuit breaker (nº/hora) e modo auto|pr (human-in-the-loop).
  4) ABRE ITSM    uma GitHub Issue no firing (corpo = análise do Claude + contexto);
     no resolved, gera um rascunho de post-mortem com timeline, comenta e fecha.
  5) AUDITA        cada decisão como um K8s Event (kubectl get events) — rastreável.
  6) EXPÕE MÉTRICAS em /metrics (Prometheus): contadores de remediação/falha e
     histogramas de duração (detecção->correção) e de MTTR (firing->resolved).

Só usa a stdlib (urllib, http.server, subprocess) para rodar na imagem alpine/k8s
sem precisar de pip. Todas as chamadas externas são best-effort e com timeout: uma
falha de enriquecimento nunca bloqueia a remediação nem derruba o servidor.
"""
import json
import os
import re
import ssl
import logging
import subprocess
import threading
import time
import urllib.request
import urllib.parse
from collections import deque
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("aiops-webhook")

# --- Configuração (env / Secret) --------------------------------------------
PORT = int(os.getenv("PORT", "9095"))

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
ANTHROPIC_BASE_URL = os.getenv("ANTHROPIC_BASE_URL", "https://api.anthropic.com")
AISUMMARY_MAX_TOKENS = int(os.getenv("AISUMMARY_MAX_TOKENS", "600"))

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_API = os.getenv("GITHUB_API", "https://api.github.com")
# Repositório onde as issues de incidente são abertas (ITSM leve). Precisa de um
# PAT com issues:write neste repo. Ajuste para o SEU repo de incidentes.
ITSM_REPO = os.getenv("ITSM_REPO", "")           # ex.: "brianmonteiro54/solidarytech-monitoring-gitops"
ITSM_ENABLED = os.getenv("ITSM_ENABLED", "true").lower() == "true"

# Prometheus in-cluster. ATENÇÃO: o chart usa routePrefix=/prometheus, então a API
# fica sob /prometheus/api/v1/. Confirme o nome do serviço com:
#   kubectl get svc -n monitoring | grep prometheus
PROMETHEUS_URL = os.getenv(
    "PROMETHEUS_URL",
    "http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090/prometheus",
)

# Segurança / maturidade
REMEDIATION_MODE = os.getenv("REMEDIATION_MODE", "auto").lower()   # auto | pr
ENABLE_RESTART = os.getenv("ENABLE_RESTART", "true").lower() == "true"
MAX_REMEDIATIONS_PER_HOUR = int(os.getenv("MAX_REMEDIATIONS_PER_HOUR", "10"))
AUDIT_EVENTS = os.getenv("AUDIT_EVENTS", "true").lower() == "true"
DEDUPE_TTL_SECONDS = int(os.getenv("DEDUPE_TTL_SECONDS", "300"))
LOG_TAIL_LINES = os.getenv("LOG_TAIL_LINES", "50")

# Mapa serviço -> namespace (para achar pods quando o alerta não traz pod/namespace).
SERVICE_NAMESPACE_MAP = {
    "donation-service": "solidarytech-donation",
    "ngo-service": "solidarytech-ngo",
    "volunteer-service": "solidarytech-volunteer",
}

# Mapa serviço -> {repo, path} do manifesto de Deployment, para abrir PR de
# rightsizing de memória (OOMKilled). Vazio = só recomenda (sem PR). JSON no env.
#   SERVICE_REPO_MAP='{"donation-service":{"repo":"org/deploy-donation-service","path":"manifests/deployment.yaml"}}'
try:
    SERVICE_REPO_MAP = json.loads(os.getenv("SERVICE_REPO_MAP", "{}"))
except Exception:
    SERVICE_REPO_MAP = {}

# Mapa serviço -> {repo, workflow, ref} do repositório de CÓDIGO (CI) do
# microsserviço. Serve para DISPARAR o build (workflow_dispatch) quando a imagem
# ainda não existe no ECR — o cenário clássico de ambiente de teste recriado do
# zero, em que o ECR volta vazio e o pod fica em ImagePullBackOff. O pipeline
# constrói + publica a imagem no ECR e atualiza o repo de deploy; o Argo CD
# sincroniza e o pod sobe. Vazio = NÃO dispara build (só diagnostica). JSON no env.
# ATENÇÃO: o PAT (GITHUB_TOKEN) precisa de actions:write nesses repos de código.
#   SERVICE_CI_REPO_MAP='{"donation-service":{"repo":"brianmonteiro54/donation-service","workflow":"ci.yaml","ref":"main"}}'
try:
    SERVICE_CI_REPO_MAP = json.loads(os.getenv("SERVICE_CI_REPO_MAP", "{}"))
except Exception:
    SERVICE_CI_REPO_MAP = {}

# Liga/desliga o auto-build (disparo do pipeline) no ImagePullBackOff.
ENABLE_IMAGE_BUILD = os.getenv("ENABLE_IMAGE_BUILD", "true").lower() == "true"
# Se a causa exata do ImagePull não puder ser lida no kubelet (mensagem ausente),
# presume "imagem ausente" e dispara o build assim mesmo — recomendado em ambiente
# de teste. false = só dispara quando o kubelet CONFIRMA imagem/repo inexistente.
IMAGE_BUILD_ON_UNKNOWN = os.getenv("IMAGE_BUILD_ON_UNKNOWN", "true").lower() == "true"
# Janela mínima entre disparos do MESMO serviço (o build leva alguns minutos;
# sem cooldown, cada reincidência do alerta reenfileiraria um build novo).
IMAGE_BUILD_COOLDOWN_SECONDS = int(os.getenv("IMAGE_BUILD_COOLDOWN_SECONDS", "900"))

# In-cluster K8s API (para criar Events de auditoria via REST, sem depender do
# subcomando kubectl create event que não existe no kubectl estável).
K8S_HOST = os.getenv("KUBERNETES_SERVICE_HOST", "")
K8S_PORT = os.getenv("KUBERNETES_SERVICE_PORT", "443")
K8S_SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"


# ===========================================================================
# Métricas Prometheus (escritas à mão — sem dependência de pip)
# ===========================================================================
class _Metrics:
    """Registro mínimo de métricas no formato de exposição do Prometheus."""

    _BUCKETS = (10, 30, 60, 120, 300, 600, 1200, 3600)  # segundos

    def __init__(self):
        self._lock = threading.Lock()
        self._counters = {}    # (name, labels_tuple) -> float
        self._hist_buckets = {}  # (name, labels_tuple) -> {le: count}
        self._hist_sum = {}
        self._hist_count = {}
        self._help = {
            "aiops_remediations_total": "Total de remediações executadas pelo agente.",
            "aiops_remediation_failures_total": "Total de remediações que falharam.",
            "aiops_remediation_duration_seconds": "Duração detecção->correção (latência do agente).",
            "aiops_incident_duration_seconds": "Duração firing->resolved do incidente (MTTR).",
        }

    @staticmethod
    def _lkey(labels):
        return tuple(sorted((labels or {}).items()))

    def inc(self, name, labels=None, value=1.0):
        with self._lock:
            k = (name, self._lkey(labels))
            self._counters[k] = self._counters.get(k, 0.0) + value

    def observe(self, name, seconds, labels=None):
        with self._lock:
            lk = self._lkey(labels)
            k = (name, lk)
            b = self._hist_buckets.setdefault(k, {le: 0 for le in self._BUCKETS})
            for le in self._BUCKETS:
                if seconds <= le:
                    b[le] += 1
            self._hist_sum[k] = self._hist_sum.get(k, 0.0) + seconds
            self._hist_count[k] = self._hist_count.get(k, 0) + 1

    @staticmethod
    def _fmt_labels(lk, extra=None):
        items = list(lk) + (list(extra.items()) if extra else [])
        if not items:
            return ""
        inner = ",".join(f'{k}="{str(v)}"' for k, v in items)
        return "{" + inner + "}"

    def render(self):
        lines = []
        with self._lock:
            seen_help = set()
            for (name, lk), val in sorted(self._counters.items()):
                if name not in seen_help:
                    lines.append(f"# HELP {name} {self._help.get(name, name)}")
                    lines.append(f"# TYPE {name} counter")
                    seen_help.add(name)
                lines.append(f"{name}{self._fmt_labels(lk)} {val}")
            for (name, lk), buckets in sorted(self._hist_buckets.items()):
                if name not in seen_help:
                    lines.append(f"# HELP {name} {self._help.get(name, name)}")
                    lines.append(f"# TYPE {name} histogram")
                    seen_help.add(name)
                # buckets[le] já é cumulativo (observe() incrementa todo le >= valor)
                for le in self._BUCKETS:
                    lines.append(f'{name}_bucket{self._fmt_labels(lk, {"le": le})} {buckets[le]}')
                lines.append(f'{name}_bucket{self._fmt_labels(lk, {"le": "+Inf"})} {self._hist_count.get((name, lk), 0)}')
                lines.append(f"{name}_sum{self._fmt_labels(lk)} {self._hist_sum.get((name, lk), 0.0)}")
                lines.append(f"{name}_count{self._fmt_labels(lk)} {self._hist_count.get((name, lk), 0)}")
        return "\n".join(lines) + "\n"


METRICS = _Metrics()


# ===========================================================================
# Circuit breaker (limita remediações/hora para evitar agente "desgovernado")
# ===========================================================================
class _CircuitBreaker:
    def __init__(self, max_per_hour):
        self.max = max_per_hour
        self._events = deque()
        self._lock = threading.Lock()

    def allow(self):
        now = time.time()
        with self._lock:
            while self._events and now - self._events[0] > 3600:
                self._events.popleft()
            if len(self._events) >= self.max:
                return False
            self._events.append(now)
            return True

    def count(self):
        now = time.time()
        with self._lock:
            while self._events and now - self._events[0] > 3600:
                self._events.popleft()
            return len(self._events)


BREAKER = _CircuitBreaker(MAX_REMEDIATIONS_PER_HOUR)

# Cache de dedupe por incidente: fingerprint -> epoch_ts do último tratamento.
_incident_cache = {}
# Issues abertas por incidente (em memória; também recuperável via busca na API).
_open_issues = {}
# Cooldown de auto-build por serviço: service_name -> epoch_ts do último disparo.
_image_build_cooldown = {}


# ===========================================================================
# Utilidades
# ===========================================================================
def sh(cmd, timeout=25):
    """Roda um comando e devolve (rc, stdout, stderr). Nunca levanta exceção."""
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, (r.stdout or "").strip(), (r.stderr or "").strip()
    except subprocess.TimeoutExpired:
        return 124, "", "timeout"
    except Exception as e:  # noqa: BLE001
        return 1, "", str(e)


def http_json(url, data=None, headers=None, method="GET", timeout=20):
    """GET/POST JSON via urllib. Devolve (status, dict|texto) ou (None, erro)."""
    hdr = {"Accept": "application/json"}
    if headers:
        hdr.update(headers)
    body = None
    if data is not None:
        body = json.dumps(data).encode("utf-8")
        hdr.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=body, headers=hdr, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            try:
                return resp.status, json.loads(raw)
            except Exception:
                return resp.status, raw
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8")[:400]
        except Exception:
            pass
        return e.code, detail
    except Exception as e:  # noqa: BLE001
        return None, str(e)


def now_utc():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


def parse_ts(s):
    """Parse de timestamp ISO do Alertmanager/K8s. Devolve datetime aware ou None."""
    if not s:
        return None
    try:
        s = s.replace("Z", "+00:00")
        # trunca nanos -> micros se necessário
        m = re.match(r"(.*\.\d{6})\d*([+\-].*)?$", s)
        if m:
            s = m.group(1) + (m.group(2) or "")
        return datetime.fromisoformat(s)
    except Exception:
        return None


# ===========================================================================
# (1) Enriquecimento de contexto
# ===========================================================================
def resolve_targets(labels):
    """Descobre (namespace, [pods]) do alerta. Usa pod/namespace do alerta se
    houver; senão mapeia service_name -> namespace e lista pods do serviço."""
    ns = labels.get("namespace") or SERVICE_NAMESPACE_MAP.get(labels.get("service_name", ""))
    pod = labels.get("pod")
    if ns and pod:
        return ns, [pod]
    if ns and labels.get("service_name"):
        rc, out, _ = sh(["kubectl", "get", "pods", "-n", ns,
                         "-l", f"app.kubernetes.io/name={labels['service_name']}",
                         "-o", "jsonpath={.items[*].metadata.name}"])
        pods = out.split() if rc == 0 and out else []
        return ns, pods[:3]
    return ns, ([pod] if pod else [])


def fetch_pod_logs(ns, pods, previous=False):
    """Últimas linhas de log dos pods (best-effort)."""
    chunks = []
    for p in pods[:2]:
        cmd = ["kubectl", "logs", p, "-n", ns, f"--tail={LOG_TAIL_LINES}"]
        if previous:
            cmd.append("--previous")
        rc, out, err = sh(cmd, timeout=20)
        text = out if rc == 0 and out else f"(sem logs: {err or 'vazio'})"
        chunks.append(f"### pod {p}{' (anterior)' if previous else ''}\n{text[-2500:]}")
    return "\n\n".join(chunks) if chunks else "(sem pods para inspecionar)"


def fetch_recent_events(ns, pod=None):
    """Events recentes do namespace (ou do pod), ordenados por tempo."""
    cmd = ["kubectl", "get", "events", "-n", ns, "--sort-by=.lastTimestamp",
           "-o", "custom-columns=TIME:.lastTimestamp,TYPE:.type,REASON:.reason,OBJ:.involvedObject.name,MSG:.message",
           "--no-headers"]
    if pod:
        cmd += ["--field-selector", f"involvedObject.name={pod}"]
    rc, out, err = sh(cmd, timeout=20)
    if rc != 0 or not out:
        return f"(sem events: {err or 'vazio'})"
    lines = out.splitlines()[-15:]
    return "\n".join(lines)


def prom_query(promql):
    """Consulta instantânea ao Prometheus. Devolve float ou None."""
    url = f"{PROMETHEUS_URL}/api/v1/query?" + urllib.parse.urlencode({"query": promql})
    status, data = http_json(url, timeout=12)
    if status != 200 or not isinstance(data, dict):
        return None
    try:
        res = data["data"]["result"]
        if not res:
            return None
        return float(res[0]["value"][1])
    except Exception:
        return None


def fetch_prometheus_snapshot(service):
    """Taxa de erro 5xx (%) e latência p95 (s) do serviço no momento do alerta.

    ATENÇÃO: os matchers de label espelham suas PrometheusRules
    (service_namespace='solidarytech', service_name, http_response_status_code).
    Se sua instrumentação usa outros nomes, ajuste aqui. Você deve validar."""
    if not service:
        return {}
    err = prom_query(
        f'sum(rate(http_server_request_duration_seconds_count{{service_namespace="solidarytech",service_name="{service}",http_response_status_code=~"5.."}}[5m]))'
        f' / clamp_min(sum(rate(http_server_request_duration_seconds_count{{service_namespace="solidarytech",service_name="{service}"}}[5m])), 0.0001) * 100'
    )
    p95 = prom_query(
        f'histogram_quantile(0.95, sum(rate(http_server_request_duration_seconds_bucket{{service_namespace="solidarytech",service_name="{service}"}}[5m])) by (le))'
    )
    snap = {}
    if err is not None:
        snap["error_rate_5xx_pct"] = round(err, 2)
    if p95 is not None:
        snap["latency_p95_s"] = round(p95, 3)
    return snap


def fetch_last_deploy(ns, service, alert_started):
    """Correlação com o último rollout: idade do ReplicaSet mais novo + imagem.
    Se o alerta começou logo após o rollout, sinaliza a suspeita."""
    if not (ns and service):
        return {}
    # imagem + change-cause do deployment
    rc, out, _ = sh(["kubectl", "get", "deploy", service, "-n", ns, "-o", "json"], timeout=15)
    image = change_cause = None
    if rc == 0 and out:
        try:
            d = json.loads(out)
            containers = d["spec"]["template"]["spec"]["containers"]
            image = containers[0].get("image") if containers else None
            change_cause = d["metadata"].get("annotations", {}).get("kubernetes.io/change-cause")
        except Exception:
            pass
    # creationTimestamp do RS mais novo (== hora do último rollout)
    rc, out, _ = sh(["kubectl", "get", "rs", "-n", ns,
                     "-l", f"app.kubernetes.io/name={service}",
                     "--sort-by=.metadata.creationTimestamp",
                     "-o", "jsonpath={.items[-1:].metadata.creationTimestamp}"], timeout=15)
    deploy_time = parse_ts(out) if rc == 0 else None
    info = {}
    if image:
        info["image"] = image
        # tenta extrair um SHA do tag da imagem (ex.: :a1b2c3d ou :sha-<hex>)
        m = re.search(r"[:@-]([0-9a-f]{7,40})$", image)
        if m:
            info["commit_from_image"] = m.group(1)
    if change_cause:
        info["change_cause"] = change_cause
    if deploy_time:
        age_min = (now_utc() - deploy_time).total_seconds() / 60.0
        info["last_rollout_minutes_ago"] = round(age_min, 1)
        if alert_started and 0 <= (alert_started - deploy_time).total_seconds() <= 900:
            info["correlated_with_deploy"] = True  # alerta começou <=15min após o rollout
    return info


def build_enriched_context(incident):
    """Monta todo o contexto enriquecido do incidente (best-effort, com timeouts)."""
    labels = incident["labels"]
    service = labels.get("service_name", "")
    ns, pods = resolve_targets(labels)
    reason = incident["reason"]  # OOMKilled / ImagePullBackOff / CrashLoopBackOff / generic
    previous = reason in ("CrashLoopBackOff", "OOMKilled")  # logs do container anterior ajudam

    enr = {
        "namespace": ns,
        "pods": pods,
        "logs": fetch_pod_logs(ns, pods, previous=previous) if ns and pods else "(sem alvo)",
        "events": fetch_recent_events(ns, pods[0] if pods else None) if ns else "(sem namespace)",
        "prometheus": fetch_prometheus_snapshot(service),
        "last_deploy": fetch_last_deploy(ns, service, incident.get("started_at")),
    }
    return enr


# ===========================================================================
# (4)/(6) GitHub — Issues (ITSM) e PRs (rightsizing)
# ===========================================================================
def _gh_headers():
    return {"Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"}


def gh_open_issue(fingerprint, title, body):
    """Abre uma issue de incidente. Devolve número da issue ou None."""
    if not (GITHUB_TOKEN and ITSM_REPO and ITSM_ENABLED):
        return None
    marker = f"\n\n<!-- aiops-incident:{fingerprint} -->"
    status, data = http_json(
        f"{GITHUB_API}/repos/{ITSM_REPO}/issues",
        data={"title": title[:250], "body": (body + marker)[:60000],
              "labels": ["incident", "aiops"]},
        headers=_gh_headers(), method="POST")
    if status in (200, 201) and isinstance(data, dict):
        num = data.get("number")
        _open_issues[fingerprint] = num
        log.info("ITSM: issue #%s aberta (%s).", num, fingerprint)
        return num
    log.error("ITSM: falha ao abrir issue (%s): %s", status, str(data)[:300])
    return None


def gh_find_open_issue(fingerprint):
    """Recupera o número da issue aberta desse incidente (sobrevive a restart)."""
    if fingerprint in _open_issues:
        return _open_issues[fingerprint]
    if not (GITHUB_TOKEN and ITSM_REPO):
        return None
    q = urllib.parse.urlencode(
        {"q": f'repo:{ITSM_REPO} is:issue is:open in:body "aiops-incident:{fingerprint}"'})
    status, data = http_json(f"{GITHUB_API}/search/issues?{q}", headers=_gh_headers())
    if status == 200 and isinstance(data, dict) and data.get("items"):
        return data["items"][0].get("number")
    return None


def gh_comment_and_close(issue_number, comment):
    if not (GITHUB_TOKEN and ITSM_REPO and issue_number):
        return
    http_json(f"{GITHUB_API}/repos/{ITSM_REPO}/issues/{issue_number}/comments",
              data={"body": comment[:60000]}, headers=_gh_headers(), method="POST")
    http_json(f"{GITHUB_API}/repos/{ITSM_REPO}/issues/{issue_number}",
              data={"state": "closed", "state_reason": "completed"},
              headers=_gh_headers(), method="PATCH")
    log.info("ITSM: issue #%s comentada e fechada.", issue_number)


def gh_open_pr_bump_memory(service, new_limit, root_cause):
    """Abre um PR bumpando o limite de memória do Deployment do serviço.
    Requer SERVICE_REPO_MAP[service] = {repo, path}. Devolve URL do PR ou None."""
    m = SERVICE_REPO_MAP.get(service)
    if not (GITHUB_TOKEN and m and m.get("repo") and m.get("path")):
        return None
    repo, path = m["repo"], m["path"]
    base = m.get("branch", "main")
    # lê o arquivo
    status, meta = http_json(f"{GITHUB_API}/repos/{repo}/contents/{path}?ref={base}",
                             headers=_gh_headers())
    if status != 200 or not isinstance(meta, dict):
        return None
    import base64
    content = base64.b64decode(meta["content"]).decode("utf-8")
    # substitui o primeiro "memory: <valor>" sob limits (heurística simples)
    new_content, n = re.subn(r"(limits:\s*\n(?:\s+\w+:.*\n)*?\s+memory:\s*)([0-9]+[GMK]i)",
                             rf"\g<1>{new_limit}", content, count=1)
    if n == 0:
        return None
    branch = f"aiops/rightsize-{service}-{int(time.time())}"
    # cria branch a partir do head do base
    status, ref = http_json(f"{GITHUB_API}/repos/{repo}/git/ref/heads/{base}", headers=_gh_headers())
    if status != 200 or not isinstance(ref, dict):
        return None
    base_sha = ref["object"]["sha"]
    http_json(f"{GITHUB_API}/repos/{repo}/git/refs",
              data={"ref": f"refs/heads/{branch}", "sha": base_sha},
              headers=_gh_headers(), method="POST")
    # commit na branch
    http_json(f"{GITHUB_API}/repos/{repo}/contents/{path}",
              data={"message": f"fix(rightsizing): {service} memory limit -> {new_limit}",
                    "content": base64.b64encode(new_content.encode()).decode(),
                    "sha": meta["sha"], "branch": branch},
              headers=_gh_headers(), method="PUT")
    # abre PR
    status, pr = http_json(
        f"{GITHUB_API}/repos/{repo}/pulls",
        data={"title": f"[AIOps] Rightsizing de memória: {service} -> {new_limit}",
              "head": branch, "base": base,
              "body": f"Auto-proposto pelo AIOps após OOMKilled.\n\n**Causa raiz:** {root_cause}\n\n"
                      f"Novo limite sugerido: `{new_limit}`. **Revise antes de aprovar.**"},
        headers=_gh_headers(), method="POST")
    if status in (200, 201) and isinstance(pr, dict):
        return pr.get("html_url")
    return None


def gh_dispatch_workflow(repo, workflow, ref="main"):
    """Dispara um workflow via workflow_dispatch (POST .../dispatches).
    Requer PAT com actions:write no repo e o gatilho `workflow_dispatch` no CI.
    Devolve (ok: bool, detail: str). A API responde 204 (No Content) no sucesso."""
    if not (GITHUB_TOKEN and repo and workflow):
        return False, "token/repo/workflow ausente"
    url = (f"{GITHUB_API}/repos/{repo}/actions/workflows/"
           f"{urllib.parse.quote(workflow)}/dispatches")
    status, data = http_json(url, data={"ref": ref}, headers=_gh_headers(), method="POST")
    if status == 204:
        log.info("CI: workflow_dispatch OK em %s :: %s (ref=%s).", repo, workflow, ref)
        return True, "204 No Content"
    log.error("CI: falha no workflow_dispatch %s :: %s -> %s %s",
              repo, workflow, status, str(data)[:300])
    return False, f"HTTP {status}: {str(data)[:200]}"


def gh_latest_run_url(repo, workflow):
    """Melhor-esforço: link para a run mais recente do workflow. A run recém
    disparada pode levar alguns segundos para aparecer na API — se ainda não
    apareceu, devolve None e o chamador cai para a aba geral de Actions."""
    if not (GITHUB_TOKEN and repo and workflow):
        return None
    url = (f"{GITHUB_API}/repos/{repo}/actions/workflows/"
           f"{urllib.parse.quote(workflow)}/runs?per_page=1")
    status, data = http_json(url, headers=_gh_headers())
    if status == 200 and isinstance(data, dict):
        runs = data.get("workflow_runs") or []
        if runs:
            return runs[0].get("html_url")
    return None


# ===========================================================================
# (3)/(5) Auditoria — K8s Event via REST (SA token + CA in-cluster)
# ===========================================================================
def emit_k8s_event(ns, obj_name, reason, message, etype="Normal"):
    if not (AUDIT_EVENTS and K8S_HOST):
        return
    try:
        with open(f"{K8S_SA_DIR}/token") as f:
            token = f.read().strip()
    except Exception:
        return
    ns = ns or "monitoring"
    ts = iso(now_utc())
    body = {
        "apiVersion": "v1", "kind": "Event",
        "metadata": {"generateName": "aiops-", "namespace": ns},
        "involvedObject": {"apiVersion": "apps/v1", "kind": "Deployment",
                           "namespace": ns, "name": obj_name or "aiops"},
        "reason": reason[:128], "message": message[:1024], "type": etype,
        "source": {"component": "aiops-webhook"},
        "firstTimestamp": ts, "lastTimestamp": ts, "count": 1,
    }
    url = f"https://{K8S_HOST}:{K8S_PORT}/api/v1/namespaces/{ns}/events"
    ctx = ssl.create_default_context(cafile=f"{K8S_SA_DIR}/ca.crt")
    req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                 headers={"Authorization": f"Bearer {token}",
                                          "Content-Type": "application/json"},
                                 method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10, context=ctx):
            pass
    except Exception as e:  # noqa: BLE001
        log.warning("Auditoria (K8s Event) falhou: %s", e)


# ===========================================================================
# Claude — diagnóstico com contexto enriquecido (UMA chamada por incidente)
# ===========================================================================
def claude_incident_analysis(incident, enr, kind="diagnosis"):
    """Gera a análise de causa raiz (ou o post-mortem) com o Claude.
    Best-effort: sem chave ou em falha, devolve um texto de fallback."""
    labels = incident["labels"]
    ann = incident["annotations"]
    prom = enr.get("prometheus", {})
    dep = enr.get("last_deploy", {})

    if kind == "postmortem":
        instruction = (
            "Escreva um RASCUNHO de post-mortem curto em pt-BR (máx. 200 palavras), com: "
            "Resumo, Linha do tempo (com horários), Causa raiz, Ação de remediação, e "
            "Ações preventivas sugeridas. Baseie-se apenas nos dados fornecidos."
        )
    else:
        instruction = (
            "Escreva uma análise de incidente curta e ACIONÁVEL em pt-BR (máx. 150 palavras), "
            "com: (1) causa raiz provável, (2) impacto para o doador, (3) próximos passos. "
            "Use os logs/events/métricas/rollout abaixo como evidência. Não invente dados."
        )

    if not ANTHROPIC_API_KEY:
        fb = f"Análise automática indisponível (sem ANTHROPIC_API_KEY). Sintoma: {incident['reason']}."
        if prom:
            fb += f" Métricas: {prom}."
        if dep.get("correlated_with_deploy"):
            fb += f" Correlação: começou logo após o rollout (imagem {dep.get('image','?')})."
        return fb

    prompt = (
        "Você é um agente de AIOps/SRE da plataforma de doações SolidaryTech "
        "(Kubernetes + Prometheus + GitOps/ArgoCD). " + instruction + "\n\n"
        "=== INCIDENTE ===\n"
        f"Alertas no grupo: {incident.get('alertnames')}\n"
        f"Serviço: {labels.get('service_name','?')} | Namespace: {enr.get('namespace','?')}\n"
        f"Sintoma detectado: {incident['reason']}\n"
        f"Severidade: {labels.get('severity','?')}\n"
        f"Início (firing): {iso(incident['started_at']) if incident.get('started_at') else '?'}\n"
        f"Resumo: {ann.get('summary','')}\n"
        f"Descrição: {ann.get('description','')}\n\n"
        "=== MÉTRICAS (Prometheus, no momento) ===\n"
        f"{json.dumps(prom, ensure_ascii=False) or 'indisponível'}\n\n"
        "=== ÚLTIMO ROLLOUT ===\n"
        f"{json.dumps(dep, ensure_ascii=False) or 'indisponível'}\n\n"
        "=== EVENTS RECENTES ===\n"
        f"{enr.get('events','')[:1500]}\n\n"
        "=== LOGS (tail) ===\n"
        f"{enr.get('logs','')[:3500]}\n"
    )
    body = {"model": ANTHROPIC_MODEL, "max_tokens": AISUMMARY_MAX_TOKENS,
            "messages": [{"role": "user", "content": prompt}]}
    status, data = http_json(f"{ANTHROPIC_BASE_URL}/v1/messages", data=body,
                             headers={"x-api-key": ANTHROPIC_API_KEY,
                                      "anthropic-version": "2023-06-01"},
                             method="POST", timeout=30)
    if status == 200 and isinstance(data, dict):
        txt = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text").strip()
        if txt:
            return txt
    log.error("Claude falhou (%s): %s", status, str(data)[:200])
    return f"Análise automática falhou. Sintoma: {incident['reason']}. Métricas: {prom or 'n/d'}."


# ===========================================================================
# Discord
# ===========================================================================
def notify_discord(incident, enr, analysis, action, result, detail, extra_fields=None):
    if not DISCORD_WEBHOOK_URL:
        log.info("DISCORD_WEBHOOK_URL ausente — pulando notificação.")
        return
    color = {"ok": 3066993, "manual": 3447003, "fail": 15158332,
             "pr": 15844367}.get(result, 3447003)
    emoji = {"ok": "✅", "manual": "🧠", "fail": "❌", "pr": "🔀"}.get(result, "🧠")
    labels = incident["labels"]
    prom = enr.get("prometheus", {})
    dep = enr.get("last_deploy", {})
    fields = [
        {"name": "Serviço", "value": labels.get("service_name", "?"), "inline": True},
        {"name": "Namespace", "value": enr.get("namespace", "?"), "inline": True},
        {"name": "Sintoma", "value": incident["reason"], "inline": True},
        {"name": "Ação", "value": action, "inline": True},
        {"name": "Resultado", "value": detail[:1000], "inline": False},
    ]
    if prom:
        fields.append({"name": "📊 Métricas (no momento)",
                       "value": f"erro 5xx: {prom.get('error_rate_5xx_pct','n/d')}% | "
                                f"p95: {prom.get('latency_p95_s','n/d')}s", "inline": False})
    if dep.get("last_rollout_minutes_ago") is not None:
        corr = " ⚠️ logo após o rollout" if dep.get("correlated_with_deploy") else ""
        fields.append({"name": "🚀 Último rollout",
                       "value": f"{dep['last_rollout_minutes_ago']} min atrás{corr}\n"
                                f"`{dep.get('image','?')}`", "inline": False})
    if analysis:
        fields.append({"name": "🧠 Análise (IA)", "value": analysis[:1024], "inline": False})
    if extra_fields:
        fields.extend(extra_fields)
    payload = {"embeds": [{
        "title": f"{emoji} Incidente: {incident.get('alertnames','?')} — {labels.get('service_name','?')}",
        "color": color, "fields": fields[:25],
        "footer": {"text": f"SolidaryTech AIOps | breaker {BREAKER.count()}/{MAX_REMEDIATIONS_PER_HOUR}h | modo {REMEDIATION_MODE}"[:2048]},
        "timestamp": iso(now_utc()),
    }]}
    http_json(DISCORD_WEBHOOK_URL, data=payload,
              headers={"User-Agent": "SolidaryTech-AIOps/2.0"}, method="POST", timeout=10)


# ===========================================================================
# Remediação (catálogo) — com circuit breaker + auditoria + métricas
# ===========================================================================
def do_rollout_restart(service, ns):
    if not ENABLE_RESTART:
        return "manual", "restart desabilitado (ENABLE_RESTART=false)"
    if not BREAKER.allow():
        return "fail", f"circuit breaker: {MAX_REMEDIATIONS_PER_HOUR} remediações/h atingido"
    rc, out, err = sh(["kubectl", "rollout", "restart", f"deployment/{service}", "-n", ns], timeout=30)
    if rc == 0:
        return "ok", out or "rollout restart disparado"
    return "fail", err or "falha no rollout restart"


def suggest_memory_bump(enr, service):
    """Sugere um novo limite de memória a partir do atual (heurística: +50%)."""
    ns = enr.get("namespace")
    rc, out, _ = sh(["kubectl", "get", "deploy", service, "-n", ns,
                     "-o", "jsonpath={.spec.template.spec.containers[0].resources.limits.memory}"], timeout=15)
    cur = out if rc == 0 and out else None
    if not cur:
        return None, None
    m = re.match(r"^(\d+)(Gi|Mi|Ki)$", cur)
    if not m:
        return cur, None
    val, unit = int(m.group(1)), m.group(2)
    new = int(val * 1.5 + 0.999)
    return cur, f"{new}{unit}"


# --- ImagePullBackOff: causa raiz + auto-build (workflow_dispatch) -----------
# Assinaturas típicas de "imagem/repositório inexistente" na mensagem do kubelet
# (o caso do ECR vazio após recriar o ambiente). Aqui, (re)construir resolve.
_MISSING_IMAGE_RE = re.compile(
    r"(not\s+found|does\s+not\s+exist|manifest\s+unknown|name\s+unknown|"
    r"repository.*not.*(found|exist)|requested\s+image\s+not\s+found|"
    r"no\s+such\s+manifest|manifest\s+for.*not\s+found|image\s+not\s+found)",
    re.IGNORECASE)
# Acesso negado do ECR. ATENÇÃO: puxar de um repo ECR inexistente costuma vir
# como "pull access denied ... repository does not exist" — por isso o texto
# acima (que casa "does not exist") é testado ANTES deste.
_AUTH_IMAGE_RE = re.compile(
    r"(access\s+denied|pull\s+access\s+denied|unauthorized|not\s+authorized|"
    r"authentication\s+required|no\s+basic\s+auth|forbidden|denied)",
    re.IGNORECASE)


def inspect_image_pull_cause(ns, pods):
    """Lê a mensagem de 'waiting' do container + Events recentes do pod e
    classifica a causa do ImagePull. Devolve {class, message, image}, onde
    class ∈ {missing_image, auth, other, unknown}. Best-effort e sem exceções."""
    if not (ns and pods):
        return {"class": "unknown", "message": "", "image": ""}
    image, waiting_msg = "", ""
    jp = ('{range .status.containerStatuses[*]}'
          '{.image}{"\\t"}{.state.waiting.reason}{"\\t"}{.state.waiting.message}{"\\n"}'
          '{end}')
    rc, out, _ = sh(["kubectl", "get", "pod", pods[0], "-n", ns, "-o", f"jsonpath={jp}"],
                    timeout=15)
    if rc == 0 and out:
        for line in out.splitlines():
            parts = line.split("\t", 2)
            img = parts[0] if parts else ""
            reason = parts[1] if len(parts) > 1 else ""
            wmsg = parts[2] if len(parts) > 2 else ""
            if img and not image:
                image = img
            if ("imagepull" in reason.lower() or "errimage" in reason.lower()) and wmsg:
                waiting_msg = wmsg
                break
    # A waiting.message do ImagePullBackOff costuma ser genérica ("Back-off
    # pulling image ..."); o texto exato ("...: not found") vem nos Events do
    # kubelet. Por isso sempre combinamos ambos antes de classificar.
    events = fetch_recent_events(ns, pods[0]) or ""
    combined = (waiting_msg + "\n" + events).strip()
    if _MISSING_IMAGE_RE.search(combined):
        klass = "missing_image"
    elif _AUTH_IMAGE_RE.search(combined):
        klass = "auth"
    elif combined:
        klass = "other"
    else:
        klass = "unknown"
    return {"class": klass, "message": combined[-600:], "image": image}


def trigger_image_build(service):
    """Dispara o pipeline de CI (workflow_dispatch) do microsserviço para
    (re)construir e publicar a imagem no ECR. Guardado por cooldown por serviço
    (o build leva alguns minutos). Devolve (result, detail, extra_fields)."""
    if not ENABLE_IMAGE_BUILD:
        return "manual", ("auto-build desligado (ENABLE_IMAGE_BUILD=false) — "
                          "publique a imagem manualmente rodando o CI do microsserviço."), None
    m = SERVICE_CI_REPO_MAP.get(service)
    if not (GITHUB_TOKEN and m and m.get("repo")):
        return "manual", ("sem SERVICE_CI_REPO_MAP para este serviço (ou sem GITHUB_TOKEN) — "
                          "não sei qual pipeline disparar. Rode o CI do microsserviço para "
                          "publicar a imagem no ECR."), None
    now = time.time()
    last = _image_build_cooldown.get(service, 0)
    if now - last < IMAGE_BUILD_COOLDOWN_SECONDS:
        restam = int(IMAGE_BUILD_COOLDOWN_SECONDS - (now - last))
        return "manual", (f"pipeline de build já disparado há pouco para {service}; "
                          f"aguardando concluir (cooldown ~{restam}s)."), None
    repo = m["repo"]
    workflow = m.get("workflow", "ci.yaml")
    ref = m.get("ref", "main")
    ok, detail = gh_dispatch_workflow(repo, workflow, ref)
    if not ok:
        return "fail", f"falha ao disparar {repo} :: {workflow} ({detail})", None
    _image_build_cooldown[service] = now
    run_url = gh_latest_run_url(repo, workflow) or f"https://github.com/{repo}/actions"
    extra = [{"name": "🏗️ Pipeline disparado",
              "value": f"[{repo} · {workflow}]({run_url})", "inline": False}]
    return "ok", (f"pipeline de build disparado em `{repo}` (workflow `{workflow}`, ref `{ref}`). "
                  f"Ele publica a imagem no ECR e atualiza o repo de deploy; o Argo CD sincroniza "
                  f"e o pod sai do ImagePullBackOff."), extra


def remediate(incident, enr, analysis):
    """Aplica a ação conforme o tipo de sintoma. Devolve (action, result, detail, extra)."""
    labels = incident["labels"]
    service = labels.get("service_name", "")
    ns = enr.get("namespace")
    reason = incident["reason"]
    extra = None

    if reason == "OOMKilled":
        cur, new = suggest_memory_bump(enr, service)
        base = f"OOMKilled. Limite atual de memória: {cur or 'desconhecido'}."
        if new and REMEDIATION_MODE == "pr":
            if not BREAKER.allow():
                return "recomendação (breaker)", "manual", f"{base} Sugerido: {new} (breaker atingido)", None
            pr = gh_open_pr_bump_memory(service, new, analysis or base)
            if pr:
                extra = [{"name": "🔀 Pull Request", "value": pr, "inline": False}]
                return "PR de rightsizing", "pr", f"{base} PR abrindo limite -> {new}.", extra
            return "recomendação", "manual", f"{base} Sugerido: {new} (PR não configurado p/ este serviço).", None
        return "recomendação de rightsizing", "manual", f"{base} Sugerido: {new or 'aumentar limite'}. Aplique via PR.", None

    if reason in ("ImagePullBackOff", "ErrImagePull"):
        cause = inspect_image_pull_cause(ns, enr.get("pods"))
        klass = cause["class"]
        # missing_image  -> imagem/repo confirmadamente ausente (ex.: ECR vazio).
        # unknown/auth    -> ambíguo; em ECR, "denied" costuma mascarar repo
        #                    inexistente, então tratamos como "provável ausente"
        #                    quando IMAGE_BUILD_ON_UNKNOWN=true.
        should_build = (klass == "missing_image") or \
                       (IMAGE_BUILD_ON_UNKNOWN and klass in ("unknown", "auth"))
        if should_build:
            result, detail, extra = trigger_image_build(service)
            motivo = {
                "missing_image": "imagem/repositório inexistente no ECR",
                "auth": "acesso negado ao ECR (provável repositório inexistente)",
                "unknown": "causa não lida no kubelet — presumindo imagem ausente",
            }.get(klass, klass)
            return "disparo do pipeline de build (workflow_dispatch)", result, \
                   f"[{motivo}] {detail}", extra
        # Causa explícita e diferente de imagem ausente (tag errada de fato,
        # registry inválido, etc.) -> diagnóstico, sem disparar build.
        return "diagnóstico (sem auto-fix)", "manual", \
               (f"ImagePull por causa não relacionada a imagem ausente. "
                f"Corrija a referência/registry no manifesto. Detalhe: {cause['message'][:220]}"), None

    if reason == "CrashLoopBackOff":
        # Diagnóstico via logs já enriquecido; opcionalmente reinicia se self_healing.
        if labels.get("self_healing") == "true" and ns and ENABLE_RESTART:
            r, d = do_rollout_restart(service, ns)
            return "rollout restart + diagnóstico", r, d, None
        return "diagnóstico (logs)", "manual", "CrashLoop: veja a análise da IA a partir dos logs.", None

    # Padrão: ServiceDown / HighErrorRate5xx -> restart se self_healing + serviço conhecido
    if labels.get("self_healing") == "true" and ns and service in SERVICE_NAMESPACE_MAP:
        r, d = do_rollout_restart(service, ns)
        return "kubectl rollout restart", r, d, None
    return "nenhuma (remediação manual)", "manual", "sem self_healing / serviço desconhecido — ver runbook", None


# ===========================================================================
# Processamento de um incidente (grupo de alertas)
# ===========================================================================
def classify_reason(alertnames, labels):
    """Deriva o 'sintoma' do incidente a partir dos alertas do grupo."""
    joined = " ".join(alertnames).lower()
    if "oom" in joined:
        return "OOMKilled"
    if "imagepull" in joined or "errimage" in joined:
        return "ImagePullBackOff"
    if "crashloop" in joined:
        return "CrashLoopBackOff"
    return alertnames[0] if alertnames else "Incident"


def handle_firing(incident):
    fp = incident["fingerprint"]
    # dedupe por janela (anti-tempestade / repeat_interval do Alertmanager)
    last = _incident_cache.get(fp)
    if last and time.time() - last < DEDUPE_TTL_SECONDS:
        log.info("Incidente %s ainda em janela de dedupe — ignorando repetição.", fp)
        return
    _incident_cache[fp] = time.time()

    t0 = time.time()
    log.info("🚨 Incidente FIRING %s | reason=%s | alerts=%s",
             fp, incident["reason"], incident["alertnames"])

    enr = build_enriched_context(incident)
    analysis = claude_incident_analysis(incident, enr, kind="diagnosis")
    action, result, detail, extra = remediate(incident, enr, analysis)

    # métricas
    labels_m = {"agent": "webhook", "service": incident["labels"].get("service_name", "?"),
                "action": action_label(action), "reason": incident["reason"]}
    METRICS.inc("aiops_remediations_total", labels_m)
    if result == "fail":
        METRICS.inc("aiops_remediation_failures_total", labels_m)
    METRICS.observe("aiops_remediation_duration_seconds", time.time() - t0,
                    {"agent": "webhook", "reason": incident["reason"]})

    # auditoria (K8s Event)
    emit_k8s_event(enr.get("namespace"), incident["labels"].get("service_name"),
                   reason=f"AIOps{result.capitalize()}",
                   message=f"{incident['reason']} -> {action}: {detail}",
                   etype="Warning" if result == "fail" else "Normal")

    # ITSM: abre issue com a análise
    issue_num = None
    if ITSM_ENABLED and GITHUB_TOKEN and ITSM_REPO:
        title = f"[Incidente] {incident.get('alertnames')} — {incident['labels'].get('service_name','?')}"
        body = (f"**Sintoma:** {incident['reason']}\n"
                f"**Serviço:** {incident['labels'].get('service_name','?')} "
                f"(`{enr.get('namespace','?')}`)\n"
                f"**Início:** {iso(incident['started_at']) if incident.get('started_at') else '?'}\n"
                f"**Ação automática:** {action} — {detail}\n\n"
                f"## Análise (IA)\n{analysis}\n\n"
                f"## Métricas\n```json\n{json.dumps(enr.get('prometheus',{}), ensure_ascii=False, indent=2)}\n```\n"
                f"## Último rollout\n```json\n{json.dumps(enr.get('last_deploy',{}), ensure_ascii=False, indent=2)}\n```\n")
        issue_num = gh_open_issue(fp, title, body)

    extra_fields = list(extra or [])
    if issue_num:
        extra_fields.append({"name": "🎫 Issue", "value": f"{ITSM_REPO}#{issue_num}", "inline": True})
    notify_discord(incident, enr, analysis, action, result, detail, extra_fields)


def handle_resolved(incident):
    """Fecha o ciclo ITSM: post-mortem + comment + close, e registra o MTTR."""
    fp = incident["fingerprint"]
    issue_num = gh_find_open_issue(fp)
    # MTTR (firing -> resolved)
    if incident.get("started_at") and incident.get("ended_at"):
        mttr = (incident["ended_at"] - incident["started_at"]).total_seconds()
        if mttr > 0:
            METRICS.observe("aiops_incident_duration_seconds", mttr,
                            {"agent": "webhook", "service": incident["labels"].get("service_name", "?")})
            log.info("Incidente %s resolvido | MTTR=%.0fs", fp, mttr)
    emit_k8s_event(SERVICE_NAMESPACE_MAP.get(incident["labels"].get("service_name", ""), "monitoring"),
                   incident["labels"].get("service_name"), reason="AIOpsResolved",
                   message=f"Incidente resolvido: {incident.get('alertnames')}", etype="Normal")
    if not issue_num:
        return
    # gera post-mortem (usa contexto leve — o incidente já foi resolvido)
    enr = {"namespace": SERVICE_NAMESPACE_MAP.get(incident["labels"].get("service_name", ""), "?"),
           "prometheus": {}, "last_deploy": {}, "events": "", "logs": ""}
    pm = claude_incident_analysis(incident, enr, kind="postmortem")
    started = iso(incident["started_at"]) if incident.get("started_at") else "?"
    ended = iso(incident["ended_at"]) if incident.get("ended_at") else "?"
    comment = (f"✅ **Incidente resolvido.**\n\n"
               f"- Início (firing): {started}\n- Fim (resolved): {ended}\n\n"
               f"## Rascunho de post-mortem (IA)\n{pm}\n\n"
               f"_Fechado automaticamente pelo AIOps._")
    gh_comment_and_close(issue_num, comment)


def action_label(action):
    a = action.lower()
    if "build" in a or "pipeline" in a or "dispatch" in a:
        return "build"
    if "restart" in a:
        return "restart"
    if "pr" in a:
        return "pr"
    if "rightsizing" in a or "recomenda" in a:
        return "recommend"
    if "diagn" in a:
        return "diagnose"
    return "none"


# ===========================================================================
# HTTP server
# ===========================================================================
def build_incident(payload):
    """Constrói o incidente (um por grupo do Alertmanager)."""
    alerts = payload.get("alerts", [])
    common = payload.get("commonLabels", {}) or {}
    group_labels = payload.get("groupLabels", {}) or {}
    # labels representativos: prioriza commonLabels, cai para o 1º alerta
    labels = dict(common)
    ann = {}
    alertnames, starts, ends = set(), [], []
    for a in alerts:
        al = a.get("labels", {})
        labels.setdefault("service_name", al.get("service_name"))
        labels.setdefault("severity", al.get("severity"))
        labels.setdefault("namespace", al.get("namespace"))
        labels.setdefault("pod", al.get("pod"))
        labels.setdefault("self_healing", al.get("self_healing"))
        if not ann:
            ann = a.get("annotations", {}) or {}
        if al.get("alertname"):
            alertnames.add(al["alertname"])
        st = parse_ts(a.get("startsAt"))
        en = parse_ts(a.get("endsAt"))
        if st:
            starts.append(st)
        if en and en.year > 1:  # endsAt "0001-01-01" = ainda firing
            ends.append(en)
    labels = {k: v for k, v in labels.items() if v is not None}
    names = sorted(alertnames) or [group_labels.get("alertname", "Incident")]
    reason = classify_reason(names, labels)
    fp = payload.get("groupKey") or f"{','.join(names)}|{labels.get('service_name','?')}"
    # fingerprint estável e curto
    fp = re.sub(r"\s+", "", fp)[:120]
    return {
        "fingerprint": fp,
        "labels": labels,
        "annotations": ann,
        "alertnames": ", ".join(names),
        "alertnames_list": names,
        "reason": reason,
        "started_at": min(starts) if starts else None,
        "ended_at": max(ends) if ends else None,
        "status": payload.get("status", "firing"),
    }


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.end_headers()
        self.wfile.write(body if isinstance(body, bytes) else body.encode())

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return self._send(400, '{"error":"invalid JSON"}')
        try:
            incident = build_incident(payload)
            if incident["status"] == "resolved":
                handle_resolved(incident)
            else:
                handle_firing(incident)
        except Exception as e:  # noqa: BLE001  (nunca derruba o servidor)
            log.exception("Erro processando incidente: %s", e)
            return self._send(200, json.dumps({"ok": False, "error": str(e)}))
        return self._send(200, json.dumps({"ok": True, "fingerprint": incident["fingerprint"]}))

    def do_GET(self):
        if self.path.startswith("/metrics"):
            return self._send(200, METRICS.render(), ctype="text/plain; version=0.0.4")
        return self._send(200, '{"status":"ok","service":"aiops-webhook"}')

    def log_message(self, fmt, *args):
        log.info("%s - %s", self.client_address[0], fmt % args)


if __name__ == "__main__":
    log.info("AIOps webhook | porta=%s | modo=%s | breaker=%s/h | ITSM=%s | prom=%s",
             PORT, REMEDIATION_MODE, MAX_REMEDIATIONS_PER_HOUR,
             ITSM_REPO or "(off)", PROMETHEUS_URL)
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
