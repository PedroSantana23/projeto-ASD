# Sprint 4 — Supervisor de Métricas e Dashboard

**Projeto:** P2P com Balanceamento de Carga Dinâmico
**Disciplina:** Arquitetura de Sistemas Distribuídos — Prof. Michel Junio Ferreira Rosa

---

## 1. Objetivo da Sprint

Cada nó **Master** deve reportar, em tempo real, seu estado de desempenho e o estado
da sua *Farm* (workers e tarefas) para um **Supervisor de Métricas** central. O Supervisor
agrega os dados de todos os grupos e exibe num **dashboard web**, permitindo acompanhar
topologia, consumo de recursos, filas e o empréstimo de workers entre nós.

O envio é feito por **TLS sobre TCP** (não HTTP), em intervalos fixos, **sem aguardar resposta**.

---

## 2. Parâmetros da conexão com o Supervisor

| Parâmetro | Valor |
|---|---|
| Host | `nuted-ia.dev` |
| Porta | `443` |
| Protocolo | TLS sobre TCP |
| SNI | `nuted-ia.dev` |
| Intervalo de envio | a cada 10 segundos |
| Dashboard | https://nuted-ia.dev/supervisor/dashboard/ |

Regras obrigatórias:

- **Não** usar bibliotecas HTTP nem caminhos de URL (ex.: `/supervisor/collector`). Em TCP, o endpoint é só host + porta.
- O cliente apenas **conecta, envia o JSON e encerra** a conexão.
- **Não** executar `recv` esperando retorno do Supervisor.
- O JSON segue o `schema` definido abaixo, terminando com `\n` (padrão de delimitação do projeto).

---

## 3. Identificação do nó (`server_uuid`)

O campo `server_uuid` deve usar o identificador de *farm* atribuído pelo professor
(ex.: `michel_1`, `michel_2`). **Cada grupo usa um valor diferente** — se dois nós enviarem
o mesmo `server_uuid`, eles se sobrescrevem no dashboard.

No código, isso é configurável:

```bash
python master.py --supervisor-uuid michel_1
```

> Esses valores **não** fazem parte do endereço de conexão com o Supervisor; são apenas o
> rótulo do nó no payload.

---

## 4. Estrutura do Payload (`performance_report`)

O Master envia um único objeto JSON por ciclo:

```json
{
  "server_uuid": "michel_1",
  "hostname": "michel_1.farm.local",
  "role": "master",
  "task": "performance_report",
  "timestamp": "2026-06-08T12:34:56Z",
  "message_id": "<uuid v4>",
  "payload_version": "sprint4-monitor",
  "performance": {
    "system": { "...": "métricas de CPU/MEM/DISK do host" },
    "farm_state": { "...": "estado de workers e tarefas" },
    "config_thresholds": { "...": "limiares de saturação/liberação" },
    "neighbors": [ { "...": "masters vizinhos conhecidos" } ]
  }
}
```

### 4.1. Campos de cabeçalho

| Campo | Tipo | Descrição |
|---|---|---|
| `server_uuid` | string | Identificador do nó (ex.: `michel_1`) |
| `hostname` | string | Nome DNS do nó (padrão: `<uuid>.farm.local`) |
| `role` | string | Papel do nó: `"master"` |
| `task` | string | Tipo de relatório: `"performance_report"` |
| `timestamp` | string (ISO-8601) | Momento da coleta, formato `YYYY-MM-DDTHH:MM:SSZ` (UTC) |
| `message_id` | string (UUID) | Identificador único da mensagem |
| `payload_version` | string | Versão do schema (`sprint4-monitor`) |

### 4.2. `performance.system`

| Campo | Tipo | Origem no código |
|---|---|---|
| `uptime_seconds` | int | tempo desde o início do processo (`START_TIME`) |
| `load_average_1m` / `load_average_5m` | float | `os.getloadavg()` (0 em Windows) |
| `cpu.usage_percent` | float | `psutil.cpu_percent()` |
| `cpu.count_logical` / `cpu.count_physical` | int | `psutil.cpu_count()` |
| `memory.total_mb` / `available_mb` / `memory_used` | int | `psutil.virtual_memory()` |
| `memory.percent_used` | float | `psutil.virtual_memory().percent` |
| `disk.total_gb` / `free_gb` / `percent_used` | float | `psutil.disk_usage()` |

> Se `psutil` não estiver instalado, as métricas de sistema vão zeradas, mas o nó **ainda aparece**
> no dashboard. Para métricas reais: `pip install psutil`.

### 4.3. `performance.farm_state.workers`

| Campo | Tipo | Significado | Origem no código |
|---|---|---|---|
| `total_registered` | int | Workers registrados no nó | `len(known_local_workers) + len(workers_borrowed)` |
| `workers_utilization` | int | Workers executando tarefa | contador `tasks_running` |
| `workers_alive` | int | Workers vivos/respondendo | `total_registered` |
| `workers_idle` | int | Workers ociosos | `len(known_local_workers)` |
| `workers_borrowed` | int | Workers que **este nó emprestou** para fora | `len(lent_out)` |
| `workers_received` | int | Workers **recebidos** de outros nós | `len(workers_borrowed)` |
| `workers_failed` | int | Workers com falha | `0` |
| `workers_home` | int | Workers nativos (sem empréstimo) | `len(known_local_workers)` |
| `workers_available_capacity` | int | Capacidade ociosa (= idle) | `len(known_local_workers)` |
| `borrowed_workers` | array | Lista de empréstimos com direção/peer | `lent_out` + `workers_borrowed` |

Cada item de `borrowed_workers`:

```json
{ "direction": "out", "peer_uuid": "Master_2" }   // emprestou para Master_2
{ "direction": "in",  "peer_uuid": "Master_1" }   // recebeu de Master_1
```

> **Atenção ao nome dos campos:** `workers_borrowed` (no payload) = workers **cedidos para fora**;
> `workers_received` = workers **recebidos de fora**. Internamente, o dicionário `workers_borrowed`
> do código guarda os **recebidos**, enquanto `lent_out` guarda os **cedidos** — por isso o
> mapeamento acima é cruzado.

### 4.4. `performance.farm_state.tasks`

| Campo | Tipo | Origem |
|---|---|---|
| `tasks_pending` | int | `task_queue.qsize()` |
| `tasks_running` | int | contador `tasks_running` |
| `tasks_completed` | int | contador (incrementa em `STATUS = OK`) |
| `tasks_failed` | int | contador (incrementa em `STATUS = NOK`) |
| `oldest_task_age_s` | int | `0` (não rastreado) |

### 4.5. `performance.config_thresholds`

| Campo | Origem |
|---|---|
| `max_task` | `CAPACITY` (`--capacity`) |
| `warn_cpu_percent` | `85` |
| `warn_memory_percent` | `85` |
| `release_task` | `RELEASE_THRESHOLD` (`--release`) |

### 4.6. `performance.neighbors[]`

| Campo | Tipo | Descrição |
|---|---|---|
| `server_uuid` | string | ID do master vizinho (do diretório `NEIGHBOR_MASTERS`) |
| `status` | string | `"available"` |
| `last_heartbeat` | string (ISO-8601) | timestamp do envio |

---

## 5. Como está implementado (no `master.py`)

A Sprint 4 roda numa **thread daemon dedicada**, em paralelo ao servidor TCP, à descoberta
UDP e ao monitor de carga. As funções relevantes:

| Função | Responsabilidade |
|---|---|
| `coletar_sistema()` | Lê CPU/MEM/DISK/load via `psutil` (com fallback) |
| `coletar_farm_state()` | Monta `workers` e `tasks` a partir do estado interno (sob `state_lock`) |
| `montar_payload_supervisor()` | Constrói o JSON completo do `performance_report` |
| `enviar_ao_supervisor()` | Abre TLS, faz `sendall`, **sem `recv`**, e fecha |
| `loop_supervisor()` | Repete o envio a cada `SUP_INTERVAL` segundos |

Trecho central do envio (apenas SEND, conforme a spec):

```python
ctx = ssl.create_default_context()
with socket.create_connection((SUP_HOST, SUP_PORT), timeout=5) as raw:
    with ctx.wrap_socket(raw, server_hostname=SUP_HOST) as s:
        s.sendall((json.dumps(payload) + "\n").encode("utf-8"))
```

Há um *fallback* para TLS sem verificação de certificado caso o ambiente da sala não tenha as
CAs configuradas — assim o nó aparece no dashboard mesmo em rede restritiva.

---

## 6. Como executar

```bash
# Reporter ligado por padrão (envia a cada 10s)
python master.py --id Master_1 --supervisor-uuid michel_1

# Ajustar intervalo
python master.py --supervisor-interval 10

# Desligar o reporter (se você já tem outro arquivo enviando métricas)
python master.py --no-supervisor
```

Flags relacionadas à Sprint 4:

| Flag | Padrão | Descrição |
|---|---|---|
| `--supervisor-uuid` | `michel_1` | Valor do campo `server_uuid` |
| `--supervisor-hostname` | `<uuid>.farm.local` | Valor do campo `hostname` |
| `--supervisor-host` | `nuted-ia.dev` | Host do Supervisor |
| `--supervisor-port` | `443` | Porta TLS |
| `--supervisor-interval` | `10` | Segundos entre relatórios |
| `--no-supervisor` | — | Desativa o envio |

---

## 7. Como validar no Dashboard

1. Inicie o master com o `--supervisor-uuid` do seu grupo.
2. Suba os workers (mesma rede). Eles se registram via descoberta UDP.
3. Abra https://nuted-ia.dev/supervisor/dashboard/ no navegador.
4. Verifique:
   - O seu nó aparece em **MASTERS ATIVOS** e em **NÓS DA INFRAESTRUTURA**.
   - **TOTAL WORKERS** reflete os workers registrados (ex.: 3).
   - CPU/MEM/DISK atualizam a cada ~10s.
   - Ao emprestar/receber workers, **WORKERS EMPRESTADOS** e a aba de topologia mudam.

---

## 8. Definição de "Pronto" (DoD)

- [ ] O master abre conexão **TLS** com `nuted-ia.dev:443` e envia o `performance_report`.
- [ ] O envio ocorre a cada 10s, **sem `recv`**, encerrando a conexão após o `send`.
- [ ] O payload segue o schema (`server_uuid`, `hostname`, `role`, `performance.*`).
- [ ] O `server_uuid` usa o identificador atribuído pelo professor (único por grupo).
- [ ] O nó aparece no dashboard com workers, CPU/MEM/DISK e estado da fila corretos.
- [ ] Os contadores de workers cedidos/recebidos refletem o empréstimo (Sprint 3).
- [ ] O envio **não derruba** o master se o Supervisor estiver indisponível (try/except).

---

## 9. Pontos de atenção

- **`server_uuid` único por grupo** — coordene com a outra equipe para não colidir.
- **Apenas o master reporta.** Os workers aparecem dentro do `farm_state` do master; eles
  **não** abrem conexão própria com o Supervisor.
- **`psutil` recomendado** para métricas reais de CPU/memória/disco.
- **Não usar HTTP.** É TLS puro sobre TCP; qualquer cliente HTTP quebra o requisito.