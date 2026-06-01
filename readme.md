# P2P com Balanceamento de Carga Dinâmico

**Disciplina:** Arquitetura de Sistemas Distribuídos  
**Professor:** Michel Junio Ferreira Rosa  

---

## Visão Geral

Sistema distribuído autônomo que implementa balanceamento de carga horizontal via arquitetura P2P. Cada nó **Master** gerencia um conjunto de nós **Worker** (uma *Farm*). Quando um Master atinge seu limiar de saturação, ele negocia dinamicamente o empréstimo de Workers de um Master vizinho através de um protocolo de consenso.

---

## Arquitetura

```
Máquina A                          Máquina B
┌─────────────────────┐            ┌─────────────────────┐
│  Master-A           │◄──────────►│  Master-B           │
│  (master.py)        │  TCP P2P   │  (master.py)        │
│                     │            │                      │
│  Worker-A1          │            │  Worker-B1          │
│  (worker.py)        │            │  (worker.py)        │
└─────────────────────┘            └─────────────────────┘
```

Quando Master-A satura, ele pede Workers emprestados ao Master-B. O Worker-B1 se reconecta temporariamente ao Master-A e volta ao Master-B quando a carga normaliza.

---

## Pré-requisitos

- Python 3.10 ou superior (usa sintaxe `dict | None`)
- Sem dependências externas — apenas bibliotecas padrão do Python
- As duas máquinas devem estar na **mesma rede**

---

## Estrutura de Arquivos

```
projeto-ASD/
├── Master-A/
│   ├── master.py       ← Master da Máquina A
│   └── worker.py       ← Worker da Máquina A
│
└── Master-B/
    ├── master.py       ← Master da Máquina B
    └── worker.py       ← Worker da Máquina B
```

> Os arquivos `master.py` e `worker.py` são os mesmos em ambas as máquinas.
> Apenas as variáveis de configuração no topo de cada arquivo são diferentes.

---

## Configuração

### Passo 1 — Descubra os IPs das máquinas

Em cada máquina, abra o terminal e rode:

```bash
# Windows
ipconfig

# Linux / Mac
hostname -I

# Ou via Python (mesmo método usado pelo sistema)
python -c "import socket; s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); s.connect(('8.8.8.8',80)); print(s.getsockname()[0]); s.close()"
```

### Passo 2 — Configure a Máquina A

Abra `Master-A/master.py` e edite as linhas:

```python
MASTER_ID        = "Master-A"
NEIGHBOR_MASTERS = [("Master-B", "IP_DA_MAQUINA_B", 8000)]
```

Abra `Master-A/worker.py` e edite:

```python
MASTER_HOST = "IP_DA_MAQUINA_A"
```

### Passo 3 — Configure a Máquina B

Abra `Master-B/master.py` e edite:

```python
MASTER_ID        = "Master-B"
NEIGHBOR_MASTERS = [("Master-A", "IP_DA_MAQUINA_A", 8000)]
```

Abra `Master-B/worker.py` e edite:

```python
MASTER_HOST = "IP_DA_MAQUINA_B"
```

### Exemplo com IPs reais

| Máquina | IP            |
|---------|---------------|
| A       | 192.168.1.97  |
| B       | 172.20.80.1   |

**Master-A/master.py:**
```python
MASTER_ID        = "Master-A"
NEIGHBOR_MASTERS = [("Master-B", "172.20.80.1", 8000)]
```

**Master-A/worker.py:**
```python
MASTER_HOST = "192.168.1.97"
```

**Master-B/master.py:**
```python
MASTER_ID        = "Master-B"
NEIGHBOR_MASTERS = [("Master-A", "192.168.1.97", 8000)]
```

**Master-B/worker.py:**
```python
MASTER_HOST = "172.20.80.1"
```

---

## Como Rodar

> **Importante:** sempre suba o `master.py` **antes** do `worker.py`.

### Máquina A — abra 2 terminais

```bash
# Terminal 1 — Master
cd Master-A
python master.py

# Terminal 2 — Worker
cd Master-A
python worker.py
```

### Máquina B — abra 2 terminais

```bash
# Terminal 1 — Master
cd Master-B
python master.py

# Terminal 2 — Worker
cd Master-B
python worker.py
```

---

## Parâmetros Configuráveis

Todas as variáveis ficam no topo de cada arquivo:

### master.py

| Variável            | Padrão | Descrição                                              |
|---------------------|--------|--------------------------------------------------------|
| `MASTER_ID`         | `"Master-A"` | Identificador único deste Master                 |
| `PORT`              | `8000` | Porta TCP em que o Master escuta                       |
| `CAPACITY`          | `10`   | Nº de tarefas pendentes que define saturação           |
| `RELEASE_THRESHOLD` | `5`    | Carga abaixo da qual devolve workers emprestados       |
| `NEIGHBOR_MASTERS`  | `[]`   | Lista de Masters vizinhos `(id, ip, porta)`            |
| `NEGOTIATION_TIMEOUT` | `5`  | Segundos para aguardar resposta na negociação          |

### worker.py

| Variável              | Padrão        | Descrição                                        |
|-----------------------|---------------|--------------------------------------------------|
| `MASTER_HOST`         | `"127.0.0.1"` | IP do Master ao qual este Worker se conecta      |
| `MASTER_PORT`         | `8000`        | Porta do Master                                  |
| `HEARTBEAT_INTERVAL`  | `30`          | Segundos entre heartbeats                        |
| `TASK_INTERVAL`       | `3`           | Segundos entre ciclos de pedido de tarefa        |
| `TIMEOUT`             | `5`           | Segundos para aguardar resposta do Master        |
| `ORIGINAL_MASTER_UUID`| `None`        | Preencher apenas se o worker já nasce emprestado |

---

## Protocolo de Comunicação

Todas as mensagens são objetos JSON terminados com `\n` trafegando via TCP.  
Valores de controle são sempre em **CAIXA ALTA** (Sprint 1/2) ou **minúsculas** (Sprint 3).

### Sprint 1 — Heartbeat

**Worker → Master**
```json
{"SERVER_UUID": "Worker-abc123", "TASK": "HEARTBEAT"}
```

**Master → Worker**
```json
{"SERVER_UUID": "Master-A", "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
```

---

### Sprint 2 — Ciclo de Tarefas

**1. Worker se apresenta (local)**
```json
{"WORKER": "ALIVE", "WORKER_UUID": "Worker-abc123"}
```

**1b. Worker se apresenta (emprestado)**
```json
{"WORKER": "ALIVE", "WORKER_UUID": "Worker-abc123", "SERVER_UUID": "Master-B"}
```

**2a. Master entrega tarefa**
```json
{"TASK": "QUERY", "USER": "Alice"}
```

**2b. Master sem tarefa**
```json
{"TASK": "NO_TASK"}
```

**3. Worker reporta resultado**
```json
{"STATUS": "OK", "TASK": "QUERY", "WORKER_UUID": "Worker-abc123"}
```

**4. Master confirma (ACK)**
```json
{"STATUS": "ACK", "WORKER_UUID": "Worker-abc123"}
```

> O ACK é enviado tanto para `OK` quanto para `NOK`.

---

### Sprint 3 — Negociação Master-to-Master

Todas as mensagens seguem a estrutura genérica:
```json
{
  "type": "tipo_da_mensagem",
  "request_id": "uuid-v4-único",
  "payload": { }
}
```

**3.1 — Master-A pede ajuda ao Master-B**
```json
{
  "type": "request_help",
  "request_id": "a1b2c3d4-...",
  "payload": {
    "master_id": "Master-A",
    "master_address": "192.168.1.97:8000",
    "current_load": 15,
    "capacity": 10,
    "workers_needed": 2
  }
}
```

**3.2a — Master-B aceita**
```json
{
  "type": "response_accepted",
  "request_id": "a1b2c3d4-...",
  "payload": {
    "workers_offered": 2,
    "worker_details": [
      {"id": "Worker-x1", "address": "172.20.80.1:8000"},
      {"id": "Worker-x2", "address": "172.20.80.1:8000"}
    ]
  }
}
```

**3.2b — Master-B recusa**
```json
{
  "type": "response_rejected",
  "request_id": "a1b2c3d4-...",
  "payload": {"reason": "high_load"}
}
```

> Motivos possíveis: `high_load`, `no_workers_available`, `refused`

**3.3 — Master-B redireciona Worker**
```json
{
  "type": "command_redirect",
  "request_id": "f0e9d8c7-...",
  "payload": {"new_master_address": "192.168.1.97:8000"}
}
```

**3.4 — Worker se registra no Master-A**
```json
{
  "type": "register_temporary_worker",
  "request_id": "c1b2a3d4-...",
  "payload": {
    "worker_id": "Worker-x1",
    "original_master_address": "172.20.80.1:8000"
  }
}
```

**3.5 — Master-A devolve Worker**
```json
{
  "type": "command_release",
  "request_id": "z9y8x7w6-...",
  "payload": {"original_master_address": "172.20.80.1:8000"}
}
```

**3.6 — Master-A notifica Master-B**
```json
{
  "type": "notify_worker_returned",
  "request_id": "m1n2b3v4-...",
  "payload": {"worker_id": "Worker-x1"}
}
```

---

## Fluxo Completo da Sprint 3

```
Master-A (saturado)      Master-B (vizinho)       Worker-B1
       |                        |                      |
       |---- request_help ----->|                      |
       |                        |                      |
       |<--- response_accepted--|                      |
       |                        |                      |
       |                        |-- command_redirect -->|
       |                        |                      |
       |<========= nova conexão TCP ==================|
       |                                               |
       |<-- register_temporary_worker -----------------|
       |                                               |
       |     ... ciclo Sprint 2 com SERVER_UUID ...    |
       |                                               |
       |--- command_release --------------------------->|
       |                        |                      |
       |--- notify_worker_returned -->|                |
       |                        |                      |
       |                        |<===== reconexão ====|
       |                        |                      |
       |                        | ... Sprint 2 normal  |
```

---

## Casos de Teste

| ID    | Cenário                        | Como testar                                                         | Resultado esperado                                      |
|-------|--------------------------------|---------------------------------------------------------------------|---------------------------------------------------------|
| CT01  | Heartbeat ativo                | Rodar master e worker na mesma máquina                              | Worker loga `Status: ALIVE` a cada 30s                  |
| CT02  | Heartbeat master offline       | Derrubar o master e observar o worker                               | Worker loga `Status: OFFLINE - Tentando Reconectar`     |
| CT03  | Tarefa distribuída             | Rodar master com fila cheia e um worker                             | Worker recebe QUERY, processa e recebe ACK               |
| CT04  | Fila vazia                     | Rodar master sem tarefas na fila                                    | Worker recebe NO_TASK                                   |
| CT05  | Reporte NOK recebe ACK         | Forçar exceção no `processar_tarefa`                                | Master envia ACK mesmo para STATUS=NOK                  |
| CT06  | Pedido de ajuda aceito         | Saturar Master-A (aumentar `CAPACITY` para valor baixo)             | Master-B responde `response_accepted` e redireciona worker |
| CT07  | Pedido de ajuda recusado       | Saturar também o Master-B antes de saturar o Master-A               | Master-B responde `response_rejected: high_load`        |
| CT08  | Timeout de negociação          | Derrubar Master-B e aguardar Master-A saturar                       | Master-A loga timeout e descarta request_id             |
| CT09  | Worker emprestado executa tarefa | Observar logs após CT06                                           | Worker loga `emprestado` e Master-A loga tarefa concluída por worker emprestado |
| CT10  | Devolução do worker            | Aguardar fila do Master-A esvaziar abaixo do `RELEASE_THRESHOLD`    | Master-A envia `command_release` e `notify_worker_returned` |
| CT11  | Worker volta ao master original | Continuação do CT10                                               | Worker reconecta ao Master-B e retoma ciclo normal      |
| CT12  | Queda do master durante empréstimo | Derrubar Master-A enquanto Worker-B está emprestado            | Worker detecta queda e tenta voltar ao Master-B         |
| CT13  | Tipo desconhecido              | Enviar JSON com `"type": "mensagem_invalida"` ao master             | Master loga aviso e continua operando normalmente       |

---

## Regras do Protocolo

1. **Delimitador:** toda mensagem JSON termina obrigatoriamente com `\n`
2. **Case Sensitivity:** valores Sprint 1/2 em CAIXA ALTA (`ALIVE`, `QUERY`, `OK`, `NOK`, `ACK`); tipos Sprint 3 em minúsculas (`request_help`, `response_accepted`, etc.)
3. **Strict Parsing:** campos obrigatórios ausentes geram log de erro sem derrubar o processo; campos desconhecidos são ignorados silenciosamente
4. **request_id:** UUID v4 gerado a cada nova requisição; a resposta reutiliza o mesmo request_id da requisição original
5. **Timeout:** 5 segundos para aguardar resposta; após timeout, o request_id é descartado e o próximo vizinho é tentado
6. **Histerese:** threshold de liberação menor que o de saturação evita o efeito "ping-pong" de empréstimo e devolução imediatos

---

## Observabilidade — Lendo os Logs

Todos os logs seguem o formato:
```
[HH:MM:SS][MASTER_ID ou WORKER_UUID][TAG][req=uuid] mensagem
```

Tags importantes:

| Tag          | Significado                                      |
|--------------|--------------------------------------------------|
| `HEARTBEAT`  | Verificação de atividade                         |
| `WORKER`     | Registro de worker (local ou emprestado)         |
| `TASK`       | Distribuição de tarefa                           |
| `STATUS`     | Resultado recebido do worker                     |
| `ACK`        | Confirmação enviada ao worker                    |
| `CARGA`      | Monitor de carga (exibido a cada 3s)             |
| `SATURAÇÃO`  | Threshold de saturação atingido                  |
| `LIBERAÇÃO`  | Carga normalizada, devolvendo workers            |
| `REQUEST_HELP` | Negociação entre masters                       |
| `RESPONSE`   | Resposta à negociação (accepted/rejected)        |
| `REDIRECT`   | command_redirect enviado ao worker               |
| `REGISTER`   | register_temporary_worker                        |
| `RELEASE`    | command_release                                  |
| `NOTIFY`     | notify_worker_returned                           |
| `CICLO_VIDA` | Início e fim do ciclo de vida de worker emprestado |
| `WORKERS`    | Contador de workers locais e emprestados         |
| `CLEANUP`    | Remoção de worker desconectado inesperadamente   |