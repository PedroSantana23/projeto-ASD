# Documentação do Projeto: P2P com Balanceamento de Carga Dinâmico

**Disciplina:** Arquitetura de Sistemas Distribuídos  
**Professor:** Michel Junio Ferreira Rosa — CEUB  
**Escopo:** Sprints 1, 2 e 3

---

## Visão Geral

O sistema é composto por dois tipos de nós que se comunicam exclusivamente via **TCP com mensagens JSON** delimitadas por `\n`:

| Nó | Arquivo | Porta padrão | Papel |
|----|---------|-------------|-------|
| **Master** | `master.py` | `8000` | Servidor — recebe Workers, gerencia fila de tarefas, negocia recursos |
| **Worker** | `worker.py` | `8001` | Cliente — envia heartbeat, solicita e processa tarefas |

> **Regra de ouro do protocolo:** toda mensagem JSON deve terminar com o caractere `\n`.  
> O receptor lê o socket até encontrar `\n` e então faz o parse do JSON completo.

---

## Convenções do Protocolo

### Case Sensitivity
Todos os valores de controle devem estar em **CAIXA ALTA**:

```
ALIVE · HEARTBEAT · QUERY · NO_TASK · OK · NOK · ACK
BORROW_REQUEST · BORROW_RESPONSE · REDIRECT · RESTORE
```

### Campos obrigatórios vs. opcionais

| Campo | Obrigatório em | Descrição |
|-------|---------------|-----------|
| `SERVER_UUID` | Heartbeat, Apresentação (emprestado) | Identifica o Master |
| `WORKER_UUID` | Apresentação, Reporte, ACK | Identifica o Worker |
| `TASK` | Toda mensagem | Tipo da operação |
| `STATUS` | Reporte e ACK | Estado da tarefa |
| `SERVER_UUID` (opcional) | Apresentação | Presente **somente** se o Worker for emprestado |

---

## Sprint 1 — Mecanismo de Heartbeat (TCP)

### Objetivo
Estabelecer a comunicação base entre Worker e Master, garantindo que o Worker consiga verificar periodicamente se seu Master está ativo.

### Fluxo

```
Worker                                    Master
  |                                          |
  |-- {"SERVER_UUID":"...","TASK":"HEARTBEAT"} \n -->|
  |                                          |
  |                              (parse JSON + valida TASK)
  |                                          |
  |<-- {"SERVER_UUID":"...","TASK":"HEARTBEAT","RESPONSE":"ALIVE"} \n --|
  |                                          |
  | Log: "Status: ALIVE"                     |
  |                                          |
  |       [aguarda HEARTBEAT_INTERVAL]       |
  |                                          |
  |        (repete o loop acima)             |
```

**Em caso de falha de conexão:**
```
Worker
  |
  | heartbeat_failures += 1
  | Log: "Status: OFFLINE - Tentando Reconectar (N/MAX_FAILURES)"
  |
  | [aguarda próximo ciclo]
```

### Payloads

**Worker → Master (requisição):**
```json
{
  "SERVER_UUID": "Master_10.62.206.207",
  "TASK": "HEARTBEAT"
}
```

**Master → Worker (resposta):**
```json
{
  "SERVER_UUID": "Master_10.62.206.207",
  "TASK": "HEARTBEAT",
  "RESPONSE": "ALIVE"
}
```

### Implementação (`worker.py`)

- Função `send_heartbeat()` — monta e envia o payload, lê a resposta
- Função `heartbeat_loop()` — executa em thread separada, dorme `HEARTBEAT_INTERVAL` (10s) entre cada ciclo
- O `SERVER_UUID` do Master é armazenado na primeira resposta bem-sucedida

### Implementação (`master.py`)

- Função `handle_heartbeat()` — detecta `"TASK": "HEARTBEAT"` e responde imediatamente com `"RESPONSE": "ALIVE"`
- Atendimento em thread separada por conexão — o heartbeat **nunca bloqueia** o processamento de tarefas

### Definição de "Pronto" (DoD)

- [x] Worker abre conexão TCP com o Master
- [x] Master recebe o JSON, faz o parse e identifica o comando `HEARTBEAT`
- [x] Worker recebe a confirmação `ALIVE` e registra no log
- [x] Falha de conexão é registrada como `OFFLINE` sem travar os processos

---

## Sprint 2 — Comunicação de Tarefas e Apresentação de Workers

### Objetivo
Implementar o fluxo completo do ciclo de vida de uma tarefa: da apresentação do Worker até a confirmação final (ACK) do Master.

### Fluxo Completo

```
Worker                                         Master
  |                                               |
  |  1. Apresentação                              |
  |-- {"WORKER":"ALIVE","WORKER_UUID":"W-123"} -->|
  |                                               |
  |                              (verifica fila de tarefas)
  |                                               |
  |  2a. [Fila com tarefa]                        |
  |<-- {"TASK":"QUERY","USER":"Alice"} -----------|
  |                                               |
  |  2b. [Fila vazia]                             |
  |<-- {"TASK":"NO_TASK"} -----------------------|
  |  (aguarda próximo ciclo)                      |
  |                                               |
  |  3. Simulação de processamento (sleep/cálculo)|
  |                                               |
  |  4. Reporte de Status                         |
  |-- {"STATUS":"OK","TASK":"QUERY",              |
  |    "WORKER_UUID":"W-123"} ---------------->   |
  |                                               |
  |                              (registra no log)
  |                                               |
  |  5. Confirmação Final (ACK)                   |
  |<-- {"STATUS":"ACK","WORKER_UUID":"W-123"} ----|
  |                                               |
  | (ciclo liberado — próxima apresentação)       |
```

### Payloads

**1. Apresentação — Worker Local:**
```json
{
  "WORKER": "ALIVE",
  "WORKER_UUID": "W-123"
}
```

**1. Apresentação — Worker Emprestado** (inclui `SERVER_UUID` do master original):
```json
{
  "WORKER": "ALIVE",
  "WORKER_UUID": "W-999",
  "SERVER_UUID": "Master_B"
}
```

**2. Entrega de Tarefa (Master → Worker):**
```json
{ "TASK": "QUERY", "USER": "Alice" }
```

**2. Sem Tarefa (Master → Worker):**
```json
{ "TASK": "NO_TASK" }
```

**3. Reporte de Status (Worker → Master):**
```json
{
  "STATUS": "OK",
  "TASK": "QUERY",
  "WORKER_UUID": "W-123"
}
```

**4. Confirmação ACK (Master → Worker):**
```json
{
  "STATUS": "ACK",
  "WORKER_UUID": "W-123"
}
```

### Cenários de Teste (CT)

| ID | Cenário | Ação do Worker | Resposta Esperada | Critério de Sucesso |
|----|---------|---------------|-------------------|---------------------|
| CT01 | Apresentação Worker Local | `{"WORKER":"ALIVE","WORKER_UUID":"W-123"}` | `{"TASK":"QUERY","USER":"Alice"}` | Master identifica Worker local e entrega tarefa |
| CT02 | Apresentação Worker Emprestado | `{"WORKER":"ALIVE","WORKER_UUID":"W-999","SERVER_UUID":"Master-B"}` | `{"TASK":"QUERY","USER":"Julia"}` | Master reconhece origem e atribui tarefa normalmente |
| CT03 | Fila de Tarefas Vazia | `{"WORKER":"ALIVE","WORKER_UUID":"W-123"}` | `{"TASK":"NO_TASK"}` | Master responde corretamente sem tarefa pendente |
| CT04 | Reporte de Sucesso | `{"STATUS":"OK","TASK":"QUERY","WORKER_UUID":"W-123"}` | `{"STATUS":"ACK"}` | Master processa o sucesso e libera o Worker com ACK |
| CT05 | Reporte de Falha | `{"STATUS":"NOK","TASK":"QUERY","WORKER_UUID":"W-123"}` | `{"STATUS":"ACK"}` | Master registra a falha, mas envia ACK para confirmar recebimento |

### Notas de Implementação

- **Timeout:** o Worker aguarda resposta do Master por no máximo **5 segundos** antes de considerar a conexão perdida
- **Strict Parsing:** campos obrigatórios ausentes são tratados como erro; campos desconhecidos são ignorados
- **Log do Master:** registra qual Worker (local ou emprestado) concluiu qual tarefa

### Definição de "Pronto" (DoD)

- [x] Worker realiza o handshake de apresentação com sucesso
- [x] Master distribui tarefa da fila ou informa `NO_TASK` corretamente
- [x] Worker processa a tarefa e o Master recebe `OK` ou `NOK`
- [x] Worker recebe o ACK final, fechando o ciclo sem perda de mensagens
- [x] Sistema trata corretamente a presença ou ausência de `SERVER_UUID` no payload

---

## Sprint 3 — Protocolo de Conversa Consensual (Balanceamento de Carga)

### Objetivo
Quando um Master atinge o limiar de saturação (`SATURATION_THRESHOLD`), ele negocia com Masters vizinhos o empréstimo de Workers, redistribuindo a carga dinamicamente — sem conhecimento prévio da implementação interna do vizinho.

### Fluxo de Negociação

```
Master A (saturado)                         Master B (vizinho)
       |                                          |
       | [fila >= SATURATION_THRESHOLD]           |
       |                                          |
       |  1. Pedido de empréstimo                 |
       |-- {"TASK":"BORROW_REQUEST",              |
       |    "FROM":"Master_A","QUANTITY":2} -->   |
       |                                          |
       |                        (avalia recursos disponíveis)
       |                                          |
       |  2. Resposta de aceite                   |
       |<-- {"TASK":"BORROW_RESPONSE",            |
       |     "FROM":"Master_B",                   |
       |     "ACCEPTED":true,"QUANTITY":2} -------|
       |                                          |
```

```
Master B                                      Worker B1
    |                                             |
    |  3. Instrução de redirecionamento           |
    |-- {"TASK":"REDIRECT",                       |
    |    "NEW_MASTER":"IP_A:8000",                |
    |    "NEW_SERVER_UUID":"Master_A"} -------->  |
    |                                             |
    |<-- {"STATUS":"ACK","WORKER_UUID":"..."} ----|
    |                                             |

Worker B1                                     Master A
    |                                             |
    |  4. Worker emprestado se apresenta          |
    |-- {"WORKER":"ALIVE",                        |
    |    "WORKER_UUID":"...",                     |
    |    "SERVER_UUID":"Master_B"} ------------>  |
    |                                             |
    |<-- {"TASK":"QUERY","USER":"..."} -----------|
```

### Payloads

**1. BORROW_REQUEST (Master A → Master B):**
```json
{
  "TASK": "BORROW_REQUEST",
  "FROM": "Master_10.62.206.207",
  "QUANTITY": 2
}
```

**2. BORROW_RESPONSE (Master B → Master A):**
```json
{
  "TASK": "BORROW_RESPONSE",
  "FROM": "Master_10.62.206.208",
  "ACCEPTED": true,
  "QUANTITY": 2
}
```

**3. REDIRECT (Master B → Worker):**
```json
{
  "TASK": "REDIRECT",
  "NEW_MASTER": "10.62.206.207:8000",
  "NEW_SERVER_UUID": "Master_10.62.206.207"
}
```

**4. RESTORE (Master B → Worker, ao recuperar o recurso):**
```json
{
  "TASK": "RESTORE"
}
```

**5. REDIRECT_WORKER (Master B → Master A, confirmando redirecionamento):**
```json
{
  "TASK": "REDIRECT_WORKER",
  "FROM": "Master_10.62.206.208",
  "WORKERS": ["uuid-worker-1", "uuid-worker-2"]
}
```

### Configuração Necessária

Em `master.py`, popule a lista `NEIGHBOR_MASTERS` com os endereços dos Masters vizinhos:

```python
NEIGHBOR_MASTERS = [
    ("10.62.206.208", 8000),
    ("10.62.206.209", 8000),
]
```

O limiar de saturação pode ser ajustado pela constante:

```python
SATURATION_THRESHOLD = 5  # número de tarefas na fila que dispara a negociação
```

### Definição de "Pronto" (DoD)

- [x] Master detecta a saturação ao atingir o threshold
- [x] Master saturado envia `BORROW_REQUEST` a pelo menos um vizinho
- [x] Master vizinho responde com `BORROW_RESPONSE` (aceite ou recusa)
- [x] Worker é redirecionado via `REDIRECT` e passa a se apresentar ao Master saturado com `SERVER_UUID` do master original
- [x] Master saturado registra os Workers emprestados em `borrowed_workers`
- [x] Sistemas de equipes diferentes interoperam exclusivamente pelo protocolo definido

---

## Arquitetura dos Arquivos

```
projeto/
├── master.py      # Nó Master — servidor TCP, fila de tarefas, consenso
└── worker.py      # Nó Worker — heartbeat, ciclo de tarefas, redirecionamento
```

### Threads em execução

**master.py**

| Thread | Função | Papel |
|--------|--------|-------|
| Principal | `start_server()` | Aceita conexões TCP e despacha handlers |
| Por conexão | `handle_client()` | Processa uma mensagem e encerra |
| Background | `simulate_incoming_tasks()` | Gera tarefas para a fila (simulação) |
| Background | `monitor_saturation()` | Verifica threshold e inicia negociação |

**worker.py**

| Thread | Função | Papel |
|--------|--------|-------|
| Background | `start_listener()` | Escuta instruções de redirecionamento |
| Background | `heartbeat_loop()` | Envia heartbeat periodicamente |
| Principal | `task_cycle()` | Loop de apresentação → tarefa → reporte |

---

## Como Executar

```bash
# Terminal 1 — iniciar o Master
python master.py

# Terminal 2 — iniciar o Worker
python worker.py
```

> Certifique-se de que o IP em `MASTER_HOST` no `worker.py` aponta para a máquina onde o `master.py` está rodando.

---