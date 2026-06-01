"""
master.py — Sistema P2P com Balanceamento de Carga Dinâmico
Sprint 1 + Sprint 2 + Sprint 3 completos
"""

import socket
import threading
import json
import queue
import time
import uuid

# ═══════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES — ajuste antes de rodar
# ═══════════════════════════════════════════════════════════════════

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()

HOST      = "192.168.1.97"
PORT      = 8001
MASTER_ID = "Master-B"
NEIGHBOR_MASTERS = [("Master-A", "192.168.1.97", 8000)]

# Thresholds (histerese obrigatória da spec)
CAPACITY          = 10   # acima disso: saturado → pede ajuda
RELEASE_THRESHOLD = 5    # abaixo disso: devolve workers emprestados

# Vizinhos conhecidos: lista de (master_id, ip, porta)
# Ex: [("Master-B", "192.168.1.50", 8000)]
NEIGHBOR_MASTERS = []

NEGOTIATION_TIMEOUT = 5   # spec: aguarda 5s antes de considerar indisponível

# ═══════════════════════════════════════════════════════════════════
# ESTADO GLOBAL (protegido por locks)
# ═══════════════════════════════════════════════════════════════════

state_lock       = threading.Lock()
task_queue       = queue.Queue()

# {worker_uuid: {"conn": conn, "addr": addr, "busy": bool}}
workers_local    = {}

# {worker_uuid: {"conn": conn, "original_master": "ip:port"}}
workers_borrowed = {}

load_counter     = 0
saturated        = False

# ═══════════════════════════════════════════════════════════════════
# UTILITÁRIOS
# ═══════════════════════════════════════════════════════════════════

def send_json(conn, data: dict):
    """Envia dict como JSON terminado com \\n (obrigatório pela spec)."""
    try:
        conn.sendall((json.dumps(data) + "\n").encode("utf-8"))
    except Exception as e:
        log("REDE", f"Erro ao enviar: {e}")

def recv_json(conn, timeout=None) -> dict | None:
    """
    Recebe dados acumulando buffer até encontrar \\n.
    Garante que mensagens fragmentadas em TCP não se percam.
    """
    buf = b""
    original_timeout = conn.gettimeout()
    if timeout is not None:
        conn.settimeout(timeout)
    try:
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                return None
            buf += chunk
            if b"\n" in buf:
                line, _ = buf.split(b"\n", 1)
                return json.loads(line.decode("utf-8"))
    except socket.timeout:
        return None
    except json.JSONDecodeError as e:
        log("PARSE", f"JSON inválido: {e} | raw={buf[:200]}")
        return None
    except OSError:
        return None
    finally:
        conn.settimeout(original_timeout)

def log(tag: str, msg: str, req_id: str = ""):
    """Log com timestamp, MASTER_ID, tag e request_id opcional."""
    ts      = time.strftime("%H:%M:%S")
    req_str = f"[req={req_id[:8]}]" if req_id else ""
    print(f"[{ts}][{MASTER_ID}][{tag}]{req_str} {msg}", flush=True)

def log_worker_state():
    """Exibe contador de workers a cada mudança (requisito de observabilidade)."""
    with state_lock:
        locais     = len(workers_local)
        emprestados = len(workers_borrowed)
    log("WORKERS", f"Locais={locais} | Emprestados={emprestados} | Fila={task_queue.qsize()}")

def strict_parse(payload: dict, required_fields: list, context: str) -> bool:
    """
    Strict parsing: loga erro e retorna False se campo obrigatório ausente.
    Campos desconhecidos são ignorados silenciosamente (compatibilidade futura).
    """
    for field in required_fields:
        if field not in payload:
            log("PARSE_ERR", f"[{context}] Campo obrigatório ausente: '{field}' | payload={payload}")
            return False
    return True

# ═══════════════════════════════════════════════════════════════════
# SIMULAÇÃO DE CARGA
# ═══════════════════════════════════════════════════════════════════

def populate_queue(n=20):
    users = ["Alice", "Bob", "Carlos", "Diana", "Eduardo",
             "Fernanda", "Gabriel", "Helena", "Igor", "Julia"]
    for i in range(n):
        task_queue.put(users[i % len(users)])
    log("FILA", f"{task_queue.qsize()} tarefas adicionadas.")

def monitor_load():
    """
    Monitora carga continuamente.
    Dispara pedido de ajuda ou devolução com histerese.
    """
    global load_counter, saturated
    while True:
        time.sleep(3)
        with state_lock:
            load_counter = task_queue.qsize()

        log("CARGA",
            f"Pendentes={load_counter} | Saturação>{CAPACITY} | Liberação<{RELEASE_THRESHOLD} | "
            f"Locais={len(workers_local)} | Emprestados={len(workers_borrowed)}")

        if load_counter > CAPACITY and not saturated:
            with state_lock:
                saturated = True
            log("SATURAÇÃO", f"Threshold atingido ({load_counter}>{CAPACITY}). Iniciando negociação.")
            threading.Thread(target=solicitar_ajuda, daemon=True).start()

        elif load_counter <= RELEASE_THRESHOLD and saturated:
            with state_lock:
                saturated = False
            log("LIBERAÇÃO", f"Carga normalizada ({load_counter}<={RELEASE_THRESHOLD}). Devolvendo workers.")
            threading.Thread(target=devolver_todos_workers, daemon=True).start()

# ═══════════════════════════════════════════════════════════════════
# SPRINT 1 — HEARTBEAT
# ═══════════════════════════════════════════════════════════════════

def handle_heartbeat(conn, payload: dict):
    """Worker verifica se o Master está ativo."""
    if not strict_parse(payload, ["SERVER_UUID", "TASK"], "HEARTBEAT"):
        return
    send_json(conn, {
        "SERVER_UUID": MASTER_ID,
        "TASK": "HEARTBEAT",
        "RESPONSE": "ALIVE"
    })
    log("HEARTBEAT", f"ALIVE → {payload['SERVER_UUID']}")

# ═══════════════════════════════════════════════════════════════════
# SPRINT 2 — CICLO DE TAREFAS (Worker → Master)
# ═══════════════════════════════════════════════════════════════════

def handle_worker_alive(conn, payload: dict):
    """
    Worker se apresenta pedindo tarefa.
    Campo SERVER_UUID presente = worker emprestado.
    """
    if not strict_parse(payload, ["WORKER", "WORKER_UUID"], "WORKER_ALIVE"):
        return

    worker_uuid     = payload["WORKER_UUID"]
    original_master = payload.get("SERVER_UUID")  # opcional — só emprestados

    with state_lock:
        if original_master:
            workers_borrowed[worker_uuid] = {"conn": conn, "original_master": original_master}
            log("WORKER", f"Emprestado REGISTRADO: {worker_uuid} | origem={original_master}")
        else:
            workers_local[worker_uuid] = {"conn": conn, "busy": False}
            log("WORKER", f"Local REGISTRADO: {worker_uuid}")

    log_worker_state()

    # Distribui tarefa ou informa que não há
    if not task_queue.empty():
        user = task_queue.get()
        send_json(conn, {"TASK": "QUERY", "USER": user})
        log("TASK", f"QUERY USER={user} → {worker_uuid} ({'emprestado' if original_master else 'local'})")
    else:
        send_json(conn, {"TASK": "NO_TASK"})
        log("TASK", f"NO_TASK → {worker_uuid}")

def handle_status(conn, payload: dict):
    """
    Recebe resultado de tarefa (OK ou NOK) e envia ACK.
    Spec: ACK deve ser enviado mesmo para NOK.
    """
    if not strict_parse(payload, ["STATUS", "TASK", "WORKER_UUID"], "STATUS"):
        return

    worker_uuid = payload["WORKER_UUID"]
    status      = payload["STATUS"].upper()

    if status not in ("OK", "NOK"):
        log("PARSE_ERR", f"STATUS inválido: '{status}' de {worker_uuid}")
        return

    origem = "EMPRESTADO" if worker_uuid in workers_borrowed else "LOCAL"
    log("STATUS", f"{worker_uuid} ({origem}) → {status} | tarefa concluída")

    # ACK obrigatório mesmo para NOK (spec CT05)
    send_json(conn, {"STATUS": "ACK", "WORKER_UUID": worker_uuid})
    log("ACK", f"ACK → {worker_uuid}")

# ═══════════════════════════════════════════════════════════════════
# SPRINT 3A — ESTE MASTER SATURADO: solicita workers
# ═══════════════════════════════════════════════════════════════════

def solicitar_ajuda():
    """
    Tenta cada vizinho em ordem até conseguir ajuda ou esgotar a lista.
    Cada tentativa tem timeout de 5s (spec).
    """
    with state_lock:
        workers_needed = max(1, (load_counter - CAPACITY) // 2 + 1)

    log("NEG", f"Buscando {workers_needed} worker(s) emprestado(s).")

    for (neighbor_id, neighbor_ip, neighbor_port) in NEIGHBOR_MASTERS:
        sucesso = pedir_ao_vizinho(neighbor_id, neighbor_ip, neighbor_port, workers_needed)
        if sucesso:
            log("NEG", f"Ajuda obtida de {neighbor_id}.")
            return

    log("NEG", "Nenhum vizinho disponível para ajudar.")

def pedir_ao_vizinho(neighbor_id: str, neighbor_ip: str, neighbor_port: int, workers_needed: int) -> bool:
    """
    Abre conexão TCP com vizinho, envia request_help com UUID v4,
    aguarda response_accepted ou response_rejected (timeout 5s).
    Valida correlação de request_id.
    """
    request_id = str(uuid.uuid4())
    log("REQUEST_HELP", f"→ {neighbor_id} ({neighbor_ip}:{neighbor_port})", req_id=request_id)

    try:
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.settimeout(NEGOTIATION_TIMEOUT)
        conn.connect((neighbor_ip, neighbor_port))
    except Exception as e:
        log("REQUEST_HELP", f"Falha ao conectar com {neighbor_id}: {e} | descartando req", req_id=request_id)
        return False

    try:
        send_json(conn, {
            "type": "request_help",
            "request_id": request_id,
            "payload": {
                "master_id":      MASTER_ID,
                "master_address": f"{HOST}:{PORT}",
                "current_load":   load_counter,
                "capacity":       CAPACITY,
                "workers_needed": workers_needed
            }
        })

        response = recv_json(conn, timeout=NEGOTIATION_TIMEOUT)
    finally:
        conn.close()

    # Timeout — spec CT07: descarta request_id, loga e tenta próximo vizinho
    if response is None:
        log("REQUEST_HELP",
            f"Timeout aguardando {neighbor_id}. request_id descartado. Tentando próximo vizinho.",
            req_id=request_id)
        return False

    r_type    = response.get("type", "")
    r_req_id  = response.get("request_id", "")
    r_payload = response.get("payload", {})

    # Valida correlação de request_id (spec CT03)
    if r_req_id != request_id:
        log("REQUEST_HELP",
            f"request_id divergente! esperado={request_id[:8]} recebido={r_req_id[:8]}. Ignorando.",
            req_id=request_id)
        return False

    if r_type == "response_accepted":
        offered = r_payload.get("workers_offered", 0)
        details = r_payload.get("worker_details", [])
        log("RESPONSE", f"✔ {neighbor_id} aceitou: {offered} worker(s) {details}", req_id=request_id)
        return True

    elif r_type == "response_rejected":
        reason = r_payload.get("reason", "?")
        log("RESPONSE", f"✘ {neighbor_id} recusou: reason={reason}", req_id=request_id)
        return False

    log("RESPONSE", f"Tipo desconhecido '{r_type}' de {neighbor_id}. Ignorado.", req_id=request_id)
    return False

# ═══════════════════════════════════════════════════════════════════
# SPRINT 3B — ESTE MASTER COMO OFERTANTE: responde ao vizinho
# ═══════════════════════════════════════════════════════════════════

def handle_request_help(conn, payload_outer: dict):
    """
    Recebe request_help de vizinho saturado.
    Avalia carga + workers ociosos e responde accepted ou rejected.
    Mantém o mesmo request_id na resposta (spec CT03).
    """
    request_id     = payload_outer.get("request_id", str(uuid.uuid4()))
    p              = payload_outer.get("payload", {})

    if not strict_parse(p, ["master_id", "master_address", "workers_needed"], "request_help"):
        return

    requester_id   = p["master_id"]
    requester_addr = p["master_address"]
    needed         = int(p["workers_needed"])

    log("REQUEST_HELP", f"← {requester_id} pediu {needed} worker(s)", req_id=request_id)

    with state_lock:
        my_load      = load_counter
        idle_workers = list(workers_local.keys())
        can_offer    = max(0, len(idle_workers) - 1)  # mantém pelo menos 1 local

    # Decide motivo de rejeição
    if my_load >= CAPACITY:
        reason = "high_load"
    elif can_offer == 0:
        reason = "no_workers_available"
    else:
        reason = None

    if reason:
        send_json(conn, {
            "type":       "response_rejected",
            "request_id": request_id,
            "payload":    {"reason": reason}
        })
        log("RESPONSE", f"Rejeitando {requester_id}: {reason}", req_id=request_id)
        return

    # Aceita e seleciona workers
    to_offer = min(needed, can_offer)
    selected = idle_workers[:to_offer]

    details = [{"id": wid, "address": f"{HOST}:{PORT}"} for wid in selected]

    send_json(conn, {
        "type":       "response_accepted",
        "request_id": request_id,
        "payload": {
            "workers_offered": to_offer,
            "worker_details":  details
        }
    })
    log("RESPONSE",
        f"✔ Aceitando {requester_id}: {to_offer} worker(s) → {[d['id'] for d in details]}",
        req_id=request_id)

    # Envia command_redirect para cada worker selecionado (em threads paralelas)
    for wid in selected:
        threading.Thread(
            target=enviar_command_redirect,
            args=(wid, requester_addr),
            daemon=True
        ).start()

def enviar_command_redirect(worker_uuid: str, new_master_address: str):
    """
    Instrui um worker local a se reconectar ao Master saturado.
    Spec: worker deve finalizar tarefa em execução antes de desconectar
    (aguardamos até busy=False com timeout).
    """
    # Aguarda worker terminar tarefa atual (máx 10s)
    deadline = time.time() + 10
    while time.time() < deadline:
        with state_lock:
            info = workers_local.get(worker_uuid)
            if info and not info.get("busy", False):
                break
        time.sleep(0.2)

    with state_lock:
        info = workers_local.get(worker_uuid)

    if not info:
        log("REDIRECT", f"Worker {worker_uuid} não encontrado.")
        return

    conn       = info["conn"]
    req_id_red = str(uuid.uuid4())

    send_json(conn, {
        "type":       "command_redirect",
        "request_id": req_id_red,
        "payload":    {"new_master_address": new_master_address}
    })
    log("REDIRECT",
        f"command_redirect → {worker_uuid} | novo master={new_master_address}",
        req_id=req_id_red)

    with state_lock:
        workers_local.pop(worker_uuid, None)

    log_worker_state()

# ═══════════════════════════════════════════════════════════════════
# SPRINT 3C — REGISTRO DE WORKER TEMPORÁRIO
# ═══════════════════════════════════════════════════════════════════

def handle_register_temporary_worker(conn, payload_outer: dict):
    """
    Worker emprestado chegou e se registra neste Master.
    Ciclo de vida completo é logado (requisito de observabilidade).
    A partir daqui o worker opera pelo protocolo Sprint 2 com SERVER_UUID.
    """
    request_id = payload_outer.get("request_id", "?")
    p          = payload_outer.get("payload", {})

    if not strict_parse(p, ["worker_id", "original_master_address"], "register_temporary_worker"):
        return

    worker_id = p["worker_id"]
    origin    = p["original_master_address"]

    with state_lock:
        workers_borrowed[worker_id] = {"conn": conn, "original_master": origin}

    log("CICLO_VIDA",
        f"[INÍCIO] Worker emprestado {worker_id} registrado | origem={origin}",
        req_id=request_id)
    log_worker_state()

# ═══════════════════════════════════════════════════════════════════
# SPRINT 3D — DEVOLUÇÃO DOS WORKERS
# ═══════════════════════════════════════════════════════════════════

def devolver_todos_workers():
    """Carga normalizou: devolve todos os workers emprestados."""
    with state_lock:
        to_return = dict(workers_borrowed)

    if not to_return:
        return

    log("DEVOLUÇÃO", f"Devolvendo {len(to_return)} worker(s).")

    for worker_id, info in to_return.items():
        conn   = info["conn"]
        origin = info["original_master"]
        enviar_command_release(worker_id, conn, origin)

def enviar_command_release(worker_id: str, conn, original_master_address: str):
    """Instrui worker emprestado a voltar ao Master de origem."""
    req_id_rel = str(uuid.uuid4())

    send_json(conn, {
        "type":       "command_release",
        "request_id": req_id_rel,
        "payload":    {"original_master_address": original_master_address}
    })
    log("RELEASE",
        f"command_release → {worker_id} | retornar a {original_master_address}",
        req_id=req_id_rel)

    with state_lock:
        workers_borrowed.pop(worker_id, None)

    log("CICLO_VIDA", f"[FIM] Worker emprestado {worker_id} devolvido a {original_master_address}")
    log_worker_state()

    # Notifica Master de origem em paralelo
    threading.Thread(
        target=notify_worker_returned,
        args=(worker_id, original_master_address),
        daemon=True
    ).start()

def notify_worker_returned(worker_id: str, original_master_address: str):
    """Envia notify_worker_returned ao Master de origem."""
    req_id_ntf = str(uuid.uuid4())
    try:
        ip, port = original_master_address.rsplit(":", 1)
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.settimeout(NEGOTIATION_TIMEOUT)
        conn.connect((ip, int(port)))

        send_json(conn, {
            "type":       "notify_worker_returned",
            "request_id": req_id_ntf,
            "payload":    {"worker_id": worker_id}
        })
        log("NOTIFY",
            f"notify_worker_returned → {original_master_address} | worker={worker_id}",
            req_id=req_id_ntf)
        conn.close()
    except Exception as e:
        log("NOTIFY", f"Falha ao notificar devolução de {worker_id}: {e}")

def handle_notify_worker_returned(payload_outer: dict):
    """Recebe notificação de que worker emprestado foi devolvido."""
    p         = payload_outer.get("payload", {})
    worker_id = p.get("worker_id", "?")
    req_id    = payload_outer.get("request_id", "")
    log("NOTIFY",
        f"Worker {worker_id} devolvido. Aguardando reconexão via Sprint 2.",
        req_id=req_id)

# ═══════════════════════════════════════════════════════════════════
# DISPATCHER CENTRAL
# ═══════════════════════════════════════════════════════════════════

def dispatch(conn, payload: dict):
    """
    Roteia mensagem para o handler correto.
    Campos desconhecidos são ignorados (spec: strict parsing sem derrubar processo).
    """
    # Sprint 1/2: identificados por campos em CAIXA ALTA
    task         = payload.get("TASK", "").upper()
    worker_field = payload.get("WORKER", "").upper()
    status_field = payload.get("STATUS", "").upper()

    if task == "HEARTBEAT":
        handle_heartbeat(conn, payload)
        return
    if worker_field == "ALIVE":
        handle_worker_alive(conn, payload)
        return
    if status_field in ("OK", "NOK"):
        handle_status(conn, payload)
        return

    # Sprint 3: identificados pelo campo "type" em minúsculas
    msg_type = payload.get("type", "").lower()

    handlers_s3 = {
        "request_help":               lambda: handle_request_help(conn, payload),
        "register_temporary_worker":  lambda: handle_register_temporary_worker(conn, payload),
        "notify_worker_returned":     lambda: handle_notify_worker_returned(payload),
    }

    if msg_type in handlers_s3:
        handlers_s3[msg_type]()
    elif msg_type:
        # Spec: tipo desconhecido → loga e ignora sem derrubar (CT09)
        log("AVISO", f"Tipo desconhecido '{msg_type}' — ignorado silenciosamente.")
    else:
        log("AVISO", f"Mensagem sem tipo reconhecido — ignorada: {payload}")

# ═══════════════════════════════════════════════════════════════════
# THREAD DE ATENDIMENTO DE CLIENTE
# ═══════════════════════════════════════════════════════════════════

def handle_client(conn, addr):
    """
    Mantém conexão persistente com um cliente (worker ou master vizinho).
    Loop infinito lendo mensagens até desconexão.
    """
    log("CONEXÃO", f"Nova conexão de {addr}")
    try:
        while True:
            payload = recv_json(conn)
            if payload is None:
                log("CONEXÃO", f"{addr} desconectou.")
                break
            log("RECEBIDO", f"[{addr}] {payload}")
            dispatch(conn, payload)
    except Exception as e:
        log("ERRO", f"[{addr}] {e}")
    finally:
        # Spec CT08: se worker emprestado perder conexão com este master,
        # remove do registro para que o master de origem possa recuperá-lo
        _limpar_worker_desconectado(conn)
        conn.close()

def _limpar_worker_desconectado(conn):
    """Remove worker do estado ao desconectar inesperadamente."""
    with state_lock:
        # Verifica workers locais
        for wid, info in list(workers_local.items()):
            if info.get("conn") is conn:
                workers_local.pop(wid)
                log("CLEANUP", f"Worker local {wid} removido por desconexão.")
                break
        # Verifica workers emprestados
        for wid, info in list(workers_borrowed.items()):
            if info.get("conn") is conn:
                workers_borrowed.pop(wid)
                log("CLEANUP",
                    f"Worker emprestado {wid} removido por desconexão. "
                    f"Ele tentará reconectar ao master original.")
                break

# ═══════════════════════════════════════════════════════════════════
# SERVIDOR PRINCIPAL
# ═══════════════════════════════════════════════════════════════════

def iniciar_servidor():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen()
    log("MASTER", f"Ouvindo em {HOST}:{PORT}")

    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    populate_queue(20)
    log("MASTER", f"Iniciando {MASTER_ID} | {HOST}:{PORT}")
    log("MASTER", f"Saturação>{CAPACITY} | Liberação<{RELEASE_THRESHOLD}")
    log("MASTER", f"Vizinhos: {NEIGHBOR_MASTERS if NEIGHBOR_MASTERS else 'nenhum configurado'}")

    threading.Thread(target=monitor_load, daemon=True).start()
    iniciar_servidor()
