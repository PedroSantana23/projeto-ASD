"""
master.py — Sprint 2.1 + Sprint 1 + Sprint 2 + Sprint 3
Descoberta Dinâmica via UDP + Balanceamento de Carga P2P
"""

import socket
import threading
import json
import queue
import time
import uuid
import argparse

# ═══════════════════════════════════════════════════════════════════
# ARGUMENTOS DE LINHA DE COMANDO
# ═══════════════════════════════════════════════════════════════════

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()

parser = argparse.ArgumentParser(description="Master — P2P com Balanceamento de Carga")
parser.add_argument("--host",      type=str, default=get_local_ip(), help="IP do Master (padrão: IP local detectado)")
parser.add_argument("--port",      type=int, default=10000,           help="Porta TCP do Master (padrão: 10000)")
parser.add_argument("--id",        type=str, default="Master_2",     help="ID do Master (padrão: Master_2)")
parser.add_argument("--udp-port",  type=int, default=8000,            help="Porta UDP de descoberta (padrão: 8000)")

# Master vizinha que pode emprestar workers.
# Formato: ID:IP:PORTA
# Exemplo: --neighbor-master Master_1:10.62.206.218:10000
parser.add_argument(
    "--neighbor-master",
    action="append",
    default=None,
    help="Master vizinha no formato ID:IP:PORTA. Pode repetir o argumento para cadastrar mais de uma."
)
parser.add_argument(
    "--no-default-neighbor",
    action="store_true",
    help="Desativa a master vizinha padrão 10.62.206.218:10000."
)

args = parser.parse_args()

HOST      = args.host
PORT      = args.port
MASTER_ID = args.id

# Porta dedicada para descoberta UDP (igual em todos os nós)
UDP_DISCOVERY_PORT = args.udp_port

# Thresholds
CAPACITY          = 10
RELEASE_THRESHOLD = 5

# Vizinhos Masters (para Sprint 3)
# Por padrão, este master vai tentar pedir workers para a master TCP 10.62.206.218:10000.
DEFAULT_NEIGHBOR_MASTERS = [
    ("Master_1", "10.62.206.218", 10000),
]

def parse_neighbor_master(spec: str) -> tuple[str, str, int] | None:
    """
    Converte uma master vizinha no formato ID:IP:PORTA.
    Exemplo: Master_1:10.62.206.218:10000
    """
    try:
        neighbor_id, neighbor_ip, neighbor_port = spec.rsplit(":", 2)
        neighbor_id = neighbor_id.strip()
        neighbor_ip = neighbor_ip.strip()
        neighbor_port = int(neighbor_port.strip())

        if not neighbor_id or not neighbor_ip:
            raise ValueError("ID/IP vazio")

        return neighbor_id, neighbor_ip, neighbor_port
    except Exception as e:
        print(f"[CONFIG] Vizinho inválido '{spec}'. Use ID:IP:PORTA. Erro: {e}", flush=True)
        return None

def build_neighbor_masters() -> list[tuple[str, str, int]]:
    neighbors: list[tuple[str, str, int]] = []

    if not args.no_default_neighbor:
        neighbors.extend(DEFAULT_NEIGHBOR_MASTERS)

    if args.neighbor_master:
        for spec in args.neighbor_master:
            parsed = parse_neighbor_master(spec)
            if parsed:
                neighbors.append(parsed)

    # Evita tentar pedir worker para si mesmo.
    clean_neighbors: list[tuple[str, str, int]] = []
    seen = set()
    for neighbor_id, neighbor_ip, neighbor_port in neighbors:
        key = (neighbor_ip, neighbor_port)
        if key == (HOST, PORT):
            print(f"[CONFIG] Ignorando vizinho {neighbor_id} porque aponta para este próprio master.", flush=True)
            continue
        if key in seen:
            continue
        seen.add(key)
        clean_neighbors.append((neighbor_id, neighbor_ip, neighbor_port))

    return clean_neighbors

NEIGHBOR_MASTERS = build_neighbor_masters()

NEGOTIATION_TIMEOUT = 5

# ═══════════════════════════════════════════════════════════════════
# ESTADO GLOBAL
# ═══════════════════════════════════════════════════════════════════

state_lock       = threading.Lock()
task_queue       = queue.Queue()
workers_local    = {}
workers_borrowed = {}
load_counter     = 0
saturated        = False

# ═══════════════════════════════════════════════════════════════════
# UTILITÁRIOS
# ═══════════════════════════════════════════════════════════════════

def send_json(conn, data: dict):
    try:
        conn.sendall((json.dumps(data) + "\n").encode("utf-8"))
    except Exception as e:
        log("REDE", f"Erro ao enviar: {e}")

def recv_json(conn, timeout=None) -> dict | None:
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
    except (json.JSONDecodeError, OSError):
        return None
    finally:
        conn.settimeout(original_timeout)

def log(tag: str, msg: str, req_id: str = ""):
    ts      = time.strftime("%H:%M:%S")
    req_str = f"[req={req_id[:8]}]" if req_id else ""
    print(f"[{ts}][{MASTER_ID}][{tag}]{req_str} {msg}", flush=True)

def log_worker_state():
    with state_lock:
        locais      = len(workers_local)
        emprestados = len(workers_borrowed)
    log("WORKERS", f"Locais={locais} | Emprestados={emprestados} | Fila={task_queue.qsize()}")

def strict_parse(payload: dict, required_fields: list, context: str) -> bool:
    for field in required_fields:
        if field not in payload:
            log("PARSE_ERR", f"[{context}] Campo obrigatório ausente: '{field}'")
            return False
    return True

# ═══════════════════════════════════════════════════════════════════
# SPRINT 2.1 — DESCOBERTA UDP
# ═══════════════════════════════════════════════════════════════════

def iniciar_servidor_udp():
    """
    Escuta broadcasts/multicasts UDP de Workers buscando Masters.
    Responde com DISCOVERY_REPLY via UDP Unicast para o Worker.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", UDP_DISCOVERY_PORT))

    log("DISCOVERY", f"Escutando UDP na porta {UDP_DISCOVERY_PORT}")

    while True:
        try:
            data, addr = sock.recvfrom(1024)
            threading.Thread(
                target=tratar_discovery_udp,
                args=(sock, data, addr),
                daemon=True
            ).start()
        except Exception as e:
            log("DISCOVERY", f"Erro UDP: {e}")

def tratar_discovery_udp(sock, data: bytes, addr: tuple):
    """
    Processa pacote DISCOVERY do Worker.
    Responde via UDP Unicast com IP e porta TCP deste Master.
    Strict parsing: ignora campos desconhecidos, loga se obrigatório ausente.
    """
    try:
        raw = data.decode("utf-8").strip()
        payload = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        log("DISCOVERY", f"Payload malformado de {addr} — descartado.")
        return

    msg_type    = payload.get("TYPE", "")
    worker_uuid = payload.get("WORKER_UUID", "")

    if msg_type != "DISCOVERY":
        return

    if not worker_uuid:
        log("DISCOVERY", f"WORKER_UUID ausente em DISCOVERY de {addr} — descartado.")
        return

    log("DISCOVERY", f"DISCOVERY recebido de Worker={worker_uuid} | {addr}")

    response = {
        "TYPE":        "DISCOVERY_REPLY",
        "MASTER_NAME": MASTER_ID,
        "MASTER_IP":   HOST,
        "MASTER_PORT": PORT,
        "STATUS":      "AVAILABLE"
    }
    msg = (json.dumps(response) + "\n").encode("utf-8")
    sock.sendto(msg, addr)
    log("DISCOVERY", f"DISCOVERY_REPLY enviado para {addr} | master={MASTER_ID} ip={HOST}:{PORT}")

# ═══════════════════════════════════════════════════════════════════
# SPRINT 2.1 — ELECTION ACK (TCP)
# ═══════════════════════════════════════════════════════════════════

def handle_election_ack(conn, payload: dict) -> bool:
    """
    Worker conectou via TCP após eleição e confirma o Master eleito.
    Master valida e responde com ELECTION_ACK ACCEPTED.
    Retorna True se o Worker deve continuar com Heartbeat/tarefas.
    """
    if not strict_parse(payload, ["TYPE", "WORKER_UUID", "SELECTED_MASTER"], "ELECTION_ACK"):
        return False

    worker_uuid      = payload["WORKER_UUID"]
    selected_master  = payload["SELECTED_MASTER"]

    # Valida se o Worker elegeu este Master
    if selected_master != MASTER_ID:
        log("ELECTION",
            f"Worker {worker_uuid} elegeu '{selected_master}' mas conectou em '{MASTER_ID}'. "
            f"Rejeitando.")
        send_json(conn, {
            "TYPE":        "ELECTION_ACK",
            "STATUS":      "REJECTED",
            "MASTER_NAME": MASTER_ID
        })
        return False

    send_json(conn, {
        "TYPE":        "ELECTION_ACK",
        "STATUS":      "ACCEPTED",
        "MASTER_NAME": MASTER_ID
    })
    log("ELECTION", f"Worker {worker_uuid} conectado e aceito após eleição.")
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
    global load_counter, saturated
    while True:
        time.sleep(3)
        with state_lock:
            load_counter = task_queue.qsize()

        log("CARGA",
            f"Pendentes={load_counter} | Sat>{CAPACITY} | Lib<{RELEASE_THRESHOLD} | "
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
    if not strict_parse(payload, ["SERVER_UUID", "TASK"], "HEARTBEAT"):
        return
    send_json(conn, {
        "SERVER_UUID": MASTER_ID,
        "TASK":        "HEARTBEAT",
        "RESPONSE":    "ALIVE"
    })
    log("HEARTBEAT", f"ALIVE → {payload['SERVER_UUID']}")

# ═══════════════════════════════════════════════════════════════════
# SPRINT 2 — CICLO DE TAREFAS
# ═══════════════════════════════════════════════════════════════════

def handle_worker_alive(conn, payload: dict):
    if not strict_parse(payload, ["WORKER", "WORKER_UUID"], "WORKER_ALIVE"):
        return

    worker_uuid     = payload["WORKER_UUID"]
    original_master = payload.get("SERVER_UUID")

    with state_lock:
        if original_master:
            workers_borrowed[worker_uuid] = {"conn": conn, "original_master": original_master}
            log("WORKER", f"Emprestado REGISTRADO: {worker_uuid} | origem={original_master}")
        else:
            workers_local[worker_uuid] = {"conn": conn, "busy": False}
            log("WORKER", f"Local REGISTRADO: {worker_uuid}")

    log_worker_state()

    if not task_queue.empty():
        user = task_queue.get()
        send_json(conn, {"TASK": "QUERY", "USER": user})
        log("TASK", f"QUERY USER={user} → {worker_uuid} ({'emprestado' if original_master else 'local'})")
    else:
        send_json(conn, {"TASK": "NO_TASK"})
        log("TASK", f"NO_TASK → {worker_uuid}")

def handle_status(conn, payload: dict):
    if not strict_parse(payload, ["STATUS", "TASK", "WORKER_UUID"], "STATUS"):
        return

    worker_uuid = payload["WORKER_UUID"]
    status      = payload["STATUS"].upper()

    if status not in ("OK", "NOK"):
        log("PARSE_ERR", f"STATUS inválido: '{status}'")
        return

    origem = "EMPRESTADO" if worker_uuid in workers_borrowed else "LOCAL"
    log("STATUS", f"{worker_uuid} ({origem}) → {status}")

    send_json(conn, {"STATUS": "ACK", "WORKER_UUID": worker_uuid})
    log("ACK", f"ACK → {worker_uuid}")

# ═══════════════════════════════════════════════════════════════════
# SPRINT 3 — NEGOCIAÇÃO MASTER-TO-MASTER
# ═══════════════════════════════════════════════════════════════════

def solicitar_ajuda():
    if not NEIGHBOR_MASTERS:
        log("NEG", "Nenhum master vizinho configurado para pedir workers.")
        return

    with state_lock:
        workers_needed = max(1, (load_counter - CAPACITY) // 2 + 1)

    log("NEG", f"Buscando {workers_needed} worker(s) em {len(NEIGHBOR_MASTERS)} master(s) vizinha(s).")

    for (neighbor_id, neighbor_ip, neighbor_port) in NEIGHBOR_MASTERS:
        if (neighbor_ip, neighbor_port) == (HOST, PORT):
            log("NEG", f"Ignorando {neighbor_id}, pois aponta para este próprio master.")
            continue

        if pedir_ao_vizinho(neighbor_id, neighbor_ip, neighbor_port, workers_needed):
            return

    log("NEG", "Nenhum vizinho conseguiu emprestar workers.")

def pedir_ao_vizinho(neighbor_id, neighbor_ip, neighbor_port, workers_needed) -> bool:
    request_id = str(uuid.uuid4())
    log("REQUEST_HELP", f"→ {neighbor_id} ({neighbor_ip}:{neighbor_port})", req_id=request_id)
    try:
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.settimeout(NEGOTIATION_TIMEOUT)
        conn.connect((neighbor_ip, neighbor_port))
    except Exception as e:
        log("REQUEST_HELP", f"Falha ao conectar com {neighbor_id}: {e}", req_id=request_id)
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

    if response is None:
        log("REQUEST_HELP", f"Timeout. request_id descartado. Tentando próximo.", req_id=request_id)
        return False

    r_type   = response.get("type", "")
    r_req_id = response.get("request_id", "")
    r_payload = response.get("payload", {})

    if r_req_id != request_id:
        log("REQUEST_HELP", f"request_id divergente. Ignorando.", req_id=request_id)
        return False

    if r_type == "response_accepted":
        log("RESPONSE", f"✔ {neighbor_id} aceitou: {r_payload.get('workers_offered')} worker(s)", req_id=request_id)
        return True
    elif r_type == "response_rejected":
        log("RESPONSE", f"✘ {neighbor_id} recusou: {r_payload.get('reason')}", req_id=request_id)
        return False

    return False

def handle_request_help(conn, payload_outer: dict):
    request_id    = payload_outer.get("request_id", str(uuid.uuid4()))
    p             = payload_outer.get("payload", {})
    if not strict_parse(p, ["master_id", "master_address", "workers_needed"], "request_help"):
        return

    requester_id  = p["master_id"]
    requester_addr = p["master_address"]
    needed        = int(p["workers_needed"])

    log("REQUEST_HELP", f"← {requester_id} pediu {needed} worker(s)", req_id=request_id)

    with state_lock:
        my_load      = load_counter
        idle_workers = list(workers_local.keys())
        can_offer    = max(0, len(idle_workers) - 1)

    if my_load >= CAPACITY:
        reason = "high_load"
    elif can_offer == 0:
        reason = "no_workers_available"
    else:
        reason = None

    if reason:
        send_json(conn, {"type": "response_rejected", "request_id": request_id, "payload": {"reason": reason}})
        log("RESPONSE", f"Rejeitando {requester_id}: {reason}", req_id=request_id)
        return

    to_offer = min(needed, can_offer)
    selected = idle_workers[:to_offer]
    details  = [{"id": wid, "address": f"{HOST}:{PORT}"} for wid in selected]

    send_json(conn, {
        "type": "response_accepted",
        "request_id": request_id,
        "payload": {"workers_offered": to_offer, "worker_details": details}
    })
    log("RESPONSE", f"✔ Aceitando {requester_id}: {to_offer} worker(s)", req_id=request_id)

    for wid in selected:
        threading.Thread(target=enviar_command_redirect, args=(wid, requester_addr), daemon=True).start()

def enviar_command_redirect(worker_uuid: str, new_master_address: str):
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
        return

    req_id_red = str(uuid.uuid4())
    send_json(info["conn"], {
        "type": "command_redirect",
        "request_id": req_id_red,
        "payload": {"new_master_address": new_master_address}
    })
    log("REDIRECT", f"command_redirect → {worker_uuid} | {new_master_address}", req_id=req_id_red)
    with state_lock:
        workers_local.pop(worker_uuid, None)
    log_worker_state()

def handle_register_temporary_worker(conn, payload_outer: dict):
    request_id = payload_outer.get("request_id", "?")
    p          = payload_outer.get("payload", {})
    if not strict_parse(p, ["worker_id", "original_master_address"], "register_temporary_worker"):
        return
    worker_id = p["worker_id"]
    origin    = p["original_master_address"]
    with state_lock:
        workers_borrowed[worker_id] = {"conn": conn, "original_master": origin}
    log("CICLO_VIDA", f"[INÍCIO] Worker emprestado {worker_id} | origem={origin}", req_id=request_id)
    log_worker_state()

def devolver_todos_workers():
    with state_lock:
        to_return = dict(workers_borrowed)
    if not to_return:
        return
    log("DEVOLUÇÃO", f"Devolvendo {len(to_return)} worker(s).")
    for worker_id, info in to_return.items():
        enviar_command_release(worker_id, info["conn"], info["original_master"])

def enviar_command_release(worker_id, conn, original_master_address):
    req_id_rel = str(uuid.uuid4())
    send_json(conn, {
        "type": "command_release",
        "request_id": req_id_rel,
        "payload": {"original_master_address": original_master_address}
    })
    log("RELEASE", f"command_release → {worker_id} | retornar a {original_master_address}", req_id=req_id_rel)
    with state_lock:
        workers_borrowed.pop(worker_id, None)
    log("CICLO_VIDA", f"[FIM] Worker emprestado {worker_id} devolvido a {original_master_address}")
    log_worker_state()
    threading.Thread(target=notify_worker_returned, args=(worker_id, original_master_address), daemon=True).start()

def notify_worker_returned(worker_id, original_master_address):
    req_id_ntf = str(uuid.uuid4())
    try:
        ip, port = original_master_address.rsplit(":", 1)
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.settimeout(NEGOTIATION_TIMEOUT)
        conn.connect((ip, int(port)))
        send_json(conn, {
            "type": "notify_worker_returned",
            "request_id": req_id_ntf,
            "payload": {"worker_id": worker_id}
        })
        log("NOTIFY", f"notify_worker_returned → {original_master_address} | worker={worker_id}", req_id=req_id_ntf)
        conn.close()
    except Exception as e:
        log("NOTIFY", f"Falha: {e}")

def handle_notify_worker_returned(payload_outer: dict):
    worker_id = payload_outer.get("payload", {}).get("worker_id", "?")
    log("NOTIFY", f"Worker {worker_id} devolvido. Aguardando reconexão.")

# ═══════════════════════════════════════════════════════════════════
# DISPATCHER
# ═══════════════════════════════════════════════════════════════════

def dispatch(conn, payload: dict):
    # Sprint 2.1 — Election ACK (primeira mensagem TCP após eleição)
    if payload.get("TYPE") == "ELECTION_ACK" and "SELECTED_MASTER" in payload:
        handle_election_ack(conn, payload)
        return

    # Sprint 1/2
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

    # Sprint 3
    msg_type = payload.get("type", "").lower()
    handlers_s3 = {
        "request_help":               lambda: handle_request_help(conn, payload),
        "register_temporary_worker":  lambda: handle_register_temporary_worker(conn, payload),
        "notify_worker_returned":     lambda: handle_notify_worker_returned(payload),
    }

    if msg_type in handlers_s3:
        handlers_s3[msg_type]()
    elif msg_type:
        log("AVISO", f"Tipo desconhecido '{msg_type}' — ignorado.")
    else:
        log("AVISO", f"Mensagem sem tipo reconhecido — ignorada: {payload}")

# ═══════════════════════════════════════════════════════════════════
# THREAD DE CLIENTE TCP
# ═══════════════════════════════════════════════════════════════════

def handle_client(conn, addr):
    log("CONEXÃO", f"Nova conexão TCP de {addr}")
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
        _limpar_worker_desconectado(conn)
        conn.close()

def _limpar_worker_desconectado(conn):
    with state_lock:
        for wid, info in list(workers_local.items()):
            if info.get("conn") is conn:
                workers_local.pop(wid)
                log("CLEANUP", f"Worker local {wid} removido por desconexão.")
                break
        for wid, info in list(workers_borrowed.items()):
            if info.get("conn") is conn:
                workers_borrowed.pop(wid)
                log("CLEANUP", f"Worker emprestado {wid} removido por desconexão.")
                break

# ═══════════════════════════════════════════════════════════════════
# SERVIDOR TCP
# ═══════════════════════════════════════════════════════════════════

def iniciar_servidor_tcp():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen()
    log("MASTER", f"Servidor TCP ouvindo em {HOST}:{PORT}")
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

    # Thread UDP — descoberta de workers
    threading.Thread(target=iniciar_servidor_udp, daemon=True).start()

    # Thread de monitoramento de carga
    threading.Thread(target=monitor_load, daemon=True).start()

    # Servidor TCP principal (bloqueante)
    iniciar_servidor_tcp()