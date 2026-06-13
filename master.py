"""
master.py — VERSÃO FINAL (Sprints 1, 2, 2.1, 3 e 4)
P2P com Balanceamento de Carga Dinâmico + Descoberta UDP + Supervisor (Dashboard)

CENÁRIO DE USO (apresentação):
  - Este master aparece no dashboard do professor (Sprint 4, via TLS).
  - Seus workers (1 nesta máquina + 2 em outros PCs) descobrem este master por UDP
    e aparecem no dashboard dentro do farm_state.
  - Se o master de OUTRO grupo pedir workers (request_help), este master EMPRESTA.

EXEMPLO DE EXECUÇÃO:
  python master.py --id MASTER_2 \
      --supervisor-uuid michel_1 \
      --neighbor-master Master_2:<IP_DO_OUTRO_GRUPO>:10000

  (use o server_uuid que o PROFESSOR atribuiu ao seu grupo em --supervisor-uuid;
   o outro grupo usa um diferente, ex.: michel_2)
"""

import socket
import threading
import json
import queue
import time
import uuid
import argparse
import os
import ssl

try:
    import psutil
except ImportError:
    psutil = None

# ═══════════════════════════════════════════════════════════════════
# ARGUMENTOS
# ═══════════════════════════════════════════════════════════════════

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()

parser = argparse.ArgumentParser(description="Master — P2P com Balanceamento de Carga + Supervisor")
parser.add_argument("--host",      type=str, default=get_local_ip(), help="IP TCP deste master (padrão: IP local da LAN)")
parser.add_argument("--port",      type=int, default=10000,          help="Porta TCP do master (padrão: 10000)")
parser.add_argument("--id",        type=str, default="MASTER_2",     help="Nome/ID do master (ex.: MASTER_2)")
parser.add_argument("--udp-port",  type=int, default=5000,           help="Porta UDP de descoberta (padrão: 5000, IGUAL ao worker)")

parser.add_argument("--capacity",       type=int, default=10, help="Threshold de saturação (padrão: 10)")
parser.add_argument("--release",        type=int, default=5,  help="Threshold de liberação/histerese (padrão: 5)")
parser.add_argument("--initial-tasks",  type=int, default=6,  help="Tarefas iniciais na fila (padrão: 6, baixo p/ poder emprestar)")
parser.add_argument("--task-interval",  type=int, default=4,  help="Segundos entre geração de novas tarefas (0 = desliga)")

parser.add_argument("--neighbor-master", action="append", default=None,
                    help="Master vizinho no formato ID:IP:PORTA (pode repetir)")
parser.add_argument("--no-default-neighbor", action="store_true",
                    help="Desativa o vizinho padrão embutido")

# ── Sprint 4: Supervisor / Dashboard ──
parser.add_argument("--supervisor-uuid",     type=str, default="michel_1",
                    help="server_uuid no payload do supervisor (use o que o professor atribuiu!)")
parser.add_argument("--supervisor-hostname", type=str, default=None,
                    help="hostname no payload (padrão: <uuid>.farm.local)")
parser.add_argument("--supervisor-host",     type=str, default="nuted-ia.dev")
parser.add_argument("--supervisor-port",     type=int, default=443)
parser.add_argument("--supervisor-interval", type=int, default=10, help="Segundos entre relatórios (padrão: 10)")
parser.add_argument("--no-supervisor", action="store_true",
                    help="Não enviar métricas ao supervisor (use se já tiver outro reporter)")

args = parser.parse_args()

HOST      = args.host
PORT      = args.port
MASTER_ID = args.id
UDP_DISCOVERY_PORT = args.udp_port

CAPACITY          = args.capacity
RELEASE_THRESHOLD = args.release
TASK_INTERVAL_GEN = args.task_interval
WORKER_TTL        = 30
NEGOTIATION_TIMEOUT = 5

SUP_UUID     = args.supervisor_uuid
SUP_HOSTNAME = args.supervisor_hostname or f"{SUP_UUID}.farm.local"
SUP_HOST     = args.supervisor_host
SUP_PORT     = args.supervisor_port
SUP_INTERVAL = args.supervisor_interval
SUP_ENABLED  = not args.no_supervisor

START_TIME = time.time()

DEFAULT_NEIGHBOR_MASTERS = [
    ("MASTER_2", "10.62.206.218", 10000),
]

def parse_neighbor_master(spec: str):
    try:
        nid, nip, nport = spec.rsplit(":", 2)
        nid, nip, nport = nid.strip(), nip.strip(), int(nport.strip())
        if not nid or not nip:
            raise ValueError("ID/IP vazio")
        return nid, nip, nport
    except Exception as e:
        print(f"[CONFIG] Vizinho inválido '{spec}'. Use ID:IP:PORTA. Erro: {e}", flush=True)
        return None

def build_neighbor_masters():
    neighbors = []
    if not args.no_default_neighbor:
        neighbors.extend(DEFAULT_NEIGHBOR_MASTERS)
    if args.neighbor_master:
        for spec in args.neighbor_master:
            p = parse_neighbor_master(spec)
            if p:
                neighbors.append(p)
    clean, seen = [], set()
    for nid, nip, nport in neighbors:
        key = (nip, nport)
        if key == (HOST, PORT) or key in seen:
            continue
        seen.add(key)
        clean.append((nid, nip, nport))
    return clean

NEIGHBOR_MASTERS = build_neighbor_masters()

# ═══════════════════════════════════════════════════════════════════
# ESTADO GLOBAL
# ═══════════════════════════════════════════════════════════════════

state_lock          = threading.Lock()
task_queue          = queue.Queue()

workers_local       = {}   # uuid -> {"conn","busy"}  (conexão atual, efêmera)
workers_borrowed    = {}   # uuid -> {"conn","origin_name"}  (recebidos de outro master)
known_local_workers = {}   # uuid -> last_seen  (registro persistente p/ ofertar/contar)
pending_redirects   = {}   # uuid -> endereço do master solicitante
pending_releases    = {}   # uuid -> (marcado p/ devolução)
lent_out            = {}   # uuid -> endereço do master que pegou emprestado
lent_to_peer        = {}   # uuid -> nome (master_id) de quem pegou emprestado
borrowed_origin_addr= {}   # uuid -> endereço de origem (do register_temporary_worker)

load_counter        = 0
saturated           = False

tasks_completed     = 0
tasks_failed        = 0
tasks_running       = 0

# ═══════════════════════════════════════════════════════════════════
# UTILITÁRIOS
# ═══════════════════════════════════════════════════════════════════

def send_json(conn, data: dict):
    try:
        conn.sendall((json.dumps(data) + "\n").encode("utf-8"))
    except Exception as e:
        log("REDE", f"Erro ao enviar: {e}")

def recv_json(conn, timeout=None):
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
        log("WORKERS",
            f"Locais-ativos={len(workers_local)} | Conhecidos={len(known_local_workers)} | "
            f"Cedidos={len(lent_out)} | Recebidos={len(workers_borrowed)} | Fila={task_queue.qsize()}")

def strict_parse(payload, required_fields, context) -> bool:
    for field in required_fields:
        if field not in payload:
            log("PARSE_ERR", f"[{context}] Campo obrigatório ausente: '{field}'")
            return False
    return True

def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def resolver_endereco_vizinho(master_id, conn):
    """
    Descobre o endereço (ip:porta) do master solicitante.
    A spec do request_help NÃO traz master_address, então resolvemos pelo
    diretório de vizinhos (por master_id) e, em último caso, pelo IP da conexão.
    """
    for (nid, nip, nport) in NEIGHBOR_MASTERS:
        if nid == master_id:
            return f"{nip}:{nport}"
    try:
        peer_ip = conn.getpeername()[0]
        return f"{peer_ip}:{PORT}"   # assume mesma porta TCP dos demais masters
    except Exception:
        return None

# ═══════════════════════════════════════════════════════════════════
# SPRINT 2.1 — DESCOBERTA UDP
# ═══════════════════════════════════════════════════════════════════

def iniciar_servidor_udp():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", UDP_DISCOVERY_PORT))
    log("DISCOVERY", f"Escutando UDP na porta {UDP_DISCOVERY_PORT}")
    while True:
        try:
            data, addr = sock.recvfrom(1024)
            threading.Thread(target=tratar_discovery_udp, args=(sock, data, addr), daemon=True).start()
        except Exception as e:
            log("DISCOVERY", f"Erro UDP: {e}")

def tratar_discovery_udp(sock, data, addr):
    try:
        payload = json.loads(data.decode("utf-8").strip())
    except (json.JSONDecodeError, UnicodeDecodeError):
        log("DISCOVERY", f"Payload malformado de {addr} — descartado.")
        return
    if payload.get("TYPE") != "DISCOVERY":
        return
    worker_uuid = payload.get("WORKER_UUID", "")
    if not worker_uuid:
        log("DISCOVERY", f"WORKER_UUID ausente de {addr} — descartado.")
        return
    log("DISCOVERY", f"DISCOVERY de Worker={worker_uuid} | {addr}")
    resp = {"TYPE": "DISCOVERY_REPLY", "MASTER_NAME": MASTER_ID,
            "MASTER_IP": HOST, "MASTER_PORT": PORT, "STATUS": "AVAILABLE"}
    sock.sendto((json.dumps(resp) + "\n").encode("utf-8"), addr)
    log("DISCOVERY", f"DISCOVERY_REPLY → {addr} | {MASTER_ID} {HOST}:{PORT}")

# ═══════════════════════════════════════════════════════════════════
# SPRINT 2.1 — ELECTION ACK
# ═══════════════════════════════════════════════════════════════════

def handle_election_ack(conn, payload):
    if not strict_parse(payload, ["TYPE", "WORKER_UUID", "SELECTED_MASTER"], "ELECTION_ACK"):
        return
    if payload["SELECTED_MASTER"] != MASTER_ID:
        log("ELECTION", f"Worker {payload['WORKER_UUID']} elegeu '{payload['SELECTED_MASTER']}' — rejeitando.")
        send_json(conn, {"TYPE": "ELECTION_ACK", "STATUS": "REJECTED", "MASTER_NAME": MASTER_ID})
        return
    send_json(conn, {"TYPE": "ELECTION_ACK", "STATUS": "ACCEPTED", "MASTER_NAME": MASTER_ID})
    log("ELECTION", f"Worker {payload['WORKER_UUID']} aceito após eleição.")

# ═══════════════════════════════════════════════════════════════════
# CARGA: geração + monitor de saturação
# ═══════════════════════════════════════════════════════════════════

USERS = ["Alice", "Bob", "Carlos", "Diana", "Eduardo", "Fernanda", "Gabriel", "Helena", "Igor", "Julia"]

def populate_queue(n):
    for i in range(n):
        task_queue.put(USERS[i % len(USERS)])
    log("FILA", f"{task_queue.qsize()} tarefas iniciais.")

def gerador_de_carga():
    """Mantém atividade leve sem saturar, para os workers terem o que fazer."""
    if TASK_INTERVAL_GEN <= 0:
        return
    i = 0
    while True:
        time.sleep(TASK_INTERVAL_GEN)
        if task_queue.qsize() < max(1, CAPACITY - 2):
            task_queue.put(USERS[i % len(USERS)])
            i += 1

def monitor_load():
    global load_counter, saturated
    while True:
        time.sleep(3)
        now = time.time()
        with state_lock:
            load_counter = task_queue.qsize()
            for w in [w for w, ts in known_local_workers.items() if now - ts > WORKER_TTL]:
                known_local_workers.pop(w, None)
                pending_redirects.pop(w, None)
                lent_out.pop(w, None)
                lent_to_peer.pop(w, None)

        log("CARGA", f"Pendentes={load_counter} | Sat>{CAPACITY} | Lib<{RELEASE_THRESHOLD} | "
                     f"Conhecidos={len(known_local_workers)} | Cedidos={len(lent_out)} | Recebidos={len(workers_borrowed)}")

        if load_counter > CAPACITY and not saturated:
            with state_lock:
                saturated = True
            log("SATURAÇÃO", f"{load_counter}>{CAPACITY}. Pedindo ajuda.")
            threading.Thread(target=solicitar_ajuda, daemon=True).start()
        elif load_counter <= RELEASE_THRESHOLD and saturated:
            with state_lock:
                saturated = False
            log("LIBERAÇÃO", f"{load_counter}<={RELEASE_THRESHOLD}. Devolvendo emprestados.")
            devolver_todos_workers()

# ═══════════════════════════════════════════════════════════════════
# SPRINT 1 — HEARTBEAT
# ═══════════════════════════════════════════════════════════════════

def handle_heartbeat(conn, payload):
    if not strict_parse(payload, ["SERVER_UUID", "TASK"], "HEARTBEAT"):
        return
    send_json(conn, {"SERVER_UUID": MASTER_ID, "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"})
    log("HEARTBEAT", f"ALIVE → {payload['SERVER_UUID']}")

# ═══════════════════════════════════════════════════════════════════
# SPRINT 2 + 3 — CICLO DE TAREFAS / COMANDOS VIA RESPOSTA AO ALIVE
# ═══════════════════════════════════════════════════════════════════

def _entregar_tarefa(conn, worker_uuid, emprestado):
    global tasks_running
    if not task_queue.empty():
        user = task_queue.get()
        with state_lock:
            tasks_running += 1
        send_json(conn, {"TASK": "QUERY", "USER": user})
        log("TASK", f"QUERY USER={user} → {worker_uuid} ({'emprestado' if emprestado else 'local'})")
    else:
        send_json(conn, {"TASK": "NO_TASK"})
        log("TASK", f"NO_TASK → {worker_uuid}")

def handle_worker_alive(conn, payload):
    if not strict_parse(payload, ["WORKER", "WORKER_UUID"], "WORKER_ALIVE"):
        return
    worker_uuid     = payload["WORKER_UUID"]
    origin_name     = payload.get("SERVER_UUID")   # presente => worker emprestado (nome do master de origem)

    # ── Worker EMPRESTADO (chegou de outro master) ──
    if origin_name:
        with state_lock:
            workers_borrowed[worker_uuid] = {"conn": conn, "origin_name": origin_name}
            release = pending_releases.pop(worker_uuid, None) is not None
            addr = borrowed_origin_addr.get(worker_uuid) or (origin_name if ":" in str(origin_name) else "")
        log("WORKER", f"Emprestado presente: {worker_uuid} | origem={origin_name}")
        log_worker_state()

        if release:
            send_json(conn, {"type": "command_release", "request_id": str(uuid.uuid4()),
                             "payload": {"original_master_address": addr}})
            log("RELEASE", f"command_release → {worker_uuid} | retornar a {addr or '(origem do worker)'}")
            with state_lock:
                workers_borrowed.pop(worker_uuid, None)
                borrowed_origin_addr.pop(worker_uuid, None)
            log("CICLO_VIDA", f"[FIM] Worker emprestado {worker_uuid} devolvido.")
            log_worker_state()
            if addr:
                threading.Thread(target=notify_worker_returned, args=(worker_uuid, addr), daemon=True).start()
            return

        _entregar_tarefa(conn, worker_uuid, emprestado=True)
        return

    # ── Worker LOCAL ──
    with state_lock:
        known_local_workers[worker_uuid] = time.time()
        workers_local[worker_uuid] = {"conn": conn, "busy": False}
        lent_out.pop(worker_uuid, None)
        lent_to_peer.pop(worker_uuid, None)
        redirect_target = pending_redirects.pop(worker_uuid, None)
        if redirect_target:
            lent_out[worker_uuid] = redirect_target

    if redirect_target:
        send_json(conn, {"type": "command_redirect", "request_id": str(uuid.uuid4()),
                         "payload": {"new_master_address": redirect_target}})
        log("REDIRECT", f"command_redirect → {worker_uuid} | novo master={redirect_target}")
        with state_lock:
            workers_local.pop(worker_uuid, None)
            known_local_workers.pop(worker_uuid, None)
        log("CICLO_VIDA", f"[EMPRÉSTIMO] Worker {worker_uuid} cedido a {redirect_target}")
        log_worker_state()
        return

    log("WORKER", f"Local presente: {worker_uuid}")
    log_worker_state()
    _entregar_tarefa(conn, worker_uuid, emprestado=False)

def handle_status(conn, payload):
    global tasks_completed, tasks_failed, tasks_running
    if not strict_parse(payload, ["STATUS", "TASK", "WORKER_UUID"], "STATUS"):
        return
    worker_uuid = payload["WORKER_UUID"]
    status      = payload["STATUS"].upper()
    if status not in ("OK", "NOK"):
        log("PARSE_ERR", f"STATUS inválido: '{status}'")
        return
    with state_lock:
        tasks_running = max(0, tasks_running - 1)
        if status == "OK":
            tasks_completed += 1
        else:
            tasks_failed += 1
    origem = "EMPRESTADO" if worker_uuid in workers_borrowed else "LOCAL"
    log("STATUS", f"{worker_uuid} ({origem}) → {status}")
    send_json(conn, {"STATUS": "ACK", "WORKER_UUID": worker_uuid})
    log("ACK", f"ACK → {worker_uuid}")

# ═══════════════════════════════════════════════════════════════════
# SPRINT 3 — NEGOCIAÇÃO (SOLICITANTE)
# ═══════════════════════════════════════════════════════════════════

def solicitar_ajuda():
    if not NEIGHBOR_MASTERS:
        log("NEG", "Nenhum vizinho configurado.")
        return
    with state_lock:
        workers_needed = max(1, (load_counter - CAPACITY) // 2 + 1)
    log("NEG", f"Buscando {workers_needed} worker(s) em {len(NEIGHBOR_MASTERS)} vizinho(s).")
    for (nid, nip, nport) in NEIGHBOR_MASTERS:
        if (nip, nport) == (HOST, PORT):
            continue
        if pedir_ao_vizinho(nid, nip, nport, workers_needed):
            return
    log("NEG", "Nenhum vizinho emprestou workers.")

def pedir_ao_vizinho(nid, nip, nport, workers_needed) -> bool:
    request_id = str(uuid.uuid4())
    log("REQUEST_HELP", f"→ {nid} ({nip}:{nport})", req_id=request_id)
    try:
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.settimeout(NEGOTIATION_TIMEOUT)
        conn.connect((nip, nport))
    except Exception as e:
        log("REQUEST_HELP", f"Falha ao conectar com {nid}: {e}", req_id=request_id)
        return False
    try:
        send_json(conn, {"type": "request_help", "request_id": request_id,
                         "payload": {"master_id": MASTER_ID, "master_address": f"{HOST}:{PORT}",
                                     "current_load": load_counter, "capacity": CAPACITY,
                                     "workers_needed": workers_needed}})
        response = recv_json(conn, timeout=NEGOTIATION_TIMEOUT)
    finally:
        conn.close()
    if response is None:
        log("REQUEST_HELP", "Timeout. Tentando próximo.", req_id=request_id)
        return False
    if response.get("request_id") != request_id:
        log("REQUEST_HELP", "request_id divergente. Ignorando.", req_id=request_id)
        return False
    if response.get("type") == "response_accepted":
        log("RESPONSE", f"✔ {nid} aceitou {response.get('payload', {}).get('workers_offered')} worker(s).", req_id=request_id)
        return True
    if response.get("type") == "response_rejected":
        log("RESPONSE", f"✘ {nid} recusou: {response.get('payload', {}).get('reason')}", req_id=request_id)
    return False

# ═══════════════════════════════════════════════════════════════════
# SPRINT 3 — NEGOCIAÇÃO (OFERTANTE) — é aqui que VOCÊ empresta!
# ═══════════════════════════════════════════════════════════════════

def handle_request_help(conn, payload_outer):
    request_id = payload_outer.get("request_id", str(uuid.uuid4()))
    p          = payload_outer.get("payload", {})
    if not strict_parse(p, ["master_id", "workers_needed"], "request_help"):
        return
    requester_id   = p["master_id"]
    needed         = int(p["workers_needed"])
    # master_address é opcional na spec — resolvemos pelo diretório/IP da conexão.
    requester_addr = p.get("master_address") or resolver_endereco_vizinho(requester_id, conn)

    log("REQUEST_HELP", f"← {requester_id} pediu {needed} worker(s) | destino={requester_addr}", req_id=request_id)

    if not requester_addr:
        send_json(conn, {"type": "response_rejected", "request_id": request_id,
                         "payload": {"reason": "refused"}})
        log("RESPONSE", f"Sem endereço de destino para {requester_id} — recusado.", req_id=request_id)
        return

    with state_lock:
        my_load    = load_counter
        candidatos = [w for w in known_local_workers if w not in pending_redirects and w not in lent_out]
        can_offer  = max(0, len(candidatos) - 1)   # mantém ao menos 1 para si

    reason = "high_load" if my_load >= CAPACITY else ("no_workers_available" if can_offer == 0 else None)
    if reason:
        send_json(conn, {"type": "response_rejected", "request_id": request_id, "payload": {"reason": reason}})
        log("RESPONSE", f"Rejeitando {requester_id}: {reason}", req_id=request_id)
        return

    to_offer = min(needed, can_offer)
    selected = candidatos[:to_offer]
    with state_lock:
        for wid in selected:
            pending_redirects[wid] = requester_addr
            lent_to_peer[wid] = requester_id
    details = [{"id": wid, "address": f"{HOST}:{PORT}"} for wid in selected]
    send_json(conn, {"type": "response_accepted", "request_id": request_id,
                     "payload": {"workers_offered": to_offer, "worker_details": details}})
    log("RESPONSE", f"✔ Emprestando {to_offer} worker(s) a {requester_id} (redirecionam no próximo ciclo).", req_id=request_id)

def handle_register_temporary_worker(conn, payload_outer):
    request_id = payload_outer.get("request_id", "?")
    p          = payload_outer.get("payload", {})
    if not strict_parse(p, ["worker_id", "original_master_address"], "register_temporary_worker"):
        return
    worker_id = p["worker_id"]
    origin    = p["original_master_address"]
    with state_lock:
        workers_borrowed.setdefault(worker_id, {"conn": conn, "origin_name": origin})
        workers_borrowed[worker_id]["conn"] = conn
        borrowed_origin_addr[worker_id] = origin
    log("CICLO_VIDA", f"[INÍCIO] Worker emprestado {worker_id} registrado | origem={origin}", req_id=request_id)
    log_worker_state()

def devolver_todos_workers():
    with state_lock:
        count = 0
        for wid in list(workers_borrowed.keys()):
            pending_releases[wid] = True
            count += 1
    if count:
        log("DEVOLUÇÃO", f"{count} worker(s) marcado(s) p/ devolução no próximo ciclo.")

def notify_worker_returned(worker_id, original_master_address):
    req_id = str(uuid.uuid4())
    try:
        ip, port = original_master_address.rsplit(":", 1)
        c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        c.settimeout(NEGOTIATION_TIMEOUT)
        c.connect((ip, int(port)))
        send_json(c, {"type": "notify_worker_returned", "request_id": req_id, "payload": {"worker_id": worker_id}})
        log("NOTIFY", f"notify_worker_returned → {original_master_address} | worker={worker_id}", req_id=req_id)
        c.close()
    except Exception as e:
        log("NOTIFY", f"Falha (worker volta sozinho mesmo assim): {e}")

def handle_notify_worker_returned(payload_outer):
    worker_id = payload_outer.get("payload", {}).get("worker_id", "?")
    log("NOTIFY", f"Worker {worker_id} devolvido. Aguardando reconexão.")

# ═══════════════════════════════════════════════════════════════════
# SPRINT 4 — SUPERVISOR / DASHBOARD
# ═══════════════════════════════════════════════════════════════════

def coletar_sistema():
    m = {"uptime_seconds": int(time.time() - START_TIME),
         "load_average_1m": 0.0, "load_average_5m": 0.0,
         "cpu": {"usage_percent": 0.0, "count_logical": 1, "count_physical": 1},
         "memory": {"total_mb": 0, "available_mb": 0, "percent_used": 0.0, "memory_used": 0},
         "disk": {"total_gb": 0.0, "free_gb": 0.0, "percent_used": 0.0}}
    try:
        la1, la5, _ = os.getloadavg()
        m["load_average_1m"], m["load_average_5m"] = round(la1, 2), round(la5, 2)
    except (OSError, AttributeError):
        pass
    if psutil:
        try:
            m["cpu"]["usage_percent"]   = round(psutil.cpu_percent(interval=None), 2)
            m["cpu"]["count_logical"]   = psutil.cpu_count(logical=True) or 1
            m["cpu"]["count_physical"]  = psutil.cpu_count(logical=False) or 1
            vm = psutil.virtual_memory()
            m["memory"] = {"total_mb": int(vm.total / 1048576), "available_mb": int(vm.available / 1048576),
                           "percent_used": round(vm.percent, 2), "memory_used": int((vm.total - vm.available) / 1048576)}
            du = psutil.disk_usage("C:\\" if os.name == "nt" else "/")
            m["disk"] = {"total_gb": round(du.total / 1073741824, 1), "free_gb": round(du.free / 1073741824, 1),
                         "percent_used": round(du.percent, 1)}
        except Exception:
            pass
    return m

def coletar_farm_state():
    with state_lock:
        home      = len(known_local_workers)
        recebidos = len(workers_borrowed)
        cedidos   = len(lent_out)
        bw = ([{"direction": "out", "peer_uuid": lent_to_peer.get(w, lent_out.get(w, "?"))} for w in lent_out] +
              [{"direction": "in",  "peer_uuid": info.get("origin_name", "?")} for info in workers_borrowed.values()])
        running = max(0, tasks_running)
        return {
            "workers": {
                "total_registered": home + recebidos,
                "workers_utilization": running,
                "workers_alive": home + recebidos,
                "workers_idle": home,
                "workers_borrowed": cedidos,        # cedidos para fora
                "workers_received": recebidos,      # recebidos de fora
                "workers_failed": 0,
                "workers_home": home,
                "workers_available_capacity": home,
                "borrowed_workers": bw,
            },
            "tasks": {
                "tasks_pending": task_queue.qsize(),
                "tasks_running": running,
                "tasks_completed": tasks_completed,
                "tasks_failed": tasks_failed,
                "oldest_task_age_s": 0,
            },
        }

def montar_payload_supervisor():
    return {
        "server_uuid": SUP_UUID, "hostname": SUP_HOSTNAME, "role": "master",
        "task": "performance_report", "timestamp": now_iso(),
        "message_id": str(uuid.uuid4()), "payload_version": "sprint4-monitor",
        "performance": {
            "system": coletar_sistema(),
            "farm_state": coletar_farm_state(),
            "config_thresholds": {"max_task": CAPACITY, "warn_cpu_percent": 85,
                                  "warn_memory_percent": 85, "release_task": RELEASE_THRESHOLD},
            "neighbors": [{"server_uuid": nid, "status": "available", "last_heartbeat": now_iso()}
                          for (nid, _, _) in NEIGHBOR_MASTERS],
        },
    }

def enviar_ao_supervisor(payload):
    data = (json.dumps(payload) + "\n").encode("utf-8")
    # 1ª tentativa: TLS com verificação de certificado.
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection((SUP_HOST, SUP_PORT), timeout=5) as raw:
            with ctx.wrap_socket(raw, server_hostname=SUP_HOST) as s:
                s.sendall(data)   # apenas SEND, sem RECV (conforme spec)
        return True
    except ssl.SSLError as e:
        log("SUPERVISOR", f"TLS verificado falhou ({e}); tentando sem verificação.")
    except Exception as e:
        log("SUPERVISOR", f"Falha ao enviar: {e}")
        return False
    # 2ª tentativa: TLS sem verificação (fallback de sala de aula).
    try:
        ctx = ssl._create_unverified_context()
        with socket.create_connection((SUP_HOST, SUP_PORT), timeout=5) as raw:
            with ctx.wrap_socket(raw, server_hostname=SUP_HOST) as s:
                s.sendall(data)
        return True
    except Exception as e:
        log("SUPERVISOR", f"Falha (fallback) ao enviar: {e}")
        return False

def loop_supervisor():
    log("SUPERVISOR", f"Reporter ON → {SUP_HOST}:{SUP_PORT} a cada {SUP_INTERVAL}s | server_uuid={SUP_UUID}")
    while True:
        if enviar_ao_supervisor(montar_payload_supervisor()):
            log("SUPERVISOR", "Relatório enviado.")
        time.sleep(SUP_INTERVAL)

# ═══════════════════════════════════════════════════════════════════
# DISPATCHER + SERVIDOR TCP
# ═══════════════════════════════════════════════════════════════════

def dispatch(conn, payload):
    if payload.get("TYPE") == "ELECTION_ACK" and "SELECTED_MASTER" in payload:
        handle_election_ack(conn, payload); return
    task = payload.get("TASK", "").upper()
    worker_field = payload.get("WORKER", "").upper()
    status_field = payload.get("STATUS", "").upper()
    if task == "HEARTBEAT":
        handle_heartbeat(conn, payload); return
    if worker_field == "ALIVE":
        handle_worker_alive(conn, payload); return
    if status_field in ("OK", "NOK"):
        handle_status(conn, payload); return
    msg_type = payload.get("type", "").lower()
    handlers = {
        "request_help":              lambda: handle_request_help(conn, payload),
        "register_temporary_worker": lambda: handle_register_temporary_worker(conn, payload),
        "notify_worker_returned":    lambda: handle_notify_worker_returned(payload),
    }
    if msg_type in handlers:
        handlers[msg_type]()
    elif msg_type:
        log("AVISO", f"Tipo desconhecido '{msg_type}' — ignorado.")
    else:
        log("AVISO", f"Mensagem não reconhecida — ignorada: {payload}")

def handle_client(conn, addr):
    log("CONEXÃO", f"Nova conexão TCP de {addr}")
    try:
        while True:
            payload = recv_json(conn)
            if payload is None:
                break
            log("RECEBIDO", f"[{addr}] {payload}")
            dispatch(conn, payload)
    except Exception as e:
        log("ERRO", f"[{addr}] {e}")
    finally:
        with state_lock:
            for wid, info in list(workers_local.items()):
                if info.get("conn") is conn:
                    workers_local.pop(wid); break
            for wid, info in list(workers_borrowed.items()):
                if info.get("conn") is conn:
                    workers_borrowed.pop(wid); break
        conn.close()

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
    populate_queue(args.initial_tasks)
    log("MASTER", f"Iniciando {MASTER_ID} | TCP {HOST}:{PORT} | UDP descoberta {UDP_DISCOVERY_PORT}")
    log("MASTER", f"Saturação>{CAPACITY} | Liberação<{RELEASE_THRESHOLD}")
    log("MASTER", f"Vizinhos: {NEIGHBOR_MASTERS if NEIGHBOR_MASTERS else 'nenhum'}")
    if not psutil:
        log("MASTER", "psutil não instalado — métricas de CPU/MEM/DISK irão zeradas. (pip install psutil)")

    threading.Thread(target=iniciar_servidor_udp, daemon=True).start()
    threading.Thread(target=monitor_load, daemon=True).start()
    threading.Thread(target=gerador_de_carga, daemon=True).start()
    if SUP_ENABLED:
        threading.Thread(target=loop_supervisor, daemon=True).start()
    iniciar_servidor_tcp()