"""
worker.py — VERSÃO FINAL (Sprints 1, 2, 2.1, 3)
Descoberta Dinâmica via UDP + Balanceamento de Carga P2P

Roda SEM IP configurado: descobre o master por UDP broadcast e se conecta.
Para ter 3 workers, basta rodar este mesmo arquivo em 3 máquinas da MESMA rede.
"""

import socket
import threading
import json
import time
import uuid

# ═══════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES
# ═══════════════════════════════════════════════════════════════════

WORKER_UUID = f"Worker-{uuid.uuid4().hex[:6]}"

UDP_DISCOVERY_PORT    = 5000          # IGUAL ao master
UDP_BROADCAST_ADDR    = "255.255.255.255"
DISCOVERY_TIMEOUT     = 3

HEARTBEAT_INTERVAL    = 30
TASK_INTERVAL         = 3
TCP_TIMEOUT           = 5
MAX_RETRY_BACKOFF     = 30

state_lock            = threading.Lock()
discovery_lock        = threading.Lock()   # evita duas redescobertas simultâneas
current_master_host   = None
current_master_port   = None
current_master_name   = None               # nome do master atual (p/ SERVER_UUID quando emprestado)
original_master_addr  = None               # endereço do master de origem (após redirect)
current_original_uuid = None               # NOME do master de origem enviado no ALIVE se emprestado
task_in_progress      = False

# ═══════════════════════════════════════════════════════════════════
# UTILITÁRIOS
# ═══════════════════════════════════════════════════════════════════

def send_json(conn, data: dict):
    conn.sendall((json.dumps(data) + "\n").encode("utf-8"))

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

def criar_conexao_tcp(host=None, port=None):
    h = host if host else current_master_host
    p = port if port else current_master_port
    if not h or not p:
        return None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(TCP_TIMEOUT)
        s.connect((h, p))
        s.settimeout(None)
        return s
    except Exception as e:
        log("ERRO", f"Não foi possível conectar a {h}:{p} → {e}")
        return None

def log(tag: str, msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}][{WORKER_UUID}][{tag}] {msg}", flush=True)

# ═══════════════════════════════════════════════════════════════════
# SPRINT 2.1 — DESCOBERTA UDP + ELEIÇÃO
# ═══════════════════════════════════════════════════════════════════

def descobrir_masters() -> list:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.settimeout(DISCOVERY_TIMEOUT)

    payload = json.dumps({"TYPE": "DISCOVERY", "WORKER_UUID": WORKER_UUID}) + "\n"
    log("DISCOVERY", f"Enviando DISCOVERY via broadcast {UDP_BROADCAST_ADDR}:{UDP_DISCOVERY_PORT}")
    try:
        sock.sendto(payload.encode("utf-8"), (UDP_BROADCAST_ADDR, UDP_DISCOVERY_PORT))
    except Exception as e:
        log("DISCOVERY", f"Erro ao enviar broadcast: {e}")
        sock.close()
        return []

    masters, deadline = [], time.time() + DISCOVERY_TIMEOUT
    while time.time() < deadline:
        try:
            data, addr = sock.recvfrom(1024)
            resp = json.loads(data.decode("utf-8").strip())
            if not all(k in resp for k in ("MASTER_PORT", "MASTER_NAME", "MASTER_IP")):
                log("DISCOVERY", f"Resposta malformada de {addr} — descartada.")
                continue
            if resp.get("TYPE") == "DISCOVERY_REPLY":
                masters.append(resp)
                log("DISCOVERY", f"Master: {resp['MASTER_NAME']} em {resp['MASTER_IP']}:{resp['MASTER_PORT']}")
        except socket.timeout:
            break
        except (json.JSONDecodeError, UnicodeDecodeError):
            log("DISCOVERY", "JSON inválido — descartado.")
            continue
    sock.close()
    return masters

def eleger_master(masters: list):
    if not masters:
        return None
    eleito = sorted(masters, key=lambda m: m["MASTER_NAME"])[0]
    log("ELECTION", f"Master eleito: {eleito['MASTER_NAME']} (menor nome lexicográfico)")
    return eleito

def confirmar_eleicao_tcp(master: dict) -> bool:
    global current_master_host, current_master_port, current_master_name
    host, port, name = master["MASTER_IP"], int(master["MASTER_PORT"]), master["MASTER_NAME"]
    log("CONNECTING", f"Conectando via TCP ao Master eleito: {name} ({host}:{port})")
    conn = criar_conexao_tcp(host, port)
    if conn is None:
        log("FALLBACK", f"Falha TCP com {name}. Reiniciando descoberta.")
        return False
    try:
        send_json(conn, {"TYPE": "ELECTION_ACK", "WORKER_UUID": WORKER_UUID, "SELECTED_MASTER": name})
        log("ELECTION", f"ELECTION_ACK enviado → {name}")
        response = recv_json(conn, timeout=TCP_TIMEOUT)
        if response is None:
            log("FALLBACK", f"Timeout no ELECTION_ACK de {name}. Reiniciando.")
            conn.close(); return False
        if response.get("STATUS") == "ACCEPTED":
            log("ELECTION", f"✔ Aceito por {name}. Iniciando Heartbeat e tarefas.")
            with state_lock:
                current_master_host, current_master_port, current_master_name = host, port, name
            conn.close(); return True
        log("FALLBACK", f"{name} rejeitou (STATUS={response.get('STATUS')}). Reiniciando.")
        conn.close(); return False
    except Exception as e:
        log("FALLBACK", f"Erro no ELECTION_ACK com {name}: {e}. Reiniciando.")
        try: conn.close()
        except Exception: pass
        return False

def ciclo_descoberta() -> bool:
    masters = descobrir_masters()
    if not masters:
        log("DISCOVERY", "NO_MASTER_FOUND — aplicando backoff.")
        return False
    log("ELECTION", f"{len(masters)} Master(s): {[m['MASTER_NAME'] for m in masters]}")
    eleito = eleger_master(masters)
    return confirmar_eleicao_tcp(eleito) if eleito else False

def loop_descoberta():
    # Só uma redescoberta por vez (heartbeat e tarefas podem chamar juntos).
    with discovery_lock:
        if current_master_host:
            return
        retry_wait = 2
        while True:
            log("DISCOVERY", "Procurando Masters na rede...")
            if ciclo_descoberta():
                log("DISCOVERY", f"Master configurado: {current_master_host}:{current_master_port}")
                return
            log("DISCOVERY", f"Tentando novamente em {retry_wait}s...")
            time.sleep(retry_wait)
            retry_wait = min(retry_wait * 2, MAX_RETRY_BACKOFF)

# ═══════════════════════════════════════════════════════════════════
# SPRINT 1 — HEARTBEAT
# ═══════════════════════════════════════════════════════════════════

def enviar_heartbeat() -> bool:
    conn = criar_conexao_tcp()
    if conn is None:
        log("HEARTBEAT", "Status: OFFLINE - Tentando Reconectar")
        return False
    try:
        send_json(conn, {"SERVER_UUID": WORKER_UUID, "TASK": "HEARTBEAT"})
        response = recv_json(conn, timeout=TCP_TIMEOUT)
        if response and response.get("RESPONSE") == "ALIVE":
            log("HEARTBEAT", f"Status: ALIVE | master={current_master_host}:{current_master_port}")
            return True
        log("HEARTBEAT", f"Resposta inesperada: {response}")
        return False
    except Exception as e:
        log("HEARTBEAT", f"Erro: {e}")
        return False
    finally:
        try: conn.close()
        except Exception: pass

def loop_heartbeat():
    retry_wait = TASK_INTERVAL
    while True:
        if not current_master_host:
            time.sleep(1); continue
        if enviar_heartbeat():
            retry_wait = TASK_INTERVAL
            time.sleep(HEARTBEAT_INTERVAL)
        else:
            log("HEARTBEAT", f"Falha. Tentando em {retry_wait}s...")
            time.sleep(retry_wait)
            retry_wait = min(retry_wait * 2, MAX_RETRY_BACKOFF)
            if retry_wait >= MAX_RETRY_BACKOFF:
                log("FALLBACK", "Master parece offline. Reiniciando descoberta UDP...")
                with state_lock:
                    globals()['current_master_host'] = None
                loop_descoberta()
                retry_wait = TASK_INTERVAL

# ═══════════════════════════════════════════════════════════════════
# SPRINT 2 — CICLO DE TAREFAS
# ═══════════════════════════════════════════════════════════════════

def processar_tarefa(user: str) -> str:
    global task_in_progress
    with state_lock:
        task_in_progress = True
    try:
        log("TASK", f"Processando: USER={user} ...")
        time.sleep(2)
        log("TASK", f"Concluído: USER={user}")
        return "OK"
    except Exception as e:
        log("TASK", f"Erro: {e}")
        return "NOK"
    finally:
        with state_lock:
            task_in_progress = False

def executar_ciclo_tarefa(conn) -> str:
    with state_lock:
        orig_uuid = current_original_uuid
    payload_alive = {"WORKER": "ALIVE", "WORKER_UUID": WORKER_UUID}
    if orig_uuid:
        payload_alive["SERVER_UUID"] = orig_uuid     # NOME do master de origem (spec/CT04)

    send_json(conn, payload_alive)
    log("ALIVE", "Apresentado" + (f" | emprestado de {orig_uuid}" if orig_uuid else " | local"))

    response = recv_json(conn, timeout=TCP_TIMEOUT)
    if response is None:
        log("CICLO", "Sem resposta do Master (timeout).")
        return "error"
    log("RECEBIDO", str(response))

    if response.get("type") == "command_redirect":
        new_addr = response.get("payload", {}).get("new_master_address", "")
        return tratar_command_redirect(new_addr) if new_addr else "error"

    if response.get("type") == "command_release":
        orig_addr = response.get("payload", {}).get("original_master_address", "")
        return tratar_command_release(orig_addr)

    task = response.get("TASK", "").upper()
    if task == "NO_TASK":
        log("CICLO", "Sem tarefas.")
        return "no_task"
    if task == "QUERY":
        user = response.get("USER", "desconhecido")
        try:
            status = processar_tarefa(user)
        except Exception:
            status = "NOK"
        send_json(conn, {"STATUS": status, "TASK": "QUERY", "WORKER_UUID": WORKER_UUID})
        log("STATUS", f"Reportado: {status}")
        ack = recv_json(conn, timeout=TCP_TIMEOUT)
        if ack and ack.get("STATUS") == "ACK":
            log("ACK", "ACK recebido. Ciclo concluído.")
            return "done"
        log("ACK", f"ACK inesperado: {ack}")
        return "error"

    log("CICLO", f"Mensagem desconhecida: {response}")
    return "error"

# ═══════════════════════════════════════════════════════════════════
# SPRINT 3 — REDIRECIONAMENTO E DEVOLUÇÃO
# ═══════════════════════════════════════════════════════════════════

def tratar_command_redirect(new_master_address: str) -> str:
    global current_master_host, current_master_port
    global original_master_addr, current_original_uuid
    log("REDIRECT", f"command_redirect → novo master: {new_master_address}")
    with state_lock:
        original_master_addr  = f"{current_master_host}:{current_master_port}"      # endereço de origem
        current_original_uuid = current_master_name or original_master_addr          # NOME de origem (p/ SERVER_UUID)
        try:
            new_host, new_port  = new_master_address.rsplit(":", 1)
            current_master_host = new_host
            current_master_port = int(new_port)
        except ValueError:
            log("REDIRECT", f"Endereço inválido: {new_master_address}")
            return "error"
    log("REDIRECT", f"Novo master={current_master_host}:{current_master_port} | origem={original_master_addr} ({current_original_uuid})")
    threading.Thread(target=registrar_no_novo_master, daemon=True).start()
    return "redirect"

def registrar_no_novo_master():
    time.sleep(0.5)
    conn = criar_conexao_tcp()
    if conn is None:
        log("REGISTER", "Falha ao conectar no novo Master.")
        return
    try:
        with state_lock:
            orig_addr = original_master_addr
        req_id = str(uuid.uuid4())
        send_json(conn, {"type": "register_temporary_worker", "request_id": req_id,
                         "payload": {"worker_id": WORKER_UUID, "original_master_address": orig_addr}})
        log("REGISTER", f"register_temporary_worker enviado | origem={orig_addr} [req={req_id[:8]}]")
    except Exception as e:
        log("REGISTER", f"Erro: {e}")
    finally:
        try: conn.close()
        except Exception: pass

def tratar_command_release(orig_addr_payload: str) -> str:
    global current_master_host, current_master_port
    global original_master_addr, current_original_uuid
    with state_lock:
        # Robustez: usa o endereço de origem que o PRÓPRIO worker guardou; o payload é fallback.
        destino = original_master_addr or orig_addr_payload
    log("RELEASE", f"command_release → retornando a {destino}")
    with state_lock:
        try:
            host, port            = destino.rsplit(":", 1)
            current_master_host   = host
            current_master_port   = int(port)
            original_master_addr  = None
            current_original_uuid = None
        except (ValueError, AttributeError):
            log("RELEASE", f"Endereço de origem inválido: {destino}")
            return "error"
    log("RELEASE", f"Resetado. Master={current_master_host}:{current_master_port} | Modo=LOCAL")
    return "release"

# ═══════════════════════════════════════════════════════════════════
# LOOP PRINCIPAL DE TAREFAS
# ═══════════════════════════════════════════════════════════════════

def loop_tarefas():
    retry_wait = TASK_INTERVAL
    while True:
        if not current_master_host:
            time.sleep(1); continue
        conn = criar_conexao_tcp()
        if conn is None:
            with state_lock:
                is_borrowed = current_original_uuid is not None
                orig_addr   = original_master_addr
            if is_borrowed and orig_addr:
                log("LOOP", f"Master caiu durante empréstimo. Voltando ao original: {orig_addr}")
                tratar_command_release(orig_addr)
                retry_wait = TASK_INTERVAL
            else:
                log("LOOP", f"Master indisponível. Aguardando {retry_wait}s...")
                time.sleep(retry_wait)
                retry_wait = min(retry_wait * 2, MAX_RETRY_BACKOFF)
                if retry_wait >= MAX_RETRY_BACKOFF:
                    log("FALLBACK", "Reiniciando descoberta UDP...")
                    with state_lock:
                        globals()['current_master_host'] = None
                    loop_descoberta()
                    retry_wait = TASK_INTERVAL
            continue

        retry_wait = TASK_INTERVAL
        try:
            resultado = executar_ciclo_tarefa(conn)
        except Exception as e:
            log("LOOP", f"Exceção no ciclo: {e}")
            resultado = "error"
        finally:
            try: conn.close()
            except Exception: pass

        if resultado in ("done", "no_task", "release"):
            time.sleep(TASK_INTERVAL)
        elif resultado == "redirect":
            log("LOOP", "Redirecionado. Reconectando ao novo master...")
            time.sleep(1)
        else:
            log("LOOP", f"Erro. Aguardando {retry_wait}s...")
            time.sleep(retry_wait)
            retry_wait = min(retry_wait * 2, MAX_RETRY_BACKOFF)

# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log("WORKER", f"Iniciando | UUID={WORKER_UUID}")
    log("WORKER", "Modo: DESCOBERTA DINÂMICA — sem IP pré-configurado")
    loop_descoberta()
    threading.Thread(target=loop_heartbeat, daemon=True).start()
    loop_tarefas()