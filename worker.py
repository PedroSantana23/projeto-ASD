"""
worker.py — Sprint 2.1 + Sprint 1 + Sprint 2 + Sprint 3
Descoberta Dinâmica via UDP + Balanceamento de Carga P2P
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

# ── Sprint 2.1: Worker NÃO tem IP/porta configurados ──────────────
# Descobre o Master automaticamente via UDP Broadcast
UDP_DISCOVERY_PORT    = 8000          # mesma porta que o Master escuta
UDP_BROADCAST_ADDR    = "255.255.255.255"
DISCOVERY_TIMEOUT     = 3             # janela de coleta de respostas (spec: 3s)

# ── Resiliência ────────────────────────────────────────────────────
HEARTBEAT_INTERVAL    = 30
TASK_INTERVAL         = 3
TCP_TIMEOUT           = 5            # spec: 5s timeout TCP
MAX_RETRY_BACKOFF     = 30

# ── Estado mutável (endereço do master atual) ──────────────────────
state_lock            = threading.Lock()
current_master_host   = None
current_master_port   = None
original_master_addr  = None         # preenchido após command_redirect
current_original_uuid = None         # SERVER_UUID enviado no ALIVE se emprestado
task_in_progress      = False

# ═══════════════════════════════════════════════════════════════════
# UTILITÁRIOS
# ═══════════════════════════════════════════════════════════════════

def send_json(conn, data: dict):
    conn.sendall((json.dumps(data) + "\n").encode("utf-8"))

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

def criar_conexao_tcp(host=None, port=None) -> socket.socket | None:
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
# SPRINT 2.1 — DESCOBERTA UDP
# ═══════════════════════════════════════════════════════════════════

def descobrir_masters() -> list:
    """
    Envia pacote DISCOVERY via UDP Broadcast.
    Aguarda DISCOVERY_TIMEOUT segundos coletando respostas.
    Retorna lista de Masters respondentes:
      [{"MASTER_NAME": ..., "MASTER_IP": ..., "MASTER_PORT": ...}, ...]
    """
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

    masters_encontrados = []
    deadline = time.time() + DISCOVERY_TIMEOUT

    # Coleta todas as respostas dentro da janela de tempo (spec: janela fixa)
    while time.time() < deadline:
        try:
            data, addr = sock.recvfrom(1024)
            raw = data.decode("utf-8").strip()
            resp = json.loads(raw)

            # Strict parsing: descarta respostas sem MASTER_PORT (spec CT05)
            if "MASTER_PORT" not in resp or "MASTER_NAME" not in resp or "MASTER_IP" not in resp:
                log("DISCOVERY", f"Resposta malformada de {addr} — descartada.")
                continue

            if resp.get("TYPE") == "DISCOVERY_REPLY":
                masters_encontrados.append(resp)
                log("DISCOVERY", f"Master encontrado: {resp['MASTER_NAME']} em {resp['MASTER_IP']}:{resp['MASTER_PORT']}")

        except socket.timeout:
            break
        except (json.JSONDecodeError, UnicodeDecodeError):
            log("DISCOVERY", "Resposta com JSON inválido — descartada.")
            continue

    sock.close()
    return masters_encontrados

def eleger_master(masters: list) -> dict | None:
    """
    Eleição determinística: menor nome lexicográfico (MASTER_NAME).
    Regra idêntica em todos os Workers — sem comunicação entre si.
    Spec: MASTER_1 < MASTER_2 < MASTER_10
    """
    if not masters:
        return None
    eleito = sorted(masters, key=lambda m: m["MASTER_NAME"])[0]
    log("ELECTION", f"Master eleito: {eleito['MASTER_NAME']} (menor nome lexicográfico)")
    return eleito

def confirmar_eleicao_tcp(master: dict) -> bool:
    """
    Após eleição, abre conexão TCP com o Master eleito.
    Envia ELECTION_ACK e aguarda confirmação ACCEPTED.
    Se falhar: invalida cache e reinicia descoberta.
    """
    global current_master_host, current_master_port

    host = master["MASTER_IP"]
    port = int(master["MASTER_PORT"])
    name = master["MASTER_NAME"]

    log("CONNECTING", f"Conectando via TCP ao Master eleito: {name} ({host}:{port})")

    conn = criar_conexao_tcp(host, port)
    if conn is None:
        log("FALLBACK", f"Falha TCP ao conectar em {name}. Invalidando cache e reiniciando descoberta.")
        return False

    try:
        # Envia confirmação de eleição
        send_json(conn, {
            "TYPE":            "ELECTION_ACK",
            "WORKER_UUID":     WORKER_UUID,
            "SELECTED_MASTER": name
        })
        log("ELECTION", f"ELECTION_ACK enviado → {name}")

        # Aguarda ACK do Master (timeout 5s — spec)
        response = recv_json(conn, timeout=TCP_TIMEOUT)
        if response is None:
            log("FALLBACK", f"Timeout aguardando ELECTION_ACK de {name}. Reiniciando descoberta.")
            conn.close()
            return False

        status = response.get("STATUS", "")
        if status == "ACCEPTED":
            log("ELECTION", f"✔ Conexão aceita por {name}. Iniciando Heartbeat e ciclo de tarefas.")
            with state_lock:
                current_master_host = host
                current_master_port = port
            conn.close()
            return True
        else:
            log("FALLBACK", f"Master {name} rejeitou eleição (STATUS={status}). Reiniciando.")
            conn.close()
            return False

    except Exception as e:
        log("FALLBACK", f"Erro durante ELECTION_ACK com {name}: {e}. Reiniciando descoberta.")
        try:
            conn.close()
        except Exception:
            pass
        return False

def ciclo_descoberta() -> bool:
    """
    Ciclo completo de descoberta + eleição + confirmação TCP.
    Retorna True se o Worker está pronto para operar (master encontrado e aceito).
    """
    masters = descobrir_masters()

    if not masters:
        log("DISCOVERY", "NO_MASTER_FOUND — nenhum Master respondeu. Aplicando backoff.")
        return False

    log("ELECTION", f"{len(masters)} Master(s) encontrado(s): {[m['MASTER_NAME'] for m in masters]}")
    eleito = eleger_master(masters)

    if not eleito:
        return False

    return confirmar_eleicao_tcp(eleito)

def loop_descoberta():
    """
    Tenta descobrir um Master em loop com backoff exponencial.
    Só avança quando tiver um Master aceito.
    """
    retry_wait = 2
    while True:
        log("DISCOVERY", "Iniciando descoberta de Masters na rede...")
        sucesso = ciclo_descoberta()
        if sucesso:
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
        try:
            conn.close()
        except Exception:
            pass

def loop_heartbeat():
    """
    Loop de heartbeat com backoff exponencial.
    Se o Master cair, reinicia descoberta UDP.
    """
    retry_wait = TASK_INTERVAL
    while True:
        if not current_master_host:
            time.sleep(1)
            continue

        sucesso = enviar_heartbeat()
        if sucesso:
            retry_wait = TASK_INTERVAL
            time.sleep(HEARTBEAT_INTERVAL)
        else:
            log("HEARTBEAT", f"Falha. Tentando novamente em {retry_wait}s...")
            time.sleep(retry_wait)
            retry_wait = min(retry_wait * 2, MAX_RETRY_BACKOFF)

            # Se muitas falhas: reinicia descoberta UDP
            if retry_wait >= MAX_RETRY_BACKOFF:
                log("FALLBACK", "Master parece offline. Reiniciando descoberta UDP...")
                with state_lock:
                    pass  # reseta feito no loop_descoberta se chamado
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

def executar_ciclo_tarefa(conn: socket.socket) -> str:
    """
    Ciclo completo: ALIVE → QUERY/NO_TASK/redirect/release → STATUS → ACK
    """
    with state_lock:
        orig_uuid = current_original_uuid

    payload_alive = {"WORKER": "ALIVE", "WORKER_UUID": WORKER_UUID}
    if orig_uuid:
        payload_alive["SERVER_UUID"] = orig_uuid

    send_json(conn, payload_alive)
    log("ALIVE", "Apresentado" + (f" | emprestado de {orig_uuid}" if orig_uuid else " | local"))

    response = recv_json(conn, timeout=TCP_TIMEOUT)
    if response is None:
        log("CICLO", "Sem resposta do Master (timeout).")
        return "error"

    log("RECEBIDO", str(response))

    # Sprint 3: command_redirect
    if response.get("type") == "command_redirect":
        new_addr = response.get("payload", {}).get("new_master_address", "")
        return tratar_command_redirect(new_addr) if new_addr else "error"

    # Sprint 3: command_release
    if response.get("type") == "command_release":
        orig_addr = response.get("payload", {}).get("original_master_address", "")
        return tratar_command_release(orig_addr) if orig_addr else "error"

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
        original_master_addr   = f"{current_master_host}:{current_master_port}"
        current_original_uuid  = original_master_addr
        try:
            new_host, new_port     = new_master_address.rsplit(":", 1)
            current_master_host    = new_host
            current_master_port    = int(new_port)
        except ValueError:
            log("REDIRECT", f"Endereço inválido: {new_master_address}")
            return "error"

    log("REDIRECT", f"Novo master={current_master_host}:{current_master_port} | origem={original_master_addr}")
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
        send_json(conn, {
            "type":       "register_temporary_worker",
            "request_id": req_id,
            "payload": {
                "worker_id":               WORKER_UUID,
                "original_master_address": orig_addr
            }
        })
        log("REGISTER", f"register_temporary_worker enviado | origem={orig_addr} [req={req_id[:8]}]")
    except Exception as e:
        log("REGISTER", f"Erro: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

def tratar_command_release(orig_addr: str) -> str:
    global current_master_host, current_master_port
    global original_master_addr, current_original_uuid

    log("RELEASE", f"command_release → retornando a {orig_addr}")
    with state_lock:
        try:
            host, port             = orig_addr.rsplit(":", 1)
            current_master_host    = host
            current_master_port    = int(port)
            original_master_addr   = None
            current_original_uuid  = None
        except ValueError:
            log("RELEASE", f"Endereço inválido: {orig_addr}")
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
            time.sleep(1)
            continue

        conn = criar_conexao_tcp()
        if conn is None:
            with state_lock:
                is_borrowed = current_original_uuid is not None
                orig_addr   = original_master_addr

            # Spec CT08: se master cair durante empréstimo, volta ao original
            if is_borrowed and orig_addr:
                log("LOOP", f"Master caiu durante empréstimo. Voltando ao original: {orig_addr}")
                tratar_command_release(orig_addr)
                retry_wait = TASK_INTERVAL
            else:
                log("LOOP", f"Master indisponível. Aguardando {retry_wait}s...")
                time.sleep(retry_wait)
                retry_wait = min(retry_wait * 2, MAX_RETRY_BACKOFF)

                # Reinicia descoberta se master estiver muito tempo offline
                if retry_wait >= MAX_RETRY_BACKOFF:
                    log("FALLBACK", "Reiniciando descoberta UDP...")
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
            try:
                conn.close()
            except Exception:
                pass

        if resultado == "done":
            time.sleep(TASK_INTERVAL)
        elif resultado == "no_task":
            time.sleep(TASK_INTERVAL)
        elif resultado == "redirect":
            log("LOOP", "Redirecionado. Reconectando ao novo master...")
            time.sleep(1)
        elif resultado == "release":
            log("LOOP", "Devolvido ao master original.")
            time.sleep(TASK_INTERVAL)
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

    # Passo 1: Descobre e conecta ao Master via UDP
    loop_descoberta()

    # Passo 2: Thread de heartbeat em paralelo
    threading.Thread(target=loop_heartbeat, daemon=True).start()

    # Passo 3: Loop principal de tarefas
    loop_tarefas()