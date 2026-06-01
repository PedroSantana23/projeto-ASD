"""
worker.py — Sistema P2P com Balanceamento de Carga Dinâmico
Sprint 1 + Sprint 2 + Sprint 3 completos
"""

import socket
import threading
import json
import time
import uuid

# ═══════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES — ajuste antes de rodar
# ═══════════════════════════════════════════════════════════════════

WORKER_UUID  = f"Worker-{uuid.uuid4().hex[:6]}"
MASTER_HOST = "192.168.1.97"
MASTER_PORT = 8001

HEARTBEAT_INTERVAL = 30   # segundos entre heartbeats (spec: loop regular)
TASK_INTERVAL      = 3    # segundos entre ciclos de pedido de tarefa
TIMEOUT            = 5    # spec: aguarda 5s antes de considerar conexão perdida
MAX_RETRY_BACKOFF  = 30   # máximo de segundos de espera entre reconexões

# Se este worker for "emprestado" de outro master, preencha abaixo.
# Deixe None para worker local normal.
ORIGINAL_MASTER_UUID = None  # ex: "Master-B"

# ═══════════════════════════════════════════════════════════════════
# ESTADO GLOBAL (mutável pelo ciclo de vida)
# ═══════════════════════════════════════════════════════════════════

state_lock = threading.Lock()

# Endereço do Master ATUAL (muda após command_redirect)
current_master_host = MASTER_HOST
current_master_port = MASTER_PORT

# Endereço do Master ORIGINAL (preenchido após command_redirect)
original_master_address = None

# Se emprestado, SERVER_UUID a enviar no payload ALIVE
current_original_uuid = ORIGINAL_MASTER_UUID

# Flag: tarefa em execução (impede redirect abrupto)
task_in_progress = False

# Flag: foi redirecionado e está aguardando processamento
redirected = False

# ═══════════════════════════════════════════════════════════════════
# UTILITÁRIOS
# ═══════════════════════════════════════════════════════════════════

def send_json(conn, data: dict):
    conn.sendall((json.dumps(data) + "\n").encode("utf-8"))

def recv_json(conn, timeout=None) -> dict | None:
    """Acumula buffer TCP até encontrar \\n. Retorna None em timeout/erro."""
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

def criar_conexao(host=None, port=None) -> socket.socket | None:
    """Tenta conectar ao master atual. Retorna socket ou None."""
    h = host if host else current_master_host
    p = port if port else current_master_port
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(TIMEOUT)
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
# SPRINT 1 — HEARTBEAT
# ═══════════════════════════════════════════════════════════════════

def enviar_heartbeat():
    """
    Verifica se o Master atual está ativo.
    Loga ALIVE ou OFFLINE. Em caso de falha reconecta com backoff.
    """
    conn = criar_conexao()
    if conn is None:
        log("HEARTBEAT", "Status: OFFLINE - Tentando Reconectar")
        return False

    try:
        send_json(conn, {"SERVER_UUID": WORKER_UUID, "TASK": "HEARTBEAT"})
        response = recv_json(conn, timeout=TIMEOUT)

        if response and response.get("RESPONSE") == "ALIVE":
            log("HEARTBEAT", f"Status: ALIVE | master={current_master_host}:{current_master_port}")
            return True
        else:
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
    Loop de heartbeat com backoff exponencial em caso de falha.
    Reconecta automaticamente (spec: reestabelece sem travar).
    """
    retry_wait = TASK_INTERVAL
    while True:
        sucesso = enviar_heartbeat()
        if sucesso:
            retry_wait = TASK_INTERVAL   # reseta backoff
            time.sleep(HEARTBEAT_INTERVAL)
        else:
            log("HEARTBEAT", f"Falha. Tentando novamente em {retry_wait}s...")
            time.sleep(retry_wait)
            retry_wait = min(retry_wait * 2, MAX_RETRY_BACKOFF)   # backoff exponencial

# ═══════════════════════════════════════════════════════════════════
# SPRINT 2 — CICLO DE TAREFAS
# ═══════════════════════════════════════════════════════════════════

def processar_tarefa(user: str) -> str:
    """
    Simula processamento de tarefa.
    Flag task_in_progress garante que redirects não interrompam o trabalho.
    """
    global task_in_progress
    with state_lock:
        task_in_progress = True
    try:
        log("TASK", f"Processando: USER={user} ...")
        time.sleep(2)   # simula trabalho (cálculo / I/O)
        log("TASK", f"Concluído: USER={user}")
        return "OK"
    except Exception as e:
        log("TASK", f"Erro ao processar: {e}")
        return "NOK"
    finally:
        with state_lock:
            task_in_progress = False

def executar_ciclo_tarefa(conn: socket.socket) -> str:
    """
    Ciclo completo Sprint 2:
      ALIVE → (QUERY | NO_TASK | command_redirect | command_release)
      → STATUS → ACK

    Retorna:
      'done'     — ciclo concluído com sucesso
      'no_task'  — sem tarefa disponível
      'redirect' — recebeu command_redirect (Sprint 3)
      'release'  — recebeu command_release (Sprint 3)
      'error'    — falha de comunicação ou parsing
    """
    # ── Passo 1: Apresentação ──────────────────────────────
    with state_lock:
        orig_uuid = current_original_uuid

    payload_alive = {"WORKER": "ALIVE", "WORKER_UUID": WORKER_UUID}
    if orig_uuid:
        payload_alive["SERVER_UUID"] = orig_uuid   # identifica como emprestado

    send_json(conn, payload_alive)
    log("ALIVE", "Apresentado" + (f" | emprestado de {orig_uuid}" if orig_uuid else " | local"))

    # ── Passo 2: Aguarda resposta do Master ────────────────
    response = recv_json(conn, timeout=TIMEOUT)
    if response is None:
        log("CICLO", "Sem resposta do Master (timeout ou desconexão).")
        return "error"

    log("RECEBIDO", str(response))

    # ── Sprint 3: command_redirect ─────────────────────────
    if response.get("type") == "command_redirect":
        new_addr = response.get("payload", {}).get("new_master_address", "")
        if not new_addr:
            log("REDIRECT", "command_redirect sem new_master_address. Ignorado.")
            return "error"
        return tratar_command_redirect(new_addr)

    # ── Sprint 3: command_release ──────────────────────────
    if response.get("type") == "command_release":
        orig_addr = response.get("payload", {}).get("original_master_address", "")
        if not orig_addr:
            log("RELEASE", "command_release sem original_master_address. Ignorado.")
            return "error"
        return tratar_command_release(orig_addr)

    task = response.get("TASK", "").upper()

    # ── NO_TASK ────────────────────────────────────────────
    if task == "NO_TASK":
        log("CICLO", "Sem tarefas disponíveis.")
        return "no_task"

    # ── QUERY ──────────────────────────────────────────────
    if task == "QUERY":
        user = response.get("USER", "desconhecido")

        try:
            status = processar_tarefa(user)
        except Exception:
            status = "NOK"

        # Passo 3: Reporta STATUS
        send_json(conn, {"STATUS": status, "TASK": "QUERY", "WORKER_UUID": WORKER_UUID})
        log("STATUS", f"Reportado: {status}")

        # Passo 4: Aguarda ACK (spec: fecha o ciclo sem perda de mensagem)
        ack = recv_json(conn, timeout=TIMEOUT)
        if ack and ack.get("STATUS") == "ACK":
            log("ACK", "ACK recebido. Ciclo concluído com sucesso.")
            return "done"
        else:
            log("ACK", f"ACK inesperado ou ausente: {ack}")
            return "error"

    log("CICLO", f"Mensagem desconhecida recebida: {response}")
    return "error"

# ═══════════════════════════════════════════════════════════════════
# SPRINT 3 — REDIRECIONAMENTO E DEVOLUÇÃO
# ═══════════════════════════════════════════════════════════════════

def tratar_command_redirect(new_master_address: str) -> str:
    """
    Recebe command_redirect do Master atual.
    1. Guarda endereço do master anterior como "origem"
    2. Atualiza current_master para o novo endereço
    3. Envia register_temporary_worker ao novo master
    """
    global current_master_host, current_master_port
    global original_master_address, current_original_uuid

    log("REDIRECT", f"command_redirect recebido → novo master: {new_master_address}")

    with state_lock:
        # Salva o master atual como origem
        original_master_address = f"{current_master_host}:{current_master_port}"
        current_original_uuid   = original_master_address  # usado em SERVER_UUID

        # Atualiza para o novo master
        try:
            new_host, new_port = new_master_address.rsplit(":", 1)
            current_master_host = new_host
            current_master_port = int(new_port)
        except ValueError:
            log("REDIRECT", f"Endereço inválido: {new_master_address}")
            return "error"

    log("REDIRECT",
        f"Master atualizado: {current_master_host}:{current_master_port} | "
        f"origem salva: {original_master_address}")

    # Registra no novo master em thread separada (não bloqueia o ciclo)
    threading.Thread(target=registrar_no_novo_master, daemon=True).start()
    return "redirect"

def registrar_no_novo_master():
    """
    Envia register_temporary_worker ao novo Master (Sprint 3C).
    Spec: imediatamente após conectar ao novo Master.
    """
    time.sleep(0.5)   # pequena pausa para o novo master estar pronto para aceitar
    conn = criar_conexao()
    if conn is None:
        log("REGISTER", "Falha ao conectar no novo Master para registro.")
        return

    try:
        with state_lock:
            orig_addr = original_master_address

        req_id = str(uuid.uuid4())
        send_json(conn, {
            "type":       "register_temporary_worker",
            "request_id": req_id,
            "payload": {
                "worker_id":               WORKER_UUID,
                "original_master_address": orig_addr
            }
        })
        log("REGISTER",
            f"register_temporary_worker → {current_master_host}:{current_master_port} "
            f"| origem={orig_addr} [req={req_id[:8]}]")
    except Exception as e:
        log("REGISTER", f"Erro ao registrar: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

def tratar_command_release(orig_addr: str) -> str:
    """
    Recebe command_release do Master que estava usando este worker.
    Reseta estado para worker local e volta ao master de origem.

    Spec CT08: worker detecta queda e tenta voltar ao master original.
    """
    global current_master_host, current_master_port
    global original_master_address, current_original_uuid

    log("RELEASE", f"command_release recebido → retornando a {orig_addr}")

    with state_lock:
        try:
            host, port = orig_addr.rsplit(":", 1)
            current_master_host     = host
            current_master_port     = int(port)
            original_master_address = None
            current_original_uuid   = None   # volta a ser worker local
        except ValueError:
            log("RELEASE", f"Endereço inválido: {orig_addr}")
            return "error"

    log("RELEASE",
        f"Estado resetado. Master atual: {current_master_host}:{current_master_port} | Modo: LOCAL")
    return "release"

# ═══════════════════════════════════════════════════════════════════
# LOOP PRINCIPAL DE TAREFAS
# ═══════════════════════════════════════════════════════════════════

def loop_tarefas():
    """
    Loop principal: conecta ao master atual e executa o ciclo de tarefa.
    Trata todos os resultados possíveis do ciclo.

    Spec CT08: se master cair durante empréstimo, worker tenta voltar ao original.
    """
    retry_wait = TASK_INTERVAL

    while True:
        conn = criar_conexao()
        if conn is None:
            with state_lock:
                is_borrowed = current_original_uuid is not None
                orig_addr   = original_master_address

            if is_borrowed and orig_addr:
                # Spec CT08: master atual caiu durante empréstimo → volta ao original
                log("LOOP",
                    f"Master atual indisponível. Sou emprestado. "
                    f"Tentando voltar ao master original: {orig_addr}")
                tratar_command_release(orig_addr)
                retry_wait = TASK_INTERVAL
            else:
                log("LOOP", f"Master indisponível. Aguardando {retry_wait}s...")
                time.sleep(retry_wait)
                retry_wait = min(retry_wait * 2, MAX_RETRY_BACKOFF)
            continue

        retry_wait = TASK_INTERVAL   # reseta backoff ao conectar com sucesso

        try:
            resultado = executar_ciclo_tarefa(conn)
        except Exception as e:
            log("LOOP", f"Exceção inesperada no ciclo: {e}")
            resultado = "error"
        finally:
            try:
                conn.close()
            except Exception:
                pass

        if resultado == "done":
            time.sleep(TASK_INTERVAL)

        elif resultado == "no_task":
            log("LOOP", f"Sem tarefas. Aguardando {TASK_INTERVAL}s...")
            time.sleep(TASK_INTERVAL)

        elif resultado == "redirect":
            # Redirecionado: aguarda registro ser concluído e reconecta
            log("LOOP", "Redirecionado para novo master. Reconectando em 1s...")
            time.sleep(1)

        elif resultado == "release":
            # Devolvido: volta ao master original normalmente
            log("LOOP", "Devolvido ao master original. Retomando ciclo normal.")
            time.sleep(TASK_INTERVAL)

        else:
            # Erro: backoff antes de tentar novamente
            log("LOOP", f"Erro no ciclo. Aguardando {retry_wait}s antes de reconectar...")
            time.sleep(retry_wait)
            retry_wait = min(retry_wait * 2, MAX_RETRY_BACKOFF)

# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log("WORKER", f"Iniciando | UUID={WORKER_UUID}")
    log("WORKER", f"Master inicial: {MASTER_HOST}:{MASTER_PORT}")
    log("WORKER", "Modo: " + (
        f"EMPRESTADO (origem: {ORIGINAL_MASTER_UUID})" if ORIGINAL_MASTER_UUID else "LOCAL"
    ))

    # Thread de heartbeat — paralela ao ciclo de tarefas
    threading.Thread(target=loop_heartbeat, daemon=True).start()

    # Loop principal de tarefas (bloqueante)
    loop_tarefas()