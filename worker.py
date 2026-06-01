import socket
import threading
import json
import uuid
import time
import os
import random

# ─────────────────────────────────────────────
#  Configurações
# ─────────────────────────────────────────────

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()

WORKER_HOST = get_local_ip()
WORKER_PORT = 8001

# Master "dono" original deste worker
MASTER_HOST = "10.62.206.207"
MASTER_PORT = 8000

WORKER_UUID = str(uuid.uuid4())[:8]   # ID único deste worker

# Endereço do master ao qual o worker está reportando atualmente.
# Pode ser alterado durante a Sprint 3 (redirecionamento).
current_master_host = MASTER_HOST
current_master_port = MASTER_PORT

# UUID do master original (preenchido no primeiro heartbeat bem-sucedido)
original_server_uuid = None
# UUID do master atual (pode ser diferente se emprestado)
current_server_uuid  = None

# Controle de heartbeat
HEARTBEAT_INTERVAL = 10    # segundos entre heartbeats
MAX_FAILURES       = 4     # máximo de falhas antes de tentar reconectar
heartbeat_failures = 0

# Flag para saber se estamos "emprestados" a outro master
is_borrowed = False


# ─────────────────────────────────────────────
#  Utilitários de comunicação
# ─────────────────────────────────────────────

def recv_json(conn, timeout=5) -> dict | None:
    """Lê do socket até '\n' e decodifica JSON."""
    conn.settimeout(timeout)
    buffer = b""
    try:
        while b"\n" not in buffer:
            chunk = conn.recv(4096)
            if not chunk:
                break
            buffer += chunk
        line = buffer.split(b"\n")[0]
        return json.loads(line.decode("utf-8"))
    except (socket.timeout, json.JSONDecodeError, OSError) as e:
        print(f"[ERRO recv_json] {e}")
        return None


def send_json(conn, payload: dict):
    """Envia JSON com '\n' como delimitador."""
    try:
        msg = json.dumps(payload) + "\n"
        conn.sendall(msg.encode("utf-8"))
    except OSError as e:
        print(f"[ERRO send_json] {e}")


def open_connection(host, port, timeout=5):
    """Abre e retorna um socket TCP conectado ao host:port."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect((host, port))
    return s


# ─────────────────────────────────────────────
#  Sprint 1 – Heartbeat
#  Payload enviado: {"SERVER_UUID": "...", "TASK": "HEARTBEAT"}
#  Resposta esperada: {"SERVER_UUID": "...", "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
# ─────────────────────────────────────────────

def send_heartbeat():
    """Envia heartbeat ao master atual e atualiza contadores de falha."""
    global heartbeat_failures, original_server_uuid, current_server_uuid

    payload = {
        "SERVER_UUID": current_server_uuid or f"Master_{current_master_host}",
        "TASK": "HEARTBEAT"
    }

    try:
        with open_connection(current_master_host, current_master_port) as s:
            send_json(s, payload)
            resp = recv_json(s)

        if resp and resp.get("RESPONSE") == "ALIVE":
            # Armazena o UUID real do master na primeira resposta
            if current_server_uuid is None:
                current_server_uuid  = resp.get("SERVER_UUID")
                original_server_uuid = current_server_uuid
            heartbeat_failures = 0
            print(f"[HEARTBEAT] Status: ALIVE  (master={current_server_uuid})")
        else:
            raise ConnectionError("Resposta inválida no heartbeat")

    except Exception as e:
        heartbeat_failures += 1
        print(f"[HEARTBEAT] Status: OFFLINE - Tentando Reconectar ({heartbeat_failures}/{MAX_FAILURES}) – {e}")


def heartbeat_loop():
    """Loop de heartbeat executado em thread separada."""
    while True:
        send_heartbeat()
        time.sleep(HEARTBEAT_INTERVAL)


# ─────────────────────────────────────────────
#  Sprint 2 – Ciclo de vida de tarefas
#  1. Worker apresenta-se com WORKER: ALIVE
#  2. Master responde com tarefa (QUERY) ou NO_TASK
#  3. Worker processa e reporta STATUS OK/NOK
#  4. Master responde ACK
# ─────────────────────────────────────────────

def build_presentation_payload() -> dict:
    """
    Monta o payload de apresentação.
    Se estiver emprestado, inclui SERVER_UUID do master original.
    """
    payload = {
        "WORKER": "ALIVE",
        "WORKER_UUID": WORKER_UUID
    }
    if is_borrowed and original_server_uuid:
        payload["SERVER_UUID"] = original_server_uuid
    return payload


def process_task(task_payload: dict) -> str:
    """Simula processamento de uma tarefa QUERY. Retorna 'OK' ou 'NOK'."""
    user = task_payload.get("USER", "?")
    print(f"[TAREFA] Processando QUERY para user={user} ...")
    # Simulação: sleep aleatório + chance de falha de 10 %
    time.sleep(random.uniform(1, 3))
    status = "NOK" if random.random() < 0.1 else "OK"
    print(f"[TAREFA] Processamento concluído: {status}")
    return status


def task_cycle():
    """
    Ciclo completo Worker → Master:
      apresentação → recebe tarefa → processa → reporta status → recebe ACK
    Executa em loop contínuo.
    """
    while True:
        try:
            with open_connection(current_master_host, current_master_port) as s:
                # Passo 1 – Apresentação
                pres = build_presentation_payload()
                send_json(s, pres)
                print(f"[CICLO] Apresentação enviada: {pres}")

                # Passo 2 – Recebe tarefa ou NO_TASK
                task_resp = recv_json(s)
                if task_resp is None:
                    print("[CICLO] Sem resposta do Master. Aguardando próximo ciclo.")
                    time.sleep(5)
                    continue

                if task_resp.get("TASK") == "NO_TASK":
                    print("[CICLO] Sem tarefas disponíveis. Aguardando próximo ciclo.")
                    time.sleep(5)
                    continue

                if task_resp.get("TASK") != "QUERY":
                    print(f"[CICLO] Tarefa desconhecida: {task_resp}. Ignorando.")
                    time.sleep(5)
                    continue

            # Passo 3 – Processa a tarefa (fora do 'with' para não bloquear conexão)
            status = process_task(task_resp)

            # Passo 4 – Reporta status em nova conexão
            with open_connection(current_master_host, current_master_port) as s2:
                report = {
                    "STATUS": status,
                    "TASK": "QUERY",
                    "WORKER_UUID": WORKER_UUID
                }
                send_json(s2, report)
                print(f"[CICLO] Status reportado: {report}")

                # Passo 5 – Aguarda ACK
                ack = recv_json(s2)
                if ack and ack.get("STATUS") == "ACK":
                    print(f"[CICLO] ACK recebido. Ciclo concluído com sucesso.")
                else:
                    print(f"[CICLO] ACK inesperado ou ausente: {ack}")

        except Exception as e:
            print(f"[CICLO] Erro no ciclo de tarefas: {e}")
            time.sleep(5)


# ─────────────────────────────────────────────
#  Sprint 3 – Redirecionamento de Workers
#  O Master pode instruir este Worker a se reportar
#  a um Master diferente (emprestado).
# ─────────────────────────────────────────────

def handle_incoming(conn, addr):
    """Trata mensagens recebidas de outros nós (ex: instrução de redirecionamento)."""
    global current_master_host, current_master_port, current_server_uuid, is_borrowed

    payload = recv_json(conn)
    if payload is None:
        conn.close()
        return

    task = payload.get("TASK", "").upper()

    if task == "REDIRECT":
        # Master instrui o Worker a se reportar a outro Master
        new_master = payload.get("NEW_MASTER", "")
        parts = new_master.split(":")
        if len(parts) == 2:
            current_master_host = parts[0]
            current_master_port = int(parts[1])
            current_server_uuid  = payload.get("NEW_SERVER_UUID", None)
            is_borrowed = True
            print(f"[REDIRECIONAMENTO] Agora reportando ao master {new_master}")
            send_json(conn, {"STATUS": "ACK", "WORKER_UUID": WORKER_UUID})
        else:
            print(f"[REDIRECIONAMENTO] Endereço inválido: {new_master}")

    elif task == "RESTORE":
        # Master original recupera o worker
        current_master_host = MASTER_HOST
        current_master_port = MASTER_PORT
        current_server_uuid  = original_server_uuid
        is_borrowed = False
        print(f"[REDIRECIONAMENTO] Retornando ao master original {MASTER_HOST}:{MASTER_PORT}")
        send_json(conn, {"STATUS": "ACK", "WORKER_UUID": WORKER_UUID})

    else:
        print(f"[AVISO] Mensagem não reconhecida de {addr}: {payload}")

    conn.close()


def start_listener():
    """Escuta conexões de entrada (ex: instruções de redirecionamento)."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((WORKER_HOST, WORKER_PORT))
    srv.listen()
    print(f"[WORKER] {WORKER_UUID} escutando em {WORKER_HOST}:{WORKER_PORT}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_incoming, args=(conn, addr), daemon=True).start()


# ─────────────────────────────────────────────
#  Entrypoint
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print(f"[WORKER] Iniciando worker {WORKER_UUID} → master {MASTER_HOST}:{MASTER_PORT}")

    # Thread de escuta (Sprint 3 – redirecionamento)
    threading.Thread(target=start_listener, daemon=True).start()

    # Thread de heartbeat (Sprint 1)
    threading.Thread(target=heartbeat_loop, daemon=True).start()

    # Loop principal de tarefas (Sprint 2)
    # Aguarda o primeiro heartbeat confirmar o master antes de iniciar o ciclo
    time.sleep(2)
    try:
        task_cycle()
    except KeyboardInterrupt:
        print("[WORKER] Encerrando.")