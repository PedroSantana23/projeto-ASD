import socket
import threading
import json
import uuid
import time
import os
import queue
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

HOST = get_local_ip()
PORT = 8000
SERVER_UUID = f"Master_{HOST}"   # identificador único deste Master

# Threshold de saturação (Sprint 3)
SATURATION_THRESHOLD = 5        # nº de tarefas na fila que dispara negociação

# Endereços de Masters vizinhos conhecidos (Sprint 3)
# Ajuste conforme a topologia do laboratório
NEIGHBOR_MASTERS = [
    # ("10.62.206.X", 8000),
]

# ─────────────────────────────────────────────
#  Estado global
# ─────────────────────────────────────────────

task_queue: queue.Queue = queue.Queue()   # fila de tarefas pendentes
borrowed_workers: dict = {}               # {worker_uuid: origem_master}
negotiation_lock = threading.Lock()       # evita eleição simultânea
is_negotiating = False                    # flag de negociação em andamento


# ─────────────────────────────────────────────
#  Utilitários de comunicação
# ─────────────────────────────────────────────

def recv_json(conn, timeout=5):
    """Lê do socket até encontrar '\n' e decodifica o JSON."""
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
    """Serializa o payload em JSON e envia com '\n' como delimitador."""
    try:
        msg = json.dumps(payload) + "\n"
        conn.sendall(msg.encode("utf-8"))
    except OSError as e:
        print(f"[ERRO send_json] {e}")


def send_json_to(host, port, payload: dict, timeout=5) -> dict | None:
    """Abre conexão, envia JSON e aguarda resposta JSON. Retorna None em falha."""
    try:
        with socket.create_connection((host, port), timeout=timeout) as s:
            send_json(s, payload)
            return recv_json(s, timeout=timeout)
    except Exception as e:
        print(f"[ERRO send_json_to {host}:{port}] {e}")
        return None


# ─────────────────────────────────────────────
#  Sprint 1 – Heartbeat
#  Quando o Worker manda {"SERVER_UUID":"...","TASK":"HEARTBEAT"}
#  o Master responde {"SERVER_UUID":"...","TASK":"HEARTBEAT","RESPONSE":"ALIVE"}
# ─────────────────────────────────────────────

def handle_heartbeat(conn, payload: dict):
    resp = {
        "SERVER_UUID": SERVER_UUID,
        "TASK": "HEARTBEAT",
        "RESPONSE": "ALIVE"
    }
    send_json(conn, resp)
    print(f"[HEARTBEAT] Respondido ALIVE para {payload.get('SERVER_UUID', '?')}")


# ─────────────────────────────────────────────
#  Sprint 2 – Ciclo de vida de tarefas
#  Worker apresenta-se → Master entrega tarefa ou NO_TASK
#  Worker reporta status OK/NOK → Master responde ACK
# ─────────────────────────────────────────────

def handle_worker_alive(conn, payload: dict):
    """Worker se apresenta (WORKER: ALIVE). Master entrega tarefa ou NO_TASK."""
    worker_uuid = payload.get("WORKER_UUID", "UNKNOWN")
    server_uuid = payload.get("SERVER_UUID")   # presente se worker "emprestado"

    if server_uuid and server_uuid != SERVER_UUID:
        print(f"[INFO] Worker emprestado {worker_uuid} de {server_uuid} se apresentou.")

    try:
        task_data = task_queue.get_nowait()
        response = {"TASK": "QUERY", "USER": task_data["user"]}
        print(f"[TAREFA] Entregando tarefa user={task_data['user']} para worker {worker_uuid}")
    except queue.Empty:
        response = {"TASK": "NO_TASK"}
        print(f"[TAREFA] Fila vazia – enviando NO_TASK para worker {worker_uuid}")

    send_json(conn, response)


def handle_worker_status(conn, payload: dict):
    """Worker reporta STATUS OK ou NOK após processar tarefa."""
    worker_uuid = payload.get("WORKER_UUID", "UNKNOWN")
    status      = payload.get("STATUS", "NOK")
    task        = payload.get("TASK", "?")

    if status == "OK":
        print(f"[LOG] Worker {worker_uuid} concluiu '{task}' com SUCESSO.")
    else:
        print(f"[LOG] Worker {worker_uuid} FALHOU na tarefa '{task}'.")

    # Verifica se é worker emprestado para registrar origem
    origem = borrowed_workers.get(worker_uuid)
    if origem:
        print(f"[LOG] Worker {worker_uuid} pertence ao master {origem}.")

    ack = {"STATUS": "ACK", "WORKER_UUID": worker_uuid}
    send_json(conn, ack)
    print(f"[ACK] Enviado ACK para worker {worker_uuid}")


# ─────────────────────────────────────────────
#  Sprint 3 – Protocolo de Conversa Consensual
#  Quando saturado, o Master solicita ajuda a vizinhos.
#  Mensagens: BORROW_REQUEST / BORROW_RESPONSE / REDIRECT_WORKER / REDIRECT_ACK
# ─────────────────────────────────────────────

def handle_borrow_request(conn, payload: dict):
    """Vizinho saturado pedindo workers emprestados."""
    requester = payload.get("FROM", "?")
    qty       = int(payload.get("QUANTITY", 1))
    print(f"[CONSENSO] Recebido BORROW_REQUEST de {requester} para {qty} worker(s).")

    # Decisão: aceita se tiver pelo menos (qty+1) workers livres no futuro
    # Como simulação, aceitamos sempre (adapte conforme sua lógica)
    can_lend = True
    response = {
        "TASK": "BORROW_RESPONSE",
        "FROM": SERVER_UUID,
        "ACCEPTED": can_lend,
        "QUANTITY": qty if can_lend else 0
    }
    send_json(conn, response)
    print(f"[CONSENSO] Respondido BORROW_RESPONSE accepted={can_lend} para {requester}.")


def handle_redirect_worker(conn, payload: dict):
    """Vizinho confirma quais workers foram redirecionados para nós."""
    workers_list = payload.get("WORKERS", [])
    from_master  = payload.get("FROM", "?")
    for w in workers_list:
        borrowed_workers[w] = from_master
        print(f"[CONSENSO] Worker {w} de {from_master} agora atende este Master.")
    send_json(conn, {"TASK": "REDIRECT_ACK", "FROM": SERVER_UUID})


def request_workers_from_neighbors(qty=1):
    """Tenta pedir workers emprestados a masters vizinhos (Sprint 3)."""
    global is_negotiating
    with negotiation_lock:
        if is_negotiating:
            return
        is_negotiating = True

    try:
        print(f"[CONSENSO] Fila saturada ({task_queue.qsize()} tarefas). Solicitando {qty} worker(s) emprestado(s).")
        for (n_host, n_port) in NEIGHBOR_MASTERS:
            req = {
                "TASK": "BORROW_REQUEST",
                "FROM": SERVER_UUID,
                "QUANTITY": qty
            }
            resp = send_json_to(n_host, n_port, req)
            if resp and resp.get("ACCEPTED"):
                print(f"[CONSENSO] {n_host}:{n_port} aceitou emprestar {resp.get('QUANTITY')} worker(s).")
                # Aguarda os workers se apresentarem com SERVER_UUID deste master
                break
            else:
                print(f"[CONSENSO] {n_host}:{n_port} recusou ou não respondeu.")
    finally:
        with negotiation_lock:
            is_negotiating = False


def monitor_saturation():
    """Thread que verifica periodicamente se a fila está saturada (Sprint 3)."""
    while True:
        time.sleep(3)
        if task_queue.qsize() >= SATURATION_THRESHOLD:
            request_workers_from_neighbors(qty=2)


# ─────────────────────────────────────────────
#  Dispatcher principal de mensagens
# ─────────────────────────────────────────────

def handle_client(conn, addr):
    print(f"[CONEXÃO] Nova conexão de {addr}")
    try:
        payload = recv_json(conn)
        if payload is None:
            return

        task = payload.get("TASK", "").upper()
        worker_field = payload.get("WORKER", "").upper()

        # Sprint 1 – Heartbeat
        if task == "HEARTBEAT":
            handle_heartbeat(conn, payload)

        # Sprint 2 – Worker se apresenta
        elif worker_field == "ALIVE":
            handle_worker_alive(conn, payload)

        # Sprint 2 – Worker reporta status (OK ou NOK)
        elif payload.get("STATUS") in ("OK", "NOK"):
            handle_worker_status(conn, payload)

        # Sprint 3 – Pedido de workers emprestados de outro Master
        elif task == "BORROW_REQUEST":
            handle_borrow_request(conn, payload)

        # Sprint 3 – Vizinho informando quais workers redirecionou
        elif task == "REDIRECT_WORKER":
            handle_redirect_worker(conn, payload)

        else:
            print(f"[AVISO] Mensagem desconhecida de {addr}: {payload}")

    except Exception as e:
        print(f"[ERRO] handle_client {addr}: {e}")
    finally:
        conn.close()


# ─────────────────────────────────────────────
#  Servidor TCP
# ─────────────────────────────────────────────

def start_server():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen()
    print(f"[SERVIDOR] {SERVER_UUID} escutando em {HOST}:{PORT}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


# ─────────────────────────────────────────────
#  Simulador de chegada de tarefas
#  (substitua por injeção real conforme Sprint 2)
# ─────────────────────────────────────────────

def simulate_incoming_tasks():
    """Gera tarefas aleatórias para popular a fila."""
    users = ["Alice", "Bob", "Carol", "Dave", "Eve"]
    while True:
        time.sleep(random.uniform(1, 4))
        user = random.choice(users)
        task_queue.put({"user": user})
        print(f"[FILA] Nova tarefa adicionada (user={user}). Tarefas pendentes: {task_queue.qsize()}")


# ─────────────────────────────────────────────
#  Entrypoint
# ─────────────────────────────────────────────

if __name__ == "__main__":
    threading.Thread(target=start_server, daemon=True).start()
    threading.Thread(target=simulate_incoming_tasks, daemon=True).start()
    threading.Thread(target=monitor_saturation, daemon=True).start()

    print(f"[MASTER] {SERVER_UUID} iniciado. Aguardando conexões...")
    # Mantém a thread principal viva
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[MASTER] Encerrando.")