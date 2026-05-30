import socket
import threading
import json
import queue

# ─── Configurações ────────────────────────────────────────────────────────────
MASTER_NAME  = "MASTER_1"    # Nome lexicográfico usado na eleição (MASTER_1 < MASTER_2 …)
HOST         = "0.0.0.0"
TCP_PORT     = 8000          # Porta TCP (ciclo de tarefas / heartbeat)
UDP_PORT     = 5000          # Porta UDP dedicada à descoberta
MULTICAST_IP = "239.255.255.250"

# ─── Estado global (thread-safe) ─────────────────────────────────────────────
fila_tarefas      = queue.Queue()
lock_workers      = threading.Lock()
workers_registrados = {}   # { worker_uuid: { addr, emprestado, master_origem } }


def get_local_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


def enviar_json_tcp(conn: socket.socket, payload: dict):
    """Envia JSON + \\n pelo socket TCP."""
    conn.sendall((json.dumps(payload) + "\n").encode("utf-8"))


# ══════════════════════════════════════════════════════════════════════════════
# SPRINT 2.1 — Servidor UDP de Descoberta
# ══════════════════════════════════════════════════════════════════════════════
def servidor_udp_descoberta():
    """
    Escuta pacotes UDP de DISCOVERY (multicast ou broadcast).
    Responde via unicast com DISCOVERY_REPLY contendo IP e porta TCP.
    """
    local_ip = get_local_ip()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", UDP_PORT))

    # Adesão ao grupo multicast
    import struct
    mreq = struct.pack("4sL", socket.inet_aton(MULTICAST_IP), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    print(f"[UDP] Master '{MASTER_NAME}' escutando descoberta em :{UDP_PORT} (multicast {MULTICAST_IP})")

    while True:
        try:
            dados, origem = sock.recvfrom(1024)
            linha = dados.decode("utf-8").strip()
            if not linha:
                continue

            try:
                payload = json.loads(linha)
            except json.JSONDecodeError:
                print(f"[UDP] JSON inválido de {origem}: {linha!r} — ignorado")
                continue

            if payload.get("TYPE") != "DISCOVERY":
                continue

            worker_uuid = payload.get("WORKER_UUID", "?")
            print(f"[DISCOVERY] Recebido de {origem} — Worker: {worker_uuid}")

            resposta = {
                "TYPE"       : "DISCOVERY_REPLY",
                "MASTER_NAME": MASTER_NAME,
                "MASTER_IP"  : local_ip,
                "MASTER_PORT": TCP_PORT,
                "STATUS"     : "AVAILABLE"
            }
            sock.sendto((json.dumps(resposta) + "\n").encode("utf-8"), origem)
            print(f"[DISCOVERY] DISCOVERY_REPLY enviado para {origem}")

        except Exception as e:
            print(f"[UDP] Erro: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# SPRINT 2.1 — Tratamento do ELECTION_ACK (novo handshake TCP inicial)
# ══════════════════════════════════════════════════════════════════════════════
def tratar_election_ack(conn: socket.socket, addr, payload: dict) -> bool:
    """
    Processa o ELECTION_ACK enviado pelo Worker logo após conectar via TCP.
    Retorna True se aceito, False caso contrário.
    """
    worker_uuid     = payload.get("WORKER_UUID")
    selected_master = payload.get("SELECTED_MASTER")

    if not worker_uuid or not selected_master:
        print(f"[ELECTION] ELECTION_ACK sem campos obrigatórios de {addr} — rejeitado")
        return False

    if selected_master != MASTER_NAME:
        print(f"[ELECTION] Worker {worker_uuid} elegeu '{selected_master}', não sou eu ('{MASTER_NAME}') — rejeitado")
        resposta = {
            "TYPE"       : "ELECTION_ACK",
            "STATUS"     : "REJECTED",
            "MASTER_NAME": MASTER_NAME
        }
        enviar_json_tcp(conn, resposta)
        return False

    print(f"[ELECTION] Worker {worker_uuid} confirmou eleição de '{MASTER_NAME}' — aceito")
    resposta = {
        "TYPE"       : "ELECTION_ACK",
        "STATUS"     : "ACCEPTED",
        "MASTER_NAME": MASTER_NAME
    }
    enviar_json_tcp(conn, resposta)
    return True


# ══════════════════════════════════════════════════════════════════════════════
# SPRINTS 1 e 2 — Tratamento de cada conexão TCP
# ══════════════════════════════════════════════════════════════════════════════
def tratar_cliente(conn: socket.socket, addr):
    """
    Gerencia o ciclo completo de um Worker:
      Sprint 2.1 → ELECTION_ACK (handshake inicial)
      Sprint 1   → HEARTBEAT
      Sprint 2   → Apresentação (ALIVE) → QUERY/NO_TASK → STATUS → ACK
    """
    print(f"[TCP] Nova conexão de {addr}")
    buffer          = ""
    eleicao_ok      = False   # Torna-se True após ELECTION_ACK aceito

    try:
        while True:
            chunk = conn.recv(1024).decode("utf-8")
            if not chunk:
                break

            buffer += chunk

            while "\n" in buffer:
                linha, buffer = buffer.split("\n", 1)
                linha = linha.strip()
                if not linha:
                    continue

                try:
                    payload = json.loads(linha)
                except json.JSONDecodeError:
                    print(f"[ERRO] JSON inválido de {addr}: {linha!r}")
                    continue

                print(f"[RECEBIDO de {addr}]: {payload}")
                tipo = payload.get("TYPE", "")

                # ── ELECTION_ACK (Sprint 2.1) ─────────────────────────────────
                if tipo == "ELECTION_ACK":
                    eleicao_ok = tratar_election_ack(conn, addr, payload)
                    if not eleicao_ok:
                        return   # Conexão encerrada — Worker elegeu outro Master

                # ── HEARTBEAT (Sprint 1) ──────────────────────────────────────
                elif payload.get("TASK") == "HEARTBEAT":
                    resposta = {
                        "SERVER_UUID": MASTER_NAME,
                        "TASK"       : "HEARTBEAT",
                        "RESPONSE"   : "ALIVE"
                    }
                    enviar_json_tcp(conn, resposta)
                    print(f"[HEARTBEAT] ALIVE enviado para {addr}")

                # ── APRESENTAÇÃO (Sprint 2) ───────────────────────────────────
                elif payload.get("WORKER") == "ALIVE":
                    worker_uuid   = payload.get("WORKER_UUID")
                    master_origem = payload.get("SERVER_UUID")

                    if not worker_uuid:
                        print(f"[ERRO] ALIVE sem WORKER_UUID de {addr} — ignorado")
                        continue

                    emprestado = master_origem is not None
                    with lock_workers:
                        workers_registrados[worker_uuid] = {
                            "addr"         : addr,
                            "emprestado"   : emprestado,
                            "master_origem": master_origem
                        }

                    tipo_w = f"EMPRESTADO (origem: {master_origem})" if emprestado else "LOCAL"
                    print(f"[WORKER] {worker_uuid} registrado como {tipo_w}")

                    try:
                        tarefa   = fila_tarefas.get_nowait()
                        resposta = {"TASK": "QUERY", "USER": tarefa}
                        print(f"[FILA] Tarefa '{tarefa}' → {worker_uuid}")
                    except queue.Empty:
                        resposta = {"TASK": "NO_TASK"}
                        print(f"[FILA] Sem tarefas para {worker_uuid}")

                    enviar_json_tcp(conn, resposta)

                # ── REPORTE DE STATUS (Sprint 2) ──────────────────────────────
                elif payload.get("STATUS") in ("OK", "NOK") and payload.get("TASK") == "QUERY":
                    worker_uuid = payload.get("WORKER_UUID")
                    status      = payload.get("STATUS")

                    if not worker_uuid:
                        print(f"[ERRO] STATUS sem WORKER_UUID de {addr} — ignorado")
                        continue

                    with lock_workers:
                        info = workers_registrados.get(worker_uuid, {})

                    tipo_log = f"(emprestado de {info.get('master_origem')})" if info.get("emprestado") else "(local)"
                    print(f"[STATUS] {worker_uuid} {tipo_log} → {status}")

                    enviar_json_tcp(conn, {"STATUS": "ACK", "WORKER_UUID": worker_uuid})
                    print(f"[ACK] Enviado para {worker_uuid}")

                # ── Tipo desconhecido ─────────────────────────────────────────
                else:
                    print(f"[AVISO] Mensagem desconhecida de {addr} — ignorada: {payload}")

    except Exception as e:
        print(f"[ERRO] Conexão com {addr} encerrada: {e}")
    finally:
        print(f"[TCP] Conexão encerrada com {addr}")
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# Servidor TCP principal
# ══════════════════════════════════════════════════════════════════════════════
def servidor_tcp():
    local_ip = get_local_ip()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, TCP_PORT))
    srv.listen()
    print(f"[TCP] Master '{MASTER_NAME}' escutando em {local_ip}:{TCP_PORT}")

    while True:
        conn, addr = srv.accept()
        threading.Thread(target=tratar_cliente, args=(conn, addr), daemon=True).start()


def popular_fila():
    usuarios = ["Alice", "Bob", "Carlos", "Diana", "Eduardo",
                "Fernanda", "Gabriel", "Helena", "Igor", "Julia"]
    for u in usuarios:
        fila_tarefas.put(u)
    print(f"[FILA] {fila_tarefas.qsize()} tarefas adicionadas")


# ─── Entry-point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    popular_fila()

    # Thread UDP de descoberta (Sprint 2.1)
    threading.Thread(target=servidor_udp_descoberta, daemon=True).start()

    # Loop TCP principal (Sprints 1 e 2) — bloqueia aqui
    servidor_tcp()