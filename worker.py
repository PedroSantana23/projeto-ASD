import socket
import threading
import json
import time
import random
import struct

# ─── Configurações ────────────────────────────────────────────────────────────
WORKER_UUID        = "Worker_A1"      # Identificador único deste Worker
UDP_PORT           = 5000             # Porta UDP de descoberta (igual ao Master)
MULTICAST_IP       = "239.255.255.250"
DISCOVERY_TIMEOUT  = 3               # Segundos coletando respostas UDP
TCP_TIMEOUT        = 5               # Segundos aguardando resposta TCP
HEARTBEAT_INTERVAL = 30              # Segundos entre heartbeats
TASK_INTERVAL      = 5               # Segundos entre ciclos de tarefa
MAX_BACKOFF        = 30              # Backoff máximo em segundos

# ─── Campo opcional: preencher apenas se este worker for "emprestado" ─────────
MASTER_ORIGEM_UUID = None            # Ex: "MASTER_B" se for worker emprestado

# ─── Estado compartilhado entre threads ──────────────────────────────────────
master_atual = {
    "name": None,
    "ip"  : None,
    "port": None
}
lock_master = threading.Lock()
evento_master_eleito = threading.Event()   # Sinaliza quando master está disponível


# ══════════════════════════════════════════════════════════════════════════════
# Utilitários
# ══════════════════════════════════════════════════════════════════════════════
def enviar_json_tcp(s: socket.socket, payload: dict):
    """Serializa o payload para JSON e envia com delimitador \\n."""
    s.sendall((json.dumps(payload) + "\n").encode("utf-8"))


def receber_json_tcp(s: socket.socket) -> dict:
    """Lê do socket TCP até encontrar \\n e retorna o JSON parseado."""
    buffer = ""
    while "\n" not in buffer:
        chunk = s.recv(1024).decode("utf-8")
        if not chunk:
            raise ConnectionError("Conexão encerrada pelo Master")
        buffer += chunk
    return json.loads(buffer.split("\n")[0].strip())


# ══════════════════════════════════════════════════════════════════════════════
# SPRINT 2.1 — Descoberta UDP e Eleição
# ══════════════════════════════════════════════════════════════════════════════
def descobrir_masters() -> list[dict]:
    """
    Envia DISCOVERY via UDP multicast/broadcast e coleta respostas
    por DISCOVERY_TIMEOUT segundos.
    Retorna lista de masters descobertos: [{ name, ip, port }, ...]
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    sock.settimeout(DISCOVERY_TIMEOUT)

    payload  = {"TYPE": "DISCOVERY", "WORKER_UUID": WORKER_UUID}
    mensagem = (json.dumps(payload) + "\n").encode("utf-8")

    print(f"[DISCOVERY] Enviando broadcast para {MULTICAST_IP}:{UDP_PORT}")
    sock.sendto(mensagem, (MULTICAST_IP, UDP_PORT))

    masters = []
    deadline = time.time() + DISCOVERY_TIMEOUT

    while time.time() < deadline:
        try:
            dados, origem = sock.recvfrom(1024)
            linha = dados.decode("utf-8").strip()
            resposta = json.loads(linha)

            if resposta.get("TYPE") != "DISCOVERY_REPLY":
                continue

            # Valida campos obrigatórios
            name = resposta.get("MASTER_NAME")
            ip   = resposta.get("MASTER_IP")
            port = resposta.get("MASTER_PORT")

            if not all([name, ip, port]):
                print(f"[DISCOVERY] Resposta inválida de {origem} (campos ausentes) — descartada")
                continue

            print(f"[DISCOVERY] Master encontrado: {name} em {ip}:{port}")
            masters.append({"name": name, "ip": ip, "port": int(port)})

        except socket.timeout:
            break
        except json.JSONDecodeError as e:
            print(f"[DISCOVERY] JSON malformado recebido — ignorado: {e}")

    sock.close()
    return masters


def eleger_master(masters: list[dict]) -> dict | None:
    """
    Regra determinística: menor nome lexicográfico.
    Ex: MASTER_1 < MASTER_2 < MASTER_10
    Garante que todos os Workers, sem comunicação entre si, escolhem o mesmo Master.
    """
    if not masters:
        return None
    eleito = min(masters, key=lambda m: m["name"])
    print(f"[ELECTION] Master eleito: {eleito['name']} (de {len(masters)} disponíveis)")
    return eleito


def confirmar_eleicao_tcp(master: dict) -> bool:
    """
    Abre conexão TCP com o Master eleito, envia ELECTION_ACK e aguarda confirmação.
    Retorna True se aceito, False se rejeitado ou timeout.
    """
    print(f"[CONNECTING] Conectando a {master['name']} em {master['ip']}:{master['port']}")
    try:
        with socket.create_connection((master["ip"], master["port"]), timeout=TCP_TIMEOUT) as s:
            # Envia confirmação de eleição
            ack_payload = {
                "TYPE"           : "ELECTION_ACK",
                "WORKER_UUID"    : WORKER_UUID,
                "SELECTED_MASTER": master["name"]
            }
            enviar_json_tcp(s, ack_payload)
            print(f"[ELECTION] ELECTION_ACK enviado para {master['name']}")

            # Aguarda resposta do Master
            resposta = receber_json_tcp(s)
            status   = resposta.get("STATUS")

            if status == "ACCEPTED":
                print(f"[ELECTION] Eleição ACEITA por {master['name']} ✓")
                with lock_master:
                    master_atual["name"] = master["name"]
                    master_atual["ip"]   = master["ip"]
                    master_atual["port"] = master["port"]
                evento_master_eleito.set()
                return True
            else:
                print(f"[ELECTION] Eleição REJEITADA por {master['name']} — status: {status}")
                return False

    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        print(f"[CONNECTING] Falha TCP com {master['name']}: {e}")
        return False
    except json.JSONDecodeError as e:
        print(f"[ELECTION] Resposta inválida do Master: {e}")
        return False


def loop_descoberta():
    """
    Tenta descobrir e eleger um Master continuamente com backoff exponencial.
    Quando bem-sucedido, sinaliza evento_master_eleito e aguarda invalidação.
    """
    backoff = 2
    while True:
        evento_master_eleito.clear()

        print(f"\n[DISCOVERY] Iniciando descoberta de Masters...")
        masters = descobrir_masters()

        if not masters:
            print(f"[DISCOVERY] Nenhum Master encontrado (NO_MASTER_FOUND) — aguardando {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
            continue

        backoff = 2   # Reseta backoff após encontrar masters
        eleito  = eleger_master(masters)

        if not eleito:
            continue

        aceito = confirmar_eleicao_tcp(eleito)
        if not aceito:
            print(f"[FALLBACK] Eleição falhou — reiniciando descoberta em {backoff}s")
            time.sleep(backoff)
            continue

        # Aguarda até o master ser invalidado (queda detectada pelo heartbeat)
        evento_master_eleito.wait()


def invalidar_master():
    """Chamada pelo heartbeat quando o Master cai. Força nova descoberta."""
    print("[FALLBACK] Master inválido — reiniciando descoberta...")
    with lock_master:
        master_atual["name"] = None
        master_atual["ip"]   = None
        master_atual["port"] = None
    evento_master_eleito.clear()


# ══════════════════════════════════════════════════════════════════════════════
# SPRINT 1 — Heartbeat
# ══════════════════════════════════════════════════════════════════════════════
def enviar_heartbeat():
    """Verifica se o Master está ativo. Invalida cache em caso de falha."""
    with lock_master:
        ip   = master_atual["ip"]
        port = master_atual["port"]
        name = master_atual["name"]

    if not ip:
        return   # Ainda sem master eleito

    payload  = {"SERVER_UUID": WORKER_UUID, "TASK": "HEARTBEAT"}
    mensagem = json.dumps(payload) + "\n"

    try:
        with socket.create_connection((ip, port), timeout=TCP_TIMEOUT) as s:
            s.sendall(mensagem.encode("utf-8"))
            resposta = receber_json_tcp(s)
            if resposta.get("RESPONSE") == "ALIVE":
                print(f"[HEARTBEAT] Status: ALIVE — {name} respondeu")
            else:
                print(f"[HEARTBEAT] Resposta inesperada: {resposta}")
    except (ConnectionRefusedError, socket.timeout, OSError):
        print(f"[HEARTBEAT] Status: OFFLINE — {name} não respondeu")
        invalidar_master()


def loop_heartbeat():
    print(f"[WORKER] Heartbeat iniciado — intervalo: {HEARTBEAT_INTERVAL}s")
    while True:
        # Aguarda master ser eleito antes de iniciar heartbeat
        evento_master_eleito.wait()
        enviar_heartbeat()
        time.sleep(HEARTBEAT_INTERVAL)


# ══════════════════════════════════════════════════════════════════════════════
# SPRINT 2 — Ciclo de tarefas
# ══════════════════════════════════════════════════════════════════════════════
def processar_tarefa(user: str) -> str:
    tempo = random.uniform(0.5, 2.0)
    print(f"[TAREFA] Processando tarefa de '{user}' ({tempo:.1f}s)...")
    time.sleep(tempo)
    resultado = "NOK" if random.random() < 0.1 else "OK"
    print(f"[TAREFA] Resultado: {resultado}")
    return resultado


def ciclo_tarefa():
    """Ciclo completo: apresentação → QUERY/NO_TASK → STATUS → ACK."""
    with lock_master:
        ip   = master_atual["ip"]
        port = master_atual["port"]

    if not ip:
        return   # Ainda sem master eleito

    try:
        with socket.create_connection((ip, port), timeout=TCP_TIMEOUT) as s:
            # Apresentação
            apresentacao = {"WORKER": "ALIVE", "WORKER_UUID": WORKER_UUID}
            if MASTER_ORIGEM_UUID:
                apresentacao["SERVER_UUID"] = MASTER_ORIGEM_UUID
            enviar_json_tcp(s, apresentacao)

            # Recebe tarefa ou NO_TASK
            resposta = receber_json_tcp(s)
            task     = resposta.get("TASK")

            if task == "NO_TASK":
                print("[CICLO] Sem tarefas no momento")
                return

            if task != "QUERY":
                print(f"[CICLO] Resposta inesperada: {resposta}")
                return

            user   = resposta.get("USER", "?")
            status = processar_tarefa(user)

            # Reporta status
            enviar_json_tcp(s, {
                "STATUS"     : status,
                "TASK"       : "QUERY",
                "WORKER_UUID": WORKER_UUID
            })

            # Aguarda ACK
            ack = receber_json_tcp(s)
            if ack.get("STATUS") == "ACK":
                print(f"[CICLO] ACK recebido — ciclo concluído ✓")

    except (ConnectionRefusedError, socket.timeout, OSError) as e:
        print(f"[CICLO] Falha de conexão: {e}")
        invalidar_master()
    except json.JSONDecodeError as e:
        print(f"[CICLO] JSON inválido do Master: {e}")


def loop_tarefas():
    print(f"[WORKER] Loop de tarefas iniciado — intervalo: {TASK_INTERVAL}s")
    while True:
        evento_master_eleito.wait()
        ciclo_tarefa()
        time.sleep(TASK_INTERVAL)


# ─── Entry-point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"[WORKER] Worker '{WORKER_UUID}' iniciando sem IP pré-configurado...")
    if MASTER_ORIGEM_UUID:
        print(f"[WORKER] Modo: EMPRESTADO (Master de origem: {MASTER_ORIGEM_UUID})")
    else:
        print(f"[WORKER] Modo: LOCAL")

    # Thread de descoberta + eleição (Sprint 2.1) — roda continuamente
    threading.Thread(target=loop_descoberta, daemon=True).start()

    # Thread de heartbeat (Sprint 1)
    threading.Thread(target=loop_heartbeat, daemon=True).start()

    # Thread de ciclo de tarefas (Sprint 2)
    threading.Thread(target=loop_tarefas, daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[WORKER] Encerrado pelo usuário.")