import socket
import threading
import json
import time

# ─── Configurações ────────────────────────────────────────────────────────────
WORKER_UUID        = "Worker_A1"   # Identificador único deste Worker
MASTER_HOST        = "192.168.0.88"   # IP do Master  ← altere para o IP real
MASTER_PORT        = 8000
HEARTBEAT_INTERVAL = 30            # Segundos entre cada heartbeat
TIMEOUT            = 5             # Segundos máximos aguardando resposta


# ─── Envio do Heartbeat ───────────────────────────────────────────────────────
def enviar_heartbeat():
    """
    Abre uma conexão TCP com o Master, envia o payload de HEARTBEAT
    delimitado por '\\n' e aguarda a resposta ALIVE.
    Loga o status (ALIVE ou OFFLINE) e fecha a conexão.
    """
    payload = {
        "SERVER_UUID": WORKER_UUID,
        "TASK": "HEARTBEAT"
    }
    mensagem = json.dumps(payload) + "\n"

    try:
        with socket.create_connection((MASTER_HOST, MASTER_PORT), timeout=TIMEOUT) as s:
            s.sendall(mensagem.encode("utf-8"))

            # Lê a resposta (pode vir em pedaços)
            buffer = ""
            while "\n" not in buffer:
                chunk = s.recv(1024).decode("utf-8")
                if not chunk:
                    break
                buffer += chunk

            linha = buffer.split("\n")[0].strip()
            resposta = json.loads(linha)

            if resposta.get("RESPONSE") == "ALIVE":
                print(f"[HEARTBEAT] Status: ALIVE  — Master respondeu corretamente")
            else:
                print(f"[HEARTBEAT] Status: resposta inesperada → {resposta}")

    except (ConnectionRefusedError, socket.timeout, OSError):
        print(f"[HEARTBEAT] Status: OFFLINE — Tentando Reconectar")
    except json.JSONDecodeError as e:
        print(f"[HEARTBEAT] Erro ao parsear resposta do Master: {e}")


# ─── Loop de Heartbeat ────────────────────────────────────────────────────────
def loop_heartbeat():
    """Executa o heartbeat indefinidamente a cada HEARTBEAT_INTERVAL segundos."""
    print(f"[WORKER] Worker '{WORKER_UUID}' iniciado — heartbeat a cada {HEARTBEAT_INTERVAL}s")
    while True:
        enviar_heartbeat()
        time.sleep(HEARTBEAT_INTERVAL)


# ─── Entry-point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Roda o loop em uma thread daemon para não bloquear eventuais
    # expansões futuras no processo principal (Sprints 2, 3…)
    t = threading.Thread(target=loop_heartbeat, daemon=True)
    t.start()

    # Mantém o processo vivo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[WORKER] Encerrado pelo usuário.")