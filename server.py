import socket
import threading
import json

# ─── Configurações ────────────────────────────────────────────────────────────
SERVER_UUID = "Master_A"   # Identificador único deste Master
HOST        = "0.0.0.0"    # Escuta em todas as interfaces
PORT        = 8000


def get_local_ip() -> str:
    """Retorna o IP local da máquina."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


# ─── Tratamento de cada conexão (roda em thread própria) ──────────────────────
def tratar_cliente(conn: socket.socket, addr):
    """
    Lê mensagens JSON delimitadas por '\\n' vindas de um Worker
    e responde conforme o protocolo da Sprint 1.
    """
    print(f"[SERVIDOR] Nova conexão de {addr}")
    buffer = ""

    try:
        while True:
            chunk = conn.recv(1024).decode("utf-8")
            if not chunk:
                # Conexão encerrada pelo cliente
                break

            buffer += chunk

            # Processa todas as mensagens completas no buffer
            while "\n" in buffer:
                linha, buffer = buffer.split("\n", 1)
                linha = linha.strip()
                if not linha:
                    continue

                try:
                    payload = json.loads(linha)
                except json.JSONDecodeError:
                    print(f"[ERRO] JSON inválido recebido de {addr}: {linha!r}")
                    continue

                print(f"[RECEBIDO de {addr}]: {payload}")
                task = payload.get("TASK", "")

                # ── HEARTBEAT ────────────────────────────────────────────────
                if task == "HEARTBEAT":
                    resposta = {
                        "SERVER_UUID": SERVER_UUID,
                        "TASK": "HEARTBEAT",
                        "RESPONSE": "ALIVE"
                    }
                    conn.sendall((json.dumps(resposta) + "\n").encode("utf-8"))
                    print(f"[HEARTBEAT] Respondido ALIVE para {addr}")

                else:
                    print(f"[AVISO] TASK desconhecida '{task}' recebida de {addr} — ignorada")

    except Exception as e:
        print(f"[ERRO] Conexão com {addr} encerrada inesperadamente: {e}")
    finally:
        print(f"[SERVIDOR] Conexão encerrada com {addr}")
        conn.close()


# ─── Loop principal do servidor ───────────────────────────────────────────────
def iniciar_servidor():
    """Inicia o servidor TCP e despacha cada conexão para uma thread."""
    local_ip = get_local_ip()

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen()

    print(f"[SERVIDOR] Master '{SERVER_UUID}' escutando em {local_ip}:{PORT}")

    while True:
        conn, addr = srv.accept()
        t = threading.Thread(target=tratar_cliente, args=(conn, addr), daemon=True)
        t.start()


# ─── Entry-point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    iniciar_servidor()