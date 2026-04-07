import socket
import uuid
import json
import time
import os

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Conecta a um endereço externo para descobrir o IP local
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip

WORKER_HOST = get_local_ip()  # Obtém o IP local automaticamente
WORKER_PORT = 8001  # Porta do worker

MASTER_HOST = "10.62.206.207"  # IP inicial do master
MASTER_PORT = 8000  # Porta do master

heartbeat_failures = 0
max_failures = 4  # Número máximo de falhas antes de iniciar a eleição
is_master = False  # Indica se este worker é o master
master_addr = (MASTER_HOST, MASTER_PORT)  # Endereço do master atual


def conectar_ao_master():
    """Envia um heartbeat ao master para verificar sua disponibilidade."""
    global heartbeat_failures, master_addr
    try:
        # Criar socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(master_addr)
        print(f"[INFO] Conectado ao master {master_addr}")

        # Construir payload
        payload = {
            "type": "heartbeat",
            "worker_host": WORKER_HOST,
            "worker_port": WORKER_PORT
        }

        # Enviar payload
        s.sendall(json.dumps(payload).encode('utf-8'))

        # Aguardar resposta do master
        response = s.recv(1024)
        if response.decode('utf-8') == "ACK":
            print("[INFO] Master respondeu ao heartbeat.")
            heartbeat_failures = 0  # Resetar contador de falhas

    except Exception as e:
        heartbeat_failures += 1
        print(f"[ERRO] Falha ao conectar ao master ({heartbeat_failures}/{max_failures}): {e}")

        # Iniciar eleição se o número de falhas for excedido
        if heartbeat_failures >= max_failures:
            iniciar_eleicao()

    finally:
        s.close()


def iniciar_eleicao():
    """Inicia o processo de eleição para escolher um novo master."""
    global is_master, master_addr
    print("[ELEIÇÃO] Iniciando processo de eleição...")
    free_space = get_free_disk_space()
    responses = []

    # Enviar mensagem de eleição para outros workers
    for worker in get_workers():
        try:
            with socket.create_connection(worker, timeout=2) as s:
                payload = {"type": "election"}
                s.sendall(json.dumps(payload).encode('utf-8'))
                response = json.loads(s.recv(1024).decode('utf-8'))
                responses.append((worker, response["free_space"]))
        except Exception as e:
            print(f"[ELEIÇÃO] Falha ao se comunicar com {worker}: {e}")

    # Determinar o novo master
    responses.append((f"{WORKER_HOST}:{WORKER_PORT}", free_space))  # Incluir o próprio worker na eleição
    new_master = max(responses, key=lambda x: x[1])[0]

    if new_master == f"{WORKER_HOST}:{WORKER_PORT}":
        is_master = True
        master_addr = (WORKER_HOST, WORKER_PORT)
        print("[ELEIÇÃO] Fui eleito como o novo master!")
    else:
        master_addr = (new_master.split(":")[0], int(new_master.split(":")[1]))
        print(f"[ELEIÇÃO] Novo master eleito: {master_addr}")

    # Notificar os outros workers sobre o novo master
    if is_master:
        for worker in get_workers():
            try:
                with socket.create_connection(worker, timeout=2) as s:
                    payload = {"type": "new_master", "master_addr": f"{master_addr[0]}:{master_addr[1]}"}
                    s.sendall(json.dumps(payload).encode('utf-8'))
            except Exception as e:
                print(f"[ELEIÇÃO] Falha ao notificar {worker}: {e}")


def get_free_disk_space():
    """Retorna o espaço livre em disco em bytes."""
    statvfs = os.statvfs('/')
    return statvfs.f_frsize * statvfs.f_bavail


def get_workers():
    """Retorna a lista de workers conectados."""
    # Aqui você pode implementar uma lógica para obter a lista de workers dinamicamente.
    # Por enquanto, vamos usar uma lista fixa.
    return [
        "10.62.206.208:8001",
        "10.62.206.209:8001"
    ]


def tratar_mensagens():
    """Escuta mensagens de outros workers ou do master."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((WORKER_HOST, WORKER_PORT))
    s.listen()
    print(f"[WORKER] Escutando em {WORKER_HOST}:{WORKER_PORT}")

    while True:
        conn, addr = s.accept()
        data = conn.recv(1024)
        if data:
            payload = json.loads(data.decode('utf-8'))
            if payload["type"] == "election":
                # Responder à eleição
                free_space = get_free_disk_space()
                response = {"type": "election_response", "free_space": free_space}
                conn.sendall(json.dumps(response).encode('utf-8'))
            elif payload["type"] == "new_master":
                # Atualizar o master
                global master_addr
                master_addr = tuple(payload["master_addr"].split(":"))
                print(f"[INFO] Novo master definido: {master_addr}")
        conn.close()


if __name__ == "__main__":
    # Iniciar thread para escutar mensagens
    os.threading.Thread(target=tratar_mensagens, daemon=True).start()

    # Loop principal para enviar heartbeats
    while True:
        if not is_master:
            conectar_ao_master()
        time.sleep(os.heartbeat_interval)