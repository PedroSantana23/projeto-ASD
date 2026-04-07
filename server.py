import socket
import time
import threading
import json
import os

# Configurações do servidor
HOST = socket.gethostbyname(socket.gethostname())
PORT = 8000

# Variáveis globais
is_master = False
heartbeat_interval = 5  # Intervalo entre heartbeats (em segundos)
heartbeat_failures = 0
max_failures = 4  # Número máximo de falhas antes de iniciar a eleição
workers = [
    "10.62.206.207:8000",
    "10.62.206.208:8000"
]  # Lista de workers conectados
master_addr = None  # Endereço do master atual


def get_local_ip():
    """Obtém o IP local da máquina."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


HOST = get_local_ip()
PORT = 8000


def tratar_cliente(conn, addr):
    """Função para tratar mensagens recebidas de clientes (workers)."""
    global is_master, master_addr
    try:
        print(f"[THREAD] Iniciando atendimento para {addr}")
        
        data = conn.recv(1024)
        if data:
            payload = json.loads(data.decode('utf-8'))
            print(f"[MENSAGEM de {addr}]: {payload}")

            if payload.get("type") == "heartbeat":
                # Responder ao heartbeat
                conn.sendall(b"ACK")
            elif payload.get("type") == "election":
                # Participar da eleição
                free_space = get_free_disk_space()
                response = {"type": "election_response", "free_space": free_space}
                conn.sendall(json.dumps(response).encode('utf-8'))
            elif payload.get("type") == "new_master":
                # Atualizar o master
                master_addr = payload.get("master_addr")
                print(f"[INFO] Novo master definido: {master_addr}")
        
    except Exception as e:
        print(f"[ERRO] Falha na conexão com {addr}: {e}")
    finally:
        print(f"[THREAD] Fechando conexão com {addr}")
        conn.close()


def iniciar_servidor():
    """Inicia o servidor para aceitar conexões."""
    global is_master, master_addr
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen()
    print(f"[SERVIDOR] Servidor iniciado em {HOST}:{PORT}")

    while True:
        conn, addr = s.accept()
        thread = threading.Thread(target=tratar_cliente, args=(conn, addr))
        thread.start()


def iniciar_heartbeat():
    """Função para monitorar o master via heartbeat."""
    global heartbeat_failures, is_master, master_addr
    while True:
        if not is_master and master_addr:
            try:
                with socket.create_connection(master_addr, timeout=2) as s:
                    payload = {"type": "heartbeat"}
                    s.sendall(json.dumps(payload).encode('utf-8'))
                    response = s.recv(1024)
                    if response.decode('utf-8') == "ACK":
                        print("[HEARTBEAT] Master está ativo")
                        heartbeat_failures = 0
            except Exception:
                heartbeat_failures += 1
                print(f"[HEARTBEAT] Falha na conexão com o master ({heartbeat_failures}/{max_failures})")
                if heartbeat_failures >= max_failures:
                    iniciar_eleicao()
        time.sleep(heartbeat_interval)


def iniciar_eleicao():
    """Inicia o processo de eleição para escolher um novo master."""
    global is_master, master_addr, workers
    print("[ELEIÇÃO] Iniciando processo de eleição...")
    free_space = get_free_disk_space()
    responses = []

    for worker in workers:
        try:
            with socket.create_connection((worker.split(":")[0], int(worker.split(":")[1])), timeout=2) as s:
                payload = {"type": "election"}
                s.sendall(json.dumps(payload).encode('utf-8'))
                response = json.loads(s.recv(1024).decode('utf-8'))
                responses.append((worker, response["free_space"]))
        except Exception as e:
            print(f"[ELEIÇÃO] Falha ao se comunicar com {worker}: {e}")

    # Determinar o novo master
    responses.append((f"{HOST}:{PORT}", free_space))  # Incluir o próprio worker na eleição
    new_master = max(responses, key=lambda x: x[1])[0]

    if new_master == f"{HOST}:{PORT}":
        is_master = True
        master_addr = (HOST, PORT)
        print("[ELEIÇÃO] Fui eleito como o novo master!")
    else:
        master_addr = (new_master.split(":")[0], int(new_master.split(":")[1]))
        print(f"[ELEIÇÃO] Novo master eleito: {master_addr}")

    # Notificar os workers sobre o novo master
    for worker in workers:
        try:
            with socket.create_connection((worker.split(":")[0], int(worker.split(":")[1])), timeout=2) as s:
                payload = {"type": "new_master", "master_addr": f"{master_addr[0]}:{master_addr[1]}"}
                s.sendall(json.dumps(payload).encode('utf-8'))
        except Exception as e:
            print(f"[ELEIÇÃO] Falha ao notificar {worker}: {e}")


def get_free_disk_space():
    """Retorna o espaço livre em disco em bytes."""
    statvfs = os.statvfs('/')
    return statvfs.f_frsize * statvfs.f_bavail


if __name__ == "__main__":
    # Iniciar o servidor em uma thread separada
    server_thread = threading.Thread(target=iniciar_servidor)
    server_thread.start()

    # Iniciar o heartbeat em uma thread separada
    heartbeat_thread = threading.Thread(target=iniciar_heartbeat)
    heartbeat_thread.start()