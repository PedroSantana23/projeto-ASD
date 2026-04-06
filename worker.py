import socket
import uuid
import json
import time

# Configurações do servidor (master)
MASTER_HOST = '10.62.206.208'  # IP do servidor
MASTER_PORT = 8000             # Porta do servidor

# UUID do worker
WORKER_UUID = str(uuid.uuid4())

# UUID do servidor original (se aplicável)
SERVER_UUID = None  # Altere para um UUID válido se o worker estiver emprestado

def conectar_ao_master():
    try:
        # Criar socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((MASTER_HOST, MASTER_PORT))
        print(f"[INFO] Conectado ao master {MASTER_HOST}:{MASTER_PORT}")

        # Construir payload
        if SERVER_UUID:
            payload = {
                "WORKER": "ALIVE",
                "WORKER_UUID": WORKER_UUID,
                "SERVER_UUID": SERVER_UUID
            }
        else:
            payload = {
                "WORKER": "ALIVE",
                "WORKER_UUID": WORKER_UUID
            }

        # Enviar payload
        s.sendall(json.dumps(payload).encode('utf-8'))
        print(f"[INFO] Payload enviado: {payload}")

        # Aguardar resposta do servidor (opcional)
        response = s.recv(1024)
        if response:
            print(f"[RESPOSTA DO MASTER]: {response.decode('utf-8')}")

    except Exception as e:
        print(f"[ERRO] Falha ao conectar ao master: {e}")
    finally:
        s.close()
        print("[INFO] Conexão encerrada.")

if __name__ == "__main__":
    while True:
        conectar_ao_master()
        time.sleep(10)  # Intervalo entre conexões