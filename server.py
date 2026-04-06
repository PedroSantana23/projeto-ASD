import socket
import time
import threading
import json  # Faltava este import

HOST = '10.62.206.208'
PORT = 8000

def tratar_cliente(conn, addr):
    try:
        print(f"[THREAD] Iniciando atendimento para {addr}")
        
        data = conn.recv(1024)
        if data:
            payload = json.loads(data.decode('utf-8'))
            print(f"[MENSAGEM de {addr}]: {payload}")

            # Responder ao worker (opcional)
            conn.sendall(b"Payload recebido com sucesso!")
        
    except Exception as e:
        print(f"[ERRO] Falha na conexão com {addr}: {e}")
    finally:
        print(f"[THREAD] Fechando conexão com {addr}")
        conn.close()

def iniciar_servidor():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        s.bind((HOST, PORT))
        s.listen(100)
        print(f"Servidor ativo em {HOST}:{PORT}. Aguardando conexões...")

        while True:
            conn, addr = s.accept()
            print(f"\n[NOVA SESSÃO] Conectado por: {addr}")

            cliente_thread = threading.Thread(target=tratar_cliente, args=(conn, addr))
            cliente_thread.start()
            
            print(f"[INFO] Thread disparada. Servidor livre para nova conexão.")
    finally:
        s.close()

# Adicionar chamada da função principal
if __name__ == "__main__":
    iniciar_servidor()