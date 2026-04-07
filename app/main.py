import socket  # noqa: F401
import threading


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment the code below to pass the first stage
    '''
    Return type: list of str
    '''
    def resp_to_string(data): 
        parts = data.decode().split('\r\n')
        res = []
        
        for i in range(len(parts)):
            # If the part starts with $, the next part is our actual string
            if parts[i].startswith('$'):
                res.append(parts[i+1])
                
        return res

    def handle_connection(conn):
        try:
            while True:
                data = resp_to_string(conn.recv(2048))
                if not data:
                    break
                
                response = ""
                if data[0] == "ECHO":
                    for i in range(1, len(data)):
                        word = data[i]
                        response += f"${len(word)}\r\n{word}\r\n"
                    conn.sendall(response.encode())
                elif data[0] == "PING":
                    response = "+PONG\r\n"
                    conn.sendall(response.encode())
        finally:
            conn.close()


    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    
    try:
        while True:
            conn, _ = server_socket.accept() # wait for client     
            threading.Thread(target=handle_connection, args=(conn,)).start()   
    finally:
        server_socket.close()


if __name__ == "__main__":
    main()
