import socket  # noqa: F401
import threading

lock = threading.Lock()
database = {}

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
    
    def string_to_resp_bulk_string(data):
        data_str = str(data)
        length = len(data_str)
        return f"${length}\r\n{data_str}\r\n".encode()

    def handle_connection(conn):
        try:
            while True:
                data = resp_to_string(conn.recv(2048))
                if not data:
                    break

                command = data[0]
                
                response = ""
                if command == "ECHO":
                    for i in range(1, len(data)):
                        word = data[i]
                        response += f"${len(word)}\r\n{word}\r\n"
                    conn.sendall(response.encode())
                elif command == "PING":
                    response = "+PONG\r\n"
                    conn.sendall(response.encode())
                elif command == "SET":
                    try:
                        lock.acquire() # "I'm going in, nobody else allowed, lock this thread"
                        database[data[1]] = data[2]
                        lock.release() # "I'm going in, nobody else allowed, release this thread"
                    finally:
                        conn.sendall(b"+OK\r\n")
                elif command == "GET":
                    try:
                        lock.acquire() # "I'm going in, nobody else allowed, lock this thread"
                        if data[1] in database: 
                            response += database[data[1]]
                            response = string_to_resp_bulk_string(response)
                        else:
                            response = "$-1\r\n".encode()
                        lock.release() # "I'm going in, nobody else allowed, release this thread"
                    finally:
                        conn.sendall(response)

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
