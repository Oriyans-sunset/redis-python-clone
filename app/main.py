import socket  # noqa: F401
import threading
import time

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
                match command: 
                    case "ECHO":
                        for i in range(1, len(data)):
                            word = data[i]
                            response += f"${len(word)}\r\n{word}\r\n"
                        conn.sendall(response.encode())
                    case "PING":
                        response = "+PONG\r\n"
                        conn.sendall(response.encode())
                    case "SET":
                        try:
                            lock.acquire() # "I'm going in, nobody else allowed, lock this thread"
                            if len(data) > 3 and data[3] == "PX":
                                database[data[1]] = (data[2], data[4], time.time())
                            else:
                                database[data[1]] = (data[2], None, None)
                            lock.release() # "I'm going in, nobody else allowed, release this thread"
                        finally:
                            conn.sendall(b"+OK\r\n")
                    case "GET":
                        key = data[1]
                        try:
                            lock.acquire() # "I'm going in, nobody else allowed, lock this thread"
                            if key in database: 
                                if database[key][1] != None: # there is a PX value, check time
                                    if abs(database[key][2] - time.time()) <= float(database[key][1])/1000: 
                                        response += database[key][0]
                                        response = string_to_resp_bulk_string(response)
                                    else:
                                        response = "$-1\r\n".encode()
                                else:
                                    response += database[key][0]
                                    response = string_to_resp_bulk_string(response)
                            else:
                                response = "$-1\r\n".encode()
                            lock.release() # "I'm going in, nobody else allowed, release this thread"
                        finally:
                            conn.sendall(response)
                    case "RPUSH":
                        try:
                            key = data[1]
                            lock.acquire()
                            if key in database:
                                database[key].append(data[2])
                            else:
                                database[key] = [data[2]]
                            response = f":{len(database[key])}\r\n".encode()
                            lock.release()
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
