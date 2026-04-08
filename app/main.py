import socket  # noqa: F401
import threading
import time
import hiredis

lock = threading.Lock()
database = {}

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment the code below to pass the first stage
    '''
    Return type: list of str
    '''
    # def resp_to_string(data): 
    #     parts = data.decode().split('\r\n')
    #     res = []
        
    #     for i in range(len(parts)):
    #         # If the part starts with $, the next part is our actual string
    #         if parts[i].startswith('$'):
    #             res.append(parts[i+1])
                
    #     return res
    
    def resp_to_string(data):
        reader = hiredis.Reader()
        reader.feed(data)
        parsed = reader.gets()

        if not isinstance(parsed, list):
            parsed = [parsed] if parsed is not None else []

        # Convert to string ONLY if it's bytes; otherwise, just stringify it
        return [x.decode('utf-8') if isinstance(x, bytes) else str(x) for x in parsed]
    
    # def string_to_resp_bulk_string(data):
    #     data_str = str(data)
    #     length = len(data_str)
    #     return f"${length}\r\n{data_str}\r\n".encode()
    
    def to_resp(data, target_type='bulk'):
        # Ensure we are working with strings for the content
        # RESP is binary-safe, so we ultimately return bytes
        CRLF = b"\r\n"

        if data is None:
            if target_type == 'array': return b"*-1\r\n"
            return b"$-1\r\n"

        match target_type.lower():
            case 'simple':
                # Format: +<string>\r\n
                return f"+{data}".encode('utf-8') + CRLF

            case 'error':
                # Format: -<string>\r\n
                return f"-{data}".encode('utf-8') + CRLF

            case 'int':
                # Format: :<number>\r\n
                return f":{int(data)}".encode('utf-8') + CRLF

            case 'bulk':
                # Format: $<length>\r\n<data>\r\n
                encoded_data = str(data).encode('utf-8')
                return f"${len(encoded_data)}".encode('utf-8') + CRLF + encoded_data + CRLF

            case 'array':
                # Format: *<count>\r\n<elements...>
                # This recursively calls to_resp for each item in the list
                if not isinstance(data, list):
                    data = [data]
                
                header = f"*{len(data)}".encode('utf-8') + CRLF
                # We default nested elements to 'bulk' as that is standard Redis behavior
                body = b"".join([to_resp(item, 'bulk') for item in data])
                return header + body

            case _:
                raise ValueError(f"Unknown RESP type: {target_type}")

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
                                        response = to_resp(response, "bulk")
                                    else:
                                        response = "$-1\r\n".encode()
                                else:
                                    response += database[key][0]
                                    response = to_resp(response, "bulk")
                            else:
                                response = "$-1\r\n".encode()
                            lock.release() # "I'm going in, nobody else allowed, release this thread"
                        finally:
                            conn.sendall(response)
                    case "RPUSH":
                        try:
                            key = data[1]
                            lock.acquire()
                            for i in range(2, len(data)):
                                if key in database:
                                    database[key].append(data[i])
                                else:
                                    database[key] = [data[i]]
                            response = f":{len(database[key])}\r\n".encode()
                            lock.release()
                        finally:
                            conn.sendall(response)
                    case "LRANGE":
                        try:
                            key = data[1]
                            start = int(data[2])
                            stop = int(data[3])
                            if key not in database or start >= len(database[key]) or start >= stop: 
                                response = "*0\r\n".encode()
                            else:
                                print(stop)
                                if stop >= len(database[key]): 
                                    stop = len(database) - 1
                                print(start)
                                print(stop)
                                    
                                response = to_resp(database[key][start:stop+1], "array")
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
