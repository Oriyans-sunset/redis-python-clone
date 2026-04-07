import socket  # noqa: F401
import threading


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment the code below to pass the first stage

    def handle_connection(conn):
        try:
            while True:
                data = conn.recv(2048)
                print(data)
                if not data:
                    break
                conn.sendall(b"+PONG\r\n")
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
