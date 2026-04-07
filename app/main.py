import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment the code below to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    connection, _ = server_socket.accept() # wait for client
    try:
        while True:
            data = connection.recv(4096)  
            if not data:                  # if response is empty bytes = client disconnected
                break
            connection.send(b"+PONG\r\n")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
