package redis;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

public class Server {

    private static final int PORT = 6379;

    public static void main(String[] args) throws IOException {

        ServerSocket serverSocket = new ServerSocket(PORT);
        serverSocket.setReuseAddress(true);

        System.out.println("Redis server started on port " + PORT);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("Client connected: " +
                clientSocket.getInetAddress());
            handleClient(clientSocket);
        }
    }

    private static void handleClient(Socket clientSocket) {
        try {
            // Create a parser for this client's input stream
            RespParser parser = new RespParser(
                clientSocket.getInputStream()
            );

            // Get the output stream to send responses back
            OutputStream output = clientSocket.getOutputStream();

            while (true) {

                // Parse one complete command
                // Returns ["PING"] or ["SET", "name", "Atharva"] etc
                List<String> command = parser.parse();

                // null means client disconnected
                if (command == null) {
                    System.out.println("Client disconnected");
                    break;
                }

                // Print what we received — nicely formatted
                System.out.println("Command: " + command);

                // Get the command name — always first element
                // toUpperCase so PING and ping both work
                String commandName = command.get(0).toUpperCase();

                // Handle commands
                byte[] response;

                if (commandName.equals("PING")) {
                    // PING with no argument → PONG
                    // PING "hello" → "hello"
                    if (command.size() == 1) {
                        response = RespSerializer.simpleString("PONG");
                    } else {
                        response = RespSerializer.bulkString(command.get(1));
                    }
                } else if (commandName.equals("ECHO")) {
                    // ECHO just returns its argument
                    response = RespSerializer.bulkString(command.get(1));
                } else {
                    // Unknown command
                    response = RespSerializer.error(
                        "unknown command '" + command.get(0) + "'"
                    );
                }

                output.write(response);
                output.flush();
            }

        } catch (IOException e) {
                if (!e.getMessage().contains("Connection reset")) {
                 System.out.println("Client error: " + e.getMessage());
            }

        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.out.println("Error closing: " + e.getMessage());
            }
        }
    }
}