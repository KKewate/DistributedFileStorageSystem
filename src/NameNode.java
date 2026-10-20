import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * NameNode class acts as the central coordinator in the Distributed File System.
 * It manages metadata about file locations and coordinates communication with DataNode instances to handle client requests for reading and appending data.
 */
public class NameNode {
    private static final int MB = 4194304; // 4MB size for segmenting data
    private static final int NUM_DATANODES = 3; // DataNodes D1..D3
    private static final int BASE_DATANODE_PORT = 65530; // D1 -> 65530, D2 -> 65531, D3 -> 65532
    private static final Map<String, List<Pair>> fileToBlockMap = new HashMap<>(); // Map that stores the filename and the list of data blocks
    private static final Object mapLock = new Object(); // Lock for synchronizing access to the map
    private static final java.util.concurrent.atomic.AtomicInteger nextDataNode = new java.util.concurrent.atomic.AtomicInteger(0); // round-robin start node

    private ServerSocket serverSocket;
    private List<NameNodeHandler> handlers = new ArrayList<>();
    private volatile boolean running = true;

    public static void main(String[] args) {
        NameNode server = new NameNode();
        server.start(5558);
    }

    /**
     * Starts the NameNode server on the specified port.
     * 
     * @param port The port number on which the server will listen.
     */
    public void start(int port) {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("[DEBUG] NameNode started on port: " + port);
            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    NameNodeHandler handler = new NameNodeHandler(clientSocket);
                    handlers.add(handler);
                    handler.start();
                } catch (SocketException e) {
                    if (!running) {
                        System.out.println("[DEBUG] Server socket closed.");
                    } else {
                        throw e;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Initiates the shutdown process, closing all active connections and the server socket.
     */
    public void initiateShutdown() {
        running = false;
        try {
            serverSocket.close();
            for (NameNodeHandler handler : handlers) {
                handler.shutdown();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("[DEBUG] NameNode has been shut down.");
    }

    /**
     * Stops the NameNode server by closing the server socket.
     */
    public void stop() {
        initiateShutdown();
    }

    private class NameNodeHandler extends Thread {
        private final Socket clientSocket; // one socket per handler (must not be static/shared)

        public NameNodeHandler(Socket socket) {
            this.clientSocket = socket;
            System.out.println("[DEBUG] New client connection accepted: " + clientSocket.getRemoteSocketAddress());
        }

        public void run() {
            try (PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {

                String inputLine;
                while ((inputLine = in.readLine()) != null && running) {
                    System.out.println("[DEBUG] NameNode received: " + inputLine);
                    if (".".equals(inputLine)) {
                        break;
                    }

                    // Handle shutdown command
                    if ("shutdown".equalsIgnoreCase(inputLine.trim())) {
                        out.println("NameNode is shutting down.");
                        initiateShutdown();
                        break;
                    }

                    // Handle other commands (append, read, etc.)
                    NameNodeHandlerClient dataNodeClient = new NameNodeHandlerClient();
                    String[] tokens = inputLine.split(" ");
                    String filename;
                    if (tokens[0].equalsIgnoreCase("read") && tokens.length == 2) {
                        filename = tokens[1];
                        read(filename, dataNodeClient);
                    } else if (tokens[0].equalsIgnoreCase("append") && tokens.length >= 3) {
                        filename = tokens[1];
                        String content = inputLine.split(" ", 3)[2];
                        append(filename, content, dataNodeClient);
                    } else {
                        System.out.println("[DEBUG] NameNode ERROR: Failed to parse string in NameNode");
                        sendResponse("ERROR: Invalid command. Use READ <file> or APPEND <file> <text>");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    clientSocket.close();
                    System.out.println("[DEBUG] Closed client connection: " + clientSocket.getRemoteSocketAddress());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        /**
         * Shuts down the current handler by closing the client socket.
         */
        public void shutdown() {
            try {
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * Appends content to the specified file, distributing it across DataNodes.
         * 
         * @param filename The name of the file.
         * @param content The content to append.
         * @param dataNodeClient The client for communication with DataNodes.
         */
        public void append(String filename, String content, NameNodeHandlerClient dataNodeClient) {
            // Split the content into segments of at most MB characters (ceiling division)
            int blockCount = Math.max(1, (content.length() + MB - 1) / MB);
            List<String> contentSegments = new ArrayList<>();
            for (int i = 0; i < blockCount; i++) {
                int startIdx = Math.min(i * MB, content.length());
                int endIdx = Math.min((i + 1) * MB, content.length());
                contentSegments.add(content.substring(startIdx, endIdx));
            }

            List<Pair> blockList = new ArrayList<>();
            int blocksReceived = 0;
            // Start at a different DataNode for every append so files spread across all nodes
            int dataNodeSelector = nextDataNode.getAndIncrement();
            int failedAttempts = 0; // consecutive DataNodes that could not allocate a block

            while (blocksReceived < blockCount) {
                if (failedAttempts >= NUM_DATANODES) {
                    System.out.println("[DEBUG] NameNode ERROR: no DataNode could allocate a block");
                    sendResponse("ERROR: No DataNode has free space (or DataNodes are not running)");
                    return;
                }
                int node = dataNodeSelector % NUM_DATANODES;
                int port = BASE_DATANODE_PORT + node;
                String dataNodeId = "D" + (node + 1);
                dataNodeClient.startConnection("127.0.0.1", port);
                String blockIdStr = dataNodeClient.sendMessage("Alloc");
                dataNodeClient.stopConnection();

                int blockId = -1;
                try {
                    blockId = Integer.parseInt(blockIdStr == null ? "" : blockIdStr.trim());
                } catch (NumberFormatException e) {
                    // DataNode unreachable or sent a bad reply: treat as "no block"
                }

                if (blockId != -1) {
                    blockList.add(new Pair(dataNodeId, blockId));
                    String message = "Write " + blockId + " " + contentSegments.get(blocksReceived);
                    dataNodeClient.startConnection("127.0.0.1", port);
                    dataNodeClient.sendMessage(message);
                    dataNodeClient.stopConnection();
                    blocksReceived++;
                    failedAttempts = 0;
                } else {
                    failedAttempts++;
                }
                // Round-robin so blocks are spread across all DataNodes
                dataNodeSelector++;
            }

            synchronized (mapLock) {
                if (fileToBlockMap.containsKey(filename)) {
                    fileToBlockMap.get(filename).addAll(blockList);
                } else {
                    fileToBlockMap.put(filename, blockList);
                }
            }
            sendResponse("COMPLETE");
        }

        /**
         * Reads the content of the specified file by retrieving data from the appropriate DataNodes.
         * 
         * @param filename The name of the file.
         * @param dataNodeClient The client for communication with DataNodes.
         */
        public void read(String filename, NameNodeHandlerClient dataNodeClient) {
            List<Pair> blockList;
            synchronized (mapLock) {
                // copy so we don't iterate while another client appends to the same file
                blockList = new ArrayList<>(fileToBlockMap.getOrDefault(filename, new ArrayList<>()));
            }
            List<String> contentSegments = new ArrayList<>();

            for (Pair block : blockList) {
                int port = BASE_DATANODE_PORT + (Integer.parseInt(block.getDataNodeId().substring(1)) - 1);
                dataNodeClient.startConnection("127.0.0.1", port);
                String blockContent = dataNodeClient.sendMessage("Read " + block.getBlockNumber());
                dataNodeClient.stopConnection();
                contentSegments.add(blockContent == null ? "" : blockContent);
            }

            String fullContent = String.join(" ", contentSegments);
            System.out.println("Output: " + fullContent);
            sendResponse(fullContent);
        }

        /**
         * Sends a response back to the client.
         * 
         * @param message The message to send.
         */
        private void sendResponse(String message) {
            try {
                // One line per response, connection stays open so the client can send more commands.
                // (Do not close the writer here: that would close the client socket.)
                PrintWriter responseWriter = new PrintWriter(clientSocket.getOutputStream(), true);
                responseWriter.println(message);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * NameNodeHandlerClient class handles communication with DataNode instances.
         */
        public class NameNodeHandlerClient {
            private Socket clientSocket;
            private PrintWriter out;
            private BufferedReader in;

            /**
             * Starts a connection to the specified DataNode.
             * 
             * @param ip The IP address of the DataNode.
             * @param port The port number of the DataNode.
             */
            public void startConnection(String ip, int port) {
                try {
                    clientSocket = new Socket(ip, port);
                    out = new PrintWriter(clientSocket.getOutputStream(), true);
                    in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            /**
             * Sends a message to the DataNode and returns the response.
             * 
             * @param msg The message to send.
             * @return The response from the DataNode.
             */
            public String sendMessage(String msg) {
                try {
                    out.println(msg);
                    return in.readLine();
                } catch (SocketException e) {
                    System.err.println("Connection reset by server. Server might be down.");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return "";
            }

            /**
             * Stops the connection to the DataNode.
             */
            public void stopConnection() {
                try {
                    in.close();
                    out.close();
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
