// Broker will:
// Listen on port 9092
// Accept TCP connections
// Read requests
// Append / fetch from file
package Broker;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicLong;

public class BrokerServer {

    private static final int PORT = 9092;
    private static final String LOG_FILE = "data/orders.log";

    // Global offset counter (single partition)
    private static final AtomicLong nextOffset = new AtomicLong(0);

    public static void main(String[] args) throws Exception {
        //creates if not exist
        Files.createDirectories(Paths.get("data"));

        // Recover offset on restart
        recoverOffset();

        ServerSocket serverSocket = new ServerSocket(PORT);
        System.out.println("Broker started on port " + PORT);

        while (true) {
            Socket socket = serverSocket.accept();
            new Thread(() -> handleClient(socket)).start();
        }
    }

    private static void handleClient(Socket socket) {
        try (
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream())
        ) {
            while (true) {
                int totalLength;
                try {
                    totalLength = in.readInt();
                } catch (EOFException e) {
                    break; // client closed connection
                }

                int requestType = in.readInt();

                if (requestType == 1) {
                    handleProduce(in, out);
                } else if (requestType == 2) {
                    handleFetch(in, out);
                } else {
                    System.out.println("Unknown request type");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ================= PRODUCE =================
    private static synchronized void handleProduce(DataInputStream in, DataOutputStream out)
            throws IOException {

        int payloadLength = in.readInt();
        byte[] payload = new byte[payloadLength];
        in.readFully(payload);

        long offset = nextOffset.getAndIncrement();

        try (DataOutputStream fileOut =
                     new DataOutputStream(
                         new BufferedOutputStream(
                             new FileOutputStream(LOG_FILE, true)))) {

            fileOut.writeInt(payloadLength);
            fileOut.writeLong(offset);
            fileOut.write(payload);
            fileOut.flush();
        }

        // Send ACK
        out.writeLong(offset);
        out.flush();

        System.out.println("Stored message at offset " + offset);
    }

    // ================= FETCH =================
    private static void handleFetch(DataInputStream in, DataOutputStream out)
            throws IOException {

        long fetchOffset = in.readLong();

        try (DataInputStream fileIn =
                     new DataInputStream(
                         new BufferedInputStream(
                             new FileInputStream(LOG_FILE)))) {

            int count = 0;
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream tempOut = new DataOutputStream(buffer);

            while (fileIn.available() > 0) {
                int payloadLength = fileIn.readInt();
                long offset = fileIn.readLong();
                byte[] payload = new byte[payloadLength];
                fileIn.readFully(payload);

                if (offset >= fetchOffset) {
                    tempOut.writeLong(offset);
                    tempOut.writeInt(payloadLength);
                    tempOut.write(payload);
                    count++;
                }
            }

            // Send response
            out.writeInt(count);
            out.write(buffer.toByteArray());
            out.flush();
        }
    }

    // ================= RECOVERY =================
    private static void recoverOffset() throws IOException {
        if (!Files.exists(Paths.get(LOG_FILE))) return;

        try (DataInputStream fileIn =
                     new DataInputStream(
                         new BufferedInputStream(
                             new FileInputStream(LOG_FILE)))) {

            long lastOffset = -1;

            while (fileIn.available() > 0) {
                int payloadLength = fileIn.readInt();
                long offset = fileIn.readLong();
                fileIn.skipBytes(payloadLength);
                lastOffset = offset;
            }

            nextOffset.set(lastOffset + 1);
            System.out.println("Recovered offset = " + nextOffset.get());
        }
    }
}
