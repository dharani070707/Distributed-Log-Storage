package Consumer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class ConsumerClient {

    private static final String HOST = "localhost";
    private static final int PORT = 9092;

    public static void main(String[] args) throws Exception {

        long offset = 0; // start reading from offset 0

        // 1. Connect to broker
        Socket socket = new Socket(HOST, PORT);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(socket.getInputStream());

        // 2. Send FETCH request
        // totalLength = requestType (4) + offset (8)
        int totalLength = 4 + 8;

        out.writeInt(totalLength);
        out.writeInt(2);       // request type = FETCH
        out.writeLong(offset);
        out.flush();

        // 3. Read response
        int messageCount = in.readInt();

        for (int i = 0; i < messageCount; i++) {
            long msgOffset = in.readLong();
            int payloadLength = in.readInt();
            byte[] payload = new byte[payloadLength];
            in.readFully(payload);

            String message = new String(payload);
            System.out.println(
                "Consumed offset " + msgOffset + " -> " + message
            );

            // Update offset to next message
            offset = msgOffset + 1;
        }

        // 4. Close connection
        socket.close();
    }
}
