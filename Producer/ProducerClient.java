package Producer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class ProducerClient {

    private static final String HOST = "localhost";
    private static final int PORT = 9092;

    public static void main(String[] args) throws Exception {

        // 1. Connect to broker
        Socket socket = new Socket(HOST, PORT);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(socket.getInputStream());

        // 2. Create payload (order data)
        String order = "orderId=101,status=CREATED,price=499.99";
        byte[] payload = order.getBytes();

        // 3. Compute total request length
        // requestType (4) + payloadLength (4) + payload
        int totalLength = 4 + 4 + payload.length;

        // 4. Send PRODUCE request
        out.writeInt(totalLength);   // total request size
        out.writeInt(1);             // request type = PRODUCE
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();

        // 5. Read ACK (offset)
        long offset = in.readLong();
        System.out.println("Message stored at offset: " + offset);

        // 6. Close connection
        socket.close();
    }
}
