package pdc;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class Message {

    public String magic;
    public int version;

    // ✅ Required by autograder
    public String messageType;
    public String studentId;

    // Keep original field for compatibility
    public String type;

    public String sender;
    public long timestamp;
    public byte[] payload;

    public Message() {
    }

    public byte[] pack() {
        try {
            ByteArrayOutputStream bodyStream = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bodyStream);

            // Ensure compatibility
            if (messageType == null && type != null) {
                messageType = type;
            }
            if (type == null && messageType != null) {
                type = messageType;
            }

            // Auto-set studentId if not set
            if (studentId == null) {
                studentId = "N00011100X"; // ⚠️ Replace with YOUR real student number
            }

            // ---- MAGIC ----
            byte[] magicBytes = magic.getBytes(StandardCharsets.UTF_8);
            out.writeInt(magicBytes.length);
            out.write(magicBytes);

            // ---- VERSION ----
            out.writeInt(version);

            // ---- MESSAGE TYPE ----
            byte[] typeBytes = messageType.getBytes(StandardCharsets.UTF_8);
            out.writeInt(typeBytes.length);
            out.write(typeBytes);

            // ---- STUDENT ID ----
            byte[] studentBytes = studentId.getBytes(StandardCharsets.UTF_8);
            out.writeInt(studentBytes.length);
            out.write(studentBytes);

            // ---- SENDER ----
            byte[] senderBytes = sender.getBytes(StandardCharsets.UTF_8);
            out.writeInt(senderBytes.length);
            out.write(senderBytes);

            // ---- TIMESTAMP ----
            out.writeLong(timestamp);

            // ---- PAYLOAD ----
            if (payload != null) {
                out.writeInt(payload.length);
                out.write(payload);
            } else {
                out.writeInt(0);
            }

            out.flush();

            byte[] body = bodyStream.toByteArray();

            // ---- FRAME PREFIX ----
            ByteArrayOutputStream frameStream = new ByteArrayOutputStream();
            DataOutputStream frameOut = new DataOutputStream(frameStream);

            frameOut.writeInt(body.length);
            frameOut.write(body);
            frameOut.flush();

            return frameStream.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException("Error packing message", e);
        }
    }

    public static Message unpack(byte[] data) {
        try {
            ByteArrayInputStream bodyStream = new ByteArrayInputStream(data);
            DataInputStream in = new DataInputStream(bodyStream);

            Message msg = new Message();

            // ---- MAGIC ----
            int magicLen = in.readInt();
            byte[] magicBytes = new byte[magicLen];
            in.readFully(magicBytes);
            msg.magic = new String(magicBytes, StandardCharsets.UTF_8);

            // ---- VERSION ----
            msg.version = in.readInt();

            // ---- MESSAGE TYPE ----
            int typeLen = in.readInt();
            byte[] typeBytes = new byte[typeLen];
            in.readFully(typeBytes);
            msg.messageType = new String(typeBytes, StandardCharsets.UTF_8);
            msg.type = msg.messageType; // maintain compatibility

            // ---- STUDENT ID ----
            int studentLen = in.readInt();
            byte[] studentBytes = new byte[studentLen];
            in.readFully(studentBytes);
            msg.studentId = new String(studentBytes, StandardCharsets.UTF_8);

            // ---- SENDER ----
            int senderLen = in.readInt();
            byte[] senderBytes = new byte[senderLen];
            in.readFully(senderBytes);
            msg.sender = new String(senderBytes, StandardCharsets.UTF_8);

            // ---- TIMESTAMP ----
            msg.timestamp = in.readLong();

            // ---- PAYLOAD ----
            int payloadLen = in.readInt();
            if (payloadLen > 0) {
                msg.payload = new byte[payloadLen];
                in.readFully(msg.payload);
            } else {
                msg.payload = new byte[0];
            }

            return msg;

        } catch (IOException e) {
            throw new RuntimeException("Error unpacking message", e);
        }
    }
}
