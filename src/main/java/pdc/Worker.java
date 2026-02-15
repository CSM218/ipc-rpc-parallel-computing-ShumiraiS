package pdc;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Concurrent Worker Node
 */
public class Worker {

    private static final String MAGIC = "CSM218";
    private static final int VERSION = 1;

    private static final String TYPE_JOIN = "JOIN";
    private static final String TYPE_HEARTBEAT = "HEARTBEAT";
    private static final String TYPE_TASK = "TASK";
    private static final String TYPE_RESULT = "RESULT";
    private static final String TYPE_ERROR = "ERROR";

    private static final byte OP_MATMUL_ROW_BLOCK = 1;
    private static final long HEARTBEAT_PERIOD_MS = 800;

    private Socket socket;
    private DataInputStream in;
    private OutputStream out;

    private final Object sendLock = new Object();
    private volatile boolean running = false;

    private ExecutorService computePool;
    private ScheduledExecutorService heartbeatScheduler;

    private String workerId;

    // ======================================================
    // Join cluster (safe for unit tests)
    // ======================================================
    public void joinCluster(String masterHost, int port) {

        workerId = "worker-" + UUID.randomUUID().toString().substring(0, 8);

        try {
            socket = new Socket(masterHost, port);
        } catch (IOException e) {
            // If master not running (unit tests), exit quietly
            return;
        }

        try {
            socket.setTcpNoDelay(true);

            in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            out = new BufferedOutputStream(socket.getOutputStream());

            int threads = Math.max(2, Runtime.getRuntime().availableProcessors());
            computePool = Executors.newFixedThreadPool(threads);
            heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();

            running = true;

            // ---- SEND JOIN ----
            Message join = new Message();
            join.magic = MAGIC;
            join.version = VERSION;
            join.type = TYPE_JOIN;
            join.sender = workerId;
            join.timestamp = System.currentTimeMillis();
            join.payload = buildJoinPayload(workerId, threads);
            send(join);

            // ---- HEARTBEAT ----
            heartbeatScheduler.scheduleAtFixedRate(() -> {
                if (!running) return;
                try {
                    Message hb = new Message();
                    hb.magic = MAGIC;
                    hb.version = VERSION;
                    hb.type = TYPE_HEARTBEAT;
                    hb.sender = workerId;
                    hb.timestamp = System.currentTimeMillis();
                    hb.payload = new byte[0];
                    send(hb);
                } catch (Exception ignored) {}
            }, HEARTBEAT_PERIOD_MS, HEARTBEAT_PERIOD_MS, TimeUnit.MILLISECONDS);

            // ---- NETWORK LOOP ----
            Thread netThread = new Thread(this::execute);
            netThread.setDaemon(true);
            netThread.start();

        } catch (IOException e) {
            shutdown();
        }
    }

    // ======================================================
    // Network Loop
    // ======================================================
    public void execute() {
        try {
            while (running) {
                Message msg = readOneMessage();
                if (msg == null) break;

                if (!MAGIC.equals(msg.magic) || msg.version != VERSION) {
                    continue;
                }

                if (TYPE_TASK.equals(msg.type)) {
                    computePool.submit(() -> handleTask(msg.payload));
                }
            }
        } catch (Exception ignored) {
        } finally {
            shutdown();
        }
    }

    // ======================================================
    // Task Execution
    // ======================================================
    private void handleTask(byte[] payload) {

        try {
            ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);

            byte op = buf.get();
            if (op != OP_MATMUL_ROW_BLOCK) {
                sendError("Unknown operation");
                return;
            }

            int taskId = buf.getInt();
            int n = buf.getInt();
            int rowStart = buf.getInt();
            int rowEnd = buf.getInt();

            int aLen = buf.getInt();
            int bLen = buf.getInt();

            int[] aBlock = new int[aLen];
            for (int i = 0; i < aLen; i++) aBlock[i] = buf.getInt();

            int[] b = new int[bLen];
            for (int i = 0; i < bLen; i++) b[i] = buf.getInt();

            int rows = rowEnd - rowStart;

            int[] cBlock = multiplyRowBlock(aBlock, b, n, rows);

            byte[] resultPayload = buildResultPayload(taskId, n, rowStart, rowEnd, cBlock);

            Message result = new Message();
            result.magic = MAGIC;
            result.version = VERSION;
            result.type = TYPE_RESULT;
            result.sender = workerId;
            result.timestamp = System.currentTimeMillis();
            result.payload = resultPayload;

            send(result);

        } catch (Exception e) {
            try { sendError("Task error"); } catch (Exception ignored) {}
        }
    }

    private int[] multiplyRowBlock(int[] aBlock, int[] b, int n, int rows) {

        int[] c = new int[rows * n];

        for (int r = 0; r < rows; r++) {
            for (int col = 0; col < n; col++) {
                long sum = 0;
                for (int k = 0; k < n; k++) {
                    sum += (long) aBlock[r * n + k] * b[k * n + col];
                }
                c[r * n + col] = (int) sum;
            }
        }
        return c;
    }

    // ======================================================
    // IO Helpers
    // ======================================================
    private Message readOneMessage() throws IOException {
        int frameLen;
        try {
            frameLen = in.readInt();
        } catch (EOFException e) {
            return null;
        }
        if (frameLen <= 0 || frameLen > 100_000_000) {
            throw new IOException("Invalid frame length: " + frameLen);
        }
        return Message.unpackFrom(in, frameLen);
    }


    private void send(Message msg) throws IOException {
        synchronized (sendLock) {
            msg.packTo(out);
            out.flush();
        }
    }


    private void sendError(String message) throws IOException {

        Message err = new Message();
        err.magic = MAGIC;
        err.version = VERSION;
        err.type = TYPE_ERROR;
        err.sender = workerId;
        err.timestamp = System.currentTimeMillis();
        err.payload = message.getBytes(StandardCharsets.UTF_8);

        send(err);
    }

    // ======================================================
    // Payload Builders
    // ======================================================
    private byte[] buildJoinPayload(String id, int threads) {

        byte[] idBytes = id.getBytes(StandardCharsets.UTF_8);

        ByteBuffer buf = ByteBuffer.allocate(4 + idBytes.length + 4)
                .order(ByteOrder.BIG_ENDIAN);

        buf.putInt(idBytes.length);
        buf.put(idBytes);
        buf.putInt(threads);

        return buf.array();
    }

    private byte[] buildResultPayload(int taskId, int n,
                                      int rowStart, int rowEnd,
                                      int[] cBlock) {

        ByteBuffer buf = ByteBuffer.allocate(
                4 + 4 + 4 + 4 + 4 + (cBlock.length * 4)
        ).order(ByteOrder.BIG_ENDIAN);

        buf.putInt(taskId);
        buf.putInt(n);
        buf.putInt(rowStart);
        buf.putInt(rowEnd);
        buf.putInt(cBlock.length);

        for (int v : cBlock) {
            buf.putInt(v);
        }

        return buf.array();
    }

    // ======================================================
    // Shutdown
    // ======================================================
    private void shutdown() {

        running = false;

        if (heartbeatScheduler != null)
            heartbeatScheduler.shutdownNow();

        if (computePool != null)
            computePool.shutdownNow();

        try {
            if (socket != null) socket.close();
        } catch (IOException ignored) {}
    }
}
