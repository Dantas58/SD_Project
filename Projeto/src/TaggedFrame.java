import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.locks.ReentrantLock;

public class TaggedFrame {
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private ReentrantLock sendLock;
    private ReentrantLock receiveLock;


    public static class Frame {
        public final byte[] code;
        public final int memory;
        public final int tag;
        public String command;
        public String optional;

        public Frame(String command, int tag, byte[] code, int memory, String optional) {
            this.command = command;
            this.tag = tag;
            this.code = code;
            this.memory = memory;
            this.optional = optional;
        }

        public int getMemory(){
            return this.memory;
        }

        public int getTag(){
            return this.tag;
        }

        public String getCommand(){
            return this.command;
        }

        public String getOptional(){
            return this.optional;
        }

        public byte[] getCode(){
            return this.code;
        }
    }

    public TaggedFrame (Socket socket) throws IOException, IOException {
        this.socket = socket;
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
        this.sendLock = new ReentrantLock();
        this.receiveLock = new ReentrantLock();
    }

    public void send_id_TaggedFrame(int id_TaggedFrame) throws IOException {
        sendLock.lock();
        try {
            out.writeInt(id_TaggedFrame);
            out.flush();
        } finally {
            sendLock.unlock();
        }
    }

    public int receive_id_TaggedFrame() throws IOException {
        receiveLock.lock();
        try {
            int id = in.readInt();
            return id;
        } finally {
            receiveLock.unlock();
        }
    }


    public void send(Frame frame) throws IOException {
        send(frame.command, frame.tag, frame.code, frame.memory, frame.optional);
    }
    
    public void send(String command, int tag, byte[] code, int memory, String optional) throws IOException {
        sendLock.lock();
        try {
            out.writeUTF(command);
            out.writeInt(tag);
            out.writeInt(code.length);
            out.write(code);
            out.writeInt(memory);
            out.writeUTF(optional);
            out.flush();
        } finally {
            sendLock.unlock();
        }
    }

    public Frame receive() throws IOException {
        receiveLock.lock();
        try {
            String command = in.readUTF();
            int tag = in.readInt();
            int codeLength = in.readInt();
            byte[] code = new byte[codeLength];
            in.readFully(code);
            int memory = in.readInt();
            String optional = in.readUTF();
            return new Frame(command, tag, code, memory, optional);
        } finally {
            receiveLock.unlock();
        }
    }

    public void close() throws IOException {
        receiveLock.lock();
        sendLock.lock();
        try {
            in.close();
            out.close();
            socket.close();
        } finally {
            receiveLock.unlock();
            sendLock.unlock();
        }
    }

}

