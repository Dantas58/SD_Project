import sd23.JobFunction;
import sd23.JobFunctionException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

public class Workers {
    private int AVAILABLE_MEMORY;
    private Socket socket;
    private static final String HOST = "localhost";
    private static final int PORT = 9091;
    private DataOutputStream out;
    private DataInputStream in;
    TaggedFrame taggedFrame;
    private static final ExecutorService taskExecutorService = Executors.newFixedThreadPool(10);


    public Workers(Socket socket, int AVAILABLE_MEMORY) {
        this.socket = socket;
        this.AVAILABLE_MEMORY = AVAILABLE_MEMORY;
    }

    public Workers() {
    }

    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter Available Memory:");
        int memory = scanner.nextInt();
        Workers worker = new Workers();
        worker.setupServerConnection(memory);
        worker.handle_TaggedFrames();
    }


    private void setupServerConnection(int memory){
        try{
            socket = new Socket(HOST, PORT);
            out = new DataOutputStream(socket.getOutputStream());
            in = new DataInputStream(socket.getInputStream());
            taggedFrame = new TaggedFrame(socket);
            AVAILABLE_MEMORY = memory;
            register_worker();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void register_worker() throws IOException {
        taggedFrame.send("register", 0, new byte[0], AVAILABLE_MEMORY, "");
        System.out.println("Registering Worker with " + AVAILABLE_MEMORY + " MB of Memory...");
    }



    private void handle_TaggedFrames() throws IOException {
        while (true) {
            // submit every received task to executor
            TaggedFrame.Frame frame = taggedFrame.receive();
            int id = taggedFrame.receive_id_TaggedFrame();
            System.out.println("(" + id + ") Received Task... " + frame.getCommand());
            taskExecutorService.submit(createTask(frame, taggedFrame, id));
        }
    }


    private Runnable createTask(TaggedFrame.Frame frame, TaggedFrame TaggedFrame, int id_TaggedFrame) {
        return () -> {
            try {
                System.out.println("(" + id_TaggedFrame + ") Executing Task...");
                byte[] result = JobFunction.execute(frame.getCode());
                //int result_bytes = (JobFunction.execute(frame.getCodigo())).length;

                TaggedFrame.send("execute", frame.getTag(), new byte[0], frame.getMemory(), "Task Executed Successfully, Returned " + result.length + " Bytes");
                TaggedFrame.send_id_TaggedFrame(id_TaggedFrame);
                System.out.println("(" + id_TaggedFrame + ") Task Executed Successfully");
            } catch (JobFunctionException a) {
                String error = "Job Failed: Code = " + a.getCode() + " message = " + a.getMessage();
                try {
                    TaggedFrame.send("execute", frame.getTag(), new byte[0], frame.getMemory(), error);
                    TaggedFrame.send_id_TaggedFrame(id_TaggedFrame);
                    System.out.println("(" + id_TaggedFrame + ") Task Failed");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
    }



}
