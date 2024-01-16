import java.io.IOException;
import java.net.Socket;


public class WorkerHandler implements Runnable {
    private Socket workerSocket;
    private TaggedFrame taggedFrame;
    private Server server;
    public WorkerHandler(Socket socket, TaggedFrame taggedFrame, Server server) {
        this.workerSocket = socket;
        this.taggedFrame = taggedFrame;
        this.server = server;
    }

    public void run() {
        while (true) {
            try {
                TaggedFrame.Frame frame = taggedFrame.receive();
                String command = frame.getCommand();
                if(command.equals("register")){
                    server.addWorker(taggedFrame, frame.getMemory());
                    if(frame.getMemory() > server.WORKER_WITH_MAX_MEMORY){
                        server.WORKER_WITH_MAX_MEMORY = frame.getMemory();
                    }
                    System.out.println("Worker Registered");
                }
                else {
                    server.increaseMemory(taggedFrame, frame.getMemory());
                    int id = taggedFrame.receive_id_TaggedFrame();
                    TaggedFrame task = server.getTaggedFrame(id);
                    System.out.println(("(" + id + ") Received Task from Worker"));
                    task.send(frame);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}

