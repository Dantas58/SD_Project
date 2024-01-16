import sd23.JobFunction;
import sd23.JobFunctionException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class ClientHandler implements Runnable {
    private Socket socket;
    private UserDatabase userDatabase;
    private TaggedFrame taggedFrame;
    private Server server;

    private volatile boolean running = true;

    public ClientHandler(Socket socket, UserDatabase userDatabase, TaggedFrame taggedFrame, Server server) {
        this.socket = socket;
        this.userDatabase = userDatabase;
        this.taggedFrame = taggedFrame;
        this.server = server;
    }


    @Override
    public void run() {

        DataInputStream in = null;
        DataOutputStream out = null;
        try {
            in = new DataInputStream(socket.getInputStream());
            out = new DataOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        while (running) {
            try {

                TaggedFrame.Frame frame = taggedFrame.receive();
                String command = frame.getCommand();
                int tag = frame.getTag();

                switch (command){
                    case "register":
                        //System.out.println("Registering...");
                        String[] parts = frame.getOptional().split(";");
                        boolean success = userDatabase.register(parts[0], parts[1]);

                        if (success) {
                            System.out.println(parts[0] + " Register Successful");
                            taggedFrame.send(command, tag, new byte[0], 0, "Register Successful");
                        } else {
                            System.out.println(parts[0] + " Register Failed");
                            taggedFrame.send(command, tag, new byte[0], 0, "Register Failed: Username Already Exists");
                        }
                        break;

                    case "login":
                        System.out.println("Logging in...");
                        parts = frame.getOptional().split(";");
                        success = userDatabase.login(parts[0], parts[1]);

                        if (success) { 
                            System.out.println(parts[0] + " Login Successful");
                            taggedFrame.send(command, tag, new byte[0], 0, "Login Successful");
                        } else {
                            System.out.println(parts[0] + " Login Failed");
                            taggedFrame.send(command, tag, new byte[0], 0, "Login Failed: Wrong Username or Password");
                        }
                        break;

                    case "execute":
                        if(frame.getMemory() > server.WORKER_WITH_MAX_MEMORY){
                            taggedFrame.send(command, tag, new byte[0], 0, "No Worker with enough Memory");
                            break;
                        }
                        server.addTaskToQueue(frame, taggedFrame);
                        break;
                    case "status":
                        int queue_size = server.getqueueSize();
                        String workersMemory = server.getWorkersMemory();
                        taggedFrame.send(command, tag, new byte[0], queue_size, workersMemory);
                        break;
                    case "exit":
                        System.out.println("Exiting & Aborting all Client tasks...");
                        server.removeTasksFromQueue(taggedFrame);
                        taggedFrame.send(command, tag, new byte[0], 0, "");
                        running = false;
                        break;
                    default:
                        System.out.println("Unknown Error");
                        break;
                }

            }catch(Exception e){
                e.printStackTrace();
            }
        }

        try {
            taggedFrame.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}