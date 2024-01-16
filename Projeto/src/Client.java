import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

import javax.xml.crypto.Data;

public class Client {

    private static final String HOST = "localhost";
    private static final int PORT = 9090;
    private Socket socket;
    private TaggedFrame taggedFrame;
    private DataOutputStream out;
    private DataInputStream in;
    private String result_file;

    private volatile boolean running = true;
    private volatile boolean authenticated = false;

    private void registerOrLogin(String command, int requestId, String infos) throws IOException{

        TaggedFrame.Frame frame = new TaggedFrame.Frame(command, requestId, new byte[0], 0, infos);
        System.out.println("[" + requestId + "] Sending " + command + " Request...");
        taggedFrame.send(frame);
    }

    private void execute(int requestId, String infos) throws IOException{

        TaggedFrame.Frame frame = new TaggedFrame.Frame("execute", requestId, (infos.split(" ")[0].getBytes()), Integer.parseInt(infos.split(" ")[1]), "");
        System.out.println("[" + requestId + "] Sending Execute (" + infos + ") Request...");
        taggedFrame.send(frame);
    }

    private void status(int requestId) throws IOException{

        TaggedFrame.Frame frame = new TaggedFrame.Frame("status", requestId, new byte[0], 0, "");
        System.out.println("[" + requestId + "] Sending Status Request...");
        taggedFrame.send(frame);
    }

    private void exit() throws IOException{

        TaggedFrame.Frame frame = new TaggedFrame.Frame("exit", 0, new byte[0], 0, "");

        taggedFrame.send(frame);

        System.out.println("Exiting & Aborting Tasks currently being Executed...");
        running = false;
    }

    private void setupServerConnection(){

        try{
            socket = new Socket(HOST, PORT);
            out = new DataOutputStream(socket.getOutputStream());
            in = new DataInputStream(socket.getInputStream());
            taggedFrame = new TaggedFrame(socket);
        }catch (Exception e){
            e.printStackTrace();
        }        
    }

    private void commandHandler() throws IOException{

        Scanner scanner = new Scanner(System.in);   
        int requestId = 0;

        System.out.println("Enter File Path for Results:");
        result_file = scanner.nextLine();
        assignFile();
        
        while(running){

            System.out.println("Waiting for command...");
            String command = scanner.nextLine().toLowerCase();

            switch (command) {
                case "register":
                case "login":
                    System.out.println("Enter username:");
                    String username = scanner.nextLine();
                    System.out.println("Enter password:");
                    String infos = username + ";" + scanner.nextLine();
                    registerOrLogin(command, requestId, infos);
                    break;
            
                case "execute":
                    if(!authenticated){
                        System.out.println("You must be logged in to execute tasks");
                    }
                    else{
                        System.out.println("Enter task code & necessary memory:");
                        String taskMemory = scanner.nextLine();
                        execute(requestId, taskMemory);
                    }
                    break;

                case "status":
                    if(!authenticated){
                        System.out.println("You must be logged in to check status");
                    }
                    else{
                        status(requestId);
                    }
                    break;

                case "execute file":
                    if(!authenticated){
                        System.out.println("You must be logged in to execute tasks");
                    }
                    else{
                        System.out.println("Enter File Path:");
                        String filePath = scanner.nextLine();
                        if(!Files.exists(Paths.get(filePath))){
                            System.out.println("File Does Not Exist");
                            break;
                        }
                        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                execute(requestId++, line);
                            }
                        } catch (IOException e) {
                            System.out.println("Error Reading File: " + e.getMessage());
                        }
                    }
                    break;

                case "exit":
                    exit();
                    scanner.close();
                    return;

                default:
                    System.out.println("Invalid command");
                    break;
            }

            requestId++;
        }
    }

    private void assignFile() {
        File file = new File(result_file);
        if (file.isFile()) {
            try (FileWriter fileWriter = new FileWriter(result_file, false)) {
            } catch (IOException e) {
                System.out.println("Error Clearing File: " + e.getMessage());
            }
        } else if (file.isDirectory()) {
            result_file = Paths.get(result_file, "results.txt").toString();
            Path path = Paths.get(result_file);
            if (!Files.exists(path)) {
                try {
                    Files.createFile(path);
                } catch (IOException e) {
                    System.out.println("Error creating file: " + e.getMessage());
                }
            }
        } else {
            System.out.println(result_file + " is not a File or a Directory.");
        }
    }

    private void printMemory(String tag, String memory){
        String[] parts = memory.split(";");
        for(int i = 0; i < parts.length; i++){
            System.out.println(tag + " Worker " + i + " Available Memory: " + parts[i]);
        }
    }

    private void responseHandler() throws IOException{

        Thread responseThread = new Thread(new Runnable() {

            PrintWriter writer; 
            @Override
            public void run() {
                while (running) {
                    
                    try {
                        TaggedFrame.Frame frame = taggedFrame.receive();
                        String command = frame.getCommand();
                        String tag = "[" + frame.getTag() + "] ";

                        switch (command) {
                            case "register":
                                System.out.println(tag + frame.getOptional());
                                break;

                            case "login":
                                System.out.println(tag + frame.getOptional());
                                if(frame.getOptional().equals("Login Successful")){
                                    authenticated = true;
                                    writer = new PrintWriter(new FileWriter(result_file, true));
                                }
                                break;
                        
                            case "execute":
                                String result = new String(tag + "Task Result: " + frame.getOptional());
                                System.out.println(result);
                                writer.println(result);
                                writer.flush();
                                break;

                            case "status":
                                System.out.println(tag + " Queue Size: " + frame.getMemory());
                                printMemory(tag, frame.getOptional());
                                break;

                            case "exit":
                                writer.close();
                                taggedFrame.close();
                                return;

                            default:
                                System.out.println("Unknown Error");
                                break;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        responseThread.start();
    }
    

    public static void main(String[] args) throws IOException {

            Client client = new Client();

            client.setupServerConnection();
            client.responseHandler();
            client.commandHandler();
        }
}
