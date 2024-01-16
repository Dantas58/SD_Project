import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.net.Socket;
import java.net.ServerSocket;

import com.sun.xml.internal.fastinfoset.tools.XML_SAX_StAX_FI;
import sd23.JobFunction;
import sd23.JobFunctionException;


class UserDatabase {
    private Map<String, String> users = new HashMap<>(); // user map -> (username, password)
    private final Lock lock_userDatabase = new ReentrantLock();

    public boolean register(String username, String password) throws Exception {
        lock_userDatabase.lock();

        if (users.containsKey(username)) {
            lock_userDatabase.unlock();
            return false;
        }
        else {
            users.put(username, password);
            lock_userDatabase.unlock();
            return true;
        }
    }
    public boolean login(String username, String password) throws Exception {
        lock_userDatabase.lock();

        if (!users.containsKey(username)) {
                lock_userDatabase.unlock();
                return false;
        }
        else{
            lock_userDatabase.unlock();
            return users.get(username).equals(password);
        }
    }
}
class TaggedFramePair {
    private final TaggedFrame taggedFrame;
    private final TaggedFrame.Frame frame;

    public TaggedFramePair(TaggedFrame taggedFrame, TaggedFrame.Frame frame) {
        this.taggedFrame = taggedFrame;
        this.frame = frame;
    }

    public TaggedFrame getTaggedFrame() {
        return this.taggedFrame;
    }

    public TaggedFrame.Frame getFrame() {
        return this.frame;
    }
}



public class Server {
    ReentrantLock lock_server = new ReentrantLock();
    private BlockingQueue<TaggedFramePair> queue = new ArrayBlockingQueue<>(1000);
    private Map<Integer,TaggedFrame> id_TaggedFrames = new HashMap<>(); // id TaggedFrame, used to identify which client to send the result to
    Condition newTaskAdded = lock_server.newCondition();
    private Map<TaggedFrame, Integer> workers_memory = new HashMap<>(); // worker memory, used to save and update the available memory of each worker
    private ServerSocket clientServerSocket;
    private ServerSocket workerServerSocket;
    public int WORKER_WITH_MAX_MEMORY = 0;

    public Server() {
        Thread thread = new Thread(() -> {
            try {
                processQueue();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        thread.start();
    }

    public int getqueueSize() {
        lock_server.lock();
        try {
            return queue.size();
        } finally {
            lock_server.unlock();
        }
    }

    public String getWorkersMemory(){

        lock_server.lock();
        try {
            StringBuilder result = new StringBuilder();
            for(Map.Entry<TaggedFrame, Integer> entry : workers_memory.entrySet()){
                result.append(entry.getValue()).append(";");
            }
            result.setLength(result.length() - 1);
            return result.toString();

        } finally {
            lock_server.unlock();
        }
    }

    public TaggedFrame getTaggedFrame(int id) {
        lock_server.lock();
        try {
            TaggedFrame taggedFrame = id_TaggedFrames.get(id);
            id_TaggedFrames.remove(id);
            return taggedFrame;
        } finally {
            lock_server.unlock();
        }
    }


    public void addWorker(TaggedFrame worker, int memory) {
        lock_server.lock();
        try {
            workers_memory.put(worker, memory);
        } finally {
            lock_server.unlock();
        }
    }



    // TaggedFrame com mais memoria disponivel
    public int getCurrentMaxtMemory() {
        lock_server.lock();
        try {
            int max = 0;
            TaggedFrame taggedFrame = null;
            for (Map.Entry<TaggedFrame, Integer> entry : workers_memory.entrySet()) {
                if (entry.getValue() > max) {
                    max = entry.getValue();
                    taggedFrame = entry.getKey();
                }
            }
            return max;
        } finally {
            lock_server.unlock();
        }
    }

    public TaggedFrame getWorkerWithMostMemory() {
        lock_server.lock();
        try {
            int max = 0;
            TaggedFrame taggedFrame = null;
            for (Map.Entry<TaggedFrame, Integer> entry : workers_memory.entrySet()) {
                if (entry.getValue() > max) {
                    max = entry.getValue();
                    taggedFrame = entry.getKey();
                }
            }
            //System.out.println("Worker with most Memory: " + max);
            return taggedFrame;
        } finally {
            lock_server.unlock();
        }
    }


    private void processQueue() throws IOException {
        int id = 0;
        while (!Thread.currentThread().isInterrupted()) {
            lock_server.lock();
            try {
                while (queue.isEmpty() || getCurrentMaxtMemory() < queue.peek().getFrame().getMemory()) {
                    System.out.println("Waiting for new task...");
                    newTaskAdded.await(); // Wait until a new task is added or memory is available
                }

                //System.out.println("Processing task...");
                TaggedFramePair pair = queue.poll();
                TaggedFrame taggedFrame = pair.getTaggedFrame();
                TaggedFrame.Frame frame = pair.getFrame();
                TaggedFrame worker = getWorkerWithMostMemory();
                id_TaggedFrames.put(id, taggedFrame);
                worker.send(frame);
                worker.send_id_TaggedFrame(id);
                System.out.println("(" + id + ") Sent task to Worker");
                decreaseMemory(worker,frame.getMemory());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore interrupted status
                break;
            } finally {
                id++;
                lock_server.unlock();
            }
        }
    }

    public void decreaseMemory(TaggedFrame worker,int memory) {
        lock_server.lock();
        try {
            workers_memory.put(worker, workers_memory.get(worker) - memory);
        } finally {
            lock_server.unlock();
        }
    }

    public void increaseMemory(TaggedFrame worker, int memory) {
        lock_server.lock();
        try {
            int currentMemory = workers_memory.getOrDefault(worker, 0);
            workers_memory.put(worker, currentMemory + memory);
            newTaskAdded.signalAll();
        } finally {
            lock_server.unlock();
        }
    }


    public void addTaskToQueue(TaggedFrame.Frame frame, TaggedFrame taggedFrame) {
        lock_server.lock();
        try {
            TaggedFramePair pair = new TaggedFramePair(taggedFrame, frame);
            queue.put(pair);
            newTaskAdded.signalAll(); // Signal that a new task has been added
            //System.out.println("signaled new task");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock_server.unlock();
        }
    }

    public void removeTasksFromQueue(TaggedFrame taggedFrameToRemove) {
        lock_server.lock();
        try {
            queue.removeIf(pair -> pair.getTaggedFrame().equals(taggedFrameToRemove));
        } finally {
            lock_server.unlock();
        }
    }



    public static void main(String[] args) throws IOException {
        Server server = new Server();
        server.startServer(server);
    }

    public void startServer(Server server) throws IOException {
        int client_port = 9090;
        int worker_port = 9091;
        server.clientServerSocket = new ServerSocket(client_port);
        server.workerServerSocket = new ServerSocket(worker_port);
        UserDatabase userDatabase = new UserDatabase();

        System.out.println("Server Listening for Clients on port " + client_port);
        System.out.println("Server Listening for Workers on port " + worker_port);


        Thread worker_thread = new Thread(() -> {
            try {
                handleWorkerConnections(server);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        worker_thread.start();


        while (true) {
            Socket clientSocket = clientServerSocket.accept();
            TaggedFrame taggedFrame = new TaggedFrame(clientSocket);
            
            Thread thread = new Thread(new ClientHandler(clientSocket, userDatabase, taggedFrame, server));
            thread.start();
        }
    }

    private void handleWorkerConnections(Server server) throws IOException {
        while (true) {
            Socket workerSocket = workerServerSocket.accept();
            TaggedFrame taggedFrame_worker = new TaggedFrame(workerSocket);
            
            new Thread(new WorkerHandler(workerSocket, taggedFrame_worker, server)).start();
        }
    }


}