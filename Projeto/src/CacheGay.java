import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

interface Cache {
    void put(int key, byte[] value) throws InterruptedException;
    byte[] get(int key);
    void evict(int key);
}

public class CacheGay implements Cache {

    private int maxKeys;
    private Map<Integer, byte[]> mapa;
    private ReentrantLock l = new ReentrantLock();
    private Map<Integer, List<Condition>> conditions;
    private List<Integer> keys_espera;

    public CacheGay(int N){

        this.maxKeys = N;
        this.mapa = new HashMap<>();
        this.conditions = new HashMap<>();
        this.keys_espera = new ArrayList<>();
    }


    public void put(int key, byte[] value) throws InterruptedException{

        l.lock();
        try{
            
            if(mapa.size() >= maxKeys && !mapa.containsKey(key)){

                Condition c = l.newCondition();

                if(!conditions.containsKey(key)){
                    List<Condition> queue = new ArrayList<>();
                    queue.add(c);
                    conditions.put(key, queue);
                    keys_espera.add(key);

                }else{
                    conditions.get(key).add(c);
                }
                c.await();
            }

            mapa.put(key, value);
        }
        finally{
            l.unlock();
        }
    }

    public byte[] get(int key){

        l.lock();
        try{
            if(mapa.containsKey(key)){
                return mapa.get(key);
            }
            else{
                return null;
            }
        }
        finally{
            l.unlock();
        }
    }

    public void evict(int key){

        l.lock();
        try{
            mapa.remove(key);
            if(!keys_espera.isEmpty()){
                int next_key = keys_espera.remove(0);
                for(int i = 0; i<conditions.get(next_key).size(); i++){
                    Condition c = conditions.get(next_key).remove(0);
                    c.signal();
                }
                conditions.remove(next_key);
            }
        }
        finally{
            l.unlock();
        }
    }
}

