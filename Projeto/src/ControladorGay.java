import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


interface Controlador {
    int reserva() throws InterruptedException;
    void preparado(int kart) throws InterruptedException;
    void completaVolta(int kart);
    int[] voltasCompletas();
    int vencedor() throws InterruptedException;
}
public class ControladorGay implements Controlador{
    

    private Map<Integer, Integer> voltas_feitas;
    private int em_curso;
    private boolean corrida_ativa;
    private Map<Integer, Boolean> karts_preparados;
    private Map<Integer, Boolean> karts_reservados;
    private int total_voltas;
    private int total_karts;
    private ReentrantLock lock_reserva = new ReentrantLock();
    private ReentrantLock lock_preparado = new ReentrantLock();
    private ReentrantLock lock_vencedor = new ReentrantLock();
    private Condition condition_reserva = lock_reserva.newCondition();
    private Condition condition_preparado = lock_preparado.newCondition();
    private Condition condition_vencedor = lock_vencedor.newCondition();
    private int vencedor = -1;


    public ControladorGay(int N, int V){

        this.total_karts = N;
        this.total_voltas = V;
        this.voltas_feitas = new HashMap<>();
        this.em_curso = 0;
        this.corrida_ativa = false;
        this.karts_preparados = new HashMap<>();
        this.karts_reservados = new HashMap<>();
        for(int i = 0; i<N; i++){
            voltas_feitas.put(i, 0);
            karts_preparados.put(i, false);
            karts_reservados.put(i, false);
        }
    }

    public int reserva() throws InterruptedException{

        lock_preparado.lock();
        try{

            while(!karts_reservados.containsValue(false)){
                
                condition_reserva.await();
            }

            for(int i = 0; i<total_karts; i++){
                if(!karts_reservados.get(i)){
                    karts_reservados.put(i, true);
                    return i;
                }
            }

            return -1;
        }
        finally{
            lock_preparado.unlock();
        }
    }

    public void preparado(int kart) throws InterruptedException{

        lock_preparado.lock();
        try{

            karts_preparados.put(kart, true);
            em_curso++;
            if(em_curso == total_karts) condition_preparado.signalAll();

            while(em_curso < total_karts){
                condition_preparado.await();
            }

            corrida_ativa = true;
        }
        finally{
            lock_preparado.unlock();
        }
    }

    public void completaVolta(int kart){

        lock_preparado.lock();
        try{
            voltas_feitas.put(kart, voltas_feitas.get(kart) + 1);
            if(voltas_feitas.get(kart) == total_voltas){
                if(vencedor == -1){
                    vencedor = kart;
                    corrida_ativa = false;
                }
            }

            if(!corrida_ativa){
                karts_preparados.put(kart, false);
                karts_reservados.put(kart, false);
                condition_reserva.signal();
                em_curso--;
            }

            if(em_curso == 0) condition_vencedor.signalAll();
        }
        finally{
            lock_preparado.unlock();
        }
    }

    public int[] voltasCompletas(){

        lock_preparado.lock();
        try{
            int[] result = new int[total_karts];
            for(int i = 0; i<total_karts; i++){
                result[i] = voltas_feitas.get(i);
            }

            return result;
        }
        finally{
            lock_preparado.unlock();
        }
    }

    public int vencedor() throws InterruptedException{

        lock_vencedor.lock();
        try{

            while(vencedor == -1){
                condition_vencedor.await();
            }

            for(int i = 0; i<total_karts; i++){
                voltas_feitas.put(i, 0);
            }

            return vencedor;
        }
        finally{
            vencedor = -1;
            lock_vencedor.unlock();
        }
    }
}
