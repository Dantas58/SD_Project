import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

interface Reunião {
    void participa(int lista) throws InterruptedException;
    void abandona(int lista);
    int naSala();
    int aEspera();
}
public class ReuniãoGay implements Reunião{

    private Map<Integer, Integer> listas;
    private int lista_ativa;
    private int nr_participantes;
    private int espera;
    private ReentrantLock sala_lock;
    private Condition sala_ocupada;

    public ReuniãoGay(){
        this.listas = new HashMap<>();
        this.lista_ativa = -1;
        this.nr_participantes = 0;
        this.espera = 0;
        this.sala_lock = new ReentrantLock();
        this.sala_ocupada = sala_lock.newCondition();
    }

    /* 
    public int listaMax() {
        return listas.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse(-1); // return -1 if the map is empty
    }
    */

    public int listaMax() {
        int maxKey = -1;
        int maxValue = Integer.MIN_VALUE;
    
        for (Map.Entry<Integer, Integer> entry : listas.entrySet()) {
            if (entry.getValue() > maxValue) {
                maxValue = entry.getValue();
                maxKey = entry.getKey();
            }
        }
    
        return maxKey; 
    }

    public void participa(int lista) throws InterruptedException {
        
        sala_lock.lock();
        try{
            if(!listas.containsKey(lista)){
                listas.put(lista, 1);
            }else{
                listas.put(lista, listas.get(lista) + 1);
            }
            
            if(lista_ativa == -1) lista_ativa = lista;

            espera++;
            while(lista_ativa != lista){
                sala_ocupada.await();
            }

            nr_participantes++;
            espera--;
        }
        finally{
            sala_lock.unlock();
        }
    }

    public void abandona(int lista) {
        
        sala_lock.lock();
        try{

            if(lista == lista_ativa){
                nr_participantes--;
                listas.put(lista, nr_participantes);
                if(nr_participantes == 0){
                    lista_ativa = listaMax();
                    sala_ocupada.signalAll();
                }
            }else{
                listas.put(lista, listas.get(lista) -1);
                espera--;
            }
        }
        finally{
            sala_lock.unlock();
        }
    }

    public int naSala() {
        sala_lock.lock();
        try{
            return nr_participantes;
        }
        finally{
            sala_lock.unlock();
        }
    }

    public int aEspera() {
        
        sala_lock.lock();
        try{
            return espera;
        }
        finally{
            sala_lock.unlock();
        }
    }
}