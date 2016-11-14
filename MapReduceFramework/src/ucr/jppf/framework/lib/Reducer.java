/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ucr.jppf.framework.lib;

import java.util.HashMap;
import org.jppf.node.protocol.AbstractTask;

/**
 *
 * @author David
 */
public abstract class Reducer<K, V>
        extends AbstractTask<HashMap<K, V>> implements ReducerInterface {

    
    protected HashMap<K, V> hashMapLeft = new HashMap<>();    
    protected HashMap<K, V> hashMapRight = new HashMap<>();

    public void setHashMapLeft(HashMap<K, V> hashMapLeft) {
        this.hashMapLeft = hashMapLeft;
    }

    public void setHashMapRight(HashMap<K, V> hashMapRight) {
        this.hashMapRight = hashMapRight;
    }        
    
    @Override
    public void run() {
        reduce();
        setResult(hashMapLeft);
    }
}
