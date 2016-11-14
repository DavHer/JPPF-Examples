/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ucr.jppf.framework;

import java.util.HashMap;
import java.util.Map;
import ucr.jppf.framework.lib.Reducer;

/**
 *
 * @author David
 */
public class BigramCountReducer extends Reducer<Bigrama, Integer>{

    @Override
    public void reduce() {
        for (Map.Entry<Bigrama, Integer> entry : hashMapRight.entrySet()) {
            hashMapLeft.merge(entry.getKey(), entry.getValue(), Integer::sum);
        }
    }
    
}
