/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ucr.jppf.framework;

import java.nio.MappedByteBuffer;
import java.util.HashMap;
import ucr.jppf.framework.lib.Mapper;

/**
 *
 * @author David
 */
public class BigramCountMapper extends Mapper<Bigrama,Integer>{

    @Override
    public void map(MappedByteBuffer buffer) {
        String bigramaString = "";
        Bigrama bigrama;
        int cantEspacios = 0;
        Integer valor;
        
        for (long i = getBufferInit(); i < getBufferLength();i++) {
            if ((char)buffer.get((int)i) == ' ' && i != 0) {
                cantEspacios++;
                if(cantEspacios == 2) {
                    cantEspacios = 0;
                    bigrama = new Bigrama(bigramaString.split(" "));
                    valor = getHashMap().get(bigrama);
                    getHashMap().put(bigrama, (valor == null ? 0 : valor) + 1);
                    i -= bigrama.getWord2().length()+1;
                    bigramaString = "";
                } else {
                    bigramaString += (char)buffer.get((int)i);
                }
            } else {
                bigramaString += (char)buffer.get((int)i);
            }
        }
    }
}
