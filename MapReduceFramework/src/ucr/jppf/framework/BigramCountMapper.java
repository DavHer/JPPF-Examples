/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ucr.jppf.framework;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.HashMap;
import ucr.jppf.framework.lib.Mapper;

/**
 *
 * @author David
 */
public class BigramCountMapper extends Mapper<Bigrama>{

    @Override    
    public void map(String line) {

        String[] tokens = line.split("\\s+");
        this.emitBigram("", tokens[0]);
        for (int i = 0; i < tokens.length - 1; i++) {
            this.emitBigram(tokens[i], tokens[i + 1]);
        }
        this.emitBigram(tokens[tokens.length - 1], "");
    }

    private void emitBigram(String a, String b) {
        Bigrama bigrama = new Bigrama(a, b);
        write(bigrama, 1);
    }
}
