/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ucr.jppf.framework;

import java.io.Serializable;
import java.util.Objects;

/**
 *
 * @author david
 */
public class Bigrama implements Serializable{
    String word1;
    String word2;

    public Bigrama(String word1) {
        this.word1 = word1.intern();
        this.word2 = null;
    }

    public Bigrama(String word1, String word2) {
        this.word1 = word1.intern();
        this.word2 = word2.intern();
    }

    public Bigrama(String words[]) {
        if(words.length == 2){
            this.word1 = words[0].intern();
            this.word2 = words[1].intern();
        }
        else if(words.length == 1){
            this.word1 = words[0].intern();
            this.word2 = "".intern();
        }
        else{
            this.word1 = "".intern();
            this.word2 = "".intern();
        }
    }

    public String getWord1() {
        return word1;
    }

    public String getWord2() {
        return word2;
    }

    public void setWord1(String word1) {
        this.word1 = word1.intern();
    }

    public void setWord2(String word2) {
        this.word2 = word2.intern();
    }

    @Override
    public String toString() {
        return word1 + " " + word2;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 67 * hash + (word1 != null ? word1.hashCode() : 0);
        hash = 67 * hash + (word2 != null ? word2.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Bigrama other = (Bigrama) obj;
        if (!Objects.equals(this.word1, other.word1)) {
            return false;
        }
        if (!Objects.equals(this.word2, other.word2)) {
            return false;
        }
        return true;
    }
}

