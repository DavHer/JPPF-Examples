/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ucr.jppf.framework.lib;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.jppf.node.protocol.AbstractTask;

/**
 *
 * @author David
 */
public abstract class Mapper<K>
        extends AbstractTask<HashMap<K, Integer>>
        implements MapperInterface {

    HashMap<K, Integer> hashMap;
    long bufferLength;
    long bufferInit;
    String arch;

    public void write(K key, Integer cant) {
        Integer valor = hashMap.get(key);
        if (valor == null) {
            hashMap.put(key, cant);
        } else {
            hashMap.put(key, valor + cant);
        }
    }

    public long getBufferInit() {
        return bufferInit;
    }

    public long getBufferLength() {
        return bufferLength;
    }

    public void setBufferInit(long bufferInit) {
        this.bufferInit = bufferInit;
    }

    public void setBufferLength(long bufferLength) {
        this.bufferLength = bufferLength;
    }

    private MappedByteBuffer openFile(String f) throws FileNotFoundException, IOException {
        //Create file object
        File file = new File(f);
        MappedByteBuffer buffer;
        try (FileChannel channel = new RandomAccessFile(file, "r").getChannel()) {
            buffer = channel.map(FileChannel.MapMode.READ_ONLY, bufferInit, bufferLength - bufferInit);
        }
        return buffer;
    }

    @Override
    public void run() {
        hashMap = new HashMap<>();
        System.out.println("Node start executing " + getId());
        LineIterator it = null;
        try {
            File file = new File(arch);
            it = FileUtils.lineIterator(file, "UTF-8");
            long cont = 0;
            String line = "";
            while (it.hasNext()) {
                line = it.nextLine();
                cont = cont + line.length();
                if(cont >= bufferInit){
                    break;
                }
            }
            System.out.println("Node buffer init " + getId());
            int last_n = 0;
            map(line);
            while (it.hasNext()) {
                line = it.nextLine();
                cont = cont + line.length();
                float n = (float)(cont - bufferInit)/(float)(bufferLength-bufferInit) * 100;
                if(((int)n) != last_n)
                    System.out.println((int)n + "%");
                map(line);
                if(cont >= bufferLength)
                    break;
                last_n = (int)n;
            }
            
            System.out.println("Node finish parsing file " + getId());
            
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            LineIterator.closeQuietly(it);
        }
        Properties properties = new Properties();
        for (Map.Entry<K, Integer> entry : hashMap.entrySet()) {
            properties.put(entry.getKey().toString(), entry.getValue() + "");
        }
        try {
            FileOutputStream st = new FileOutputStream(getId() + ".txt");
            properties.store(st, null);
            st.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        setResult(hashMap);
    }

    public void setArch(String arch) {
        this.arch = arch;
    }
}
