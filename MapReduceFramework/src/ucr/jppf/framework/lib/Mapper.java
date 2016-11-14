/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ucr.jppf.framework.lib;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jppf.node.protocol.AbstractTask;

/**
 *
 * @author David
 */
public abstract class Mapper<K, V>
        extends AbstractTask<HashMap<K, V>>
        implements MapperInterface {

    HashMap<K, V> hashMap;
    long bufferLength;
    long bufferInit;
    String arch;

    public HashMap<K, V> getHashMap() {
        return hashMap;
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
            long size = channel.size() % Integer.MAX_VALUE;
            buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, size);
        }
        return buffer;
    }

    @Override
    public void run() {
        hashMap = new HashMap<>();
        MappedByteBuffer buffer = null;
        try {
            buffer = openFile(arch);
            map(buffer);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        Properties properties = new Properties();
        for (Map.Entry<K, V> entry : hashMap.entrySet()) {
            properties.put(entry.getKey().toString(), entry.getValue() + "");
        }
        try {
            properties.store(new FileOutputStream(getId() + ".txt"), null);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        setResult(hashMap);
    }

    public void setArch(String arch) {
        this.arch = arch;
    }
}
