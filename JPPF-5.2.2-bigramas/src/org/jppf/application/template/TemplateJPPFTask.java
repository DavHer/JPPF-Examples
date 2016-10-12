
package org.jppf.application.template;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jppf.node.protocol.AbstractTask;

public class TemplateJPPFTask extends AbstractTask<HashMap<Bigrama, Integer>> {

    private long from;
    private long to;
    private String archivoNombre;

    public TemplateJPPFTask(String archivoNombre, long from, long to) {
        this.from = from;
        this.to = to;
        this.archivoNombre = archivoNombre;
    }

    public MappedByteBuffer abrirArchivo(String archivo) throws FileNotFoundException, IOException{
        //Create file object
        File file = new File(archivo);
        FileChannel channel = new RandomAccessFile(file, "r").getChannel();
        long size = channel.size()% Integer.MAX_VALUE;
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, size);
        channel.close();
        return buffer;
    }
   
    public HashMap<Bigrama, Integer> cargarBigramas(MappedByteBuffer buffer) {
        String bigramaString = "";
        Bigrama bigrama;
        int cantEspacios = 0;
        Integer valor;
        HashMap<Bigrama, Integer> hashMap = new HashMap<Bigrama, Integer>();
        
        for (long i = from; i < to;i++) {
            if ((char)buffer.get((int)i) == ' ' && i != 0) {
                cantEspacios++;
                if(cantEspacios == 2) {
                    cantEspacios = 0;
                    bigrama = new Bigrama(bigramaString.split(" "));
                    valor = hashMap.get(bigrama);
                    hashMap.put(bigrama, (valor == null ? 0 : valor) + 1);
                    i -= bigrama.getWord2().length()+1;
                    bigramaString = "";
                } else {
                    bigramaString += (char)buffer.get((int)i);
                }
            } else {
                bigramaString += (char)buffer.get((int)i);
            }
        }
        return hashMap;
    }

    @Override
    public void run() {
        // write your task code here.
        HashMap<Bigrama,Integer> resultado = new HashMap<>();
        MappedByteBuffer buffer = null;
        try {
            buffer = abrirArchivo(archivoNombre);
            resultado = cargarBigramas(buffer);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        
        setResult(resultado);
    }
}
