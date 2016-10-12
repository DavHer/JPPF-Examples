
package org.jppf.application.template;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jppf.node.protocol.AbstractTask;

public class TemplateJPPFTask extends AbstractTask<Long> {

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
        
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        channel.close();
        return buffer;
    }

    public Integer tryParse(String text) {
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
    
    private boolean isPrime(Integer num) {
        if (num < 2) return false;
        if (num == 2) return true;
        if (num % 2 == 0) return false;
        for (int i = 3; i * i <= num; i += 2)
            if (num % i == 0) return false;
        return true;
    }
    
    private Long contarNumeros(MappedByteBuffer buffer){
        String numString = "";
        Long resultado = 0L;
        long bufferLimit = buffer.limit();
        long porcentaje = to / 100;
        Integer numero;

        for (long i = from; i < to; i++) {
            if (i % porcentaje == 0) {
                System.out.println(getId() + ": "+ (i/porcentaje) + "%");
            }
            if (((char)buffer.get((int)i) == ' ' && i != 0) ||
                i == bufferLimit) {
                
                numero = tryParse(numString);
                resultado += isPrime(numero)? numero : 0;
                numString = "";
            }
            else {
                numString += (char)buffer.get((int)i);
            }
        }

        return resultado;
    }

    @Override
    public void run() {
        // write your task code here.
        Long resultado = 0L;
        MappedByteBuffer buffer = null;
        try {
            buffer = abrirArchivo(archivoNombre);
            resultado = contarNumeros(buffer);
        } catch (Exception ex) {
            ex.printStackTrace();
            Logger.getLogger(TemplateJPPFTask.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        setResult(resultado);
    }
}
