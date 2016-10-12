
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

public class TemplateJPPFTask extends AbstractTask<Integer> {

    private long from;
    private long to;

    public TemplateJPPFTask(long from, long to) {
        this.from = from;
        this.to = to;
    }

    public MappedByteBuffer abrirArchivo(String archivo) throws FileNotFoundException, IOException{
        //Create file object
        File file = new File(archivo);
        FileChannel channel = new RandomAccessFile(file, "r").getChannel();
        
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        channel.close();
        

        // the buffer now reads the file as if it were loaded in memory.
//        System.out.println(buffer.isLoaded());  //prints false
//        System.out.println(buffer.capacity());  //Get the size based on content size of file
//        System.out.println(buffer.limit());
        return buffer;
    }

    public Integer contarNumeros(MappedByteBuffer buffer){
        int primerIndex = 0;
        int segundoIndex = 0;
        String numString = "";
        Integer resultado = 0;
        long bufferLimit = buffer.limit();

        for (long i = from; i < to; i++) {
            if (((char)buffer.get((int)i) == ' ' && i != 0) ||
                i == bufferLimit) {
                System.out.println(numString+"<<");
                resultado += Integer.parseInt(numString);
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
        Integer resultado = 0;
        MappedByteBuffer buffer = null;
        
        System.out.println("Hello, this is the node executing a template JPPF task");
        try {
            buffer = abrirArchivo("numeros.txt");
            System.out.println("File loaded");
            resultado = contarNumeros(buffer);
            System.out.println("contados");
        } catch (Exception ex) {
            ex.printStackTrace();
            Logger.getLogger(TemplateJPPFTask.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        // ...
        // eventually set the execution results
        System.out.println("Res: " + resultado);
        setResult(resultado);
    }
}
