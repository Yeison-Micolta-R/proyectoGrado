/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.udenardbms;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;


/**
 *
 * @author yesec
 */
public class IA implements Runnable {

    
    private String[] ruta;
    private CountDownLatch latch;
    private boolean ident;
     
    public IA(CountDownLatch latch, String[] rut,boolean ident) {
        this.latch = latch;
        this.ruta = rut;
        this.ident = ident;
        
    }

    @Override
    public void run() {
        final String HOST = "127.0.0.1";
        final int PUERTO = 5000;

        DataInputStream in;
        DataOutputStream out;

        try {
            Socket sc = new Socket(HOST, PUERTO);

            in = new DataInputStream(sc.getInputStream());
            out = new DataOutputStream(sc.getOutputStream());
            
          //  System.err.println("tamaño de ruta" + ruta.length);
            String[]  rutaUnique = FunctionClass.uniqueArray(ruta);
            //  System.err.println("tamaño de rutaq" + rutaUnique.length);
              
            if (rutaUnique.length>0) {
                int index = 1;
                
             
                String paths;
                paths= String.join(",", Arrays.asList(rutaUnique));   
                //System.out.println("pathss-> " + paths);
                
                byte[] pathsBytes = paths.getBytes("UTF-8");
               
                out.writeInt(pathsBytes.length);
                out.write(pathsBytes);
              
              
                while (index <= rutaUnique.length) {          
                    int clasePredicha = in.readInt();
                    
                    FunctionClass.clasesList.add(String.valueOf(clasePredicha));
                 
                    if (ident) {
                        FunctionClass.setClasepredicha(clasePredicha);
                    }
                    
                   
                   // System.out.println("Clase predicha: " + clasePredicha);
                   
                     index++;
                }
               // System.out.println("index" + index);
                
              //  FunctionClass.uniqueray(ruta, rutaUnique);
                
            } else {
                System.out.println("El archivo de imagen no existe.");
            }
          //  System.out.println("clase predichasnn " + FunctionClass.clasesList.toString() );
            sc.close();
        }  catch (UnknownHostException e) {
            System.err.println("Host desconocido: " );
            e.printStackTrace();
        } catch (java.net.ConnectException e) {
            FunctionClass.status = true;
            System.err.println("Conexión rechazada: " + e.getMessage());
            
           // e.printStackTrace();
        } catch (IOException e) {
                     
            System.err.println("Error de E/S: " + e.getMessage());
            e.printStackTrace();
        }
        latch.countDown();
    }
}
