/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Project/Maven2/JavaApp/src/main/java/${packagePath}/${mainClassName}.java to edit this template
 */

package com.mycompany.udenarsql;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import java.io.OutputStream;

import java.net.Socket;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author yese
 */
public class UDENARSQL implements Runnable{
    
    Thread hilo;

    public UDENARSQL() {
        hilo=new Thread(this);
        hilo.start();
    }
    
    public static void main(String[] args) {
        new UDENARSQL();
    }

    @Override
    public void run() {
        Scanner sc = new Scanner(System.in);
        DataOutputStream out ;
        while (true) {
            try {
                Socket client = new Socket("localhost", 5050);
                InputStream in = client.getInputStream();
                //OutputStream out = client.getOutputStream();
                out = new DataOutputStream(client.getOutputStream());
                System.out.print(">> ");
                String command = sc.nextLine().trim();
                
                if (command == null || command.isEmpty()) {
                    System.out.println("El comando es nulo o vacío (verifica si dejaste espacios).");
                    client.close();
                    continue; // Salta a la siguiente iteración del bucle
                }

                long starTime =System.currentTimeMillis();
                out.writeUTF(command);
                long endTime = System.currentTimeMillis();
                long elapsetTime = endTime - starTime;
                System.out.println("Tiempo de envío: " + elapsetTime + " msec.");
                
                // Recibir datos grandes
                long starTime2 =System.currentTimeMillis();
                String response = receiveLargeData(in);
                long endTime2 = System.currentTimeMillis();
                long elapsetTime2 = endTime2 - starTime2;
                System.out.println("Tiempo de recibo: " + elapsetTime2 + " msec.");
                //Impresion
                long starTime3 =System.currentTimeMillis();
                System.out.println(response);
                long endTime3 = System.currentTimeMillis();
                long elapsetTime3 = endTime3 - starTime3;
                System.out.println("Tiempo de lectura: " + elapsetTime3 + " msec.");

                client.close();
                
            } catch (IOException ex) {
                Logger.getLogger(UDENARSQL.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private void sendLargeData(OutputStream out, String data) throws IOException {
        DataOutputStream dos = new DataOutputStream(out);
        byte[] bytes = data.getBytes("UTF-8");
        dos.writeInt(bytes.length);
        dos.write(bytes);
        dos.flush();
    }

    private String receiveLargeData(InputStream in) throws IOException {
        
        DataInputStream dis = new DataInputStream(in);
        int length = dis.readInt();
        byte[] bytes = new byte[length];
        dis.readFully(bytes);
        return new String(bytes, "UTF-8");
    }
}
