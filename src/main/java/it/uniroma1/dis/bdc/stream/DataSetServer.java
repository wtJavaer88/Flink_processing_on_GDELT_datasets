package it.uniroma1.dis.bdc.stream;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

/**
 * Simple socket server that replays the data set file and introduces small delay between each line.
 *
 * @author ichatz@gmail.com
 */
public final class DataSetServer {

    /**
     * Default server port.
     */
    public final static int SOCKET_PORT = 9999;

    /**
     * Main entry point for command line execution.
     *
     * @param args the arguments as received from the command link. They are used to extract the filename of the dataset.
     * @throws Exception exceptions generated during the transfer of the dataset.
     */
    public static void main(String[] args)
            throws Exception {

        // the filename to use as input dataset
        final String filename;
        try {
            // access the arguments of the command line tool
            if (args.length < 1) {
                System.err.println("No filename specified. Please run 'DataSetServer " +
                        "<filename>, where filename is the name of the dataset in csv format");
                return;
            } else {
                filename = args[0];
            }

        } catch (Exception ex) {
            System.err.println("No filename specified. Please run 'DataSetServer " +
                    "<filename>, where filename is the name of the dataset in csv format");
            return;
        }

        ServerSocket servsock = null;

        try {
            servsock = new ServerSocket(SOCKET_PORT);
            while (true) {
                System.out.println("Waiting...");

                Socket sock = null;

                try {
                    // receive connection
                    sock = servsock.accept();
                    System.out.println("Accepted connection : " + sock);

                    // access output stream
                    final OutputStream os = sock.getOutputStream();

                    // send file
                    final File myFile = new File(filename);
                    BufferedReader br = new BufferedReader(new FileReader(myFile));
                    try {
                        String line = br.readLine();

                        // skip the first line
                        line = br.readLine();

                        while (line != null) {
                            os.write(line.getBytes("US-ASCII"));
                            os.write("\n".getBytes("US-ASCII"));
                            os.flush();
                            System.out.println(line);

                            // produce a delay based on the actual occurance of the crime
                            final String minutes = line.substring(line.length() - 3);
                            TimeUnit.MILLISECONDS.sleep(100 * Integer.parseInt(minutes.substring(0, 1)));

                            line = br.readLine();
                        }

                    } finally {
                        br.close();
                    }

                    os.flush();
                    os.close();
                    System.out.println("Done.");

                } finally {
                    if (sock != null) {
                        sock.close();
                    }
                }
            }
        } finally {
            if (servsock != null) {
                servsock.close();
            }
        }
    }

}
