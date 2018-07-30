import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileReceiver extends Thread{

    private ServerSocket ss;
    static AtomicBoolean allTransfersCompleted = new AtomicBoolean(false);
    String baseDir = "/storage/data1/earslan/";

    static long totalTransferredBytes = 0L;
    static long totalChecksumBytes = 0L;
    boolean debug = true;
    long startTime;

    Semaphore transferLock = new Semaphore(0);
    Semaphore checksumLock = new Semaphore(0);

    class Item {
        byte[] buffer;
        int length;

        public Item(byte[] buffer, int length){
            this.buffer = buffer;
            this.length = length;
        }
    }

    public FileReceiver(int port) {
        try {
            ss = new ServerSocket(port);
            ss.setReceiveBufferSize(134217728);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        while (true) {
            try {
                Socket clientSock = ss.accept();
                clientSock.setSoTimeout(10000);
                System.out.println("Connection established from  " + clientSock.getInetAddress());
                saveFile(clientSock);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private void saveFile(Socket clientSock) throws IOException {


        startTime = System.currentTimeMillis();
        DataInputStream dataInputStream  = new DataInputStream(clientSock.getInputStream());
        int numOfFiles = dataInputStream.readInt();
        System.out.println("Will receive " + numOfFiles +" file(s)");

        allTransfersCompleted.set(false);
        totalTransferredBytes = 0L;
        totalChecksumBytes = 0L;

        ChecksumRunnable checksumRunnable  = new ChecksumRunnable(numOfFiles);
        Thread checksumThread = new Thread(checksumRunnable, "checksumThread");
        checksumThread.start();


        MonitorThread monitorThread = new MonitorThread();
        monitorThread.start();
        byte[] buffer = new byte[128 * 1024];
        for (int i = 0; i < numOfFiles; i++) {
            String fileName = dataInputStream.readUTF();
            long fileSize = dataInputStream.readLong();
            if (debug) {
                System.out.println("File " + i + "\t" + fileName + "\t" +
                        humanReadableByteCount(fileSize, false) + " bytes" +
                        " time:" + (System.currentTimeMillis() - startTime)/1000.0 + " s");
            }
            checksumRunnable.setFileMetadata(baseDir + fileName, fileSize);
            //monitorThread.setCurrentFile(fileName);

            FileOutputStream fos = new FileOutputStream(baseDir + fileName);
            long remaining = fileSize;
            int read;
            long fileTransferredBytes = 0;
            while ((read = dataInputStream.read(buffer, 0, (int) Math.min(buffer.length, remaining))) > 0) {
                if (fileTransferredBytes == 0) {
                    transferLock.release();
                }
                fileTransferredBytes += read;
                totalTransferredBytes += read;
                remaining -= read;
                fos.write(buffer, 0, read);
                //fos.flush();
            }
            fos.close();
            if (read == -1) {
                System.out.println("Read -1, closing the connection...");
                return;
            }
            if (debug) {
                System.out.println("Transfer FINISH " + i + "\t" + fileName + "\t" +
                        "time:" + (System.currentTimeMillis()-startTime)/1000.0);
            }

            try {
                checksumLock.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
        dataInputStream.close();
        allTransfersCompleted.set(true);
        System.out.println("FEVER Total Time " + (System.currentTimeMillis() - startTime)/1000.0 + " s");

    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }


    public static void main (String[] args) {
        FileReceiver fs = new FileReceiver(2008);
        fs.start();
    }

    public class ChecksumRunnable implements Runnable {
        MessageDigest md = null;
        String fileName;
        long fileSize;
        int totalFileCount;

        public ChecksumRunnable (int totalFileCount) {
            this.totalFileCount = totalFileCount;
        }

        public void setFileMetadata (String fileName, long fileSize) {
            this.fileName = fileName;
            this.fileSize = fileSize;
        }

        @Override
        public void run() {
            System.out.println("MyThread - START "+Thread.currentThread().getName());
            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            byte[] buffer = new byte[128 * 1024];
            InputStream is = null;
            int checksumCompletedFileCount  = 0;
            while (checksumCompletedFileCount <  totalFileCount) {
                try {
                    transferLock.acquire();
                    File file = new File(fileName);
                    while (!file.exists()) {
                        Thread.sleep(100);
                    }
                    is = new FileInputStream(file);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (debug) {
                    System.out.println("Starting to checksum:" + fileName + " size:" + fileSize);
                }
                long init = System.currentTimeMillis();
                DigestInputStream dis = new DigestInputStream(is, md);
                long read;
                long remaining = fileSize;
                try {
                    while (remaining > 0) {
                        read = dis.read(buffer, 0, (int) Math.min((long) buffer.length, remaining));
                        if (read > 0) {
                            remaining -= read;
                            totalChecksumBytes += read;
                        } else {
                            Thread.sleep(100);
                        }
                    }
                    dis.close();
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                byte[] digest = md.digest();
                String hex = (new HexBinaryAdapter()).marshal(digest);
                if(debug) {
                    System.out.println("File:" + fileName + "Checksum  " + hex + " duration:" +
                            (System.currentTimeMillis() - init) / 1000.0 + " s");
                }
                checksumCompletedFileCount++;
                md.reset();
                checksumLock.release();
            }
            //System.out.println("MyThread - END " + Thread.currentThread().getName());
        }

    }

    public class MonitorThread extends Thread {
        long lastReceivedBytes = 0;
        long lastReadBytes = 0;
        String currentFile;
        public void setCurrentFile(String currentFile) {
            this.currentFile = currentFile;
        }
        @Override
        public void run() {
            try {
                while (!allTransfersCompleted.get()) {
                    double thrInMbps = 8 * (totalTransferredBytes - lastReceivedBytes) / (1000*1000);
                    double readThrInMbps = 8 * (totalChecksumBytes - lastReadBytes) / (1000*1000);
                    System.out.println("Transfer Thr:" + thrInMbps + " Mb/s Checksum thr:" + readThrInMbps);
                    lastReceivedBytes = totalTransferredBytes;
                    lastReadBytes = totalChecksumBytes;
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

}