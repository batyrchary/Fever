import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.xml.bind.annotation.adapters.HexBinaryAdapter;


public class FileSender {
    private Socket s;

    static boolean allFileTransfersCompleted = false;


    File[] files;

    static long totalTransferredBytes = 0;
    static long totalChecksumBytes = 0;
    long startTime;
    boolean debug = true;

    static String fileOrdering = "shuffle";

    static LinkedBlockingQueue<Item> items = new LinkedBlockingQueue<>(10000);
    static LinkedBlockingQueue<Item> items2 = new LinkedBlockingQueue<>(10000);

    public FileSender(String host, int port) {
        try {
            s = new Socket(host, port);
            s.setSoTimeout(10000);
            s.setSendBufferSize(134217728);
            //s.setSendBufferSize(134217728);
        } catch (Exception e) {
            System.out.println(e);
        }
    }


    public void sendFile(String path) throws IOException {

        new MonitorThread().start();
        startTime = System.currentTimeMillis();
        DataOutputStream dos = new DataOutputStream(s.getOutputStream());

        File file =new File(path);

        if(file.isDirectory()) {
            files = file.listFiles();
        } else {
            files = new File[] {file};
        }
        System.out.println("Will transfer " + files.length+ " files");

        if (fileOrdering.compareTo("shuffle") == 0){
            List<File> fileList = Arrays.asList(files);
            Collections.shuffle(fileList);
            files = fileList.toArray(new File[fileList.size()]);
        } else if (fileOrdering.compareTo("sort") == 0) {
            Arrays.sort(files, new Comparator<File>() {
                public int compare(File f1, File f2) {
                    try {
                        int i1 = Integer.parseInt(f1.getName());
                        int i2 = Integer.parseInt(f2.getName());
                        return i1 - i2;
                    } catch(NumberFormatException e) {
                        throw new AssertionError(e);
                    }
                }
            });
        } else {
            System.out.println("Undefined file ordering:" + fileOrdering);
            System.exit(-1);
        }

        dos.writeInt(files.length);
        ChecksumThread worker = new ChecksumThread(files.length);
        Thread thread = new Thread(worker);
        thread.start();

        byte[] buffer = new byte[128 * 1024];
        int n;
        for (int i = 0; i <files.length ; i++) {
            //send file metadata
            File currentFile = files[i];
            dos.writeUTF(currentFile.getName());
            dos.writeLong(currentFile.length());
            if (debug) {
                System.out.println("Transfer START file " + currentFile.getName() + " size:" +
                        humanReadableByteCount(currentFile.length(), false) + " time:" +
                        (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
            }
            long fileTransferStartTime = System.currentTimeMillis();
            FileInputStream fis = new FileInputStream(currentFile);
            try {
                while ((n = fis.read(buffer)) > 0) {
                    dos.write(buffer, 0, n);
                    if (i % 2 == 0) {
                        items.offer(new Item(buffer, n), Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                    } else {
                        items2.offer(new Item(buffer, n), Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                    }
                    totalTransferredBytes += n;
                }
            } catch (InterruptedException e) {
                    e.printStackTrace();
            }
            if (debug) {
                System.out.println("Transfer END file " + currentFile.getName() + "\t duration:" +
                        (System.currentTimeMillis() - fileTransferStartTime) / 1000.0 + " time:" +
                        (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
            }
            fis.close();
        }
        dos.close();
        allFileTransfersCompleted = true;
    }


    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public static void main(String[] args) {
        String destIp = args[0];
        String path = args[1];
        if (args.length > 2) {
            fileOrdering = args[2];
        }
        FileSender fc = new FileSender(destIp, 2008);
        try {
            fc.sendFile(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    class Item {
        byte[] buffer;
        int length;

        public Item(byte[] buffer, int length){
            this.buffer = buffer;
            this.length = length;
        }
    }


    public class ChecksumThread implements Runnable {
        int totalFileCount;
        MessageDigest md = null;

        public ChecksumThread (int totalFileCount) {
            this.totalFileCount = totalFileCount;
        }



        public void reset () {
            md.reset();
        }

        @Override
        public void run() {
            System.out.println("MyThread - START " + Thread.currentThread().getName());
            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            //long startTime = System.currentTimeMillis();
            for (int i = 0; i < files.length; i++) {
                long fileSize = files[i].length();
                long remaining = fileSize;
                if (debug) {
                    System.out.println("Checksum START file:" + files[i].getName() +  " size:" +
                            humanReadableByteCount(fileSize, false) + "  time:" +
                            (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
                }
                long fileStartTime = System.currentTimeMillis();
                while (remaining > 0) {
                    Item item = null;
                    try {
                        if (i % 2 == 0) {
                            item = items.poll(100, TimeUnit.MILLISECONDS);
                        } else {
                            item = items2.poll(100, TimeUnit.MILLISECONDS);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (item == null) {
                        continue;
                    }
                    md.update(item.buffer, 0, item.length);
                    totalChecksumBytes += item.length;
                    remaining -= item.length;
                }
                byte[] digest = md.digest();
                String hex = (new HexBinaryAdapter()).marshal(digest);

                double duration = (System.currentTimeMillis() - fileStartTime) / 1000.0;
                if (debug) {
                    System.out.println("Checksum  END " + hex + " file:" + files[i].getName() +
                            "  duration:" + duration + " time :" +
                            (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
                }
            }
            System.out.println("MyThread - END " + Thread.currentThread().getName());
            double totalDuration = (System.currentTimeMillis() - startTime) / 1000.0;
            System.out.println("Total duration: " + totalDuration);

        }

    }


    public class MonitorThread extends Thread {
        long lastTransferredBytes = 0;
        long lastChecksumBytes = 0;

        @Override
        public void run() {
            try {
                while (!allFileTransfersCompleted || !items.isEmpty()) {
                    double transferThrInMbps = 8 * (totalTransferredBytes-lastTransferredBytes)/(1000*1000);
                    double checksumThrInMbps = 8 * (totalChecksumBytes-lastChecksumBytes)/(1024*1024);
                    System.out.println("Network thr:" + transferThrInMbps + "Mb/s I/O thr:" + checksumThrInMbps + " Mb/s" +
                    " items:" + items.size() + "\t items2:" + items2.size());
                    lastTransferredBytes = totalTransferredBytes;
                    lastChecksumBytes = totalChecksumBytes;
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}