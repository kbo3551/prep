package pub.data.ftp;
import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

public class UngzFile {
    private static final int THREAD_COUNT = 6;

    public static void main(String[] args) throws InterruptedException {
        String inputDirPath = "C:/pubmed/";
        String xmlDirPath = inputDirPath + "xml/";
        String failDirPath = inputDirPath + "fail/";

        new File(xmlDirPath).mkdirs();
        new File(failDirPath).mkdirs();

        File inputDir = new File(inputDirPath+ "baseline/");
        File[] files = inputDir.listFiles((dir, name) -> name.endsWith(".gz"));
        if (files == null || files.length == 0) {
            System.out.println("압축 해제할 .gz 파일이 없습니다.");
            return;
        }
        
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        for (File file : files) {
            executor.submit(() -> {
                boolean success = decompressGzipToXml(file, xmlDirPath);
                if (success) {
                    if (file.delete()) {
                        System.out.println("성공 및 원본 삭제: " + file.getName());
                    } else {
                        System.err.println("성공했지만 원본 삭제 실패: " + file.getName());
                    }
                    successCount.incrementAndGet();
                } else {
                    moveToFailFolder(file, failDirPath);
                    System.err.println("실패: " + file.getName());
                    failCount.incrementAndGet();
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

        System.out.println("\n=== 완료 ===");
        System.out.println("성공: " + successCount.get());
        System.out.println("실패: " + failCount.get());
    }

    private static boolean decompressGzipToXml(File gzipFile, String xmlDirPath) {
        String xmlFileName = gzipFile.getName().replaceAll("\\.gz$", "");
        File xmlFile = new File(xmlDirPath, xmlFileName);

        try (
            GZIPInputStream gis = new GZIPInputStream(new FileInputStream(gzipFile));
            FileOutputStream fos = new FileOutputStream(xmlFile)
        ) {
            byte[] buffer = new byte[4096];
            int len;
            while ((len = gis.read(buffer)) > 0) {
                fos.write(buffer, 0, len);
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static void moveToFailFolder(File file, String failDirPath) {
        File dest = new File(failDirPath, file.getName());
        if (file.exists()) {
            file.renameTo(dest);
        }
    }
}
