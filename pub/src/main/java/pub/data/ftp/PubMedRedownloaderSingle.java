package pub.data.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTP;
import java.io.*;

public class PubMedRedownloaderSingle {

    private static final int MAX_RETRIES = 5;

    public static void main(String[] args) {
        String server = "ftp.ncbi.nlm.nih.gov";
        int port = 21;
        String user = "anonymous";
        String pass = "guest@example.com";

        String remoteDir = "/pubmed/baseline/";
//        String remoteDir = "/pubmed/updatefiles/";
        String failDirPath = "C:/pubmed/fail/";
        String pubmedDirPath = "C:/pubmed/";

        File failDir = new File(failDirPath);
        File[] files = failDir.listFiles((dir, name) -> name.endsWith(".gz"));
        if (files == null || files.length == 0) {
            System.out.println("재다운로드할 파일이 없습니다.");
            return;
        }

        for (File failFile : files) {
            String fileName = failFile.getName();
            File saveFile = new File(failDirPath, fileName);
            boolean success = false;

            for (int attempt = 1; attempt <= MAX_RETRIES && !success; attempt++) {
                FTPClient ftpClient = new FTPClient();
                try (OutputStream outputStream = new FileOutputStream(saveFile)) {
                    ftpClient.connect(server, port);
                    ftpClient.login(user, pass);
                    ftpClient.enterLocalPassiveMode();
                    ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
                    ftpClient.changeWorkingDirectory(remoteDir);

                    success = ftpClient.retrieveFile(fileName, outputStream);
                    if (success) {
                        System.out.println("재다운로드 성공: " + fileName);
                    } else {
                        System.err.println("재다운로드 실패 (시도 " + attempt + "): " + fileName);
                    }

                } catch (IOException e) {
                    System.err.println("오류 발생 (시도 " + attempt + "): " + fileName + " - " + e.getMessage());
                } finally {
                    try {
                        if (ftpClient.isConnected()) {
                            ftpClient.logout();
                            ftpClient.disconnect();
                        }
                    } catch (IOException ignored) {}
                }

                if (!success && attempt < MAX_RETRIES) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {}
                }
            }

            if (success) {
                File movedFile = new File(pubmedDirPath, fileName);
                if (saveFile.renameTo(movedFile)) {
                    System.out.println("파일 이동 완료: " + movedFile.getAbsolutePath());
                } else {
                    System.err.println("파일 이동 실패: " + fileName);
                }
            }
        }

        System.out.println("=== 모든 재다운로드 작업 완료 ===");
    }
}
