package pub.data.ftp;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPConnectionClosedException;

public class PubMedFTP {
  
  public static void main(String[] args) {
    String server = "ftp.ncbi.nlm.nih.gov";
    int port = 21;
    String user = "anonymous";
    String pass = "guest@example.com";

//    String remoteDirPath = "/pubmed/baseline/";
    String remoteDirPath = "/pubmed/updatefiles/";
    String saveDirPath = "D:/pubmed_xml/pubmed/Update Data/pubmed/";

      try {
          downloadFilesFromFTP(server, port, user, pass, remoteDirPath, saveDirPath);
          System.out.println("success");
      } catch (IOException e) {
          e.printStackTrace();
      }
  }
  
  public static void downloadFilesFromFTP(String server, int port, String user, String pass, String remoteDirPath, String saveDirPath) throws IOException {
      FTPClient ftpClient = new FTPClient();
      try {
          ftpClient.connect(server, port);
          ftpClient.login(user, pass);
          ftpClient.enterLocalPassiveMode();
          ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
  
          ftpClient.changeWorkingDirectory(remoteDirPath);
  
          String[] fileNames = ftpClient.listNames(); // FTP 파일목록
  
          if (fileNames != null) {
              for (String fileName : fileNames) {
                  String saveFilePath = saveDirPath + fileName;
                  if (fileExistsLocally(saveFilePath)) {
                      System.out.println("■■■■■■■■■■■■■파일이 이미 존재함. 파일명: " + fileName);
                      continue; // 파일이 이미 로컬에 존재하면 다운로드를 건너뜀
                  }
  
                  try (OutputStream outputStream = new FileOutputStream(saveFilePath)) {
                      ftpClient.retrieveFile(fileName, outputStream);
                      System.out.println("■■■■■■■■■■■■■파일 저장됨 - 파일 이름: " + fileName);
                  } catch (FTPConnectionClosedException e) {
                      System.err.println("■■■■■■■■■■■■■■■■■■■■■■■■■■FTP 연결이 끊킴 파일 다운로드 재시도■■■■■■■■■■■■■■■■■■■■■■■■■■");
                      // 파일 다운로드중 연결이 끊킬 시 재시도
                      downloadFilesFromFTP(server, port, user, pass, remoteDirPath, saveDirPath);
                      return;
                  }
              }
          }
      } finally {
          if (ftpClient.isConnected()) {
              ftpClient.logout();
              ftpClient.disconnect();
          }
      }
  }
  
  private static boolean fileExistsLocally(String filePath) {
      return new File(filePath).exists();
  }
}

