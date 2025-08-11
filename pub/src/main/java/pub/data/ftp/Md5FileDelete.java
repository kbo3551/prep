package pub.data.ftp;

import java.io.File;

public class Md5FileDelete {
/**
 *         String remoteDirPath = "/pubmed/updatefiles/";
//        String remoteDirPath = "/pubmed/baseline/";
        String saveDirPath = "C:/pubmed/update/";      
 * @param args
 */
  public static void main(String[] args) {
    String directoryPath = "C:/pubmed/";
    
    deleteMd5Files(directoryPath);
  }
  
  public static void deleteMd5Files(String directoryPath) {
      File directory = new File(directoryPath);
  
      if (directory.exists() && directory.isDirectory()) {
          File[] files = directory.listFiles();
  
          if (files != null) {
              for (File file : files) {
                  if (file.isFile() && file.getName().endsWith(".txt")) {
                      // 파일 삭제
                      if (file.delete()) {
                          System.out.println("Deleted: " + file.getName());
                      } else {
                          System.out.println("Failed to delete: " + file.getName());
                      }
                  }
              }
          } else {
              System.out.println("Failed to list files in directory: " + directoryPath);
          }
      } else {
          System.out.println("Directory does not exist or is not a directory: " + directoryPath);
      }
  }
}
