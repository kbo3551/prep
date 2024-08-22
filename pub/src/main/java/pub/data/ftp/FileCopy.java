package pub.data.ftp;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;

public class FileCopy {

  public static void main(String[] args) {
    // 원본 디렉토리 경로
    Path sourceDir = Paths.get("D:\\pubmed_xml");

      try {
          // 하위 디렉토리 파일을 원본 디렉토리로 복사
          copyFilesToRoot(sourceDir);
      } catch (IOException e) {
          e.printStackTrace();
      }
  }
  
  public static void copyFilesToRoot(Path sourceDir) throws IOException {
      // 파일 방문자 구현
      Files.walkFileTree(sourceDir, new SimpleFileVisitor<Path>() {
        private int copyCount = 0; // 파일 복사 횟수
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
              // 파일을 원본 디렉토리로 복사
              Path targetFile = sourceDir.resolve(file.getFileName());
  
              // 파일 이름 중복 방지
              int count = 1;
              while (Files.exists(targetFile)) {
                  String fileName = file.getFileName().toString();
                  String newFileName;
//                  targetFile = sourceDir.resolve(fileName);
                  int dotIndex = fileName.lastIndexOf('.');
                  if (dotIndex != -1) {
                      newFileName = fileName.substring(0, dotIndex) + "_" + count + fileName.substring(dotIndex);
                  } else {
                      newFileName = fileName + "_" + count;
                  }
                  targetFile = sourceDir.resolve(newFileName);
                  System.out.println("파일 D:\\pubmed_xml 경로로 복사 중 파일명 : "+ newFileName + "현재 " + copyCount + "번째 실행중임");
                  count++;
              }
              copyCount++; // 파일 복사 횟수 증가
              Files.copy(file, targetFile, StandardCopyOption.REPLACE_EXISTING);
              return FileVisitResult.CONTINUE;
          }
  
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
              // 원본 디렉토리는 건너뛰기
              if (sourceDir.equals(dir)) {
                  return FileVisitResult.CONTINUE;
              }
              return FileVisitResult.CONTINUE;
          }
      });
  }

}
