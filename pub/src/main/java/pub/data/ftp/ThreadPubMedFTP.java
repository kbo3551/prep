package pub.data.ftp;

import org.apache.commons.net.ftp.*;
import java.io.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPubMedFTP {

	private static final int MAX_RETRIES = 1000;
	private static final int THREAD_COUNT = 2;

	public static void main(String[] args) throws Exception {
		String server = "ftp.ncbi.nlm.nih.gov";
		int port = 21;
		String user = "anonymous";
		String pass = "guest@example.com";
		String remoteDirPath = "/pubmed/updatefiles/";
//		String remoteDirPath = "/pubmed/baseline/";
//		String saveDirPath = "C:/pubmed/baseline/";
		String saveDirPath = "C:/pubmed/updatefiles/";
		String logDirPath = "C:/pubmed/log/";
		String logFilePath = logDirPath + "failed-downloads.txt";

		Set<String> failedFiles = ConcurrentHashMap.newKeySet();
		Queue<String> fileQueue = new ConcurrentLinkedQueue<>();
		Map<String, Integer> retryCountMap = new ConcurrentHashMap<>();
		AtomicInteger successCount = new AtomicInteger(0);
		AtomicInteger failCount = new AtomicInteger(0);

		// FTP에서 파일 목록 가져오기
		FTPClient tempClient = new FTPClient();
		tempClient.connect(server, port);
		tempClient.login(user, pass);
		tempClient.enterLocalPassiveMode();
		tempClient.setFileType(FTP.BINARY_FILE_TYPE);
		tempClient.changeWorkingDirectory(remoteDirPath);
		String[] allFileNames = tempClient.listNames();
		tempClient.logout();
		tempClient.disconnect();

		if (allFileNames == null || allFileNames.length == 0) {
			System.out.println("■■■■ 다운로드할 파일이 없습니다. ■■■■");
			return;
		}

		for (String name : allFileNames) {
			if (name.endsWith(".gz")) fileQueue.add(name);
		}

		ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

		for (int i = 0; i < THREAD_COUNT; i++) {
			executor.submit(() -> {
				while (true) {
					String fileName = fileQueue.poll();
					if (fileName == null) break;

					String savePath = saveDirPath + fileName;
					if (fileExistsLocally(savePath)) {
						System.out.println("이미 존재함: " + fileName);
						successCount.incrementAndGet();
						continue;
					}

					boolean success = false;
					try {
						success = downloadWithHashCheck(server, port, user, pass, remoteDirPath, fileName, savePath);
					} catch (Exception ignored) {}

					if (success) {
						System.out.println("✅ 성공: " + fileName);
						successCount.incrementAndGet();
					} else {
						int retries = retryCountMap.getOrDefault(fileName, 0);
						if (retries < MAX_RETRIES) {
							retryCountMap.put(fileName, retries + 1);
							fileQueue.add(fileName);
							System.out.println("■■■■ 재시도 요청: " + fileName + " (" + (retries + 1) + "/" + MAX_RETRIES + ")");
						} else {
							System.err.println("■■■■ 실패 (최대 재시도 초과): " + fileName);
							failCount.incrementAndGet();
							failedFiles.add(fileName);
						}
					}
				}
			});
		}

		executor.shutdown();
		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

		// 실패 로그 저장
		if (!failedFiles.isEmpty()) {
			new File(logDirPath).mkdirs();
			try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFilePath))) {
				for (String failFile : failedFiles) {
					writer.write(failFile);
					writer.newLine();
				}
				System.out.println("■■■■ 실패 파일 목록 저장 완료: " + logFilePath);
			} catch (IOException e) {
				System.err.println("■■■■ 실패 로그 저장 오류: " + e.getMessage());
			}
		}

		System.out.println("\n=== 완료 ===");
		System.out.println("성공: " + successCount.get());
		System.out.println("실패: " + failCount.get());
	}

	/**
	 * FTP로 다운 받은 파일 md5파일 이용해서 교차 파일 해쉬값 체크
	 * @param server
	 * @param port
	 * @param user
	 * @param pass
	 * @param remoteDir
	 * @param fileName
	 * @param savePath
	 * @return
	 */
	private static boolean downloadWithHashCheck(String server, int port, String user, String pass, String remoteDir,
												 String fileName, String savePath) {
		FTPClient ftpClient = new FTPClient();
		ftpClient.setBufferSize(8 * 1024 * 1024);
		ftpClient.setDataTimeout(1_800_000);
		ftpClient.setDefaultTimeout(1_800_000);
		ftpClient.setConnectTimeout(200_000);

		try {
			ftpClient.connect(server, port);
			ftpClient.login(user, pass);
			ftpClient.enterLocalPassiveMode();
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
			ftpClient.changeWorkingDirectory(remoteDir);

			try (OutputStream output = new BufferedOutputStream(new FileOutputStream(savePath))) {
				if (!ftpClient.retrieveFile(fileName, output)) {
					System.err.println("■■■■ 다운로드 실패: " + fileName);
					return false;
				}
			}

			ByteArrayOutputStream md5Out = new ByteArrayOutputStream();
			if (!ftpClient.retrieveFile(fileName + ".md5", md5Out)) {
				System.err.println("■■■■ MD5 파일 없음: " + fileName + ".md5");
				return false;
			}

			String md5Content = md5Out.toString("UTF-8").trim();
			String expectedMd5;

			if (md5Content.contains("=")) {
				expectedMd5 = md5Content.split("=")[1].trim();
			} else if (md5Content.matches("^[a-fA-F0-9]{32}\\s+.*")) {
				expectedMd5 = md5Content.split("\\s+")[0].trim();
			} else {
				System.err.println("■■■■ 이상한 MD5 형식: " + md5Content);
				return false;
			}

			String actualMd5 = calculateMD5(new File(savePath));

			System.out.println("■■■■ MD5 비교 : " + fileName);
			System.out.println("■■■■ 기대값 : " + expectedMd5);
			System.out.println("■■■■ 실제값 : " + actualMd5);

			if (!expectedMd5.equalsIgnoreCase(actualMd5)) {
				System.err.println("■■■■ 해시 불일치 삭제 후 재시도: " + fileName);
				new File(savePath).delete();
				return false;
			}

			ftpClient.logout();
			return true;
		} catch (Exception e) {
			System.err.println("■■■■ 예외 발생: " + fileName + " - " + e.getMessage());
			return false;
		} finally {
			try {
				if (ftpClient.isConnected()) ftpClient.disconnect();
			} catch (IOException ignored) {}
		}
	}

	private static String calculateMD5(File file) throws Exception {
		MessageDigest md = MessageDigest.getInstance("MD5");
		try (InputStream fis = new FileInputStream(file)) {
			byte[] buffer = new byte[8192];
			int len;
			while ((len = fis.read(buffer)) > 0) {
				md.update(buffer, 0, len);
			}
		}
		StringBuilder sb = new StringBuilder();
		for (byte b : md.digest()) {
			sb.append(String.format("%02x", b));
		}
		return sb.toString();
	}

	private static boolean fileExistsLocally(String filePath) {
		return new File(filePath).exists();
	}
}
