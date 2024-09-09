package pub.prep.convert;

//import java.io.File;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.node.ObjectNode;
//
///**
// * JSON 전처리 메서드 멀티 스레드
// */
//public class JsonPrep_Multithread {
//
//    private static Map<String, String[]> orgData; // {기관명: [기관명, 국가코드, 국가명]}
//    private static Map<String, String> countryData; // {국가명: 국가코드}
//
//    public static void main(String[] args) {
//        // 로컬 테스트용 
////        String jsonDirectoryPath = "C:\\Users\\mediazen0\\Desktop\\JSON_PREP";
////        String rorDataPath = "C:\\Users\\mediazen0\\Desktop\\v1.51-2024-08-21-ror-data\\ror-prep.json";
////        String jsonDirPath = jsonDirectoryPath.replace("json_prep", "json_prep_end");
//
//        // 서버에서 연산 작업용
//        String jsonDirectoryPath = args[0];
//        String rorDataPath = "/home/mediazen/kbds_pubmed/java_jar/java_config/ror-prep.json";
//        String jsonDirPath = jsonDirectoryPath.replace("json", "json_prep");
//
////        String rorDataPath = "/home/mediazen/kbds_pubmed/java_jar/java_config/v1.49-2024-07-11-ror-data.json";
//        try {
//            System.out.println("Loading ror-data {기관명: [기관명, 국가코드, 국가명]}");
//            orgData = LoadRORData.loadOrganizationData(rorDataPath);
//
//            System.out.println("Loading country {국가명: 국가코드}");
//            countryData = LoadRORData.loadCountryData(rorDataPath);
//
//            System.out.println("Processing documents from JSON files in directory: " + jsonDirectoryPath);
//            processDocumentsFromDirectory(jsonDirectoryPath, jsonDirPath);
//            System.out.println("Documents processed.");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private static void processDocumentsFromDirectory(String directoryPath, String jsonDirPath) throws IOException {
//        List<File> jsonFiles = findJsonFiles(directoryPath);
//        int totalFiles = jsonFiles.size();
//        AtomicInteger fileCount = new AtomicInteger(0);
//
//        jsonFiles.parallelStream().forEach(jsonFile -> {
//            try {
//                int currentCount = fileCount.incrementAndGet();
//                System.out.println("■ 진행 상태 : " + currentCount + "번째 진행 중 ■ 전체 파일수 : " + totalFiles + " ■ 파일명 " + jsonFile.getName());
//
//                String jsonString = new String(Files.readAllBytes(jsonFile.toPath()));
//                String updatedJsonString = processJsonContent(jsonString);
//
//                // 일치하는 데이터가 없는 경우 저장하지 않음
//                if (updatedJsonString != null) {
//                    String fileNameWithoutExtension = jsonFile.getName().replace(".json", "");
//                    saveJsonToFile(jsonDirPath, fileNameWithoutExtension + "_prep", updatedJsonString);
//                    System.out.println("■■■■■■■■■■■■■■■■■파일명 : " + jsonFile.getName() + "■■■■■■■■■■■■■■■■■");
//                } else {
//                    System.out.println("일치하는 데이터가 없어 파일을 저장하지 않음: " + jsonFile.getName());
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
//    }
//
//    public static List<File> findJsonFiles(String directoryPath) throws IOException {
//        System.out.println("대상 폴더: " + directoryPath);
//        try (Stream<Path> paths = Files.walk(Paths.get(directoryPath))) {
//            List<File> jsonFiles = paths.filter(Files::isRegularFile)
//                    .filter(path -> path.toString().endsWith(".json"))
//                    .map(Path::toFile).collect(Collectors.toList());
//            System.out.println("찾은 JSON 파일 갯수 : " + jsonFiles.size());
//            return jsonFiles;
//        } catch (IOException e) {
//            System.err.println("Error 대상 경로: " + directoryPath);
//            throw e;
//        }
//    }
//
//    private static String processJsonContent(String jsonString) throws IOException {
//        ObjectMapper objectMapper = new ObjectMapper();
//        JsonNode rootNode = objectMapper.readTree(jsonString);
//        boolean hasMatchingOrgData = false;
//        boolean hasMatchingCountryData = false;
//
//        if (rootNode.isObject()) {
//            JsonNode medlineCitationNode = rootNode.path("MedlineCitation");
//            if (medlineCitationNode.isObject()) {
//                JsonNode articleNode = medlineCitationNode.path("Article");
//                JsonNode authorListNode = articleNode.path("AuthorList").path("Author");
//
//                if (authorListNode.isArray()) {
//                    for (JsonNode authorNode : authorListNode) {
//                        JsonNode affiliationInfoNode = authorNode.path("AffiliationInfo");
//                        ObjectNode affiliationUpdate;
//
//                        if (affiliationInfoNode.isMissingNode()) {
//                            affiliationUpdate = (ObjectNode) ((ObjectNode) authorNode).putObject("AffiliationInfo");
//                        } else if (affiliationInfoNode.isObject()) {
//                            affiliationUpdate = (ObjectNode) affiliationInfoNode;
//                        } else {
//                            System.err.println("Unexpected node type for AffiliationInfo: " + affiliationInfoNode.getNodeType());
//                            continue;
//                        }
//
//                        String affiliation = affiliationUpdate.path("Affiliation").path("value").asText();
//                        System.out.println("Processing document ID: " + rootNode.path("PMID").path("value").asText() + " with affiliation: " + affiliation);
//
//                        // Affiliation key가 존재할 때만 진행
//                        if (affiliation != null && !"".equals(affiliation) && !"null".equals(affiliation)) {
//                            String[] orgDataArray = findMatchingOrganizationData(affiliation);
//                            if (orgDataArray != null) {
//                                // 기관명 매칭 후 기관명 PUT
//                                affiliationUpdate.put("institution_name", orgDataArray[0]);
//                                hasMatchingOrgData = true;
//                            }
//
//                            Entry<String, String> countryEntry = findMatchingCountryCode(affiliation);
//                            if (countryEntry != null) {
//                                // 국가 코드 매칭 후 국가 정보 PUT
//                                affiliationUpdate.put("country_code", countryEntry.getKey());
//                                affiliationUpdate.put("country_name", countryEntry.getValue());
//                                hasMatchingCountryData = true;
//                            }
//                        }
//                    }
//
//                    // 두 조건이 모두 충족되는 경우에만 JSON을 저장
//                    if (hasMatchingOrgData && hasMatchingCountryData) {
////                        System.out.println("Final Updated JSON: " + rootNode.toString());
//                        return objectMapper.writeValueAsString(rootNode);
//                    } else {
//                        System.out.println("일치하는 기관명 또는 국가 코드가 모두 존재하지 않아 업데이트 하지 않음.");
//                    }
//                }
//            }
//        }
//
//        return null; // 일치하는 데이터가 없을 경우 null 반환
//    }
//
//    private static void saveJsonToFile(String directoryPath, String fileNameWithoutExtension, String jsonString) throws IOException {
//        // Ensure directory exists
//        Path directory = Paths.get(directoryPath);
//        if (!Files.exists(directory)) {
//            Files.createDirectories(directory);
//        }
//
//        Path filePath = Paths.get(directoryPath, fileNameWithoutExtension + ".json");
//        Files.write(filePath, jsonString.getBytes());
//
//        System.out.println("JSON 파일 저장 완료: " + filePath);
//    }
//
//    private static String[] findMatchingOrganizationData(String affiliation) {
//        for (Map.Entry<String, String[]> entry : orgData.entrySet()) {
//            if (affiliation.contains(entry.getKey())) {
//                System.out.println("포함된 기준 비교 대상 문자열: " + entry.getKey());
//                System.out.println("포함 비교 대상 문자열: " + affiliation);
//                return entry.getValue();
//            }
//        }
//        return null;
//    }
//
//    private static Entry<String, String> findMatchingCountryCode(String affiliation) {
//        for (Map.Entry<String, String> entry : countryData.entrySet()) {
//            if (affiliation.contains(entry.getKey())) {
//                return entry;
//            }
//        }
//        return null;
//    }
//}
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * JSON 전처리 메서드 멀티 스레드
 */
public class JsonPrep_Multithread {

    private static Map<String, String[]> orgData; // {기관명: [기관명, 국가코드, 국가명]}
    private static Map<String, String> countryData; // {국가명: 국가코드}
    private static final int MAX_FILES_PER_FOLDER = 100000; // 한 폴더당 최대 파일 수
    private static final ReentrantLock folderLock = new ReentrantLock();
    private static AtomicInteger fileCount = new AtomicInteger(0);
    private static AtomicInteger folderIndex = new AtomicInteger(1);
    private static BlockingQueue<FileTask> taskQueue = new LinkedBlockingQueue<>();

    public static void main(String[] args) {
        // 서버에서 연산 작업용
        String jsonDirectoryPath = args[0];
        String rorDataPath = "/home/mediazen/kbds_pubmed/java_jar/java_config/ror-prep.json";
        String baseFolderPath = jsonDirectoryPath.replace("json", "json_prep");

        try {
            System.out.println("Loading ror-data {기관명: [기관명, 국가코드, 국가명]}");
            orgData = LoadRORData.loadOrganizationData(rorDataPath);

            System.out.println("Loading country {국가명: 국가코드}");
            countryData = LoadRORData.loadCountryData(rorDataPath);

            System.out.println("Processing documents from JSON files in directory: " + jsonDirectoryPath);
            List<File> jsonFiles = findJsonFiles(jsonDirectoryPath);

            // Create worker threads
            int numThreads = Runtime.getRuntime().availableProcessors();
            for (int i = 0; i < numThreads; i++) {
                new Thread(new Worker(baseFolderPath)).start();
            }

            // Add tasks to the queue
            for (File jsonFile : jsonFiles) {
                taskQueue.put(new FileTask(jsonFile));
            }

            // Shutdown the worker threads after processing all tasks
            for (int i = 0; i < numThreads; i++) {
                taskQueue.put(new FileTask(null)); // Signal the threads to stop
            }

            System.out.println("Documents processing started.");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static List<File> findJsonFiles(String directoryPath) throws IOException {
        System.out.println("대상 폴더: " + directoryPath);
        try (Stream<Path> paths = Files.walk(Paths.get(directoryPath))) {
            List<File> jsonFiles = paths.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".json"))
                    .map(Path::toFile).collect(Collectors.toList());
            System.out.println("찾은 JSON 파일 갯수 : " + jsonFiles.size());
            return jsonFiles;
        } catch (IOException e) {
            System.err.println("Error 대상 경로: " + directoryPath);
            throw e;
        }
    }

    private static class Worker implements Runnable {
        private final String baseFolderPath;

        public Worker(String baseFolderPath) {
            this.baseFolderPath = baseFolderPath;
        }

        @Override
        public void run() {
            try {
                FileTask task;
                while ((task = taskQueue.take()).getFile() != null) {
                    processFile(task.getFile());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private void processFile(File jsonFile) {
            try {
                int currentCount = fileCount.incrementAndGet();
                String folderPath = baseFolderPath + "/batch_" + ((currentCount - 1) / MAX_FILES_PER_FOLDER + 1);

                // Ensure folder exists
                folderLock.lock();
                try {
                    Path folder = Paths.get(folderPath);
                    if (!Files.exists(folder)) {
                        Files.createDirectories(folder);
                    }
                } finally {
                    folderLock.unlock();
                }

                System.out.println("■ 진행 상태 : " + currentCount + "번째 진행 중 ■ 파일명 " + jsonFile.getName());

                String jsonString = new String(Files.readAllBytes(jsonFile.toPath()));
                String updatedJsonString = processJsonContent(jsonString);

                // 일치하는 데이터가 없는 경우 저장하지 않음
                if (updatedJsonString != null) {
                    String fileNameWithoutExtension = jsonFile.getName().replace(".json", "");
                    saveJsonToFile(folderPath, fileNameWithoutExtension + "_prep", updatedJsonString);
                    System.out.println("■■■■■■■■■■■■■■■■■파일명 : " + jsonFile.getName() + "■■■■■■■■■■■■■■■■■");
                } else {
                    System.out.println("일치하는 데이터가 없어 파일을 저장하지 않음: " + jsonFile.getName());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static class FileTask {
        private final File file;

        public FileTask(File file) {
            this.file = file;
        }

        public File getFile() {
            return file;
        }
    }

    private static String processJsonContent(String jsonString) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(jsonString);
        boolean hasMatchingOrgData = false;
        boolean hasMatchingCountryData = false;

        if (rootNode.isObject()) {
            JsonNode medlineCitationNode = rootNode.path("MedlineCitation");
            if (medlineCitationNode.isObject()) {
                JsonNode articleNode = medlineCitationNode.path("Article");
                JsonNode authorListNode = articleNode.path("AuthorList").path("Author");

                if (authorListNode.isArray()) {
                    for (JsonNode authorNode : authorListNode) {
                        JsonNode affiliationInfoNode = authorNode.path("AffiliationInfo");
                        ObjectNode affiliationUpdate;

                        if (affiliationInfoNode.isMissingNode()) {
                            affiliationUpdate = (ObjectNode) ((ObjectNode) authorNode).putObject("AffiliationInfo");
                        } else if (affiliationInfoNode.isObject()) {
                            affiliationUpdate = (ObjectNode) affiliationInfoNode;
                        } else {
                            System.err.println("Unexpected node type for AffiliationInfo: " + affiliationInfoNode.getNodeType());
                            continue;
                        }

                        String affiliation = affiliationUpdate.path("Affiliation").path("value").asText();
                        System.out.println("Processing document ID: " + rootNode.path("PMID").path("value").asText() + " with affiliation: " + affiliation);

                        // Affiliation key가 존재할 때만 진행
                        if (affiliation != null && !"".equals(affiliation) && !"null".equals(affiliation)) {
                            String[] orgDataArray = findMatchingOrganizationData(affiliation);
                            if (orgDataArray != null) {
                                // 기관명 매칭 후 기관명 PUT
                                affiliationUpdate.put("institution_name", orgDataArray[0]);
                                hasMatchingOrgData = true;
                            }

                            Entry<String, String> countryEntry = findMatchingCountryCode(affiliation);
                            if (countryEntry != null) {
                                // 국가 코드 매칭 후 국가 정보 PUT
                                affiliationUpdate.put("country_code", countryEntry.getKey());
                                affiliationUpdate.put("country_name", countryEntry.getValue());
                                hasMatchingCountryData = true;
                            }
                        }
                    }

                    // 두 조건이 모두 충족되는 경우에만 JSON을 저장
                    if (hasMatchingOrgData && hasMatchingCountryData) {
                        return objectMapper.writeValueAsString(rootNode);
                    } else {
                        System.out.println("일치하는 기관명 또는 국가 코드가 모두 존재하지 않아 업데이트 하지 않음.");
                    }
                }
            }
        }

        return null; // 일치하는 데이터가 없을 경우 null 반환
    }

    private static void saveJsonToFile(String directoryPath, String fileNameWithoutExtension, String jsonString) throws IOException {
        // Ensure directory exists
        Path directory = Paths.get(directoryPath);
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
        }

        Path filePath = Paths.get(directoryPath, fileNameWithoutExtension + ".json");
        Files.write(filePath, jsonString.getBytes());

        System.out.println("JSON 파일 저장 완료: " + filePath);
    }

    private static String[] findMatchingOrganizationData(String affiliation) {
        for (Map.Entry<String, String[]> entry : orgData.entrySet()) {
            if (affiliation.contains(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static Entry<String, String> findMatchingCountryCode(String affiliation) {
        for (Map.Entry<String, String> entry : countryData.entrySet()) {
            if (affiliation.contains(entry.getKey())) {
                return entry;
            }
        }
        return null;
    }
}
