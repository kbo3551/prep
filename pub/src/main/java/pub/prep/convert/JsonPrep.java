package pub.prep.convert;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * JSON 전처리 메서드
 */
public class JsonPrep {

    private static Map<String, String[]> orgData; // {기관명: [기관명, 국가코드, 국가명]}
    private static Map<String, String> countryData; // {국가명: 국가코드}

    public static void main(String[] args) {
        // 로컬 테스트용 
//        String jsonDirectoryPath = "C:\\Users\\mediazen0\\Desktop\\test_JSON";
//        String rorDataPath = "C:\\Users\\mediazen0\\Desktop\\temp\\v1.49-2024-07-11-ror-data.json";
//        String jsonDirPath = jsonDirectoryPath.replace("json", "json_prep");

        // 서버에서 연산 작업용
        String jsonDirectoryPath = args[0];
        String rorDataPath = "/home/mediazen/kbds_pubmed/java_jar/java_config/v1.49-2024-07-11-ror-data.json";
        String jsonDirPath = jsonDirectoryPath.replace("json", "json_prep");

        try {
            System.out.println("Loading ror-data {기관명: [기관명, 국가코드, 국가명]}");
            orgData = LoadRORData.loadOrganizationData(rorDataPath);

            System.out.println("Loading country {국가명: 국가코드}");
            countryData = LoadRORData.loadCountryData(rorDataPath);

            System.out.println("Processing documents from JSON files in directory: " + jsonDirectoryPath);
            processDocumentsFromDirectory(jsonDirectoryPath, jsonDirPath);
            System.out.println("Documents processed.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processDocumentsFromDirectory(String directoryPath, String jsonDirPath) throws IOException {
        List<File> jsonFiles = findJsonFiles(directoryPath);
        int totalFiles = jsonFiles.size();
        int fileCount = 0;

        for (File jsonFile : jsonFiles) {
            fileCount++;
            System.out.println("■ 진행 상태 : " + fileCount + "번째 진행 중 ■ 전체 파일수 : " + totalFiles + " ■ 파일명 " + jsonFile.getName());

            String jsonString = new String(Files.readAllBytes(jsonFile.toPath()));
            String updatedJsonString = processJsonContent(jsonString);

            // Save updated JSON to a new file
            String fileNameWithoutExtension = jsonFile.getName().replace(".json", "");
            saveJsonToFile(jsonDirPath, fileNameWithoutExtension + "_prep", updatedJsonString);

            System.out.println("■■■■■■■■■■■■■■■■■파일명 : " + jsonFile.getName() + "■■■■■■■■■■■■■■■■■");
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

    private static String processJsonContent(String jsonString) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(jsonString);

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
                                affiliationUpdate.put("country_code", orgDataArray[1]);
                                affiliationUpdate.put("country_name", orgDataArray[2]);
                            } else {
                                // 국가 코드 매칭 후 국가 정보 PUT
                                Entry<String, String> countryEntry = findMatchingCountryCode(affiliation);
                                if (countryEntry != null) {
                                    affiliationUpdate.put("country_code", countryEntry.getKey());
                                    affiliationUpdate.put("country_name", countryEntry.getValue());
                                }
                            }
                        }
                    }

                    System.out.println("Final Updated JSON: " + rootNode.toString());
                }
            }
        }

        return objectMapper.writeValueAsString(rootNode);
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
                System.out.println("포함된 기준 비교 대상 문자열: " + entry.getKey());
                System.out.println("포함 비교 대상 문자열: " + affiliation);
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
