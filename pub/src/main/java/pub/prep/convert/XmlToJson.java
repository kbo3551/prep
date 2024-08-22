package pub.prep.convert;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.entity.StringEntity;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("deprecation")
public class XmlToJson {

    public static void main(String[] args) throws ParserConfigurationException, SAXException {
        String dirPath = args[0];
//        String dirPath = inputDirectoryPathFromCmd();
        String jsonDirPath = dirPath.replace("xml", "json");

        try {
            List<File> xmlFiles = findXmlFiles(dirPath);
            int totalFiles = xmlFiles.size(); // 전체 파일 개수
            int fileCount = 0; // 파일 카운트 초기화

            for (File xmlFile : xmlFiles) {
                fileCount++; // 파일 카운트 증가
                System.out.println("■ 진행 상태 : " + fileCount + "번째 진행 중 ■ 전체 파일수 : " + totalFiles + " ■ 파일명 " + xmlFile.getName());
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                Document doc = dBuilder.parse(xmlFile);
                doc.getDocumentElement().normalize();

                NodeList pubmedArticles = doc.getElementsByTagName("PubmedArticle");
                for (int i = 0; i < pubmedArticles.getLength(); i++) {
                    Element pubmedArticle = (Element) pubmedArticles.item(i);

                    // PubmedArticle 엘리먼트를 Map으로 변환
                    Map<String, Object> map = xmlToMap(pubmedArticle);

                    // Jackson을 사용하여 Map을 JSON으로 변환
                    ObjectMapper objectMapper = new ObjectMapper();
                    String jsonString = objectMapper.writeValueAsString(map);

                    // JSON 문자열 출력
                    System.out.println(jsonString);
                    String customId = xmlFile.getName().replace(".xml", "") + "_" + i;
                    // json파일 저장
                    saveJsonToFile(jsonDirPath, customId, map);
                    // 엘라스틱서치에 전송
//                    sendToElasticSearch(jsonString, customId);
                    System.out.println("■■■■■■■■■■■■■■■■■파일명 : " + xmlFile.getName() + "■■■■■■■■■■■■■■■■■");
                    System.out.println("현재 XML의 전체 " + pubmedArticles.getLength() + "에서 " + i + " 번째 진행 중");
                }
            }
        } catch (IOException e) {
            System.err.println("파일을 JSON으로 변환 하는 중 오류가 발생했습니다.");
            e.printStackTrace();
        }
    }

    private static Map<String, Object> xmlToMap(Element element) {
        Map<String, Object> map = new HashMap<>();

        // Element의 속성을 Map에 추가
        NamedNodeMap attributes = element.getAttributes();
        for (int i = 0; i < attributes.getLength(); i++) {
            Node attr = attributes.item(i);
            map.put(attr.getNodeName(), attr.getNodeValue());
        }

        // Element의 자식 노드를 Map에 추가
        NodeList nodeList = element.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                Element childElement = (Element) node;
                Map<String, Object> childMap = xmlToMap(childElement);
                
                // Text content가 있는 경우에 추가
                if (childElement.hasChildNodes() && childElement.getChildNodes().getLength() == 1
                        && childElement.getFirstChild().getNodeType() == Node.TEXT_NODE) {
                    childMap.put("value", childElement.getTextContent());
                }

                // 동일한 이름의 요소를 리스트로 처리
                if (map.containsKey(childElement.getNodeName())) {
                    Object existingValue = map.get(childElement.getNodeName());
                    if (existingValue instanceof List<?>) {
                        @SuppressWarnings("unchecked")
                        List<Object> list = (List<Object>) existingValue;
                        list.add(childMap);
                    } else {
                        List<Object> list = new ArrayList<>();
                        list.add(existingValue);
                        list.add(childMap);
                        map.put(childElement.getNodeName(), list);
                    }
                } else {
                    map.put(childElement.getNodeName(), childMap);
                }
            }
        }

        return map;
    }



    // 지정된 디렉토리에서 모든 XML 파일 찾기
    public static List<File> findXmlFiles(String directoryPath) throws IOException {
        System.out.println("대상 폴더: " + directoryPath);
        try (Stream<Path> paths = Files.walk(Paths.get(directoryPath))) {
            List<File> xmlFiles = paths.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".xml"))
                    .map(Path::toFile).collect(Collectors.toList());
            System.out.println("찾은 xml파일 갯수 : " + xmlFiles.size());
            return xmlFiles;
        } catch (IOException e) {
            System.err.println("Error 대상 경로: " + directoryPath);
            throw e;
        }
    }

    // 엘라스틱서치 PUT 요청 데이터 입력
    private static void sendToElasticSearch(String jsonString, String customId) {
        String elasticsearchUrl = "https://192.168.103.41:9200/pubmed_data/_doc/" + customId;
        String username = "boryeong";
        String password = "mediazen1!";

        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(username, password)
        );

        try (
                CloseableHttpClient httpClient = HttpClients.custom()
                        .setDefaultCredentialsProvider(credsProvider)
                        .setSSLContext(new SSLContextBuilder().loadTrustMaterial(null, (chain, authType) -> true).build())
                        .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                        .setRedirectStrategy(new LaxRedirectStrategy())
                        .build()) {

            HttpPut request = new HttpPut(URI.create(elasticsearchUrl));
            request.setHeader("Content-Type", "application/json");
            request.setEntity(new StringEntity(jsonString));

            try (CloseableHttpResponse response = httpClient.execute(request)) {
                System.out.println("Response Code: " + response.getStatusLine().getStatusCode());
                System.out.println("Response Body: " + response.getEntity().getContent().toString());
            }
        } catch (Exception e) {
            System.err.println("ELK 통신 데이터 저장 통신 실패");
            e.printStackTrace();
        }
    }

    // cmd 화면에서 XML 파일이 있는 폴더의 경로를 입력받는 메서드
    private static String inputDirectoryPathFromCmd() {
        Scanner scanner = new Scanner(System.in);
        System.out.print("XML 파일이 있는 폴더의 경로를 입력하세요: ");
        String directoryPath = scanner.nextLine();
        scanner.close();
        return directoryPath;
    }
    
    // JSON 파일 저장 메서드
    private static void saveJsonToFile(String directoryPath, String customId, Map<String, Object> map) throws IOException {
        // 저장할 디렉토리 경로 생성
        Path directory = Paths.get(directoryPath);
       
        // 디렉토리가 없으면 생성
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
        }

        // 파일 경로 생성
        Path filePath = directory.resolve(customId + ".json");

        // JSON 파일로 저장
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.writeValue(filePath.toFile(), map);

        System.out.println("JSON 파일 저장 완료" + filePath);
    }
}
