package pub.prep.transfer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import pub.mapper.ParseMapper;

public class MeshTreeDataToMysql_Atomic {

	private static SqlSessionFactory sqlSessionFactory;

    public static void main(String[] args) {
        String resource = "D:\\xml-mybatis-config.xml";
        String directoryPath = "D:\\pubmed_xml\\MeSH\\desc2024";

        try {
            InputStream inputStream = new FileInputStream(resource);
            sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
            processXmlFilesInDirectory(directoryPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void processXmlFilesInDirectory(String directoryPath) throws Exception {
        List<File> xmlFiles = findXmlFiles(directoryPath);
        int totalFiles = xmlFiles.size();
        AtomicInteger fileCount = new AtomicInteger(0);

        xmlFiles.parallelStream().forEach(xmlFile -> {
            try {
                int currentCount = fileCount.incrementAndGet();
                System.out.println("■ 진행 상태 : " + currentCount + "번째 진행 중 ■ 전체 파일수 : " + totalFiles + " ■ 파일명 " + xmlFile.getName());
                processXmlFile(xmlFile.getAbsolutePath());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public static void processXmlFile(String xmlFilePath) throws Exception {
        File xmlFile = new File(xmlFilePath);
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(xmlFile);
        doc.getDocumentElement().normalize();

        try (SqlSession session = sqlSessionFactory.openSession()) {
        	ParseMapper mapper = session.getMapper(ParseMapper.class);

            // Get all DescriptorRecord elements
            NodeList descriptorRecords = doc.getElementsByTagName("DescriptorRecord");

            for (int i = 0; i < descriptorRecords.getLength(); i++) {
                Element descriptorRecord = (Element) descriptorRecords.item(i);

                // Extract DescriptorUI
                String descriptorUi = descriptorRecord.getElementsByTagName("DescriptorUI").item(0).getTextContent();

                // Extract DescriptorName
                Element descriptorNameElement = (Element) descriptorRecord.getElementsByTagName("DescriptorName").item(0);
                String descriptorName = descriptorNameElement.getElementsByTagName("String").item(0).getTextContent();

                // Insert into mesh_desc_list
                Map<String, Object> descParams = new HashMap<>();
                descParams.put("descriptorUi", descriptorUi);
                descParams.put("descriptorName", descriptorName);
                mapper.insertMeshDescList(descParams);

                // Extract and insert TreeNumbers
                NodeList treeNumberList = descriptorRecord.getElementsByTagName("TreeNumber");
                for (int j = 0; j < treeNumberList.getLength(); j++) {
                    String treeNumber = treeNumberList.item(j).getTextContent();
                    Map<String, Object> treeParams = new HashMap<>();
                    treeParams.put("descriptorUi", descriptorUi);
                    treeParams.put("meshCategory", treeNumber.substring(0, 1));
                    treeParams.put("meshTree", treeNumber);
                    mapper.insertMeshTrees(treeParams);
                }
            }

            session.commit();
        }
    }

    public static List<File> findXmlFiles(String directoryPath) throws IOException {
        System.out.println("대상 폴더: " + directoryPath);
        try (Stream<Path> paths = Files.walk(Paths.get(directoryPath))) {
            List<File> xmlFiles = paths.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".xml"))
                    .map(Path::toFile).collect(Collectors.toList());
            System.out.println("찾은 XML 파일 갯수 : " + xmlFiles.size());
            return xmlFiles;
        } catch (IOException e) {
            System.err.println("Error 대상 경로: " + directoryPath);
            throw e;
        }
    }
}