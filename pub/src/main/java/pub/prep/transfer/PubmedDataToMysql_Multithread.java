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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import pub.mapper.ParseMapper;

public class PubmedDataToMysql_Multithread {

	private static final Logger logger = LoggerFactory.getLogger(PubmedDataToMysql.class);
//    private static final String ERROR_DIR = "/home/mediazen/kbds_pubmed/java_jar/java_config/error_dir";
	private static final String ERROR_DIR = "/home/mediazen/kbds_pubmed/java_jar/java_config/error2_dir";

    private static SqlSessionFactory sqlSessionFactory;

    public static void main(String[] args) {
        String resource = "/home/mediazen/kbds_pubmed/java_jar/java_config/xml-mybatis-config.xml";
        String directoryPath = args[0];
//      String resource = "D:\\xml-mybatis-config.xml";
//      String directoryPath = "C:\\Users\\mediazen0\\Desktop\\JSON_PREP";

        try (InputStream inputStream = new FileInputStream(resource)) {
            sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
            processJsonFilesInDirectory(directoryPath);
        } catch (IOException e) {
            logger.error("Failed to initialize SQL Session Factory", e);
        }
    }

    public static void processJsonFilesInDirectory(String directoryPath) throws IOException {
        List<File> jsonFiles = findJsonFiles(directoryPath);
        int totalFiles = jsonFiles.size();

        // 사용 가능한 프로세서 수에 기반한 스레드 풀 생성
        int nThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
        final AtomicInteger fileCount = new AtomicInteger(0);

        for (File jsonFile : jsonFiles) {
            executorService.submit(() -> {
                int currentCount = fileCount.incrementAndGet();
                logger.info("Processing file {} of {}: {}", currentCount, totalFiles, jsonFile.getName());

                if (!processJsonFile(jsonFile.getAbsolutePath())) {
                    logger.info("Skipping file due to missing country code or institution name: {}", jsonFile.getName());
                }
            });
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error("ExecutorService was interrupted", e);
        }
    }

    public static boolean processJsonFile(String jsonFilePath) {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode;
        try {
            rootNode = objectMapper.readTree(new File(jsonFilePath));
        } catch (IOException e) {
            logger.error("Failed to read JSON file: {}", jsonFilePath, e);
            moveFileToErrorDir(jsonFilePath, "IOException");
            return false;
        }

        JsonNode authorsNode = rootNode.path("MedlineCitation").path("Article").path("AuthorList").path("Author");
        boolean hasCountryCode = false;
        boolean hasInstitutionName = false;

        for (JsonNode authorNode : authorsNode) {
            JsonNode affiliationInfoNode = authorNode.path("AffiliationInfo");

            if (affiliationInfoNode.isMissingNode()) {
                continue; // Skip
            }

            JsonNode countryCodeNode = affiliationInfoNode.path("country_code");
            JsonNode institutionNameNode = affiliationInfoNode.path("institution_name");

            String countryCode = countryCodeNode.isMissingNode() ? "" : countryCodeNode.asText().trim();
            String institutionName = institutionNameNode.isMissingNode() ? "" : institutionNameNode.asText().trim();

            if (!countryCode.isEmpty()) {
                hasCountryCode = true;
            }
            if (!institutionName.isEmpty()) {
                hasInstitutionName = true;
            }
        }

        if (!hasCountryCode || !hasInstitutionName) {
            logger.info("Skipping file due to missing country code or institution name");
            return false; // Skip
        }

        try (SqlSession session = sqlSessionFactory.openSession()) {
            ParseMapper mapper = session.getMapper(ParseMapper.class);

            int pmid = rootNode.path("MedlineCitation").path("PMID").path("value").asInt();

            mapper.insertPmidList(pmid);

            insertPubDate(mapper, rootNode, pmid);
            insertArticleTitle(mapper, rootNode, pmid);
            insertAuthors(mapper, rootNode, pmid);
            insertJournal(mapper, rootNode, pmid);
            insertMeshHeadings(mapper, rootNode, pmid);
            insertReferences(mapper, rootNode, pmid);
            insertDatabankList(mapper, rootNode, pmid);
            insertKeywordList(mapper, rootNode, pmid);

            session.commit();
        } catch (Exception e) {
            logger.error("Database error occurred while processing file: {}", jsonFilePath, e);
            moveFileToErrorDir(jsonFilePath, "PersistenceException");
            return false;
        }

        return true;
    }

    private static void insertPubDate(ParseMapper mapper, JsonNode rootNode, int pmid) {
        JsonNode pubDateNode = rootNode.path("PubmedData").path("History").path("PubMedPubDate");
        for (JsonNode dateNode : pubDateNode) {
            if ("pubmed".equals(dateNode.path("PubStatus").asText())) {
                Map<String, Object> params = new HashMap<>();
                params.put("pmid", pmid);
                params.put("year", dateNode.path("Year").path("value").asInt());
                params.put("month", dateNode.path("Month").path("value").asInt());
                params.put("day", dateNode.path("Day").path("value").asInt());
                mapper.insertPubdate(params);
                break;
            }
        }
    }

    private static void insertArticleTitle(ParseMapper mapper, JsonNode rootNode, int pmid) {
        Map<String, Object> params = new HashMap<>();
        params.put("pmid", pmid);
        params.put("articleTitle", rootNode.path("MedlineCitation").path("Article").path("ArticleTitle").path("value").asText());
        mapper.insertArticleTitle(params);
    }

    private static void insertAuthors(ParseMapper mapper, JsonNode rootNode, int pmid) {
        JsonNode authorListNode = rootNode.path("MedlineCitation").path("Article").path("AuthorList").path("Author");
        for (JsonNode authorNode : authorListNode) {
        	int authorNo = mapper.getNextAuthorNo();
            Map<String, Object> params = new HashMap<>();
            params.put("pmid", pmid);
            params.put("authorNo", authorNo);
            params.put("validYn", authorNode.path("ValidYN").asText());
            params.put("lastname", authorNode.path("LastName").path("value").asText());
            params.put("forename", authorNode.path("ForeName").path("value").asText());
            params.put("initials", authorNode.path("Initials").path("value").asText());
            params.put("suffix", authorNode.path("Suffix").path("value").asText());
            params.put("identifierSource", authorNode.path("Identifier").path("Source").asText());
            params.put("identifier", authorNode.path("Identifier").path("value").asText());
            mapper.insertAuthor(params);

            // Insert Affiliation
            JsonNode affiliationNode = authorNode.path("AffiliationInfo").path("Affiliation");
            if (!affiliationNode.isMissingNode()) {
                Map<String, Object> affParams = new HashMap<>();
                int affiliationNo = mapper.getNextAffiliationNo();
                affParams.put("pmid", pmid);
                affParams.put("affiliationIdentifier", authorNode.path("Identifier").path("value").asText());
                affParams.put("affiliationIdentifierSource", authorNode.path("Identifier").path("Source").asText());
                affParams.put("affiliationNo", affiliationNo);
                affParams.put("affiliation", affiliationNode.path("value").asText());
                affParams.put("affiliationCountry", authorNode.path("AffiliationInfo").path("country_code").asText());
                affParams.put("affiliationOrganization", authorNode.path("AffiliationInfo").path("institution_name").asText());
                mapper.insertAffiliation(affParams);

                // Insert Author-Affiliation 시퀀스 값 받아서 추가
                Map<String, Object> authorAffParams = new HashMap<>();
                authorAffParams.put("pmid", pmid);
                authorAffParams.put("authorNo", authorNo);
                authorAffParams.put("affiliationNo", affiliationNo);
                mapper.insertAuthorAffiliation(authorAffParams);
            }
        }
    }

    private static void insertJournal(ParseMapper mapper, JsonNode rootNode, int pmid) {
        JsonNode journalNode = rootNode.path("MedlineCitation").path("Article").path("Journal");
        Map<String, Object> params = new HashMap<>();
        params.put("pmid", pmid);
        params.put("title", journalNode.path("Title").path("value").asText());
        params.put("isoAbbreviation", journalNode.path("ISOAbbreviation").path("value").asText());
        params.put("volume", journalNode.path("JournalIssue").path("Volume").path("value").asText());
        params.put("issue", journalNode.path("JournalIssue").path("Issue").path("value").asText());
        params.put("pubYear", journalNode.path("JournalIssue").path("PubDate").path("Year").path("value").asInt());
        params.put("pubMonth", journalNode.path("JournalIssue").path("PubDate").path("Month").path("value").asText());
        params.put("pubDay", journalNode.path("JournalIssue").path("PubDate").path("Day").path("value").asInt());
        mapper.insertJournal(params);
    }

    private static void insertMeshHeadings(ParseMapper mapper, JsonNode rootNode, int pmid) {
        JsonNode meshHeadingListNode = rootNode.path("MedlineCitation").path("MeshHeadingList").path("MeshHeading");
        for (JsonNode meshHeadingNode : meshHeadingListNode) {
            Map<String, Object> params = new HashMap<>();
            int descriptorIndex = mapper.getNextDescriptorIndex();
            params.put("pmid", pmid);
            params.put("descriptorIndex", descriptorIndex);
            params.put("descriptorName", meshHeadingNode.path("DescriptorName").path("value").asText());
            params.put("descriptorMajorTopicYn", meshHeadingNode.path("DescriptorName").path("MajorTopicYN").asText());
            params.put("descriptorUi", meshHeadingNode.path("DescriptorName").path("UI").asText());

            JsonNode qualifierNode = meshHeadingNode.path("QualifierName");
            if (!qualifierNode.isMissingNode()) {
                params.put("qualifierName", qualifierNode.path("value").asText());
                params.put("qualifierMajorTopicYn", qualifierNode.path("MajorTopicYN").asText());
                params.put("qualifierUi", qualifierNode.path("UI").asText());
            }

            mapper.insertMeshHeading(params);
        }
    }

    private static void insertReferences(ParseMapper mapper, JsonNode rootNode, int pmid) {
        JsonNode referenceListNode = rootNode.path("PubmedData").path("ReferenceList").path("Reference");

        for (JsonNode referenceNode : referenceListNode) {
            int refNo = mapper.getNextRefNo();
            Map<String, Object> params = new HashMap<>();
            params.put("pmid", pmid);
            params.put("refNo", refNo);
            params.put("citation", referenceNode.path("Citation").path("value").asText());

            JsonNode articleIdListNode = referenceNode.path("ArticleIdList").path("ArticleId");

            // ArticleId가 배열인 경우를 처리
            if (articleIdListNode.isArray()) {
                for (JsonNode articleIdNode : articleIdListNode) {
                    String idType = articleIdNode.path("IdType").asText();
                    String value = articleIdNode.path("value").asText();

                    switch (idType.toLowerCase()) {
                        case "pubmed":
                            params.put("refArticleIdPubmed", value);
                            break;
                        case "doi":
                            params.put("refArticleIdDoi", value);
                            break;
                        case "pmcid":
                            params.put("refArticleIdPmcid", value);
                            break;
                        case "pii":
                            params.put("refArticleIdPii", value);
                            break;
                    }
                }
            } else {
                // ArticleId가 단일 객체인 경우 (예외 처리)
                String idType = articleIdListNode.path("IdType").asText();
                String value = articleIdListNode.path("value").asText();

                switch (idType.toLowerCase()) {
                    case "pubmed":
                        params.put("refArticleIdPubmed", value);
                        break;
                    case "doi":
                        params.put("refArticleIdDoi", value);
                        break;
                    case "pmcid":
                        params.put("refArticleIdPmcid", value);
                        break;
                    case "pii":
                        params.put("refArticleIdPii", value);
                        break;
                }
            }

            mapper.insertReference(params);
        }
    }

    private static void insertDatabankList(ParseMapper mapper, JsonNode rootNode, int pmid) {
        JsonNode databankListNode = rootNode.path("MedlineCitation").path("Article").path("DataBankList");
        String completeYn = databankListNode.path("CompleteYN").asText();
        JsonNode databanks = databankListNode.path("DataBank");

        for (JsonNode databank : databanks) {
            Map<String, Object> params = new HashMap<>();
            params.put("pmid", pmid);
            params.put("databankListCompleteYn", completeYn);
            params.put("databankName", databank.path("DataBankName").asText());

            JsonNode accessionNumbers = databank.path("AccessionNumberList").path("AccessionNumber");
            for (JsonNode accessionNumber : accessionNumbers) {
                params.put("accessionNumber", accessionNumber.asText());
                mapper.insertDatabankList(params);
            }
        }
    }

    private static void insertKeywordList(ParseMapper mapper, JsonNode rootNode, int pmid) {
        JsonNode keywordListNode = rootNode.path("MedlineCitation").path("KeywordList").path("Keyword");
        for (JsonNode keyword : keywordListNode) {
            Map<String, Object> params = new HashMap<>();
            int index = mapper.getNextKeywordIndex();
            params.put("keyword_index", index);
            params.put("pmid", pmid);
            params.put("keyword", keyword.path("value").asText());
            params.put("keywordMajorTopicYn", keyword.path("MajorTopicYN").asText());
            mapper.insertKeywordList(params);
        }
    }

    public static List<File> findJsonFiles(String directoryPath) throws IOException {
        try (Stream<Path> paths = Files.walk(Paths.get(directoryPath))) {
            return paths.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".json"))
                    .map(Path::toFile)
                    .collect(Collectors.toList());
        }
    }

    private static void moveFileToErrorDir(String filePath, String errorType) {
        Path sourcePath = Paths.get(filePath);
        Path targetPath = Paths.get(ERROR_DIR, sourcePath.getFileName().toString());

        try {
            Files.move(sourcePath, targetPath);
            logger.info("Moved file {} to error directory due to {}", filePath, errorType);
        } catch (IOException e) {
            logger.error("Failed to move file {} to error directory", filePath, e);
        }
    }
}