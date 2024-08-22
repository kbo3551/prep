package pub.prep.convert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LoadRORData {

	/**
	 * 1. 기관명만 반환하는 메서드 정의
	 * 2. 국가명,국가코드만 반환하는 메서드 정의
	 * 
	 */
//	
//	/**
//	 * 기관명만 반환하는 메서드
//	 * @param filePath
//	 * @return
//	 * @throws IOException
//	 */
//	public static Map<String, String> loadInstitutionData(String filePath) throws IOException {
//		Map<String,String> institutionMap = new HashMap<>();
//		ObjectMapper mapper = new ObjectMapper();
//	    JsonNode root = mapper.readTree(new File(filePath));
//	    
//	    if (root.isArray()) {
//	    	for (JsonNode org : root) {
//	    		String name = org.path("name").asText();
//	    		institutionMap.put(name, name);
//	    	}
//	    }
//	    
//	    return institutionMap;
//	}
	
	
	
	
	public static Map<String, String[]> loadOrganizationData(String filePath) throws IOException {
        Map<String, String[]> orgData = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(new File(filePath));

        if (root.isArray()) {
            for (JsonNode org : root) {
                String name = org.path("name").asText();
                JsonNode countryNode = org.path("country");
                String countryCode = countryNode.path("country_code").asText();
                String countryName = countryNode.path("country_name").asText();
                orgData.put(name, new String[]{name, countryCode, countryName});
            }
        }
        return orgData;
    }

	/**
	 * 국가명,국가코드 반환 메서드
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
    public static Map<String, String> loadCountryData(String filePath) throws IOException {
        Map<String, String> countryData = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(new File(filePath));

        if (root.isArray()) {
            for (JsonNode org : root) {
                JsonNode countryNode = org.path("country");
                String countryCode = countryNode.path("country_code").asText();
                String countryName = countryNode.path("country_name").asText();
                countryData.put(countryName, countryCode);
            }
        }
        return countryData;
    }
}
