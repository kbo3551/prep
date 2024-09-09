package pub.prep.convert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;
/**
 * 추가적인 기관명 이슈사항에 대한 대처 
 * 1. ror데이터에 University Hospital, Pharmac 와 같은 기관명을 정제하여 제거 및 킵 해주기
 * 2. 기준이 되는 ror데이터에서 고객에서 검토한 기관명 list를 엑셀로 받아와 JSON에서 remove해주기
 * 
 */
public class JsonFilterFromExcel {

    public static void main(String[] args) throws Exception {
        // JSON 파일 로드
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode jsonArray = (ArrayNode) mapper.readTree(new File("C:\\Users\\mediazen0\\Desktop\\v1.51-2024-08-21-ror-data\\v1.51-2024-08-21-ror-data.json"));

        // Excel 파일 로드
        FileInputStream fis = new FileInputStream(new File("C:\\Users\\mediazen0\\Desktop\\ror-json_delete.xlsx"));
        Workbook workbook = new XSSFWorkbook(fis);
        Sheet sheet = workbook.getSheetAt(0);

        // Excel 파일의 각 행을 순회
        Iterator<Row> rowIterator = sheet.iterator();
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();

            // Excel 첫 번째 행(헤더) 스킵
            if (row.getRowNum() == 0) continue;

            // "MP 팀", "NO", "ROR기관명1" 열 가져오기
            String action = row.getCell(0) != null ? row.getCell(0).getStringCellValue().trim() : "";
            String rorName = row.getCell(2) != null ? row.getCell(2).getStringCellValue().trim() : "";

            // "Delete"일 경우 JSON에서 제거
            if ("Delete".equalsIgnoreCase(action) && !rorName.isEmpty()) {
                System.out.println("Deleting: " + rorName); // 로그 출력
                removeJsonNodeByName(jsonArray, rorName);
            }
        }

        // 리소스 해제
        workbook.close();
        fis.close();

        // 수정된 JSON을 파일로 저장
        mapper.writerWithDefaultPrettyPrinter().writeValue(new File("C:\\Users\\mediazen0\\Desktop\\ror-prep.json"), jsonArray);
    }

    private static void removeJsonNodeByName(ArrayNode jsonArray, String nameToRemove) {
        Iterator<JsonNode> iterator = jsonArray.iterator();

        while (iterator.hasNext()) {
            JsonNode node = iterator.next();
            String name = node.get("name").asText().trim();

            if (nameToRemove.equalsIgnoreCase(name)) {
                iterator.remove();
                System.out.println("Removed: " + nameToRemove); // 로그 출력
            }
        }
    }
}
