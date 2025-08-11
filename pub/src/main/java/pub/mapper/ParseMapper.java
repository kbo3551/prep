package pub.mapper;

import java.util.Map;

import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface ParseMapper {
//    @Select("SELECT NEXT VALUE FOR author_no_seq")
    int getNextAuthorNo();

//    @Select("SELECT NEXT VALUE FOR affiliation_no_seq")
    int getNextAffiliationNo();

//    @Select("SELECT NEXT VALUE FOR ref_no_seq")
    int getNextRefNo();

//    @Select("SELECT NEXT VALUE FOR descriptor_index")
    int getNextDescriptorIndex();
    
    int getNextKeywordIndex();

    void insertPmidList(int pmid);
    void insertPubdate(Map<String, Object> params);
    void insertArticleTitle(Map<String, Object> params);
    void insertAuthor(Map<String, Object> params);
    void insertAffiliation(Map<String, Object> params);
    void insertAuthorAffiliation(Map<String, Object> params);
    void insertDatabankList(Map<String, Object> params);
    void insertJournal(Map<String, Object> params);
    void insertKeywordList(Map<String, Object> params);
    void insertReference(Map<String, Object> params);
    void insertMeshHeading(Map<String, Object> params);
    void insertMeshDescList(Map<String, Object> params);
    void insertMeshTrees(Map<String, Object> params);
}
