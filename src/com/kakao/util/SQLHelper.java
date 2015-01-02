package com.kakao.util;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class SQLHelper {
	public static String removeSqlComment(String statement) throws Exception{
		String query = statement;
		boolean haveComment = true;
		while(haveComment){
			if(query.charAt(0)=='/' && query.charAt(1)=='*'){
				int pos = query.indexOf("*/");
				if(pos>=0){
					query = query.substring(pos+2).trim();
					haveComment = true;
				}else{
					System.out.println("Can't remove multi-line comment, query \""+query+"\"");
					return null;
				}
			}else if(query.charAt(0)=='-' && query.charAt(1)=='-'){
				int pos1 = query.indexOf('\r');
				int pos2 = query.indexOf('\n');
				int pos = (pos1>pos2) ? pos1 : pos2;
				if(pos>=0){
					query = query.substring(pos+1).trim();
					haveComment = true;
				}else{
					System.out.println("Can't remove single-line comment, query \""+query+"\"");
					return null;
				}
			}else{
				haveComment = false;
			}
		}
		
		return query;
	}
	
	public static boolean isSelectQuery(String statement) throws Exception{
		if(statement==null || statement.length()<=2)
			return false;
		
		char currCh, nextCh;
		int currPosition = 0;
		boolean inSingleLineComment = false;
		boolean inBlockComment = false;
		
		for(currPosition=0; currPosition<(statement.length()-1); currPosition++){
			currCh = statement.charAt(currPosition);
			nextCh = statement.charAt(currPosition+1);
			
			if(inSingleLineComment){
				if(currCh=='\r' || currCh=='\n'){
					inSingleLineComment = false;
				}
				continue;
			}else if(inBlockComment){
				if(currCh=='*' && nextCh=='/'){
					inBlockComment = false;
					currPosition++;
				}
				continue;
			}else if(currCh=='-' && nextCh=='-'){
				inSingleLineComment = true;
				currPosition++;
				continue;
			}else if(currCh=='/' && nextCh=='*'){
				inBlockComment = true;
				currPosition++;
				continue;
			}
			
			if(currCh==' ' || currCh=='\t' || currCh=='\r' || currCh=='\n' || currCh=='('){
				continue;
			}
			
			if(currCh=='s' || currCh=='S'){
				return true;
			}
			
			return false;
		}
		
		return false;
	}
	
	public static String getNextToken(String text, Map<String, String> ignoreKeywordMap, String delimiter){
		StringTokenizer st = new StringTokenizer(text, delimiter);
		 
		while (st.hasMoreElements()) {
			String token = st.nextToken();
			if(ignoreKeywordMap.containsKey(token)){
				continue;
			}
			
			return token;
		}
		
		return null;
	}
	
	protected static boolean examineSqlObjectName(String name){
		if(name==null || name.length()<=0)
			return true;
		
		for (int i=0; i<name.length(); i++) {
			char c = name.charAt(i);
			if (c=='$' || c=='_') continue;
			if (c < 0x30 || (c >= 0x3a && c <= 0x40) || (c > 0x5a && c <= 0x60) || c > 0x7a){
				return false;
			}
		}
		return true;
	}
	
	/**
	 * return String[]{dbName, tableName}
	 */
	public static String[] normalizeTableName(String dbTable){
		if(dbTable==null) return null;
		
		String dbName = null;
		String tableName = null;
		
		int pos = dbTable.indexOf('.');
		if(pos<0){
			dbName = null;
			tableName = dbTable;
		}else{
			dbName = dbTable.substring(0,pos);
			tableName = dbTable.substring(pos+1);
		}
		
		if(examineSqlObjectName(dbName) || examineSqlObjectName(tableName))
			return new String[]{dbName, tableName};
		
		return null;
	}
	
	private static Map<String, String> ignoreKeywordMap = new HashMap<String, String>();
	static{
		ignoreKeywordMap.put("insert", "1");
		ignoreKeywordMap.put("update", "1");
		ignoreKeywordMap.put("delete", "1");
		ignoreKeywordMap.put("select", "1");
		ignoreKeywordMap.put("replace", "1");
		ignoreKeywordMap.put("truncate", "1");
		ignoreKeywordMap.put("drop", "1");
		ignoreKeywordMap.put("alter", "1");

		ignoreKeywordMap.put("low_priority", "1");
		ignoreKeywordMap.put("high_priority", "1");
		ignoreKeywordMap.put("quick", "1");
		ignoreKeywordMap.put("ignore", "1");

		ignoreKeywordMap.put("into", "1");
		ignoreKeywordMap.put("from", "1");

		ignoreKeywordMap.put("table", "1");
	}
	
	public static String[] extractFirstTableName(String statement){
		// Remove all string literal
		String strippedStatement = statement.replaceAll("\"(\\.|[^\"])*\"", "");
		strippedStatement = strippedStatement.replaceAll("'(\\.|[^'])*'", "");
		// Remove single line comment
		strippedStatement = strippedStatement.replaceAll("--[^\r\n]*", ""); // "^(([^']+|'[^']*')*)--[^\r\n]*"
		// Remove block comment
		strippedStatement = strippedStatement.replaceAll("/\\*(?:.|[\n\r])*?\\*/","");
		// Replace all new line and white-space(space and tab) to space 
		strippedStatement = strippedStatement.replaceAll("[\r\n\t ]+"," ");
		// Remove all back-quotation mark
		strippedStatement = strippedStatement.replaceAll("`"," ");
		
		// Remove initial "(" and white-space for "(select ...) union all (select ...)" or "(select * from tab)"
		int removePos = 0;
		for(removePos=0; removePos<strippedStatement.length(); removePos++){
			char c = strippedStatement.charAt(removePos);
			if(c=='(' || c=='\r' || c=='\n' || c=='\t' || c==' '){
				continue;
			}else{
				break;
			}
		}
		if(removePos>0)
			strippedStatement = strippedStatement.substring(removePos);
		
		// Make it to lowercase
		strippedStatement = strippedStatement.trim().toLowerCase();

		String tableName = null;
		if(strippedStatement.startsWith("select") || strippedStatement.startsWith("delete")){
			// SELECT ... FROM table_references ...
			// DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM tbl_name ...
			boolean found = false;
			int scanPosition = 0;
			while(!found && scanPosition<strippedStatement.length()){
				int pos1 = strippedStatement.indexOf("from", scanPosition);
				if(pos1<0) return null;

				tableName = getNextToken(strippedStatement.substring(pos1+4), ignoreKeywordMap, " ,;)");
				if(tableName==null) return null;
				if(tableName.charAt(0)!='('){
					found = true;
				}else{
					found = false;
					scanPosition = pos1 + 4;
				}

				if(found){
					return normalizeTableName(tableName);
				}
			}
		}else if(strippedStatement.startsWith("insert") || strippedStatement.startsWith("update") || strippedStatement.startsWith("replace")){
			// INSERT [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE] [INTO] tbl_name ...
			// UPDATE [LOW_PRIORITY] [IGNORE] table_reference ...
			// REPLACE [LOW_PRIORITY | DELAYED] [INTO] tbl_name
			tableName = getNextToken(strippedStatement, ignoreKeywordMap, " ,;)");
			if(tableName==null) return null;
			return normalizeTableName(tableName);
		}else if(strippedStatement.startsWith("truncate") || strippedStatement.startsWith("drop") || strippedStatement.startsWith("alter")){
			tableName = getNextToken(strippedStatement, ignoreKeywordMap, " ,;)");
			if(tableName==null) return null;
			return normalizeTableName(tableName);
		}
		
		return null;
	}
	
	public static void main(String[] args) throws Exception{
		String[] testQuerys = new String[]{
				"sELect g.*, A.K from /*df*/ B, KLJ as A",
				"select * from -- not_table\nTABLE_A;",
				"select * from TABLE_A",
				"select * from TABLE_A LIMIT 34",
				"select * from TABLE_A LIMIT ?",
				"select * from TABLE_A LIMIT 34,?",
				"select * from TABLE_A LIMIT ?,?",
				"select * from TABLE_A LIMIT ? OFFSET 3",
				"select * from TABLE_A LIMIT ? OFFSET ?",
				"select * from TABLE_A LIMIT ALL OFFSET ?",
				"select * from TABLE_A LIMIT ALL OFFSET 3",
				"select * from TABLE_A OFFSET 3",
				"select A,sdf,sch.tab.col from TABLE_A;",
				"  	 select k, * from K as skldjfl where i=0",
				" select MAX(k+2), COUNT(*), MYCOL from K",
				"SELECT * FROM TA2 LEFT JOIN O USING (col1, col2)\nwhere D.OasSD = 'asdf' And (kj >= 4 OR l < 'sdf') ",
				"seLECT 	my as KIO, lio aS\nNE fRom TA2 LEFT OUter 		JOIN O as TA3\nwhere D.OasSD = 'asdf' And (kj >= 4 OR l < 'sdf') \n",
				"\nselect * from a \nINNer Join TAB_2 ON i.o = p.l whEre 'sdf'>'asdf' AND \n	(\n	OL<>? \n			OR \n	L NOT IN (SELECT * FROM KJSD)\n	)",
				"select * from k where L IS NOT NUll;",
				"(select sdf from sdfd) UNION (select * from k);",
				"update mytab set jk=das, d=kasd+asd/d+3 where KL>= ds OR (k not in (SELECT K from KS));",
				"insert into tabName VALUES ('sdgf', ?, ?)",
				"insert into myschama.tabName2 (col1, col2, col3) VALUES ('sdgf', ?, ?)",
				"delete from jk",
				"delete from asdff where INI = 94 OR (ASD>9 AND (SELECT MAX(ID) from myt) > ?)",
				"\nselect  *  from  (  SELECT  intermediate.id  as  id  ,  intermediate.date  as  \ndate  FROM  (  SELECT  DISTINCT  ON  (  id  )  *  -- FROM  (  SELECT  \nwct_workflows.workflow_id  as  id  ,  wct_transaction.date  as  date  FROM  \nwct_audit_entry  ,  wct_transaction  ,  wct_workflows  WHERE  \n(  wct_audit_entry.privilege  =  'W'  or  wct_audit_entry.privilege  =  \n'C'  )  and  wct_audit_entry.outcome  =  't'  and  \nwct_audit_entry.transaction_id  =  wct_transaction.transaction_id  and  \nwct_transaction.user_id  = 164 and  wct_audit_entry.object_id  =  \nwct_workflows.active_version_id)))",
				"\n(select  *  from  (  SELECT  intermediate.id  as  id  ,  intermediate.date  as  \ndate  FROM  (  SELECT  DISTINCT   (  id  )  FROM  (  SELECT  \nwct_workflows.workflow_id  as  id  ,  wct_transaction.date  as  date  FROM  \nwct_audit_entry  ,  wct_transaction  ,  wct_workflows  WHERE  \n(  wct_audit_entry.privilege  =  'W'  or  wct_audit_entry.privilege  =  \n'C'  )  and  wct_audit_entry.outcome  =  't'  and  \nwct_audit_entry.transaction_id  =  wct_transaction.transaction_id  and  \nwct_transaction.user_id  = 164 and  wct_audit_entry.object_id  =  \nwct_workflows.active_version_id))))  UNION ( SELECT  wct_workflows.workflow_id  as  \nid  ,  wct_transaction.date  as  date  FROM  wct_audit_entry  ,  \nwct_transaction  ,  wct_workflows  WHERE  (  wct_audit_entry.privilege  =  \n'W'  or  wct_audit_entry.privilege  =  'C'  )  and  wct_audit_entry.outcome  \n=  't'  and  wct_audit_entry.transaction_id  =  \nwct_transaction.transaction_id  and  wct_transaction.user_id  = 164 and  \np= 'asd') ",
				"select  *  from  (  SELECT  intermediate.id  as  id  ,  intermediate.date  as  \ndate  FROM  (  SELECT  DISTINCT   (  id  )   FROM  (  SELECT  \nwct_workflows.workflow_id  as  id  ,  wct_transaction.date  as  date  FROM  \nwct_audit_entry  ,  wct_transaction  ,  wct_workflows  WHERE  \n(  wct_audit_entry.privilege  =  'W'  or  wct_audit_entry.privilege  =  \n'C'  )  and  wct_audit_entry.outcome  =  't'  and  \nwct_audit_entry.transaction_id  =  wct_transaction.transaction_id  and  \nwct_transaction.user_id  = 164 and  wct_audit_entry.object_id  =  \nwct_workflows.active_version_id ))) UNION  SELECT  wct_workflows.workflow_id  as  \nid  ,  wct_transaction.date  as  date  FROM  wct_audit_entry  ,  \nwct_transaction  ,  wct_workflows  WHERE  (  wct_audit_entry.privilege  =  \n'W'  or  wct_audit_entry.privilege  =  'C'  )  and  wct_audit_entry.outcome  \n=  't'  and  wct_audit_entry.transaction_id  =  \nwct_transaction.transaction_id  and  wct_transaction.user_id  = 164 and  \nafdf=  (  select  wct_audit_entry.object_id  from  wct_audit_entry  ,  \nwct_workflow_archive  where  wct_audit_entry.object_id  =  \nwct_workflow_archive.archive_id  and  wct_workflows.workflow_id  =  \nwct_workflow_archive.workflow_id  ) \nUNION  SELECT  wct_workflows.workflow_id  \nas  id  ,  wct_transaction.date  as  date  FROM  wct_audit_entry  ,  \nwct_transaction  ,  wct_workflows  WHERE  (  wct_audit_entry.privilege  =  \n'W'  OR  wct_audit_entry.privilege  =  'E'  OR  wct_audit_entry.privilege  =  \n'A'  )  and  wct_audit_entry.outcome  =  't'  and  \nwct_audit_entry.transaction_id  =  wct_transaction.transaction_id  and  \nwct_transaction.user_id  = 164 and  wct_audit_entry.object_id  =  \nwct_workflows.workflow_id    UNION SELECT * FROM interm2  ,  wct_workflow_docs  WHERE  \ninterm2.id  =  wct_workflow_docs.document_id  ORDER BY  id  ,  date  DESC  ",
				"replace df set ki='oasdf', dsd=asd+dd",
				"(select sdf from sdfd) UNION (select * from k) ORDER BY 1,2;",
				"(select sdf from sdfd) UNION (select * from k) ORDER BY 1,asd.sd ;",
				"(select sdf from sdfd) UNION (select * from k) UNION (select * from k2) LIMIT 0,2;",
				"select sdf from sdfd UNION select * from k join j on k.p = asdf.f;",
				"select  *  from  (  select  persistence_dynamic_ot.pdl_id  , \nacs_objects.default_domain_class  as  attribute0  , \nacs_objects.object_type  as  attribute1  ,  acs_objects.display_name \nas  attribute2  ,  persistence_dynamic_ot.dynamic_object_type  as \nattribute3  ,  persistence_dynamic_ot.pdl_file  as  attribute4  from \npersistence_dynamic_ot  ,  acs_objects  where \npersistence_dynamic_ot.pdl_id  =  acs_objects.object_id  )",
				"SELECT * FROM table1 WHERE column1 > ALL (SELECT column2 FROM table1)",
				"INSERT INTO mytable (col1, col2, col3) SELECT * FROM mytable2",
				"insert into foo ( x )  select a from b ",
				"select (case when a > 0 then b + a else 0 end) p from mytable",
				"-- insert dkdkdsss\nSELECT BTI.*, BTI_PREDECESSOR.objid AS predecessor_objid, BTI_PREDECESSOR.item_id\nAS predecessor_item_id, BTIT_PREDECESSOR.bt_item_type_key AS predecessor_type_key,\nCAT.catalog_key, S.objid AS state_objid, S.state_key, S.is_init_state,\nS.is_final_state, mlS.name AS state, BTIT.bt_item_type_key, BTP.bt_processor_key,\nmlBTP.name AS bt_processor_name , CU.objid AS cust_user_objid , CU.title AS\ncust_user_title , CU.firstname AS cust_user_firstname , CU.lastname AS\ncust_user_lastname , CU.salutation2pv AS cust_user_salutation2pv , PV_CU.name_option\nAS cust_user_salutation , A_CU.email AS cust_user_email , '' AS use_option_field,\n'' AS use_readerlist , BTI_QUOTATION.quotation_type2pv , BTI_QUOTATION.is_mandatory\nAS quotation_is_mandatory , BTI_QUOTATION.is_multiple AS quotation_is_multiple\n, BTI_QUOTATION.expiration_datetime AS quotation_expiration_datetime ,\nBTI_QUOTATION.hint_internal AS quotation_hint_internal , BTI_QUOTATION.hint_external\nAS quotation_hint_external , BTI_QUOTATION.filter_value AS quotation_filter_value\n, BTI_QUOTATION.email_cc AS quotation_email_cc , BTI_QUOTATION.notification1_datetime\nAS notification1_datetime , BTI_QUOTATION.notification2_datetime AS\nnotification2_datetime , BTI_RFQ.filter_value AS request_for_quotation_filter_value\nFROM tBusinessTransactionItem BTI LEFT OUTER JOIN tBusinessTransactionItem_Quotation\nBTI_QUOTATION ON BTI_QUOTATION.this2business_transaction_item = BTI.objid LEFT\nOUTER JOIN tBusinessTransactionItem_RequestForQuotation BTI_RFQ ON\nBTI_RFQ.this2business_transaction_item = BTI.objid LEFT OUTER JOIN\ntBusinessTransactionItem BTI_PREDECESSOR ON BTI_PREDECESSOR.objid\n= BTI.predecessor2bt_item, tBusinessTransactionItemType BTIT_PREDECESSOR\n, tBusinessTransactionItemType BTIT, tBusinessTransactionProcessor BTP,\nmltBusinessTransactionProcessor mlBTP, tLanguagePriority LP_BTP, tState S, mltState\nmlS, tLanguagePriority LP_S, tCatalog CAT\n, tBusinessTransactionItem2BusinessTransaction BTI2BT ,\ntBusinessTransactionItem2SessionCart BTI2SC , tSessionCart SC , tCustUser CU_MASTER\n, tCustUser CU , tPopValue PV_CU , tAddress A_CU , tAddress2CustUser A2CU WHERE\nBTI.objid <> -1 AND BTI_PREDECESSOR.this2bt_item_type = BTIT_PREDECESSOR.objid\nAND BTI.this2bt_item_type = BTIT.objid AND BTI.this2bt_processor = BTP.objid\nAND mlBTP.this2master = BTP.objid AND mlBTP.this2language = LP_BTP.item2language\nAND LP_BTP.master2language = 0 AND LP_BTP.this2shop = 0 AND LP_BTP.priority\n= (SELECT MIN(LP_BTP2.priority) FROM tLanguagePriority LP_BTP2,\nmltBusinessTransactionProcessor mlBTP2 WHERE LP_BTP2.master2language = 0 AND\nLP_BTP2.this2shop = 0 AND LP_BTP2.item2language = mlBTP2.this2language\nAND mlBTP2.this2master = BTP.objid ) AND BTI.this2catalog = CAT.objid AND S.objid\n= BTI.bt_item2state AND mlS.this2master = S.objid AND mlS.this2language\n= LP_S.item2language AND LP_S.master2language = 0 AND LP_S.this2shop = 0 AND\nLP_S.priority = (SELECT MIN(LP_S2.priority) FROM tLanguagePriority LP_S2, mltState\nmlS2 WHERE LP_S2.master2language = 0 AND LP_S2.this2shop = 0 AND LP_S2.item2language\n= mlS2.this2language AND mlS2.this2master = S.objid ) AND BTI.objid\n= BTI2BT.this2business_transaction_item AND CU_MASTER.objid = 1101 AND\nCU.this2customer = CU_MASTER.this2customer AND SC.this2custuser = CU.objid AND\nBTI.objid = BTI2SC.this2business_transaction_item AND BTI.bt_item2state = 6664\nAND BTI2SC.is_master_cart_item = 1 AND BTI2SC.this2session_cart = SC.objid AND\nEXISTS (SELECT NULL FROM tBusinessTransaction BT, tBusinessTransactionType BTT\nWHERE BT.objid = BTI2BT.this2business_transaction AND BTT.objid = BT.this2bt_type\nAND BTT.business_transaction_type_key = 'order:master_cart') AND PV_CU.objid\n= CU.salutation2pv AND A2CU.this2custuser = CU.objid AND A2CU.is_billing_default\n= 1 AND A2CU.this2address = A_CU.objid ORDER BY BTI.dbobj_create_datetime DESC",
				"/*test*/ select * from Person where deptname='it' AND NOT (age=24)"
		};
		
		for(int idx=0; idx<testQuerys.length; idx++){
			System.out.print("[Query "+(idx+1)+"] ");
			String[] dbTables = extractFirstTableName(testQuerys[idx]);
			if(dbTables==null){
				System.out.println("--> Failed");
			}else{
				System.out.println("--> Succeed ["+dbTables[0]+"]["+dbTables[1]+"]");
			}
		}
		
		for(int idx=0; idx<testQuerys.length; idx++){
			System.out.print("[Query "+(idx+1)+"] ");
			boolean isSelect = isSelectQuery(testQuerys[idx]);
			if(isSelect){
				System.out.println("--> SELECT");
			}else{
				System.out.println("--> NOT SELECT");
			}
		}
	}
}
