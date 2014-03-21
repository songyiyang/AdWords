
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class adwords {
	public static int QUERY_LENGTH;
	private static boolean newLineInd = false;
	// The names of input files 
	public static final String ADVERTISERS = "Advertisers.dat";
	public static final String KEYWORDS = "Keywords.dat";
	public static final String QUERIES = "Queries.dat";
	public static final String INPUT = "system.in";
	// The names of output files 
	public static final String OUTPUT1 = "system.out.1";
	public static final String OUTPUT2 = "system.out.2";
	public static final String OUTPUT3 = "system.out.3";
	public static final String OUTPUT4 = "system.out.4";
	public static final String OUTPUT5 = "system.out.5";
	public static final String OUTPUT6 = "system.out.6";
	// Delimiters to parse
	public static final String DELIMITER = "	";
	public static final String SUBDELIMITER = "=";
	// The input parameters
	private static String username = "";
	private static String password = "";
	private static String pTask1 = "";
	private static String pTask2 = "";
	private static String pTask3 = "";
	private static String pTask4 = "";
	private static String pTask5 = "";
	private static String pTask6 = "";
	
	/********************************** All the pure SQL statements.**********************************/
	// SQLs to drop tables 
	private static final String DROP_ADVERTISERS = "DROP TABLE ADVERTISERS";
	private static final String DROP_KEYWORDS = "DROP TABLE KEYWORDS";
	private static final String DROP_QUERIES = "DROP TABLE QUERIES";
    private static final String DROP_TEMP = "DROP TABLE TEMP";
	private static final String DROP_TEMP2 = "DROP TABLE TEMP2";
	private static final String DROP_BALANCE = "DROP TABLE balance";
	private static final String DROP_CTC = "DROP TABLE CTC";
	// SQLs to create tables
    private static final String CREATE_ADVERTISERS = "CREATE TABLE ADVERTISERS (ADVERTISER_ID NUMBER(8) PRIMARY KEY, BUDGET REAL, CTC REAL)";
	private static final String CREATE_KEYWORDS = "CREATE TABLE KEYWORDS (ADVERTISER_ID NUMBER(8), KEYWORD VARCHAR2(100 BYTE), BID REAL, PRIMARY KEY (ADVERTISER_ID, KEYWORD), FOREIGN KEY (ADVERTISER_ID) REFERENCES ADVERTISERS(ADVERTISER_ID))";
	private static final String CREATE_QUERIES = "CREATE TABLE QUERIES (QUERY_ID NUMBER(8) PRIMARY KEY, QUERY VARCHAR2(350 BYTE))";
	private static final String CREATE_TEMP = "CREATE TABLE TEMP(qid number(8), rankvalue real, advertiserID number(8), balance real, budget real, ctc real, cost real)";
	private static final String CREATE_TEMP2 = "Create Table Temp2(Advertiser_id number(8), keyword varchar2(100 BYTE), query varchar2(350 BYTE), bid number, ctc number, query_id number, budget real, balance real)";
	private static final String CREATE_BALANCE = "Create Table balance(Advertiser_id number(8), balance real)";
	private static final String CREATE_CTC = "CREATE TABLE CTC(advertiser_id number(8), ctccurrent real)";
	//SQLs to create functions
	private static final String CREATE_FUNCTION_SUBSTRBYSEP = 
		"create or replace function substrbysep(sourceString    varchar2, " +
                "destString      varchar2, " +
                "appearPosition  number) " +
			"return varchar2 is " +
			"substring varchar2(255);  " + 
			"begin " +
			"substring := substr(destString || sourceString || destString, " +
			"instr(destString || sourceString || destString, " +
			     "destString, " +
			     "1, " +
			     "appearPosition) + 1, " +
			"instr(destString || sourceString || destString, " +
			     "destString, " +
			     "1, " +
			     "appearPosition + 1) - " +
			"instr(destString || sourceString || destString, " +
			     "destString, " +
			     "1, " +
			     "appearPosition) - 1); " +
			"return(substring); " +
			"end substrbysep; ";
	private static final String CREATE_FUNCTION_STR_LENGTH = 
		"CREATE OR REPLACE FUNCTION str_length(str VARCHAR2) RETURN NUMBER IS " +
		  "len  NUMBER := length(str); " +
		  "nums NUMBER := 0; " +
		  "c    CHAR(1); " +
		"BEGIN " +
		 "if str is null then " +
		 "return 0; " +
		 "end if; " +
		  "FOR i IN 1 .. len  " +
		    "LOOP " +
		    "c := substr(str, i, 1); " +
		    "IF c=' ' THEN " +
		      "nums := nums + 1; " +
		    "END IF; " +
		  "END LOOP; " +
		  "RETURN nums+1; " +
		"END; ";
	private static final String CREATE_FUNCTION_REMOVESAMESTR = 
		"create or replace function RemoveSameStr(oldStr varchar2, sign varchar2) " +
	  "return varchar2 is " +
	"str          varchar2(1000); " +
	  "currentIndex number; " +
	  "startIndex   number; " +
	  "endIndex     number; " +
	  "type str_type is table of varchar2(30) index by binary_integer; " +
	  "arr str_type; " +
	  "Result varchar2(1000); " +
	"begin " +
	  "if oldStr is null then " +
	    "return(''); " +
	  "end if; " +
	  "if length(oldStr) > 1000 then " +
	    "return(oldStr); " +
	  "end if; " +
	  "str := oldStr; " +
	  "currentIndex := 0; " +
	  "startIndex   := 0; " +
	  "loop " +
	    "currentIndex := currentIndex + 1; " +
	    "endIndex     := instr(str, sign, 1, currentIndex); " +
	    "if (endIndex <= 0) then " +
	      "exit; " +
	    "end if; " +
	    "arr(currentIndex) := trim(substr(str, " +
	                                     "startIndex + 1, " +
	                                     "endIndex - startIndex - 1)); " +
	    "startIndex := endIndex; " +
	  "end loop; " +
	  "arr(currentIndex) := substr(str, startIndex + 1, length(str)); " +
	  "for i in 1 .. currentIndex - 1 loop " +
	    "for j in i + 1 .. currentIndex loop " +
	      "if arr(i) = arr(j) then " +
	        "arr(j) := ''; " +
	      "end if; " +
	    "end loop; " +
	  "end loop; " +
	  "str := ''; " +
	  "for i in 1 .. currentIndex loop " +
	    "if arr(i) is not null then " +
	      "str := str || sign || arr(i); " +
	      "arr(i) := ''; " +
	    "end if; " +
	  "end loop; " +
	  "Result := substr(str, 2, length(str)); " +
	  "return(Result); " +
	"end RemoveSameStr; ";
		private static final String CREATE_FUNCTION_NUMOFCHAR = 
			"CREATE OR REPLACE FUNCTION numsofchar(keyword VARCHAR2, query VARCHAR2) RETURN NUMBER IS "+
			"len  NUMBER := str_length(query);"+
			"nums NUMBER := 0;"+
			"BEGIN "+
				"if keyword is null or query is null then "+
					"return 0;"+
				"end if;"+
				"FOR i IN 1 .. len "+
				"LOOP "+
					"IF keyword = substrbysep(query, ' ', i) THEN "+
					"nums := nums + 1;"+
					"END IF;"+
				"END LOOP;"+
			"RETURN nums;"+
			"END;";
		private static final String CREATE_FUNCTION_SIMILARITYCALCULATE = 
			"create or replace function similarityCalculate(pstr in varchar2, pstr2 in varchar2) return real "+
			"is "+
			"i pls_integer:=1;"+
			"a integer:=0;"+
			"b integer:=0;"+
			"sum1 real:=0;"+
			"sum2 real:=0;"+
			"sum3 real:=0;"+
			"str varchar2(300) := Removesamestr(pstr2||' '||pstr, ' '); "+
			"begin "+
				"for i in 1..str_length(str) loop "+
					"a:= numsofchar(substrbysep(str, ' ', i), pstr);"+
					"b:= numsofchar(substrbysep(str, ' ', i), pstr2);"+
					"sum1 := sum1 + power(a,2);"+
					"sum2 := sum2 + power(b,2);"+
					"sum3 := sum3 + a * b;"+
				"end loop;"+
			"return sum3/(sqrt(sum1)*sqrt(sum2));"+
			"end;";
		
		private static final String CREATE_TSTRINGTYPE = 
		"CREATE OR REPLACE TYPE t_string_agg AS OBJECT "+
		"(g_string  VARCHAR2(32767), "+
		"STATIC FUNCTION ODCIAggregateInitialize(sctx  IN OUT  t_string_agg) "+
		"RETURN NUMBER, "+
        "MEMBER FUNCTION ODCIAggregateIterate(self  IN OUT  t_string_agg, value  IN  VARCHAR2) "+
		"RETURN NUMBER, "+
        "MEMBER FUNCTION ODCIAggregateTerminate(self  IN   t_string_agg, returnValue  OUT  VARCHAR2, flags  IN   NUMBER) "+
		"RETURN NUMBER, "+
        "MEMBER FUNCTION ODCIAggregateMerge(self  IN OUT  t_string_agg, ctx2  IN t_string_agg)"+
		"RETURN NUMBER);";
		private static final String CREATE_TSTRINGBODY = 
		"CREATE OR REPLACE TYPE BODY t_string_agg IS "+
		"STATIC FUNCTION ODCIAggregateInitialize(sctx  IN OUT  t_string_agg) "+
	    "RETURN NUMBER IS "+
		"BEGIN "+
	    "sctx := t_string_agg(NULL); "+
	    "RETURN ODCIConst.Success; "+
		"END; "+
		"MEMBER FUNCTION ODCIAggregateIterate(self   IN OUT  t_string_agg, value  IN      VARCHAR2 ) "+
		"RETURN NUMBER IS "+
		"BEGIN "+
		"SELF.g_string := self.g_string || ' ' || value; "+
		"RETURN ODCIConst.Success; "+
		"END; "+
		"MEMBER FUNCTION ODCIAggregateTerminate(self IN   t_string_agg, returnValue  OUT  VARCHAR2, flags  IN   NUMBER) "+
		"RETURN NUMBER IS "+
		"BEGIN "+
		"returnValue := RTRIM(LTRIM(SELF.g_string, ' '), ' '); "+
		"RETURN ODCIConst.Success; "+
		"END; "+
        "MEMBER FUNCTION ODCIAggregateMerge(self  IN OUT  t_string_agg, ctx2  IN t_string_agg) "+
		"RETURN NUMBER IS "+
	    "BEGIN "+
		"SELF.g_string := SELF.g_string || ' ' || ctx2.g_string; "+
		"RETURN ODCIConst.Success; "+
		"END; "+
		"END; ";
		private static final String CREATE_FUNCTION_STRINGAGG =
		"CREATE OR REPLACE FUNCTION string_agg (p_input VARCHAR2) "+
		"RETURN VARCHAR2 "+
		"PARALLEL_ENABLE AGGREGATE USING t_string_agg;";
		private static final String CREATE_TYPE_VARCHAR2VARRAY = "CREATE OR REPLACE TYPE Varchar2Varray IS VARRAY(100) of VARCHAR2(40) ;";
		private static final String CREATE_FUNCTION_SF_SPLIT_STRING = 
		"CREATE OR REPLACE FUNCTION sf_split_string (string VARCHAR2, substring VARCHAR2) RETURN Varchar2Varray IS "+
		"len integer := LENGTH(substring); "+
		"lastpos integer := 1 - len; "+
		"pos integer; "+
		"num integer; "+
		"i integer := 1; "+
		"ret Varchar2Varray := Varchar2Varray(NULL); "+
		"BEGIN "+
		  "LOOP "+
		    "pos := instr(string, substring, lastpos + len); "+
		    "IF pos > 0 THEN "+
		      "num := pos - (lastpos + len); "+
		    "ELSE "+
		      "num := LENGTH(string) + 1 - (lastpos + len); "+
		    "END IF; "+
		    "IF i > ret.LAST THEN "+
		      "ret.EXTEND; "+
		    "END IF; "+
		    "ret(i) := SUBSTR(string, lastpos + len, num); "+
		    "EXIT WHEN pos = 0; "+
		    "lastpos := pos; "+
		    "i := i + 1; "+
		  "END LOOP; "+
		  "RETURN ret; "+
		"END;";
	// SQLs to insert into tables
	private static final String INSERT_ADVERTISERS = "INSERT INTO ADVERTISERS (ADVERTISERS.ADVERTISER_ID, ADVERTISERS.BUDGET, ADVERTISERS.CTC) VALUES (?, ?, ?)";
	private static final String INSERT_KEYWORDS = "INSERT INTO KEYWORDS (KEYWORDS.ADVERTISER_ID, KEYWORDS.KEYWORD, KEYWORDS.BID) VALUES (?, ?, ?)";
	private static final String INSERT_TEMP2 = "INSERT INTO Temp2(advertiser_id, keyword, query, bid, ctc, query_id, budget) SELECT advertiser_id, keyword, query, bid, ctc, query_id, budget  From (SELECT k.ADVERTISER_ID, KEYWORD, BID, CTC, BUDGET, q.QUERY, q.query_id   FROM KEYWORDS k, ADVERTISERS a, QUERIES q WHERE k.ADVERTISER_ID = a.ADVERTISER_ID  AND q.query_id = ? AND k.keyword IN (select column_value from table(select sf_split_string(qu.query, ' ') from queries qu where q.query_id = qu.query_id)) ORDER BY a.advertiser_id)";
	private static final String INSERT_QUERIES = "INSERT INTO QUERIES (QUERIES.QUERY_ID, QUERIES.QUERY) VALUES (?, ?)";	
	private static final String INSERT_BALANCE = "insert into balance(advertiser_id, balance) select advertiser_id, budget from advertisers";
	private static final String INSERT_CTC = "insert into CTC(advertiser_id, ctccurrent) select advertiser_id, ctc from advertisers";
	// SQLs to delete data from tables
	private static final String DELETE_TEMP = "DELETE FROM TEMP";
	private static final String DELETE_TEMP2 = "DELETE FROM TEMP2";
	private static final String DELETE_BALANCE = "DELETE FROM BALANCE";
	private static final String DELETE_CTC = "DELETE FROM　CTC";
	// SQLs to update data from tables
	private static final String UPDATE_CTC = "UPDATE CTC SET ctccurrent = ctccurrent - 0.01 WHERE advertiser_id = ?";
	private static final String UPDATE_BALANCE = "update balance set balance = ? WHERE advertiser_id = ?";
	// SQLs to select each task results
	private static final String SELECT_CTC = "select ctccurrent from ctc where advertiser_id = ?";
	private static final String SELECT_TEMP = "select * from temp order by rankvalue DESC";
	private static final String SELECT_TASK1 = 
		"insert into temp(qid, rankvalue, advertiserid, balance, budget, ctc, cost) "+
		"select DISTINCT query_id as qid, temp3.cost * ctc * similarityCalculate(t2.keyword, temp2.query) as rank, temp2.advertiser_id as advertiserID, balance.balance as balance, budget, ctc, temp3.cost as cost  "+
		"from temp2,   "+
		     "(select string_agg(keyword) as keyword, advertiser_id from keywords group by advertiser_id)  "+
		     " t2, "+
		     "(select advertiser_id, SUM(bid) as cost from temp2   "+
		     "group by advertiser_id) temp3, "+
		     "balance "+
		"where temp2.advertiser_id = temp3.advertiser_id AND t2.advertiser_id = temp2.advertiser_id AND balance.advertiser_id = temp2.advertiser_id AND temp3.cost <= balance.balance "+
		"order by rank DESC";
	
	private static final String SELECT_TASK2 =
		"insert into temp(qid, rankvalue, advertiserid, balance, budget, ctc, cost) "+
		"select DISTINCT query_id as qid, balance.balance * ctc * similarityCalculate(t2.keyword, temp2.query) as rank, temp2.advertiser_id as advertiserID, balance.balance as balance, budget, ctc, temp3.cost as cost "+
		"from temp2,   "+
		     "(select string_agg(keyword) as keyword, advertiser_id from keywords group by advertiser_id  "+
		     ") t2, "+
		     "(select advertiser_id, SUM(bid) as cost from temp2   "+
		     "group by advertiser_id) temp3, "+
		     "balance "+
		"where temp2.advertiser_id = temp3.advertiser_id AND t2.advertiser_id = temp2.advertiser_id AND balance.advertiser_id = temp2.advertiser_id AND temp3.cost <= balance.balance "+
		"order by rank DESC";
	private static final String SELECT_TASK3 =
		"insert into temp(qid, rankvalue, advertiserid, balance, budget, ctc, cost) "+
		"select DISTINCT query_id as qid, temp3.cost * (1-exp(-(balance.balance)/budget)) * ctc * similarityCalculate(t2.keyword, temp2.query) as rank, temp2.advertiser_id as advertiserID, balance.balance as balance, budget, ctc, temp3.cost as cost  "+
		"from temp2,   "+
		     "(select string_agg(keyword) as keyword, advertiser_id from keywords group by advertiser_id  "+
		     ") t2, "+
		     "(select advertiser_id, SUM(bid) as cost from temp2   "+
		     "group by advertiser_id) temp3, "+
		     "balance "+
		"where temp2.advertiser_id = temp3.advertiser_id AND t2.advertiser_id = temp2.advertiser_id AND balance.advertiser_id = temp2.advertiser_id AND temp3.cost <= balance.balance "+
		"order by rank DESC";
	private static final String SELECT_TASK4 = 
		"insert into temp(qid, rankvalue, advertiserid, balance, budget, ctc, cost)  "+
		"select DISTINCT query_id as qid, temp4.cost * ctc * similarityCalculate(t2.keyword, temp2.query) as rank, temp2.advertiser_id as advertiserID, balance.balance as balance, budget, ctc, t3.secbid as cost   "+
		"from  temp2,  "+
		     "(select string_agg(keyword) as keyword, advertiser_id  "+
		      "from keywords  "+
		      "group by advertiser_id) t2,  "+
		     "(select temp3.advertiser_id, MAX(temp4.cost) secbid  "+
		     " from  (select advertiser_id, SUM(bid) as cost  "+
		             "from temp2    "+
		             "group by advertiser_id  "+
		             "order by cost DESC) temp3,   "+
		            "(select advertiser_id, SUM(bid) as cost  "+
		              "from temp2    "+
		                "group by advertiser_id  "+
		                  "order by cost DESC) temp4, balance  "+
		      "where ((temp3.cost = (select MIN(temp5.cost) from  (select advertiser_id, SUM(bid) as cost  "+
		                                             "from temp2    "+
		                                             "group by advertiser_id  "+
		                                             "order by cost DESC)temp5 )  "+
		      "AND temp3.cost = temp4.cost)  "+
		      "OR  "+
		      "temp3.cost > temp4.cost)  "+
		      "AND balance.advertiser_id = temp4.advertiser_id AND temp4.cost< balance.balance  "+
		       "group by temp3.advertiser_id  "+
		       "order by secbid DESC) t3,  " +
		       "(select advertiser_id, SUM(bid) as cost from temp2  "+
		       "group by advertiser_id) temp4,"+
		       "balance   "+
		"where temp2.advertiser_id = temp4.advertiser_id AND temp2.advertiser_id = t3.advertiser_id AND temp2.advertiser_id = t2.advertiser_id AND balance.advertiser_id = temp2.advertiser_id AND temp4.cost <= balance.balance  "+
		"order by rank DESC";
	private static final String SELECT_TASK5 = 
		"insert into temp(qid, rankvalue, advertiserid, balance, budget, ctc, cost)  "+
		"select DISTINCT query_id as qid, balance.balance * ctc * similarityCalculate(t2.keyword, temp2.query) as rank, temp2.advertiser_id as advertiserID, balance.balance as balance, budget, ctc , t3.secbid as cost  "+
		"from  temp2,  "+
		     "(select string_agg(keyword) as keyword, advertiser_id  "+
		      "from keywords  "+
		      "group by advertiser_id) t2,  "+
		      "(select temp3.advertiser_id, MAX(temp4.cost) secbid  "+
			     " from  (select advertiser_id, SUM(bid) as cost  "+
			             "from temp2    "+
			             "group by advertiser_id  "+
			             "order by cost DESC) temp3,   "+
			            "(select advertiser_id, SUM(bid) as cost  "+
			              "from temp2    "+
			                "group by advertiser_id  "+
			                  "order by cost DESC) temp4, balance  "+
			      "where ((temp3.cost = (select MIN(temp5.cost) from  (select advertiser_id, SUM(bid) as cost  "+
			                                             "from temp2    "+
			                                             "group by advertiser_id  "+
			                                             "order by cost DESC)temp5 )  "+
			      "AND temp3.cost = temp4.cost)  "+
			      "OR  "+
			      "temp3.cost > temp4.cost)  "+
			      "AND balance.advertiser_id = temp4.advertiser_id AND temp4.cost< balance.balance  "+
			       "group by temp3.advertiser_id  "+
			       "order by secbid DESC) t3,  "+
			       "(select advertiser_id, SUM(bid) as cost from temp2  "+
			       "group by advertiser_id) temp4,"+
			       "balance   "+
			"where temp2.advertiser_id = temp4.advertiser_id AND temp2.advertiser_id = t3.advertiser_id AND temp2.advertiser_id = t2.advertiser_id AND balance.advertiser_id = temp2.advertiser_id AND temp4.cost <= balance.balance  "+
			"order by rank DESC";
	private static final String SELECT_TASK6 = 
		"insert into temp(qid, rankvalue, advertiserid, balance, budget, ctc, cost)  "+
		"select DISTINCT query_id as qid, temp4.cost * (1-exp(-balance.balance/budget)) * ctc * similarityCalculate(t2.keyword, temp2.query) as rank, temp2.advertiser_id as advertiserID, balance.balance as balance, budget, ctc, t3.secbid as cost  "+
		"from  temp2,  "+
		     "(select string_agg(keyword) as keyword, advertiser_id  "+
		      "from keywords  "+
		      "group by advertiser_id) t2,  "+
		      "(select temp3.advertiser_id, MAX(temp4.cost) secbid  "+
			     " from  (select advertiser_id, SUM(bid) as cost  "+
			             "from temp2    "+
			             "group by advertiser_id  "+
			             "order by cost DESC) temp3,   "+
			            "(select advertiser_id, SUM(bid) as cost  "+
			              "from temp2    "+
			                "group by advertiser_id  "+
			                  "order by cost DESC) temp4, balance  "+
			      "where ((temp3.cost = (select MIN(temp5.cost) from  (select advertiser_id, SUM(bid) as cost  "+
			                                             "from temp2    "+
			                                             "group by advertiser_id  "+
			                                             "order by cost DESC)temp5 )  "+
			      "AND temp3.cost = temp4.cost)  "+
			      "OR  "+
			      "temp3.cost > temp4.cost)  "+
			      "AND balance.advertiser_id = temp4.advertiser_id AND temp4.cost< balance.balance  "+
			       "group by temp3.advertiser_id  "+
			       "order by secbid DESC) t3,  "+
			       "(select advertiser_id, SUM(bid) as cost from temp2  "+
			       "group by advertiser_id) temp4,"+
			       "balance   "+
			"where temp2.advertiser_id = temp4.advertiser_id AND temp2.advertiser_id = t3.advertiser_id AND temp2.advertiser_id = t2.advertiser_id AND balance.advertiser_id = temp2.advertiser_id AND temp4.cost <= balance.balance  "+
			"order by rank DESC";
	/**
	 * This method is to get the connection from DB
	 * @param autoCommit		Y: autoCommit	N: not automatically commit, used for batch update
	 * @return	the DB connection
	 * @throws SQLException
	 */
	public static Connection getConn(boolean autoCommit) throws SQLException {
		DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
//	    Connection conn = DriverManager.getConnection("jdbc:oracle:thin:hr/hr@localhost:1521:orcl","scott", "tiger");
	    Connection conn = DriverManager.getConnection("jdbc:oracle:thin:hr/hr@oracle.cise.ufl.edu:1521:orcl",username, password);
		conn.setAutoCommit(autoCommit);
		return conn;
	}
	
	//check if temp2 is empty, for safety 
	private static void taskInitialization(Connection conn) throws SQLException {
		newLineInd = false;
		try {
			deleteCtc(conn);
			deleteBalance(conn);
			deleteTemp(conn);
			deleteTemp2(conn);
			insertBalance(conn);
			insertCtc(conn);
		} catch (SQLException sqle){
		}
	}
	

	/**
	 * This method is to handle with task1, applying the file system.out.1 as output.
	 * @throws SQLException
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void handleTask1() throws SQLException, IOException, FileNotFoundException {
		System.out.println("************************************************");
		System.out.println("Start handling task1...");
		Connection conn = getConn(true);
		FileOutputStream fos = new FileOutputStream(OUTPUT1);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);
		taskInitialization(conn);
		final int taskSize = Integer.parseInt(pTask1);
		handleAlgorithm(taskSize, conn, bw, 1);
		conn.commit();
		bw.close();
		osw.close();
		fos.close();
		conn.close();	
		System.out.println("This is the end of task1");
		System.out.println("************************************************");
	}
	
	/**
	 * This method is to handle with task2, applying the file system.out.2 as output.
	 * @throws SQLException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void handleTask2() throws SQLException, FileNotFoundException, IOException {
		System.out.println("************************************************");
		System.out.println("Start handling task2...");
		Connection conn = getConn(true);
		FileOutputStream fos = new FileOutputStream(OUTPUT2);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);
		taskInitialization(conn);
		final int taskSize = Integer.parseInt(pTask2);
		handleAlgorithm(taskSize, conn, bw, 2);
		conn.commit();
		bw.close();
		osw.close();
		fos.close();
		conn.close();	
		System.out.println("This is the end of task2");
		System.out.println("************************************************");
	}

	/**
	 * This method is to handle with task3, applying the file system.out.3 as output.
	 * @throws SQLException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void handleTask3() throws SQLException, FileNotFoundException, IOException {
		System.out.println("************************************************");
		System.out.println("Start handling task3...");
		Connection conn = getConn(true);
		FileOutputStream fos = new FileOutputStream(OUTPUT3);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);
		taskInitialization(conn);
		final int taskSize = Integer.parseInt(pTask3);
		handleAlgorithm(taskSize, conn, bw, 3);
		conn.commit();
		bw.close();
		osw.close();
		fos.close();
		conn.close();	
		System.out.println("This is the end of task3");
		System.out.println("************************************************");
	}

	/**
	 * This method is to handle with task4, applying the file system.out.4 as output.
	 * @throws SQLException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void handleTask4() throws SQLException, FileNotFoundException, IOException {
		System.out.println("************************************************");
		System.out.println("Start handling task4...");
		Connection conn = getConn(true);
		FileOutputStream fos = new FileOutputStream(OUTPUT4);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);
		taskInitialization(conn);
		final int taskSize = Integer.parseInt(pTask4);
		handleAlgorithm(taskSize, conn, bw, 4);
		conn.commit();
		bw.close();
		osw.close();
		fos.close();
		conn.close();	
		System.out.println("This is the end of task4");
		System.out.println("************************************************");
	}
	
	/**
	 * This method is to handle with task5, applying the file system.out.5 as output.
	 * @throws SQLException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void handleTask5() throws SQLException, FileNotFoundException, IOException {
		System.out.println("************************************************");
		System.out.println("Start handling task5...");
		Connection conn = getConn(true);
		FileOutputStream fos = new FileOutputStream(OUTPUT5);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);
		taskInitialization(conn);
		final int taskSize = Integer.parseInt(pTask5);
		handleAlgorithm(taskSize, conn, bw, 5);
		conn.commit();
		bw.close();
		osw.close();
		fos.close();
		conn.close();	
		System.out.println("This is the end of task5");
		System.out.println("************************************************");
	}
	
	
	/**
	 * This method is to handle with task6, applying the file system.out.6 as output.
	 * @throws SQLException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void handleTask6() throws SQLException, FileNotFoundException, IOException {
		System.out.println("************************************************");
		System.out.println("Start handling task6...");
		Connection conn = getConn(true);
		FileOutputStream fos = new FileOutputStream(OUTPUT6);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);
		taskInitialization(conn);
		final int taskSize = Integer.parseInt(pTask6);
		handleAlgorithm(taskSize, conn, bw, 6);
		conn.commit();
		bw.close();
		osw.close();
		fos.close();
		conn.close();	
		System.out.println("This is the end of task6");
		System.out.println("************************************************");
	}
	

	
	/**
	 * This method is the core part of using SQLs for each task, for each task, it will select the corresponding output
	 * @param 
	 * @param conn
	 * @param bw
	 * @param i
	 * @throws SQLException
	 * @throws IOException
	 */
	private static void handleAlgorithm(int taskSize, Connection conn, BufferedWriter bw, int i) throws SQLException, IOException {
		
		PreparedStatement handletask;
		PreparedStatement updatebalance = conn.prepareStatement(UPDATE_BALANCE);
		PreparedStatement inserttemp2 = conn.prepareStatement(INSERT_TEMP2);
		PreparedStatement selecttemp = conn.prepareStatement(SELECT_TEMP);
		PreparedStatement updateCtc = conn.prepareStatement(UPDATE_CTC);
		PreparedStatement selectctc = conn.prepareStatement(SELECT_CTC);

		String SELECT_TASK = "";
		switch(i){
		case 1: SELECT_TASK = SELECT_TASK1; 
		break;
		case 2: SELECT_TASK = SELECT_TASK2; 
		break;
		case 3: SELECT_TASK = SELECT_TASK3; 
		break;
		case 4: SELECT_TASK = SELECT_TASK4; 
		break;
		case 5: SELECT_TASK = SELECT_TASK5; 
		break;
		case 6: SELECT_TASK = SELECT_TASK6; 
		break;
		
		}
		
		handletask = conn.prepareStatement(SELECT_TASK);
		for(int ii =1; ii <= QUERY_LENGTH; ii++){
			inserttemp2.setInt(1, ii);
			inserttemp2.execute();
			handletask.execute();
			ResultSet rs= selecttemp.executeQuery();
			ResultSet rs2;
		int k =1;
		int id;
		double x = 0.0, y, balance, cost, b;
		while(rs.next()&& k <= taskSize){
			if (newLineInd){
	        	bw.newLine();
	        }
	        else {
	        	newLineInd = true;
	        }
			balance = Double.parseDouble(rs.getString(4));
			cost = Double.parseDouble(rs.getString(7));
			b = balance - cost;
			id = Integer.parseInt(rs.getString(3));
			y = Double.parseDouble(rs.getString(6));
			updateCtc.setInt(1, id);
			selectctc.setInt(1, id);
			rs2 = selectctc.executeQuery();
			updatebalance.setDouble(1, b);
			updatebalance.setInt(2, id);
			while (rs2.next())x = Double.parseDouble(rs2.getString(1));
			if(x > 0 || (1+x>0 && 1+x<= y)||(2+x>0 && 2+x<= y)) {
				updatebalance.execute();
				bw.write(rs.getString(1) + ", " + k + ", " + rs.getString(3) + ", " + String.format("%.2f", b) + ", " + rs.getString(5));
			}
			else bw.write(rs.getString(1) + ", " + k + ", " + rs.getString(3) + ", " + String.format("%.2f", balance) + ", " + rs.getString(5));
			updateCtc.executeUpdate();
			rs2.close();
			k++;
		}
		rs.close();
		deleteTemp2(conn);
		deleteTemp(conn);
		}
		handletask.close();
		inserttemp2.close();
		updateCtc.close();
		selectctc.close();
		updatebalance.close();
		selecttemp.close();
	}

	/**
	 * This method parse the input files:
	 * 			system.in: to generate variables
	 * 			advertisers.dat: to insert tuples to the table ADVERTISERS
	 * 			keywords.dat: to insert tuples to the table KEYWORDS
	 * 			queries.dat: to insert tuples to the table QUERIES
	 * @param fileName		the input file name
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void disposeFile(String fileName) throws SQLException, IOException {
		File file = null;
		BufferedReader reader = null;
		Connection conn = null;
		int[] num = { 0 };
		try {
			file = new File(fileName);
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			// Parse the file system.in
			if (INPUT.equals(fileName)) {
				int i = 0;
				String sys_input[] = new String[8];
				while ((tempString = reader.readLine()) != null) {
					sys_input[i] = tempString;
					i++;
				}
				parseInput(sys_input);
			} 
			// Parse the file advertisers.dat
			else if (ADVERTISERS.equals(fileName)) {
				conn = getConn(false);
				System.out.println("Start Inserting the data into Advertisers...");
				PreparedStatement ps = conn.prepareStatement(INSERT_ADVERTISERS);
				while ((tempString = reader.readLine()) != null) {
					tempString = tempString.trim();
					if (!"".equals(tempString)) {
						String[] itemTuple = tempString.split(DELIMITER);
						int advertiserID = Integer.parseInt(itemTuple[0]);
						double budget = Double.parseDouble(itemTuple[1]);
						double ctc = Double.parseDouble(itemTuple[2]);
						ps.setInt(1, advertiserID);
						ps.setDouble(2, budget);
						ps.setDouble(3, ctc);
						ps.addBatch();
						
					}
				}
				if (ps != null) {
					num = ps.executeBatch();
				}
				System.out.println("    " + num.length + " rows inserted into Advertisers...");
				ps.close();
				conn.commit();
			} 
			// Parse the file keywords.dat
			else if (KEYWORDS.equals(fileName)) {
				conn = getConn(false);
				PreparedStatement ps = conn.prepareStatement(INSERT_KEYWORDS);
				System.out.println("Start Inserting the data into Keywords...");
				while ((tempString = reader.readLine()) != null) {
					tempString = tempString.trim();
					if (!"".equals(tempString)) {
						String[] itemTuple = tempString.split(DELIMITER);
						int advertiserID = Integer.parseInt(itemTuple[0]);
						String keyword = itemTuple[1];
						double bid = Double.parseDouble(itemTuple[2]);
						/*double bid = Double.parseDouble(itemTuple[0]);*/
						ps.setInt(1, advertiserID);
						ps.setString(2, keyword);
						ps.setDouble(3, bid);
						ps.addBatch();
					}
				}
				if (ps != null) {
					num = ps.executeBatch();
				}
				System.out.println("    " + num.length + " rows inserted into Keywords...");
				ps.close();
				conn.commit();
			}
			else if (QUERIES.equals(fileName)) {
				conn = getConn(false);
				PreparedStatement ps = conn.prepareStatement(INSERT_QUERIES);
				System.out.println("Start Inserting the data into Queries...");
				while ((tempString = reader.readLine()) != null) {
					tempString = tempString.trim();
					if (!"".equals(tempString)) {
						String[] itemTuple = tempString.split(DELIMITER);
						int queryID = Integer.parseInt(itemTuple[0]);
						String query = itemTuple[1];
						ps.setInt(1, queryID);
						ps.setString(2, query);
						ps.addBatch();
					}
				}
				if (ps != null) {
					num = ps.executeBatch();
				}
				System.out.println("    " + num.length + " rows inserted into Queries...");
				QUERY_LENGTH = num.length;
				ps.close();
				conn.commit();
			}
		} catch (IOException ioe) {
			throw ioe;
		} catch (SQLException sqle) {
			// Rollback the transaction if exception occurs
			if (conn != null) {
				try {
					conn.rollback();
				} catch (SQLException sqe) {
				}
			}
			throw sqle;
		} finally {
			// Close the file reader if possible 
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException ioe) {
				}
			}
			// Close the DB connection if possible
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException sqle) {
				}
			}
		}
	}
	
	/**
	 * This method is trying to drop all the tables in case the DB contains them, without throw any exceptions if dropping failed
	 * @throws SQLException
	 */
	public static void dropTables() throws SQLException {
		System.out.println("Dropping tables...");
		PreparedStatement ps = null;
		Connection conn = getConn(true);
		try {
			ps = conn.prepareStatement(DROP_KEYWORDS);
			ps.executeUpdate();
		} catch (SQLException sqle) {
		}
		try {
			ps = conn.prepareStatement(DROP_ADVERTISERS);
			ps.executeUpdate();
		} catch (SQLException sqle) {
		}
		try {
			ps = conn.prepareStatement(DROP_QUERIES);
			ps.executeUpdate();
		} catch (SQLException sqle) {
		}
		try {
			ps = conn.prepareStatement(DROP_TEMP2);
			ps.executeUpdate();
		} catch (SQLException sqle) {
		} 
		try {
			ps = conn.prepareStatement(DROP_TEMP);
			ps.executeUpdate();
		} catch (SQLException sqle) {
		}
		try {
			ps = conn.prepareStatement(DROP_BALANCE);
			ps.executeUpdate();
		} catch (SQLException sqle) {
		}
		try {
			ps = conn.prepareStatement(DROP_CTC);
			ps.executeUpdate();
		} catch (SQLException sqle) {
		}
		finally {
			conn.commit();
			System.out.println("Tables dropped...");
			if (ps != null) {
				ps.close();
			}
			if (conn != null) {
				conn.close();
			}
		}
	}
	
	/**
	 * This method is to create the tables after trying to drop them
	 * @throws SQLException
	 */
	public static void createTables() throws SQLException {
		System.out.println("Creating tables...");
		PreparedStatement ps = null;
		Connection conn = getConn(true);
		ps = conn.prepareStatement(CREATE_ADVERTISERS);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_KEYWORDS);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_QUERIES);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_TEMP2);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_TEMP);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_BALANCE);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_CTC);
		ps.executeUpdate();
		conn.commit();
		ps.close();
		conn.close();
		System.out.println("Tables created...");
	}
	
	/**
	 * This method is to create or replace functions
	 * @throws SQLException
	 */
	
	public static void createFunctions() throws SQLException {
		System.out.println("Creating functions...");
		PreparedStatement ps = null;
		Connection conn = getConn(true);
		ps = conn.prepareStatement(CREATE_FUNCTION_SUBSTRBYSEP);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_FUNCTION_STR_LENGTH);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_FUNCTION_REMOVESAMESTR);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_FUNCTION_NUMOFCHAR);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_FUNCTION_SIMILARITYCALCULATE);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_TSTRINGTYPE);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_TSTRINGBODY);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_FUNCTION_STRINGAGG);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_TYPE_VARCHAR2VARRAY);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_FUNCTION_SF_SPLIT_STRING);
		ps.executeUpdate();
		conn.commit();
		ps.close();
		conn.close();
		System.out.println("Functions created...");
	}
	
	/**
	 * This method is to parse the input file system.in to the member variables.
	 * @param input	 parse each line from system.in to set the variables
	 */
	public static void parseInput(String[] input) {
		System.out.println("Parsing the Input...");
		// parse the line 1&2: username and password
		username = input[0].split(SUBDELIMITER)[1].trim();
		password = input[1].split(SUBDELIMITER)[1].trim();
		// parse the line 3: task1 parameter
		pTask1 = input[2].split(SUBDELIMITER)[1].trim();
		// parse the line 4: task2 parameter
		pTask2 = input[3].split(SUBDELIMITER)[1].trim();
		// parse the line 5: task3 parameters
		pTask3 = input[4].split(SUBDELIMITER)[1].trim();
		// parse the line 6: task4 parameters
		pTask4 = input[5].split(SUBDELIMITER)[1].trim();
		// parse the line 7: task5 parameters
		pTask5 = input[6].split(SUBDELIMITER)[1].trim();
		// parse the line 8: task6 parameters
		pTask6 = input[7].split(SUBDELIMITER)[1].trim();
	}

	/**
	 * these methods are for method taskInitialization
	 * @param conn
	 * @throws SQLException
	 */
	public static void deleteTemp2(Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(DELETE_TEMP2);
		ps.executeUpdate();
		ps.close();
	}
	public static void deleteTemp(Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(DELETE_TEMP);
		ps.executeUpdate();
		ps.close();
	}
	public static void insertBalance(Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(INSERT_BALANCE);
		ps.executeUpdate();
		ps.close();
	}
	public static void deleteBalance(Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(DELETE_BALANCE);
		ps.executeUpdate();
		ps.close();
	}
	public static void deleteCtc(Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(DELETE_CTC);
		ps.executeUpdate();
		ps.close();
	}
	public static void insertCtc(Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(INSERT_CTC);
		ps.executeUpdate();
		ps.close();
	}
	
	/**
	 * the main method
	 * @param args
	 * @throws SQLException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void main(String[] args) throws SQLException, FileNotFoundException, IOException {
		// Fetch the input information from system.in
		disposeFile(INPUT);
		// Drop all tables if already exist
		dropTables();
		dropTables();
		// Create the tables
		createTables();
		// Create the functions
		createFunctions();
		// Fetch the input information from advertisers.dat, keywords.dat and queries.dat
		disposeFile(ADVERTISERS);
		disposeFile(KEYWORDS);
		disposeFile(QUERIES);
		// Handle the 6 tasks
		handleTask1();
		handleTask2();
		handleTask3();
		handleTask4();
		handleTask5();
		handleTask6();
	}
}