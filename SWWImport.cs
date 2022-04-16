using System;
using System.Xml;
using Microsoft.Data.SqlClient;
using System.Text;
using System.Text.RegularExpressions;
using System.Diagnostics;
using System.Threading;
using System.Collections.Generic; 


public class SWWImport
{
	public struct GsmSession {
		public DateTime Time;
		public UInt32 SessionID;
		// 14 - 15 chars
		public string IMSI;
		public string TMSI;
		public string IMEI;
		public string MSISDN;
		// Mobile Country Code [3]
		public string MCC;
		// Mobile Network Code [5]
		public string MNC;
		// Location Area Code
		public UInt16 LAC;
		// Cell ID
		public UInt32 CellID;
		public string TimeToString() { return DateTimeToString(Time); }
		public static string DateTimeToString(DateTime dt) {
			return dt.ToString("yyyy-MM-dd HH:mm:ss");
		}

		public override string ToString() 
		{
			return $"[GsmSession] Time: {this.TimeToString()}, SessionID: {this.SessionID}, IMSI: {this.IMSI}, MCC: {this.MCC}, MNC: {this.MNC}, LAC: {this.LAC}, CellID: {this.CellID}";
		}
		public static GsmSession GetRandom(Random rnd) 
		{
			
			GsmSession ses = new GsmSession();
			ses.Time = DateTime.Now.AddMinutes(-rnd.NextDouble()*60*24*100);
			ses.SessionID = (UInt32)rnd.Next();
			ses.IMSI = Math.Truncate(rnd.NextDouble()*1000000000000).ToString();
			ses.TMSI = "";
			ses.IMEI = "";
			ses.MSISDN = "";
			ses.MCC = ((UInt32) Math.Truncate(rnd.NextDouble()*1000)).ToString();
			ses.MNC = ((UInt32) Math.Truncate(rnd.NextDouble()*1000)).ToString();
			ses.LAC = (UInt16)(rnd.Next() >> 16);
			ses.CellID = (UInt32)rnd.Next();
			return ses;
		}


	}

	static Random rnd = new Random();


	public static void ReadFileForStats(string fileName) {
		XmlReaderSettings settings = new XmlReaderSettings();
		settings.ValidationType = ValidationType.None;
		settings.ConformanceLevel = ConformanceLevel.Fragment;

		using (XmlReader xReader = XmlReader.Create(fileName, settings)) {
			Dictionary<string, List<string>> allValues = new Dictionary<string, List<string>>();
			Dictionary<string, int> counterDic = new Dictionary<string, int>();
			while(xReader.ReadToFollowing("GsmSession")) {
				using(XmlReader oReader = xReader.ReadSubtree()) {
					if(oReader.ReadToDescendant("SessionID")) {
						var obj = new GsmSession();
						string s;
						
						int i = oReader.ReadElementContentAsInt();
						if(i < 0) { throw new Exception("session id < 0"); }
						obj.SessionID = (UInt32) i;
						if(!allValues.ContainsKey("sessionID")) {
							allValues["sessionID"] = new List<string>();
							counterDic["sessionID"] = 0;
						}
						if(!allValues["sessionID"].Contains(i.ToString())) {
							allValues["sessionID"].Add(i.ToString());
						}
						counterDic["sessionID"]++;

						while (oReader.Read())
						{
							if (oReader.NodeType == XmlNodeType.Element)
							{
								string fieldName = oReader.Name;
								switch (fieldName)
								{
									case "Time": 
										break;
									case "IMSI": 
										if(!allValues.ContainsKey(fieldName)) {
											allValues[fieldName] = new List<string>();
											counterDic[fieldName] = 0;
										}
										s = oReader.ReadElementContentAsString();
										obj.IMSI = s;
										if(!String.IsNullOrWhiteSpace(s)) {	counterDic[fieldName]++; }
										if(!allValues[fieldName].Contains(s)) {
											allValues[fieldName].Add(s);
										}
										break;
									case "TMSI": 
										if(!allValues.ContainsKey(fieldName)) {
											allValues[fieldName] = new List<string>();
											counterDic[fieldName] = 0;
										}
										s = oReader.ReadElementContentAsString();
										obj.TMSI = s;
										if(!String.IsNullOrWhiteSpace(s)) {	counterDic[fieldName]++; }
										if(!allValues[fieldName].Contains(s)) {
											allValues[fieldName].Add(s);
										}
										break;
									case "IMEI": 
										if(!allValues.ContainsKey(fieldName)) {
											allValues[fieldName] = new List<string>();
											counterDic[fieldName] = 0;
										}
										s = oReader.ReadElementContentAsString();
										obj.IMEI = s;
										if(!String.IsNullOrWhiteSpace(s)) {	counterDic[fieldName]++; }
										if(!allValues[fieldName].Contains(s)) {
											allValues[fieldName].Add(s);
										}
										break;
									case "MSISDN": 
										if(!allValues.ContainsKey(fieldName)) {
											allValues[fieldName] = new List<string>();
											counterDic[fieldName] = 0;
										}
										s = oReader.ReadElementContentAsString();
										obj.MSISDN = s;
										if(!String.IsNullOrWhiteSpace(s)) {	counterDic[fieldName]++; }
										if(!allValues[fieldName].Contains(s)) {
											allValues[fieldName].Add(s);
										}
										break;
									case "MCC": 
										break;
									case "MNC": 
										break;
									case "LAC": 
										break;
									case "CellID": 
										break;
									case "index":
										break;
									case "dir":
										if(!allValues.ContainsKey(fieldName)) {
											allValues[fieldName] = new List<string>();
										}
										s = oReader.ReadElementContentAsString();
										if(!allValues[fieldName].Contains(s)) {
											allValues[fieldName].Add(s);
										}
										break;
									case "Info":
										if(!allValues.ContainsKey(fieldName)) {
											allValues[fieldName] = new List<string>();
											counterDic[fieldName] = 0;
										}
										s = oReader.ReadElementContentAsString();
										if(!String.IsNullOrWhiteSpace(s)) {	counterDic[fieldName]++; }
										if(!allValues[fieldName].Contains(s)) {
											if(!s.Contains("is now active")) {
												allValues[fieldName].Add(s);
											}
										}
										break;
									case "Duration":
										break;
									case "target_ref_TMSI":
										break;
									case "target_ref_MSISDN":
										break;
									case "record_ref":
										break;
									case "TMSI_last_update":
										break;
									case "TA":
										break;
									case "TerminationCause":
										if(!allValues.ContainsKey(fieldName)) {
											allValues[fieldName] = new List<string>();
											counterDic[fieldName] = 0;
										}
										s = oReader.ReadElementContentAsString();
										if(!String.IsNullOrWhiteSpace(s)) {	counterDic[fieldName]++; }
										if(!allValues[fieldName].Contains(s)) {
											allValues[fieldName].Add(s);
										}
										break;
									case "DtmfApo":
										break;
									case "Vocoder":
										break;
									case "Priority":
										break;
									case "cypher":
										break;
									default:
										Console.WriteLine($"Unexcepted data field {fieldName}");
										break;
								}
							}
						}
						if(!String.IsNullOrWhiteSpace(obj.TMSI) && !(String.IsNullOrWhiteSpace(obj.IMEI) || String.IsNullOrWhiteSpace(obj.IMSI) || String.IsNullOrWhiteSpace(obj.MSISDN))) 
						{
							Console.WriteLine($"Session TMSI={obj.TMSI} found id:{obj.SessionID} imei:{obj.IMEI} imsi:{obj.IMSI} msisdn:{obj.MSISDN}");
						}
					}
				}
			}
			if(allValues.ContainsKey("Info")) {
				foreach(string t in allValues["Info"]) {
					if(!t.Contains("is now active")) {
						Console.WriteLine(t); 
					}
				}
			}

			Console.WriteLine($"Analiza pliku:  {fileName.Substring(fileName.LastIndexOf('/')+1)}");
			string fname = "SessionID";
			Console.WriteLine($"Pole: {fname}: {counterDic[fname]} ({allValues[fname].Count}) {counterDic[fname] * 100 / counterDic["SessionID"]} %");
			fname = "IMSI";
			Console.WriteLine($"Pole: {fname}: {counterDic[fname]} ({allValues[fname].Count}) {counterDic[fname] * 100 / counterDic["SessionID"]} %");
			fname = "TMSI";
			Console.WriteLine($"Pole: {fname}: {counterDic[fname]} ({allValues[fname].Count}) {counterDic[fname] * 100 / counterDic["SessionID"]} %");
			fname = "IMEI";
			Console.WriteLine($"Pole: {fname}: {counterDic[fname]} ({allValues[fname].Count}) {counterDic[fname] * 100 / counterDic["SessionID"]} %");
			fname = "MSISDN";
			Console.WriteLine($"Pole: {fname}: {counterDic[fname]} ({allValues[fname].Count}) {counterDic[fname] * 100 / counterDic["SessionID"]} %");
		}
	}

	public static void ReadFileForStatsOld(string fileName) {
		XmlReaderSettings settings = new XmlReaderSettings();
		settings.ValidationType = ValidationType.None;
		settings.ConformanceLevel = ConformanceLevel.Fragment;
		Regex msisdnDigits = new Regex("[a-zA-Z]");

		using (XmlReader xReader = XmlReader.Create(fileName, settings)) {
			Dictionary<string, List<string>> allValues = new Dictionary<string, List<string>>();
			Dictionary<string, int> counterDic = new Dictionary<string, int>();
			Dictionary<string, string> tmsiDic = new Dictionary<string, string>(5000);
			while(xReader.ReadToFollowing("GsmSession")) {
				using(XmlReader oReader = xReader.ReadSubtree()) {
					if(oReader.ReadToDescendant("sessionID")) {
						var obj = new GsmSession();
						string s;
						
						int i = oReader.ReadElementContentAsInt();
						if(i < 0) { throw new Exception("session id < 0"); }
						obj.SessionID = (UInt32) i;
						if(!allValues.ContainsKey("sessionID")) {
							allValues["sessionID"] = new List<string>();
							counterDic["sessionID"] = 0;
						}
						if(!allValues["sessionID"].Contains(i.ToString())) {
							allValues["sessionID"].Add(i.ToString());
						}
						counterDic["sessionID"]++;

						while (oReader.Read())
						{
							if (oReader.NodeType == XmlNodeType.Element)
							{
								string fieldName = oReader.Name;
								switch (fieldName)
								{
									case "time": 
										break;
									case "IMSI": 
										if(!allValues.ContainsKey(fieldName)) {
											allValues[fieldName] = new List<string>();
											counterDic[fieldName] = 0;
										}
										s = oReader.ReadElementContentAsString();
										obj.IMSI = s;
										if(!String.IsNullOrWhiteSpace(s)) {	counterDic[fieldName]++; }
										if(!allValues[fieldName].Contains(s)) {
											allValues[fieldName].Add(s);
										}
										break;
									case "TMSI": 
										if(!allValues.ContainsKey(fieldName)) {
											allValues[fieldName] = new List<string>();
											counterDic[fieldName] = 0;
										}
										s = oReader.ReadElementContentAsString();
										obj.TMSI = s;
										if(!String.IsNullOrWhiteSpace(s)) {	counterDic[fieldName]++; }
										if(!allValues[fieldName].Contains(s)) {
											allValues[fieldName].Add(s);
										}
										break;
									case "IMEI": 
										if(!allValues.ContainsKey(fieldName)) {
											allValues[fieldName] = new List<string>();
											counterDic[fieldName] = 0;
										}
										s = oReader.ReadElementContentAsString();
										obj.IMEI = s;
										if(!String.IsNullOrWhiteSpace(s)) {	counterDic[fieldName]++; }
										if(!allValues[fieldName].Contains(s)) {
											allValues[fieldName].Add(s);
										}
										break;
									case "MSISDN": 
										if(!allValues.ContainsKey(fieldName)) {
											allValues[fieldName] = new List<string>();
											counterDic[fieldName] = 0;
										}
										s = oReader.ReadElementContentAsString();
										obj.MSISDN = s;
										if(!String.IsNullOrWhiteSpace(s)) {	counterDic[fieldName]++; }
										if(!allValues[fieldName].Contains(s)) {
											allValues[fieldName].Add(s);
										}
										break;
									case "MCC": 
										break;
									case "MNC": 
										break;
									case "LAC": 
										break;
									case "cellID": 
										break;
									case "index":
										break;
									case "direction":
										if(!allValues.ContainsKey(fieldName)) {
											allValues[fieldName] = new List<string>();
										}
										s = oReader.ReadElementContentAsString();
										if(!allValues[fieldName].Contains(s)) {
											allValues[fieldName].Add(s);
										}
										break;
									case "SMSText":
										if(!allValues.ContainsKey(fieldName)) {
											allValues[fieldName] = new List<string>();
											counterDic[fieldName] = 0;
										}
										s = oReader.ReadElementContentAsString();
										if(!String.IsNullOrWhiteSpace(s)) {	counterDic[fieldName]++; }
										if(!allValues[fieldName].Contains(s)) {
											if(!s.Contains("is now active")) {
												allValues[fieldName].Add(s);
											}
										}
										break;
									case "duration":
										break;
									case "target_ref_TMSI":
										if(!allValues.ContainsKey(fieldName)) {
											allValues[fieldName] = new List<string>();
										}
										s = oReader.ReadElementContentAsString();
										if(!allValues[fieldName].Contains(s)) {
											allValues[fieldName].Add(s);
										}
										break;
									case "target_ref_MSISDN":
										if(!allValues.ContainsKey(fieldName)) {
											allValues[fieldName] = new List<string>();
										}
										s = oReader.ReadElementContentAsString();
										if(!allValues[fieldName].Contains(s)) {
											allValues[fieldName].Add(s);
										}
										break;
									case "record_ref":
										if(!allValues.ContainsKey(fieldName)) {
											allValues[fieldName] = new List<string>();
										}
										s = oReader.ReadElementContentAsString();
										if(!allValues[fieldName].Contains(s)) {
											allValues[fieldName].Add(s);
										}
										break;
									case "TMSI_last_update":
										break;
									case "time_advance":
										break;
									case "termination_cause":
										if(!allValues.ContainsKey(fieldName)) {
											allValues[fieldName] = new List<string>();
											counterDic[fieldName] = 0;
										}
										s = oReader.ReadElementContentAsString();
										if(!String.IsNullOrWhiteSpace(s)) {	counterDic[fieldName]++; }
										if(!allValues[fieldName].Contains(s)) {
											allValues[fieldName].Add(s);
										}
										break;
									case "DTMF":
										break;
									case "vocoder":
										break;
									case "priority":
										break;
									case "cypher":
										break;
									default:
										Console.WriteLine($"Unexcepted data field {fieldName}");
										break;
								}
							}
						}
						if(!String.IsNullOrEmpty(obj.TMSI) && !(String.IsNullOrEmpty(obj.IMEI) && String.IsNullOrEmpty(obj.IMSI) && String.IsNullOrEmpty(obj.MSISDN))) 
						{
							if(!msisdnDigits.IsMatch(obj.IMSI)) {
								if(!String.IsNullOrEmpty(obj.IMSI)) {
									tmsiDic[obj.TMSI] = obj.IMSI;
								} else {
									if(tmsiDic.ContainsKey(obj.TMSI)) {
										obj.IMSI = tmsiDic[obj.TMSI];
									Console.WriteLine($"Session with only TMSI={obj.TMSI} found id:{obj.SessionID} imei:{obj.IMEI} imsi:{obj.IMSI} msisdn:{obj.MSISDN}");
									}
								}
							}
						}
					}
				}
			}
			if(allValues.ContainsKey("SMSText")) {
				foreach(string t in allValues["SMSText"]) {
					if(!t.Contains("is now active")) {
						Console.WriteLine(t); 
					}
				}
			}

			Console.WriteLine($"Analiza pliku:  {fileName.Substring(fileName.LastIndexOf('/')+1)}");
			string fname = "sessionID";
			Console.WriteLine($"Pole: {fname}: {counterDic[fname]} ({allValues[fname].Count}) {counterDic[fname] * 100 / counterDic["sessionID"]} %");
			fname = "IMSI";
			Console.WriteLine($"Pole: {fname}: {counterDic[fname]} ({allValues[fname].Count}) {counterDic[fname] * 100 / counterDic["sessionID"]} %");
			fname = "TMSI";
			Console.WriteLine($"Pole: {fname}: {counterDic[fname]} ({allValues[fname].Count}) {counterDic[fname] * 100 / counterDic["sessionID"]} %");
			fname = "IMEI";
			Console.WriteLine($"Pole: {fname}: {counterDic[fname]} ({allValues[fname].Count}) {counterDic[fname] * 100 / counterDic["sessionID"]} %");
			fname = "MSISDN";
			Console.WriteLine($"Pole: {fname}: {counterDic[fname]} ({allValues[fname].Count}) {counterDic[fname] * 100 / counterDic["sessionID"]} %");
		}
		Console.ReadLine();
	}

	public static void ReadFile(string fileName) {
		XmlReaderSettings settings = new XmlReaderSettings();
		settings.ValidationType = ValidationType.None;
		settings.ConformanceLevel = ConformanceLevel.Fragment;

		using (XmlReader xReader = XmlReader.Create(fileName, settings)) {
			while(xReader.ReadToFollowing("GsmSession")) {
				using(XmlReader oReader = xReader.ReadSubtree()) {
					if(oReader.ReadToDescendant("sessionID")) {
						string s;
						DateTime dt;
						var obj = new GsmSession();
						int i = oReader.ReadElementContentAsInt();
						if(i < 0) { throw new Exception("session id < 0"); }
						obj.SessionID = (UInt32) i;

						while (oReader.Read())
						{
							if (oReader.NodeType == XmlNodeType.Element)
								switch (oReader.Name)
								{
									case "time": 
										i = oReader.ReadElementContentAsInt();
										dt = DateTimeOffset.FromUnixTimeSeconds(i).LocalDateTime;
										obj.Time = dt;
										break;
									case "IMSI": 
										s = oReader.ReadElementContentAsString();
										obj.IMSI = s;
										break;
									case "TMSI": 
										s = oReader.ReadElementContentAsString();
										obj.TMSI = s;
										break;
									case "IMEI": 
										s = oReader.ReadElementContentAsString();
										obj.IMEI = s;
										break;
									case "MSISDN": 
										s = oReader.ReadElementContentAsString();
										obj.MSISDN = s;
										break;
									case "MCC": 
										s = oReader.ReadElementContentAsString();
										obj.MCC = s;
										break;
									case "MNC": 
										s = oReader.ReadElementContentAsString();
										obj.MNC = s;
										break;
									case "LAC": 
										i = oReader.ReadElementContentAsInt();
										if(i < 0) { throw new Exception("LAC < 0"); }
										obj.LAC = (UInt16) i;
										break;
									case "cellID": 
										i = oReader.ReadElementContentAsInt();
										if(i < 0) { throw new Exception("cellid < 0"); }
										obj.CellID =(UInt32) i;
										break;
							}
						}
						Console.WriteLine($"{GsmSession.DateTimeToString(obj.Time)} {obj.SessionID} {obj.CellID} {obj.IMSI}");
					}
				}
			}
		}
	}

	private static SqlConnectionStringBuilder ConnectionBuilderInit()
	{
		SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
		builder.DataSource = "localhost";
		//builder.UserID = "<your_username>";            
		//builder.Password = "<your_password>";     
		builder.InitialCatalog = "SWW1";
		builder.IntegratedSecurity = true;
		builder.TrustServerCertificate = true;
		builder.Encrypt = false;
		builder.ConnectTimeout = 6;
		builder.CommandTimeout = 180;
		return builder;
	}


	public static void TestSqlSelect() 
	{
		try
		{
			Console.WriteLine("\nQuery data example:");
			Console.WriteLine("=========================================\n");

			using (sqlConnection = new SqlConnection(sqlConnStrBuilder.ConnectionString))
			{

				String sql = @"
SELECT 
	TOP(10) 
	Time, 
	IMSI,
	CellID
FROM 
	GsmSessions 
ORDER BY 
	Id DESC";

				using (sqlCommand = new SqlCommand(sql, sqlConnection))
				{
					sqlConnection.Open();
					using (SqlDataReader reader = sqlCommand.ExecuteReader())
					{
						while (reader.Read())
						{
							Console.WriteLine("{0} {1} {2}", reader.GetDateTime(0), reader.GetInt64(1), reader.GetInt32(2));
						}
					}
				}
			}
		}
		catch (SqlException e)
		{
			Console.WriteLine(e.ToString());
		}
	}

	static String sqlInsert = $@"
INSERT INTO [dbo].[GsmSessions]
	([Time]
	,[SessionID]
	,[IMSI]
	,[TMSI]
	,[IMEI]
	,[MSISDN]
	,[MCC]
	,[MNC]
	,[LAC]
	,[CellID])
VALUES
	(@time
	,@sessionid
	,@imsi
	,@tmsi
	,@imei
	,@msisdn
	,@mcc
	,@mnc
	,@lac
	,@cellid)
";

	static SqlConnectionStringBuilder sqlConnStrBuilder;
	static SqlCommand sqlCommand;
	static SqlConnection sqlConnection;

	public static void SqlInit() 
	{
		 sqlConnStrBuilder = ConnectionBuilderInit();
	}


	public static void SessionSetInsertParameters(GsmSession ses) {
		sqlCommand.Parameters["@time"].Value = ses.Time;
		sqlCommand.Parameters["@sessionid"].Value = ses.SessionID;
		sqlCommand.Parameters["@imsi"].Value = String.IsNullOrEmpty(ses.IMSI) ? 0 : UInt64.Parse(ses.IMSI);
		sqlCommand.Parameters["@tmsi"].Value = String.IsNullOrEmpty(ses.TMSI) ? 0 : UInt64.Parse(ses.TMSI);
		sqlCommand.Parameters["@imei"].Value = String.IsNullOrEmpty(ses.IMEI) ? 0 : UInt64.Parse(ses.IMEI);
		sqlCommand.Parameters["@msisdn"].Value = String.IsNullOrEmpty(ses.MSISDN) ? 0 : UInt64.Parse(ses.MSISDN);
		sqlCommand.Parameters["@mcc"].Value = ses.MCC;
		sqlCommand.Parameters["@mnc"].Value = ses.MNC;
		sqlCommand.Parameters["@lac"].Value = ses.LAC;
		sqlCommand.Parameters["@cellid"].Value = ses.CellID;
	}
	public static void SQLexec(string sql) 
	{
		Stopwatch stopwatch = new Stopwatch();
		try
		{
			Console.WriteLine($"\nSQL Execute: {sql}");
			
			stopwatch.Start();
			using (sqlConnection = new SqlConnection(sqlConnStrBuilder.ConnectionString))
			{
				sqlConnection.Open();
				using (sqlCommand = new SqlCommand(sql, sqlConnection))
				{
					sqlCommand.CommandTimeout = 180;
					sqlCommand.ExecuteNonQuery();
				}
			}
		}
		catch (SqlException e)
		{
			Console.WriteLine(e.ToString());
		}
		stopwatch.Stop();
 
		TimeSpan ts = stopwatch.Elapsed;
 		Console.WriteLine("Elapsed Time is {0:00}:{1:00}:{2:00}.{3}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
	}

	public static void SqlPrepareInsertCommandAndParameters() 
	{
		try
		{
			SqlParameter pTime = new SqlParameter("@time", System.Data.SqlDbType.DateTime);
			sqlCommand.Parameters.Add(pTime);

			SqlParameter pSessionID = new SqlParameter("@sessionid", System.Data.SqlDbType.Int);
			sqlCommand.Parameters.Add(pSessionID);

			SqlParameter pIMSI = new SqlParameter("@imsi", System.Data.SqlDbType.BigInt);
			sqlCommand.Parameters.Add(pIMSI);

			SqlParameter pTMSI = new SqlParameter("@tmsi", System.Data.SqlDbType.BigInt);
			sqlCommand.Parameters.Add(pTMSI);

			SqlParameter pIMEI = new SqlParameter("@imei", System.Data.SqlDbType.BigInt);
			sqlCommand.Parameters.Add(pIMEI);

			SqlParameter pMSISDN = new SqlParameter("@msisdn", System.Data.SqlDbType.BigInt);
			sqlCommand.Parameters.Add(pMSISDN);

			SqlParameter pMCC = new SqlParameter("@mcc", System.Data.SqlDbType.Char);
			sqlCommand.Parameters.Add(pMCC);

			SqlParameter pMNC = new SqlParameter("@mnc", System.Data.SqlDbType.Char);
			sqlCommand.Parameters.Add(pMNC);

			SqlParameter pLAC = new SqlParameter("@lac", System.Data.SqlDbType.SmallInt);
			sqlCommand.Parameters.Add(pLAC);

			SqlParameter pCellID = new SqlParameter("@cellid", System.Data.SqlDbType.Int);
			sqlCommand.Parameters.Add(pCellID);

		}
		catch (SqlException e)
		{
			Console.WriteLine(e.ToString());
		}
	}


	public static void TestSqlInsert() 
	{
		DisableIndexes();
		GsmSession gsmSession = new GsmSession();
		Stopwatch stopwatch = new Stopwatch();
 		try
		{
			Console.WriteLine("\nSQL Insert test:");
			Console.WriteLine("=========================================\n");
			
			using (sqlConnection = new SqlConnection(sqlConnStrBuilder.ConnectionString))
			{
				sqlConnection.Open();
				using (sqlCommand = new SqlCommand(sqlInsert, sqlConnection))
				{				
					SqlPrepareInsertCommandAndParameters();
 					stopwatch.Start();
 					
					for(int i = 0; i<100*1000*100; i++)
					{
						gsmSession = GsmSession.GetRandom(rnd);
						SessionSetInsertParameters(gsmSession);
						sqlCommand.ExecuteNonQuery();
					}
				}
			}
		}
		catch (SqlException e)
		{
			Console.WriteLine(gsmSession.ToString());
			Console.WriteLine(e.ToString());
		}
		stopwatch.Stop();
 
		TimeSpan ts = stopwatch.Elapsed;
 		Console.WriteLine("Elapsed Time is {0:00}:{1:00}:{2:00}.{3}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
		
		EnableIndexes();
	}

	public static void DisableIndexes()
	{
		String sql = @"
ALTER INDEX [TimeIdx] ON [dbo].[GsmSessions] DISABLE
ALTER INDEX [BTSIdx] ON [dbo].[GsmSessions] DISABLE
ALTER INDEX [IMSIIdx] ON [dbo].[GsmSessions] DISABLE
		";
		SQLexec(sql);
	}

	public static void EnableIndexes()
	{
		String sql = @"
ALTER INDEX [TimeIdx] ON [dbo].[GsmSessions] REBUILD PARTITION = ALL WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
ALTER INDEX [BTSIdx] ON [dbo].[GsmSessions] REBUILD PARTITION = ALL WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
ALTER INDEX [IMSIIdx] ON [dbo].[GsmSessions] REBUILD PARTITION = ALL WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
		";
		SQLexec(sql);
	}

	public static void Main(string[] args)
	{
		Console.WriteLine($"Hello world! {args[0]} ");
//		SqlInit();
//		TestSqlInsert();
//		TestSqlSelect();
//		ReadFile(args[0]);
		ReadFileForStatsOld(args[0]);
	}
}
