using System.Data;
using System.Data.SqlClient;

namespace SQLrunner
{
    internal class Program
    {
        static List<string> AdditionalQueries = new List<string>{ };
        static string CurrentTable = "";
        static void Main(string[] args)
        {
            string connectionString = @"Data Source=(localdb)\local;Initial Catalog=RFS_MZ ;Integrated Security=True;"; //Default connection string
            string filePath = @"file.sql"; //Default file path

            if (args.Length > 0)
            {
                filePath = args[0];

                if (args.Length > 1)
                {
                    connectionString = args[1];
                }
            }

            using (StreamReader sr = new StreamReader(filePath))
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    SqlCommand command = connection.CreateCommand();
                    string line;
                    string sqlStatement = "";
                    ulong lineNumber = 0;
                    ulong errorCount = 0;
                    while ((line = sr.ReadLine()) != null)
                    {
                        lineNumber++;
                        //Check for commented line
                        string trimmedLine = line.TrimStart();
                        if (!trimmedLine.StartsWith('/') && !trimmedLine.StartsWith('-') && !string.IsNullOrWhiteSpace(trimmedLine))
                        {
                            string convertedLine = ConvertMySQLtoMSSQL(trimmedLine);

                            sqlStatement += convertedLine;

                            if (convertedLine.TrimEnd().EndsWith(";"))
                            {
                                try
                                {
                                    //Console.WriteLine("[INFO] Executing: " + sqlStatement);
                                    command.CommandText = sqlStatement;
                                    command.ExecuteNonQuery(); // Execute SQL Statement
                                    //sqlStatement = ""; // Reset the SQL statement

                                    if (AdditionalQueries.Count > 0)
                                    {
                                        for (int i = 0; i < AdditionalQueries.Count; i++)
                                        {
                                            //Console.WriteLine("[INFO] Executing: " + AdditionalQueries[i]);
                                            command.CommandText = AdditionalQueries[i];
                                            command.ExecuteNonQuery();
                                        }
                                        //AdditionalQueries.Clear();
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("[ERR] Executing (LINE: "+lineNumber+"): " + sqlStatement);
                                    Console.WriteLine("[ERR] Exception thrown at line" + lineNumber + ": " + ex.Message);
                                    errorCount++;
                                }
                                finally
                                {
                                    sqlStatement = "";
                                    AdditionalQueries.Clear();
                                }
                                
                            } 
                        }
                    }
                    Console.WriteLine("Completed with " + errorCount + " errors");
                }
            }
        }

        private static string ConvertMySQLtoMSSQL(string MySQLQuery)
        {
            string MSSQLQuery = MySQLQuery;
            int commentIndex = MSSQLQuery.IndexOf("--");
            if(commentIndex!=-1)
                MSSQLQuery = MSSQLQuery.Substring(0, MSSQLQuery.IndexOf("--"));
            if (string.IsNullOrEmpty(MSSQLQuery))
                return MSSQLQuery;

            if (MSSQLQuery.StartsWith("CREATE TABLE IF NOT EXISTS"))
            {
                //Console.WriteLine("[INFO] Replacing: " + MSSQLQuery);
                MSSQLQuery = MSSQLQuery.Replace("CREATE TABLE IF NOT EXISTS", "CREATE TABLE");
                //Console.WriteLine("[INFO] Replaced: " + MSSQLQuery);
                int start = MSSQLQuery.IndexOf('"');
                int end = MSSQLQuery.LastIndexOf('"');
                CurrentTable = MSSQLQuery.Substring(start, end - start + 1);
            }

            if (MSSQLQuery.Contains("COLLATE 'SQL_1xCompat_CP850_CI_AS'"))
            {
                //Console.WriteLine("[INFO] Replacing: " + MSSQLQuery);
                MSSQLQuery = MSSQLQuery.Replace("COLLATE 'SQL_1xCompat_CP850_CI_AS'", "");
                //Console.WriteLine("[INFO] Replaced: " + MSSQLQuery);
            }
            if (MSSQLQuery.StartsWith("UNIQUE INDEX"))
            {
                //Console.WriteLine("[INFO] Replacing: " + MSSQLQuery);
                MSSQLQuery = MSSQLQuery.Replace("UNIQUE INDEX", "CONSTRAINT");
                int insertIndex = MSSQLQuery.IndexOf('(') - 1;
                MSSQLQuery = MSSQLQuery.Insert(insertIndex, " UNIQUE ");
                //Console.WriteLine("[INFO] Replaced: " + MSSQLQuery);
            }
            if (MSSQLQuery.StartsWith("FOREIGN KEY"))
            {
                return "";
            }

            MSSQLQuery = MSSQLQuery.Replace("b'0'", "'0'");
            MSSQLQuery = MSSQLQuery.Replace("b'1'", "'1'");

            return MSSQLQuery;
        }

        public static int GetNthIndex(string s, char t, int n)
        {
            int count = 0;
            for (int i = 0; i < s.Length; i++)
            {
                if (s[i] == t)
                {
                    count++;
                    if (count == n)
                    {
                        return i;
                    }
                }
            }
            return -1;
        }
    }
}