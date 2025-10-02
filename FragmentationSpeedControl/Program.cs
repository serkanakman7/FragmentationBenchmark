using System.Data;
using System.Data.Common;
using System.Text;
using FragmentationSpeedControl.DataAccess;

internal class Program
{
    private static void Main(string[] args)
    {
        SqlServerDataAccess sql = new SqlServerDataAccess();
        int insertElapsedTime = sql.DoBulkInsert("HIGH", @"C:\Users\serka\OneDrive\Masaüstü\FragmentationTest\250000DummyData.txt", "", "");
        Console.WriteLine(insertElapsedTime);

        //var (MailIdData, getMailId) = sql.GetCustomerManagerMailId("B346369B5A344B9C8746018D7A72B8B3");
        //Console.WriteLine($"MailId One Item = {getMailId}");

        //var (CampIdData, getAllCampIdElapsedTime) = sql.GetAllCustomerManagerCampId();
        //Console.WriteLine($"CampId Many Item = {getAllCampIdElapsedTime}");

        //var (Data, getAllElapsedTime) = sql.GetAllCustomerManager();
        //Console.WriteLine($"Get All = {getAllElapsedTime}");

        //var (TopTenData, getTopTenElapsedTime) = sql.GetTopTenCustomerManager();
        //Console.WriteLine($"Top Ten = {getTopTenElapsedTime}");

        //sql.DeleteAllCustomerManager();

        var ds = sql.FragmentationRate();
        foreach (DataRow row in ds.Tables[0].Rows)
        {
            Console.WriteLine($"Index Name: {row["name"]}, Avg Fragmentation: {row["avg_fragmentation_in_percent"]}, Page Count: {row["page_count"]}");
        }
    }
}