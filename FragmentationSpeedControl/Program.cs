using System.Data;
using System.Data.Common;
using System.Text;
using FragmentationSpeedControl.DataAccess;

internal class Program
{
    private static void Main(string[] args)
    {
        // ToDo: Insert kısmı önemli değil. Ama sadece her test çalıştırıldığında 10 milyon kayıt var ve defrated. select + rebuild + select.
        SqlServerDataAccess sql = new SqlServerDataAccess();
        int insertElapsedTime = sql.DoBulkInsert("HIGH", @"C:\Users\serka\OneDrive\Masaüstü\FragmentationTest\250000DummyData.txt", "", "");
        Console.WriteLine(insertElapsedTime);

        //var (MailIdData, getMailId) = sql.SelectByMailId("B346369B5A344B9C8746018D7A72B8B3");
        //Console.WriteLine($"MailId One Item = {getMailId}");
        ////Elapsed:

        //var (CampIdData, getAllCampIdElapsedTime) = sql.SelectByCampId();
        //Console.WriteLine($"CampId Many Item = {getAllCampIdElapsedTime}");
        ////Elapsed:

        var (TopTenData, getTopTenElapsedTime) = sql.SelectByMailIdWithTop(10);
        Console.WriteLine($"Top Ten = {getTopTenElapsedTime}");

        //sql.DeleteAllCustomerManager();

        //ToDo: Rebuild Index

        sql.SelectByMailIdWithTop(10);
        Console.WriteLine($"Top Ten = {getTopTenElapsedTime}");
        //Elapsed:

        //var ds = sql.FragmentationRate();
        //foreach (DataRow row in ds.Tables[0].Rows)
        //{
        //    Console.WriteLine($"Index Name: {row["name"]}, Avg Fragmentation: {row["avg_fragmentation_in_percent"]}, Page Count: {row["page_count"]}");
        //}
    }
}