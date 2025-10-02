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

        //ToDo: SelectByCampIdTop 10
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

/*
1. Tüm statüler Q olacak.
2. CampId bazında x adet çekilecek.
3. Çekilen MailId ler S staüsüne geçirilecek.
3. Bu işlem tüm Q'dekiler S'ye geçene kadar devam edecek.

CampId bazında x adet çekilecek --> While'a koy. Bu sorgudan 0 sonuç gelene kadar devam et.
Counter ekle 50 defa, 100 defa, 200 defa, 500 defa, 1000 defa, 2000 defa, 5000 defa, 10000 defa

Select ve Update işlemleri için ayrı elapsed time tut.
Bir de topalm için tut.

*/