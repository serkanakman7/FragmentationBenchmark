using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Text;
using FragmentationSpeedControl.DataAccess;

internal class Program
{
    private static void Main(string[] args)
    {
        // ToDo: Insert kısmı önemli değil. Ama sadece her test çalıştırıldığında 10 milyon kayıt var ve defrated. select + rebuild + select.
        SqlServerDataAccess sql = new SqlServerDataAccess();
        //int insertelapsedtime = sql.DoBulkInsert("HIGH", @"C:\Users\serka\OneDrive\Masaüstü\FragmentationTest\10000000DummyData.txt", "", "");
        //Console.WriteLine(insertelapsedtime);

        Stopwatch swSelect = new Stopwatch();
        Stopwatch swUpdate = new Stopwatch();
        Stopwatch swTotal = new Stopwatch();
        swTotal.Start();
        int counter = 0;
        while (true)
        {
            swSelect.Start();
            string[] mails = sql.SelectNextMailsCampId(1000);//Sataus=Q
            swSelect.Stop();


            if (mails.Length == 0)
                break;

            swUpdate.Start();
            sql.UpdateStatus(mails, "W");
            swUpdate.Stop();

            counter++;

            if (counter > 2000)
                break;

        }
        swTotal.Stop();

        Console.WriteLine($"Rebuild edilmemiş Select sorgusu = {swSelect.Elapsed.TotalSeconds}");
        Console.WriteLine($"Rebuild edilmemiş Update sorgusu = {swUpdate.Elapsed.TotalSeconds}");
        Console.WriteLine($"Rebuild edilmemiş Toplam(Select + Update) sorgu = {swTotal.Elapsed.TotalSeconds}");

        var ds = sql.FragmentationRate();
        foreach (DataRow row in ds.Tables[0].Rows)
        {
            Console.WriteLine($"Index Name: {row["name"]}, Avg Fragmentation: {row["avg_fragmentation_in_percent"]}, Page Count: {row["page_count"]}");
        }
        var fragmentationRate = Convert.ToDecimal(ds.Tables[0].Rows[3]["avg_fragmentation_in_percent"]);

        if (fragmentationRate > 30)
        {
            sql.IndexsRebuild();
        }

        ds = sql.FragmentationRate();
        foreach (DataRow row in ds.Tables[0].Rows)
        {
            Console.WriteLine($"Index Name: {row["name"]}, Avg Fragmentation: {row["avg_fragmentation_in_percent"]}, Page Count: {row["page_count"]}");
        }

        swSelect.Reset();
        swUpdate.Reset();
        swTotal.Reset();
        swTotal.Start();
        counter = 0;
        while (true)
        {
            swSelect.Start();
            string[] mails = sql.SelectNextMailsCampId(500);//Sataus=Q
            swSelect.Stop();


            if (mails.Length == 0)
                break;

            swUpdate.Start();
            sql.UpdateStatus(mails, "Q");
            swUpdate.Stop();

            counter++;

            if (counter > 1000)
                break;

        }
        swTotal.Stop();

        //Console.WriteLine($"Rebuild edilmiş Select sorgusu = {swSelect.Elapsed.TotalSeconds}");
        //Console.WriteLine($"Rebuild edilmiş Update sorgusu = {swUpdate.Elapsed.TotalSeconds}");
        //Console.WriteLine($"Rebuild edilmiş Toplam(Select + Update) sorgu = {swTotal.Elapsed.TotalSeconds}");

        //ds = sql.FragmentationRate();
        //foreach (DataRow row in ds.Tables[0].Rows)
        //{
        //    Console.WriteLine($"Index Name: {row["name"]}, Avg Fragmentation: {row["avg_fragmentation_in_percent"]}, Page Count: {row["page_count"]}");
        //}


        //sql.DeleteAllCustomerManager();

        //ToDo: Rebuild Index

        //Elapsed:
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