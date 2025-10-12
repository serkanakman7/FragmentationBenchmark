using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Text;
using FragmentationSpeedControl.DataAccess;
using FragmentationSpeedControl.DataAccess.Repositories;
using FragmentationSpeedControl.DataAccess.SqlServer;
using FragmentationSpeedControl.Utilities;

internal class Program
{
    private static async Task Main(string[] args)
    {
        int retryCount = 1000;
        int pullMailIdCount = 1000;

        SqlServerDataAccess sql = new SqlServerDataAccess("deneme");
        EmailPoolHighRepository emailPoolHighRepository = new EmailPoolHighRepository(sql);
        SourceRepository sourceRepository = new SourceRepository(sql);
        //int insertelapsedtime = sql.DoBulkInsertSp("HIGH", @"C:\Users\serkan.akman\Desktop\FragmentationTest\2000000DummyData.txt", "", "");
        //Console.WriteLine($"Insert işleminin süresi = {insertelapsedtime}");

        //FileDataAccess fileDataAccess = new FileDataAccess(@"C:\Users\serkan.akman\Desktop\FragmentationTest\10000000DummyData.txt");
        //DataTable fileDataTable = await fileDataAccess.BulkInsertFromTxtAsync();
        //int insertElapsedTime = await sql.BulkInsertAsync(fileDataTable);
        //Console.WriteLine($"Insert işleminin süresi = {insertElapsedTime}");

        int insertElapsedTime = sourceRepository.InsertSelectSource();
        Console.WriteLine($"Insert işleminin süresi = {insertElapsedTime}");

        emailPoolHighRepository.UpdateStatusAllQ("Q");

        var fragmentationInfoDataSet = sql.FragmentationRate();
        ConsoleReporter.PrintFragmentationInfo(fragmentationInfoDataSet);

        Stopwatch swSelect = new Stopwatch();
        Stopwatch swUpdate = new Stopwatch();
        Stopwatch swTotal = new Stopwatch();
        swTotal.Start();
        int counter = 0;
        while (true)
        {
            swSelect.Start();
            string[] mails = emailPoolHighRepository.SelectNextMailsCampId(pullMailIdCount);
            swSelect.Stop();


            if (mails.Length == 0)
                break;

            swUpdate.Start();
            emailPoolHighRepository.UpdateStatus(mails, "W");
            swUpdate.Stop();

            counter++;

            if (counter > retryCount)
                break;

        }
        swTotal.Stop();

        ConsoleReporter.PrintRebuildInfo("Non-", swSelect.Elapsed.TotalSeconds, swUpdate.Elapsed.TotalSeconds, swTotal.Elapsed.TotalSeconds);

        //fragmentationInfoDataSet = sql.FragmentationRate();
        //ConsoleReporter.PrintFragmentationInfo(fragmentationInfoDataSet);

        var fragmentationRate = Convert.ToDecimal(fragmentationInfoDataSet.Tables[0].Rows[4]["avg_fragmentation_in_percent"]);

        if (fragmentationRate > 30)
        {
            sql.IndexsRebuild();
            Console.WriteLine();

            fragmentationInfoDataSet = sql.FragmentationRate();
            ConsoleReporter.PrintFragmentationInfo(fragmentationInfoDataSet);
        }

        swSelect.Reset();
        swUpdate.Reset();
        swTotal.Reset();
        swTotal.Start();
        counter = 0;
        while (true)
        {
            swSelect.Start();
            string[] mails = emailPoolHighRepository.SelectNextMailsCampId(pullMailIdCount);
            swSelect.Stop();


            if (mails.Length == 0)
                break;

            swUpdate.Start();
            emailPoolHighRepository.UpdateStatus(mails, "W");
            swUpdate.Stop();

            counter++;

            if (counter > retryCount)
                break;

        }
        swTotal.Stop();

        ConsoleReporter.PrintRebuildInfo("", swSelect.Elapsed.TotalSeconds, swUpdate.Elapsed.TotalSeconds, swTotal.Elapsed.TotalSeconds);

        fragmentationInfoDataSet = sql.FragmentationRate();
        ConsoleReporter.PrintFragmentationInfo(fragmentationInfoDataSet);


        emailPoolHighRepository.DeleteAllCustomerManager();
    }
}