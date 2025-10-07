using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Text;
using FragmentationSpeedControl.DataAccess;
using FragmentationSpeedControl.Utilities;

internal class Program
{
    private static void Main(string[] args)
    {
        int retryCount = 500;
        int pullMailIdCount = 100;
        SqlServerDataAccess sql = new SqlServerDataAccess();
        //int insertelapsedtime = sql.DoBulkInsert("HIGH", @"C:\Users\serka\OneDrive\Masaüstü\FragmentationTest\2000000DummyData.txt", "", "");
        //Console.WriteLine(insertelapsedtime);

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
            string[] mails = sql.SelectNextMailsCampId(pullMailIdCount);
            swSelect.Stop();


            if (mails.Length == 0)
                break;

            swUpdate.Start();
            sql.UpdateStatus(mails, "W");
            swUpdate.Stop();

            counter++;

            if (counter > retryCount)
                break;

        }
        swTotal.Stop();

        ConsoleReporter.PrintRebuildInfo("Non-Rebuild", swSelect.Elapsed.TotalSeconds, swUpdate.Elapsed.TotalSeconds, swTotal.Elapsed.TotalSeconds);

        fragmentationInfoDataSet = sql.FragmentationRate();
        ConsoleReporter.PrintFragmentationInfo(fragmentationInfoDataSet);

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
            string[] mails = sql.SelectNextMailsCampId(pullMailIdCount);
            swSelect.Stop();


            if (mails.Length == 0)
                break;

            swUpdate.Start();
            sql.UpdateStatus(mails, "W");
            swUpdate.Stop();

            counter++;

            if (counter > retryCount)
                break;

        }
        swTotal.Stop();

        ConsoleReporter.PrintRebuildInfo("Rebuild", swSelect.Elapsed.TotalSeconds, swUpdate.Elapsed.TotalSeconds, swTotal.Elapsed.TotalSeconds);

        fragmentationInfoDataSet = sql.FragmentationRate();
        ConsoleReporter.PrintFragmentationInfo(fragmentationInfoDataSet);


        //sql.DeleteAllCustomerManager();
    }
}