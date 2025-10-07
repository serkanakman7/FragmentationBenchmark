using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FragmentationSpeedControl.Utilities
{
    public static class ConsoleReporter
    {
        public static void PrintFragmentationInfo(DataSet ds)
        {
            Console.WriteLine();
            Console.WriteLine("*************************        FRAGMENTATİON INFO        *************************");
            Console.WriteLine();

            if(ds.Tables.Count == 0 || ds.Tables[0].Rows.Count == 0)
            {
                Console.WriteLine("No fragmentation data available.");
                return;
            }

            foreach (DataRow row in ds.Tables[0].Rows)
            {
                Console.WriteLine($"Index Name: {row["name"]}, Avg Fragmentation: {row["avg_fragmentation_in_percent"]}, Page Count: {row["page_count"]}");
            }
            Console.WriteLine();
        }

        public static void PrintRebuildInfo(string rebuildState, double selectTime, double updateTime, double totalTime)
        {
            Console.WriteLine();
            Console.WriteLine($"*************************        {rebuildState.ToUpper()} REBUILD INFO        *************************");
            Console.WriteLine();
            Console.WriteLine($"Select {rebuildState} Rebuild = {selectTime}");
            Console.WriteLine($"Update {rebuildState} Rebuild = {updateTime}");
            Console.WriteLine($"Total {rebuildState} Rebuild = {totalTime}");
            Console.WriteLine();
        }
    }
}
