using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;

namespace FileSplitter
{
    // ReSharper disable once ClassNeverInstantiated.Global
    internal class HousePrice
    {
        public string TransactionId { get; set; }
        public decimal Price { get; set; }
        public DateTime TransferDate { get; set; }
        public string Postcode { get; set; }
        public string PropertyType { get; set; }
        public string IsNew { get; set; }
        public string Duration { get; set; }
        public string PAON { get; set; }
        public string SAON { get; set; }
        public string Street { get; set; }
        public string Locality { get; set; }
        public string City { get; set; }
        public string District { get; set; }
        public string County { get; set; }
        public string CategoryType { get; set; }
        public string Status { get; set; }
    }
    
    // ReSharper disable once ClassNeverInstantiated.Global
    internal sealed class HousePriceMap : ClassMap<HousePrice>
    {
        public HousePriceMap()
        {
            Map( m => m.TransactionId ).Index(0);
            Map( m => m.Price ).Index(1);
            Map( m => m.TransferDate ).Index(2);
            Map( m => m.Postcode ).Index(3);
            Map( m => m.PropertyType ).Index(4);
            Map( m => m.IsNew ).Index(5);
            Map( m => m.Duration ).Index(6);
            Map( m => m.PAON ).Index(7);
            Map( m => m.SAON ).Index(8);
            Map( m => m.Street ).Index(9);
            Map( m => m.Locality ).Index(10);
            Map( m => m.City ).Index(11);
            Map( m => m.District ).Index(12);
            Map( m => m.County ).Index(13);
            Map( m => m.CategoryType ).Index(14);
            Map( m => m.Status ).Index(15);
        }
    }
    class Program
    {
        private static readonly Dictionary<string, long> Outputs = new Dictionary<string, long>();
        static async Task Main(string[] args)
        {
            const string filename = @"C:\Dev\HousePrice\Data\pp-complete.csv";
            var inputRecords = 0;

            var currentFile = string.Empty;
            using (var inputStream = new StreamReader(filename))
            {
                using (var parser = new CsvReader(inputStream))
                {
                    parser.Configuration.HasHeaderRecord = false;
                    parser.Configuration.RegisterClassMap<HousePriceMap>();
          
                    CsvWriter currentWriter = null;
                    while (await parser.ReadAsync())
                    {
                        var data = parser.GetRecord<HousePrice>();
                        inputRecords++;
                        var reqFile = data.TransferDate.Year + ".csv";

                        if (currentWriter==null || reqFile != currentFile)
                        {
                            currentFile = reqFile;
                            if (currentWriter != null)
                            {
                                currentWriter.Flush();
                                currentWriter.Dispose();
                            }

                            currentWriter = new CsvWriter(new StreamWriter(reqFile,true));
                            currentWriter.Configuration.HasHeaderRecord = false;
                            currentWriter.Configuration.RegisterClassMap<HousePriceMap>();
                        }

                        currentWriter.WriteRecord(data);
                        if (!Outputs.ContainsKey(reqFile))
                        {
                            Outputs.Add(reqFile, 0);
                        }

                        Outputs[reqFile]++;
                        await currentWriter.NextRecordAsync();
             
                        if (inputRecords % 10000 == 0)
                            Console.WriteLine(inputRecords);
                    }

                    if (currentWriter != null)
                    {
                        currentWriter.Flush();
                        currentWriter.Dispose();
                    }
                }
              
            }

            
            foreach (var key in Outputs.Keys)
            {
                Console.WriteLine($"{key}: {Outputs[key]}");
            }
            Console.WriteLine($"Total outputs: {Outputs.Values.Sum(z=>z)}");
            
            Console.WriteLine($"finished: input records: {inputRecords}");
            Console.ReadLine();
        }
    }
}