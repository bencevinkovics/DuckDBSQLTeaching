using System.Data.Common;
using System;
using DuckDB.NET.Data;

namespace DuckDB.NET.Samples
{
    class Program
    {
        static void Main(string[] args)
        {

            startProgram();

        }

        private static void startProgram()
        {
            if (!File.Exists("file.db"))
            {
                //File.Delete("file.db");
                using var duckDBConnection = new DuckDBConnection("DataSource=file.db");
                duckDBConnection.Open();

                var command = createDB(duckDBConnection);
                getSQLCommand(command);
            }
            else
            {
                using var duckDBConnection = new DuckDBConnection("DataSource=file.db");
                duckDBConnection.Open();

                var command = duckDBConnection.CreateCommand();
                getSQLCommand(command);
            }
        }

        private static void getSQLCommand(DbCommand command)
        {
            string selectStatement = Console.ReadLine();

            if (selectStatement == "restartAppNow!")
            {
                File.Delete("file.db");
                resetApp(command, selectStatement);
            }
            else
            {
                doDBOperation(command, selectStatement);
            }
        }

        private static DbCommand createDB(DuckDBConnection duckDBConnection)
        {

            var command = duckDBConnection.CreateCommand();

            //command.CommandText = "CREATE TABLE integers(foo INTEGER, bar INTEGER);";
            //var executeNonQuery = command.ExecuteNonQuery();

            command.CommandText = "CREATE TABLE pizzas AS SELECT * FROM read_csv_auto('pizzas.csv')";
            var executeNonQuery = command.ExecuteNonQuery();

            command.CommandText = "CREATE TABLE shortTest AS SELECT * FROM read_csv_auto('shortTest.csv')";
            executeNonQuery = command.ExecuteNonQuery();
            //command.CommandText = "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);";
            //executeNonQuery = command.ExecuteNonQuery();
            return command;
        }

        public static void doDBOperation(DbCommand command, string selectStatement)
        {
            try
            {
                //command.CommandText = "Not a valid Sql statement";
                //var causesError = command.ExecuteNonQuery();

                command.CommandText = selectStatement;
                //var executeScalar = command.ExecuteScalar();
                try
                {
                    var reader = command.ExecuteReader();
                    PrintQueryResults(reader, command);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    getSQLCommand(command);
                }

            }
            catch (DuckDBException e)
            {
                command.CommandText = e.Message;
            }
        }


        //A lekerdezett adatok kiiratasara szolgalo fuggveny, melyet a fo fuggveny hiv meg
        //PrintQueryResults(reader) paranccsal
        private static void PrintQueryResults(DbDataReader queryResult, DbCommand command)
        {
            //Fejleceken vegig iteral es kiirja a consolra, majd ures sor
            //queryResult.FieldCount az oszlopok szama
            //Minden oszlopot a queryResult.GetName(adott index)-el kap meg
            for (var index = 0; index < queryResult.FieldCount; index++)
            {
                var column = queryResult.GetName(index);
                Console.Write($"{column} ");
            }

            Console.WriteLine();

            //Adatok kiirasa streamReader-hez hasonoloan beolvassa az adatokat
            //For ciklussal kiiratja oket. Mezoket itt is queryResult.FieldCount-tal kapjuk
            //Ha az adat helyen NULL van azt kulon kezelni kell: Ha az adott index isDBNull akkor NULL-t iratunk ki
            //Amugy az adatokat az adott indexen levo queryResultGetValue-val kapjuk meg
            //Ertekek utan ures string, majd az egesz utan uj sor
            while (queryResult.Read())
            {
                for (int ordinal = 0; ordinal < queryResult.FieldCount; ordinal++)
                {
                    if (queryResult.IsDBNull(ordinal))
                    {
                        Console.Write("NULL");
                        continue;
                    }
                    var val = queryResult.GetValue(ordinal);
                    Console.Write(val);
                    Console.Write(" ");
                }

                Console.WriteLine();
            }
            getSQLCommand(command);
        }

        private static void resetApp(DbCommand command, string selectStatement)
        {
            command.CommandText = "DROP TABLE pizzas";
            var executeNonQuery = command.ExecuteNonQuery();

            command.CommandText = "DROP TABLE shorttest";
            executeNonQuery = command.ExecuteNonQuery();

            startProgram();
        }

    }

}