package objective;

import core.*;

import java.util.List;
import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.util.concurrent.*;
import java.util.HashMap;
/**
 * Assignment:
 * Task 1: counts the number of flights for each originating airport.
 * Initially mapper maps each unique FromAirportCode+flightID to Key with Value as 1
 * Combiner then takes first 3 characters of this key which is the FromAirportcode
 * and combines values into a list for each Airport Code.
 * Reducer then sums the values of the list for each Airport Code
 *
 * To run:
 * java Task_1 <file>
 *     i.e. java Task_1 AComp_Passenger_data_no_error.csv
 *
 * Areas to add improvement:
 * - Error checking and handling
 * - Multi-threading
 *   - Partitioning of input for parallel processing
 *   - Synchronisation and thread-safe operations
 */
class Task_1
    {
    // Configure and set-up the job using command line arguments specifying input files and job-specific mapper and
    // reducer functions
    private static AirportList aList=new AirportList(30);
    public static void main(String[] args) throws Exception {
        ReadAirports();
        Config config = new Config(args, mapper.class, reducer.class, combiner.class);
        Job job = new Job(config);
        job.run();
        DisplayTotals(aList,Job.getMap());
    }

    // Read airports file into airport list object
    public static void ReadAirports()
    {
        String csvFile1="Top30_airports_LatLong.csv";
        BufferedReader br = null;
        String line = "";
        try {
                br = new BufferedReader(new FileReader(csvFile1));
                while((line=br.readLine())!=null){
                    if (line.length()>0){
                        String[] Field = line.split(",");
                        String name=Field[0];
                        String code=Field[1];
                        double lat=Double.parseDouble(Field[2]);
                        double lon=Double.parseDouble(Field[3]);
                        Airport airport = new Airport(name,code,lat,lon);
                        aList.addAirport(airport);
                    }
                }
                br.close();
        } catch (IOException e) {
            System.out.println("IO Exception");
            e.printStackTrace();
        }
        System.out.println("***Airport List is: "+aList);
        System.out.println("*** no of airports read in is: "+aList.size());
    }
    
    private static void DisplayTotals(AirportList AirportListIn, ConcurrentHashMap mapIn){
        int total=0;
        System.out.println("*** Used Airports ***");
        for (String apCode: AirportListIn.getKeys()){
            if (mapIn.containsKey(apCode)){
                total=(int) mapIn.get(apCode);
                System.out.println("Airport: "+AirportListIn.getName(apCode)+ " Total Flights: "+total);
            }
        }
        System.out.println("*** Unused Airports ***");
        for (String apCode: AirportListIn.getKeys()){
            if (!mapIn.containsKey(apCode)){
                System.out.println("Unused Airport: "+AirportListIn.getName(apCode));
            }
        }

    }
    // FromAirportCode+Flightid count mapper:
    // Output a one for each occurrence of FromAirportCode+Flightid.
    // KEY = FromAirportCode+Flightid
    // VALUE = 1
    public static class mapper extends Mapper {
        public void map(String line) {
            String[] Fields=line.split(",");
            EmitIntermediate((Fields[2]+Fields[1]),1);
        }
    }
    // Airport Code count combiner:
    // Output the total number of occurrences of each unique Aiport Code
    // KEY = FromAirport Code
    // VALUE = count
    public static class combiner extends Combiner {
        public void combine(Object key, List values) {
            //int count = 0;
            //for (Object value : values) count += (int) value;
            EmitIntermediate2(key.toString().substring(0,3), values);
        }
    }
    // Airport Code count reducer:
    // Output the total number of occurrences of each unique Aiport Code
    // KEY = FromAirport Code
    // VALUE = total count
    public static class reducer extends Reducer {
        public void reduce(Object key, List values) {
            int count = 0;
            for (Object lst : values){
                for (Object value : (List) lst) count += (int) value;
                Emit(key, count);
            }
        }
    }
}