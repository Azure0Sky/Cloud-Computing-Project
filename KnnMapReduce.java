import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class KnnMapReduce
{
    // DataTuple keeps the data of a single item
    private static class DataTuple
    {
        ArrayList<Double> data;
        int model;

        DataTuple()
        {
            data = new ArrayList<>();
            model = -1;
        }

        // Get data from a string
        // data format: <val1> <val2> ... <valn> <model> ( seperating by space )
        void set( String line, boolean testData )
        {
            String[] vals = line.split( " " );

            int len = testData ? vals.length : vals.length - 1;
            for ( int i = 0; i < len; ++i ) {
                data.add( Double.parseDouble( vals[i] ) );
            }

            // The last number is the model if the data is not test data
            if ( !testData ) {
                model = Integer.parseInt( vals[vals.length - 1] );
            }
        }

        public ArrayList<Double> getData()
        {
            return data;
        }

        public int getModel()
        {
            return model;
        }
    }

    // KnnMapper finds the knn of one of the data, passing it to reducer
    public static class KnnMapper
            extends Mapper<Object, Text, Text, IntWritable>
    {
        int K = 5;                              // The number of nearest neighbors
        private ArrayList<DataTuple> trainData;

        // To read training data
        @Override
        public void setup( Context context ) throws IOException, InterruptedException
        {
            trainData = new ArrayList<>();

            FileSystem fs = null;
            try {
                fs = FileSystem.get( new URI( "hdfs://master:9000/" ), new Configuration() );
            } catch ( Exception exc ) {
                System.err.println( "No hdfs" );
                System.exit( 2 );
            }

            // Training data changes as required ( /data/... )
            String trainingDataPath = "hdfs://master:9000/data/iris_training.txt";
            FSDataInputStream fiStream = fs.open( new Path( trainingDataPath ) );
            BufferedReader bfReader = new BufferedReader( new InputStreamReader( fiStream ) );

            // Get data line by line
            String line = bfReader.readLine();
            while ( line != null ) {
                DataTuple tuple = new DataTuple();
                tuple.set( line, false );
                trainData.add( tuple );

                line = bfReader.readLine();
            }
        }

        @Override
        public void map( Object key, Text value, Context context ) throws IOException, InterruptedException
        {
            // In ascending order
            TreeMap<Double, ArrayList<Integer>> distanceModel = new TreeMap<>();

            DataTuple testTuple = new DataTuple();
            testTuple.set( value.toString(), true );

            // Compute the euclidean distance between the test tuple and each train data
            // Keep the distance and the model of the train data
            for ( DataTuple trainTuple : trainData ) {

                double dis = EuclideanDistance( testTuple.data, trainTuple.data );

                if ( !distanceModel.containsKey( dis ) ) {
                    ArrayList<Integer> temp = new ArrayList<>(1);       // space saving
                    temp.add( trainTuple.model );
                    distanceModel.put( dis, temp );
                } else {
                    distanceModel.get( dis ).add( trainTuple.model );
                }

            }

            // Loop K times
            int cnt = 0;
            for ( Double dis : distanceModel.keySet() ) {
                if ( cnt >= K )
                    break;

                ArrayList<Integer> temp = distanceModel.get( dis );
                for ( Integer model : temp ) {
                    context.write( value, new IntWritable( model ) );
                    if ( ++cnt >= K )
                        break;
                }
            }
        }

        // Compute the euclidean distance between two vectors
        // The dimensions of two vectors must be same
        private double EuclideanDistance( ArrayList<Double> vec1, ArrayList<Double> vec2 )
        {
            double res = 0;
            for ( int i = 0; i < vec1.size(); ++i ) {
                res += Math.pow( vec1.get( i ) - vec2.get( i ), 2 );
            }

            return Math.sqrt( res );
        }
    }

    // KnnReducer count the occurrence of each model and pick the max as the model of the test data
    public static class KnnReducer
            extends Reducer<Text, IntWritable, Text, Text>
    {
        @Override
        public void reduce( Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException
        {
            // <model, count of occurrence>
            HashMap<Integer, Integer> counter = new HashMap<>();
            for ( IntWritable model : values ) {
                counter.put( model.get(), counter.getOrDefault( model, 0 ) + 1 );
            }

            // Find max count
            int max = 0;
            Integer modelOfTest = -1;
            for ( Integer model : counter.keySet() ) {
                if ( counter.get( model ) > max ) {
                    modelOfTest = model;
                    max = counter.get( model );
                }
            }

            context.write( key, new Text( modelOfTest.toString() ) );
        }
    }

    public static void main( String[] args ) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser( conf, args ).getRemainingArgs();
        if ( otherArgs.length < 2 ) {
            System.err.println( "Usage: KnnMapReduce <in> [<in>...] <out>" );
            System.exit( 2 );
        }

        Job job = Job.getInstance( conf, "Knn MapReduce" );
        job.setJarByClass( KnnMapReduce.class );
        job.setMapperClass( KnnMapper.class );
        job.setReducerClass( KnnReducer.class );
        job.setMapOutputValueClass( IntWritable.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( Text.class );
        for ( int i = 0; i < otherArgs.length - 1; ++i ) {
            FileInputFormat.addInputPath( job, new Path( otherArgs[i] ) );
        }

        FileOutputFormat.setOutputPath( job, new Path( otherArgs[otherArgs.length - 1] ) );
        System.exit( job.waitForCompletion( true ) ? 0 : 1 );
    }
}
