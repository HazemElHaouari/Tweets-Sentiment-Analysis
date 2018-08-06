

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Iterator;
import java.io.IOException;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;


// Notre classe REDUCE - templatée avec un type générique K pour la clef, un type de valeur IntWritable, et un type de retour
// (le retour final de la fonction Reduce) Text.
public class SentimentsReduce extends
		Reducer<Text, IntWritable, Text, FloatWritable> { // Text, IntWritable,
															// Text,
															// FloatWritable

	// La fonction REDUCE elle-même. Les arguments: la clef key (d'un type
	// générique K), un Iterable de toutes les valeurs
	// qui sont associées à la clef en question, et le contexte Hadoop (un
	// handle qui nous permet de renvoyer le résultat à Hadoop).
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		//hbase
		//Configuration config = HBaseConfiguration.create();
		//Connection connection = ConnectionFactory.createConnection(config);
		String hashtag = key.toString();
		//MongoDb
		/*MongoClient mongoClient=new MongoClient();
		MongoDatabase db=mongoClient.getDatabase("Sentiments");
		MongoCollection collection=db.getCollection("Analyse");*/


		try {
			//hbase
			//Table table = connection.getTable(TableName.valueOf("analyseSentiment"));
			

			Iterator<IntWritable> i = values.iterator();
			int sum = 0;
			int n = 0;
			int countPos = 0;
			int countNeg=0;
			float tauxFinale;
			while (i.hasNext()) {
				n = i.next().get();
				sum += n;
				if (n==1){
					countPos++;
				} else {
					countNeg++;
				}
				
			}
			
			tauxFinale = (float)sum /(countPos+countNeg);
			/*Put p = new Put(Bytes.toBytes(hashtag));
			p.addColumn(Bytes.toBytes("analyseSentimentFamily"),
					Bytes.toBytes("Qualifier"),
					Bytes.toBytes(Float.toString(tauxFinale)));

			table.put(p);*/
			//mongoDb
			//collection.insertOne(new Document().append("theme",hashtag).append("Taux Finale",Float.toString(tauxFinale)));

			context.write(key, new FloatWritable(tauxFinale));
		} finally {

		}

	}

}
