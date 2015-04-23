package iScope;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class KMeansClustering implements Serializable {
	/**
	 * Default Serial Version UID
	 */
	private static final long serialVersionUID = 1L;
	// add this to test code
	private static SparkConf conf = new SparkConf().setAppName(
			"K-means Example").setMaster("local");
	private static JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) throws Exception {

		KMeansClustering kMeans = new KMeansClustering();

		if (args.length > 4) {
			System.out
					.println("Usage : <File Path> <Number of Clusters> <Number of interations> <Number of Runs> < Initialization mode 0 : Random 1. Parallel> (optional parameter are )<Train percentage>");
		} else {

			String filePath = "C://Users//marram//Desktop//normalizedf.csv";
			int numClusters = 2;
			int numInterations = 250;
			int runs = 1;
			boolean initializationMode = false;

			double trainPercentage = 0.70;

			/*
			 * String filePath = args[0]; int numClusters =
			 * Integer.parseInt(args[1]); int numInterations =
			 * Integer.parseInt(args[2]); int runs = Integer.parseInt(args[3]);
			 * int initializationMode = Integer.parseInt(args[4]);
			 * 
			 * double trainPercentage = 0.70; double testPercentage = 0.30;
			 * 
			 * if (args.length == 7) { trainPercentage =
			 * Double.parseDouble(args[5]); testPercentage =
			 * Double.parseDouble(args[6]); }
			 */

			JavaRDD<String> stringRDD = sc.textFile(filePath);
			JavaRDD<Vector> dataRDD = kMeans.loadDataForClustering(stringRDD);

			JavaRDD<Vector>[] dataSplit = kMeans
					.split(dataRDD, trainPercentage);

			if (dataSplit != null) {
				JavaRDD<Vector> trainRDD = dataSplit[0];
				JavaRDD<Vector> testRDD = dataSplit[1];

				KMeansModel trainModel = kMeans.train(trainRDD, numClusters,
						numInterations, runs, initializationMode);

				JavaRDD<Integer> varTestModel = kMeans.predict(trainModel,
						testRDD);

				System.out.println("Test Model : ");
				varTestModel.foreach(f -> System.out.println(f));

				System.out.println("Number of clusters "
						+ kMeans.getNumberOfClusters(trainModel));

				JavaRDD<Integer> pred = kMeans.predict(trainModel, testRDD);

				System.out.println("Prediction Dataset : ");
				pred.foreach(f -> System.out.println(f));

				double WSSE = kMeans.evaluate(trainModel, testRDD);
				System.out.println("WSSE is " + WSSE);

			} else {
				System.out.println("Please check input percentages");
			}
		}
	}

	/**
	 * Converts Input to Vector RDD
	 * 
	 * @param filePath
	 * @return dataRDD
	 * 
	 */
	public JavaRDD<Vector> loadDataForClustering(JavaRDD<String> data) {

		return data.map(new Function<String, Vector>() {
			/**
			 * Default serial Version UID
			 */
			private static final long serialVersionUID = 1L;

			public Vector call(String s) {
				String[] sarray = s.trim().split(",");
				double[] values = new double[sarray.length];
				for (int i = 0; i < sarray.length; i++) {
					values[i] = Double.parseDouble(sarray[i]);
				}

				return Vectors.dense(values);
			}
		});
	}

	/**
	 * Split the Given RDD into Train and Test data
	 * 
	 * @param dataRDD
	 * @param percentages
	 * @return JavaRDD if percentages are in range else null
	 * 
	 */
	public JavaRDD<Vector>[] split(JavaRDD<Vector> dataRDD,
			double trainPercentage) {
		if (trainPercentage < 0 || trainPercentage > 1)
			return null;

		// If train percentage is in the range then perform operation
		double[] percentages = new double[2];
		percentages[0] = trainPercentage;
		percentages[1] = 1 - trainPercentage;

		return dataRDD.randomSplit(percentages);
	}

	/**
	 * K-Means performs training on Given data
	 * 
	 * @param trainData
	 * @param numClusters
	 * @param numInterations
	 * @param runs
	 * @param initializationMode
	 *            True : K Means Parallel False : K Means Random
	 * @return Train Model
	 */
	public KMeansModel train(JavaRDD<Vector> trainData, int numClusters,
			int numInterations, int runs, boolean initializationMode) {
		if (trainData != null || numClusters < 0 || numInterations < 0
				|| runs < 0) {
			if (initializationMode)
				return KMeans.train(trainData.rdd(), numClusters,
						numInterations, runs, KMeans.K_MEANS_PARALLEL());
			else
				return KMeans.train(trainData.rdd(), numClusters,
						numInterations, runs, KMeans.RANDOM());
		} else
			return null;
	}

	/**
	 * Returns all the cluster centers
	 * 
	 * @param model
	 * @return Vector[]
	 */
	public Vector[] getNumberOfClusters(KMeansModel kMeansModel) {
		if (kMeansModel == null)
			return null;
		return kMeansModel.clusterCenters();
	}

	/**
	 * 
	 * @param kMeansModel
	 * @param JavaRDD
	 */
	public JavaRDD<Integer> predict(KMeansModel kMeansModel,
			JavaRDD<Vector> testDataset) {
		if (kMeansModel == null || testDataset == null)
			return null;
		return kMeansModel.predict(testDataset);
	}

	/**
	 * 
	 * @param kMeansModel
	 * @param dataset
	 * @return Computed Cost i.e Within set sum of squared errors
	 */

	// TODO change Exception to any User defined Exception in need //
	public double evaluate(KMeansModel kMeansModel, JavaRDD<Vector> dataset)
			throws Exception {
		if (kMeansModel == null || dataset == null)
			throw new Exception();
		else
			return kMeansModel.computeCost(dataset.rdd());
	}
}
