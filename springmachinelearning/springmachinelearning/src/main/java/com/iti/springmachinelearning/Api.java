package com.iti.springmachinelearning;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.OneHotEncoderModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

@Controller
public class Api implements Serializable {
    @Autowired
    Dataset<Row> machineLearning;
//    JobsDAO jobsDAO = new JobsDAO();
//    Dataset<Row> machineLearning = jobsDAO.createDataset();
    int count;
    private static transient PieChart chart;
    private static final ArrayList<Double> averageFactorization = new ArrayList<>();

    @RequestMapping(value="/")
    public String openStream(Model model) {
        model.addAttribute("imageName", "/images/index.png");
        return "home";
    }

    @RequestMapping(value="/Summary")
    public String statistics(Model model) {
        model.addAttribute("fullSchema",JobsDAO.rows.toJSON().collectAsList());
        model.addAttribute("count", JobsDAO.rows.count());
        model.addAttribute("schemaTable", JobsDAO.rows.describe().toJSON().collectAsList());
//        Print in Console Schema and Info:
//        SpringMachineLearningApplication.rows.printSchema();
//        System.out.println("Total count: " + SpringMachineLearningApplication.rows.count());
//        Dataset<Row> describe = SpringMachineLearningApplication.rows.describe();
//        describe.show();
        return "summary";
    }

    @RequestMapping("/pieChart")
    public String pieChart(Model model) {
        machineLearning.createOrReplaceTempView("The_most_demanding_companies_for_jobs");
        Dataset<Row> machine = JobsDAO.ss.sql("SELECT Company ,COUNT(Title) x FROM The_most_demanding_companies_for_jobs group by Company " +
                "order by x Desc");
        chart = new PieChartBuilder().width(2000).height(600).title("The Most Demanding Job Title").build();
        machine.foreach((ForeachFunction<Row>)
                row -> {
                    if (row.getLong(1)>20) chart.addSeries(row.getString(0), row.getLong(1));
                    else   count ++;
                });
        chart.addSeries("others",count);
//        Display the Bar Chart in the editor:
//        new SwingWrapper<PieChart>(chart).displayChart();
        String imageNameAndPath = "/images/pie_chart.png";
        model.addAttribute("imageName", imageNameAndPath);
        try{
            BitmapEncoder.saveBitmap(chart,"./src/main/resources/static"+imageNameAndPath,BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e){
            e.printStackTrace();
        }
        return "pie_chart";
    }

    @RequestMapping ("/barChart1")
    public String barChart(Model model){
        machineLearning.createOrReplaceTempView("The_most_demanding_companies_for_jobs");
        Dataset<Row> machine = JobsDAO.ss.sql("SELECT Title ,COUNT(Title) x FROM The_most_demanding_companies_for_jobs group by Title order by x Desc limit 10");
        CategoryChart chart = new CategoryChartBuilder().width(1000).height(1000).title("The Most Popular Job Titles").xAxisTitle("Job Title").yAxisTitle("count").build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.addSeries("The Most Popular Job Titles", machine.select("Title").as(Encoders.STRING()).collectAsList(),machine.select("x").as(Encoders.LONG()).collectAsList());
//        Display the Bar Chart in the editor:
//        new SwingWrapper<CategoryChart>(chart).displayChart();
        String imageNameAndPath = "/images/most_popular_job_titles.png";
        model.addAttribute("imageName", imageNameAndPath);
        try{
            BitmapEncoder.saveBitmap(chart,"./src/main/resources/static"+imageNameAndPath,BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e){
            e.printStackTrace();
        }
        return "charts";
    }

    @RequestMapping(value="/barChart2")
    public String barChartStream(Model model){
        machineLearning.createOrReplaceTempView("the_most_popular_areas");
        Dataset<Row> machine = JobsDAO.ss.sql("SELECT Location ,COUNT(Location) x FROM the_most_popular_areas group by Location order by x Desc limit 10");
        CategoryChart chart = new CategoryChartBuilder().width(1000).height(1000).title("The Most Popular Areas").xAxisTitle("Location").yAxisTitle("count").build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.addSeries("The Most Popular Areas", machine.select("Location").as(Encoders.STRING()).collectAsList(),machine.select("x").as(Encoders.LONG()).collectAsList());
//        Display the Bar Chart in the editor:
//        new SwingWrapper<CategoryChart>(chart).displayChart();
        String imageNameAndPath = "/images/areas_BarChart.png";
        model.addAttribute("imageName", imageNameAndPath);
        try{
            BitmapEncoder.saveBitmap(chart,"./src/main/resources/static"+imageNameAndPath,BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e){
            e.printStackTrace();
        }
        return "charts";
    }

    @RequestMapping("/barChart3")
    public String barchart2(Model model){
        machineLearning.createOrReplaceTempView(" the_most_important_skills_required");
        Dataset<Row> machine = JobsDAO.ss.sql("SELECT explode(split(Skills,',')) as exploded_skills  FROM the_most_important_skills_required ");
        machine.show();
        machine.createOrReplaceTempView("exploded_skills");
        Dataset<Row> exploded_skills = JobsDAO.ss.sql("SELECT exploded_Skills ,COUNT(exploded_Skills) x FROM exploded_skills group by exploded_Skills order by x Desc limit 10");
        CategoryChart chart = new CategoryChartBuilder().width(1000).height(1000).title("The Most Popular Skills Required").xAxisTitle("Skills").yAxisTitle("count").build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.addSeries("The Most Popular Skills Required", exploded_skills.select("exploded_Skills").as(Encoders.STRING()).collectAsList(),exploded_skills.select("x").as(Encoders.LONG()).collectAsList());
//        Display the Bar Chart in the editor:
//        new SwingWrapper<CategoryChart>(chart).displayChart();
        String imageNameAndPath = "/images/the_most_popular_skills_required.png";
        model.addAttribute("imageName", imageNameAndPath);
        try{
            BitmapEncoder.saveBitmap(chart,"./src/main/resources/static"+imageNameAndPath,BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e){
            e.printStackTrace();
        }
        return "charts";
    }

    @RequestMapping("/kMeans")
    public String kMeans(Model model){
        machineLearning.createOrReplaceTempView ("wuzzuf_raw");
        final Dataset<Row> title = JobsDAO.ss.sql ("SELECT CAST(Title as String)Title FROM wuzzuf_raw");
        final Dataset<Row> company = JobsDAO.ss.sql ("SELECT CAST(Company as String)Company  FROM wuzzuf_raw");
        StringIndexer indexer = new StringIndexer()
                .setInputCol("Company")
                .setOutputCol("CompanyIndex");
// Indexer and string indexer used to convert nominal data to numercial data
        Dataset<Row> indexed_company = indexer.fit(company).transform(company);
        indexed_company.show();
        StringIndexer indexer1 = new StringIndexer()
                .setInputCol("Title")
                .setOutputCol("TitleIndex");
// Indexer and string indexer used to convert nominal data to numercial data
        Dataset<Row> indexed_title = indexer1.fit(title).transform(title);
        indexed_title.show();
        OneHotEncoder encoder = new OneHotEncoder()
                .setInputCols(new String[] {"CompanyIndex"})
                .setOutputCols(new String[] {"companyOneHotE"});
        OneHotEncoderModel oneHotEncoderModel = encoder.fit(indexed_company);
        Dataset<Row> companyOnHote = oneHotEncoderModel.transform(indexed_company);
        companyOnHote.show();
        OneHotEncoder Title_encoder = new OneHotEncoder()
                .setInputCols(new String[] {"TitleIndex"})
                .setOutputCols(new String[] {"TitleOneHotE"});
        OneHotEncoderModel modelTitle = Title_encoder.fit(indexed_title);
        Dataset<Row> TitleOnHote = modelTitle.transform(indexed_title);
        companyOnHote.show();
        final VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[] { "companyOneHotE"})
                .setOutputCol("features");
        final Dataset<Row> Companyfeatures = vectorAssembler.transform(companyOnHote.na ().drop ());
        Companyfeatures.show();
        final VectorAssembler vectorAssembler1 = new VectorAssembler()
                .setInputCols(new String[] { "TitleOneHotE"})
                .setOutputCol("titlefeatures");
        // Print Schema to see column names, types and other metadata
        final Dataset<Row> featurefortitle = vectorAssembler1.transform(TitleOnHote.na ().drop ());
        //featuresData.printSchema();
        // Split the data OF COMPANY NAMES into training and test sets (30% held out for
        // testing).
        Dataset<Row>[] splits = Companyfeatures.randomSplit(new double[] { 0.8, 0.2 },42);
        Dataset<Row> CompanyTrainingData = splits[0];
        Dataset<Row> CompanyTestData = splits[1];
        // this is for jup title clustring
        Dataset<Row>[] splits1 = featurefortitle.randomSplit(new double[] { 0.8, 0.2 },42);
        Dataset<Row> trainingDataTitle = splits[0];
        Dataset<Row> testDataTitle = splits[1];
        Companyfeatures.printSchema ();
/////////////////////////  k means for company   //////////////////////////
// Trains a k-means model.
        KMeans kmeans = new KMeans().setK(1000).setSeed(1L);
        KMeansModel CompanyModel = kmeans.fit(CompanyTrainingData);
//// Make predictions
        Dataset<Row> predictions = CompanyModel.transform(CompanyTestData);
//// Evaluate clustering by computing Silhouette score
        ClusteringEvaluator CompanyEvaluator = new ClusteringEvaluator();
        double distance = CompanyEvaluator.evaluate(predictions);
        System.out.println("Distance Measure: "+CompanyEvaluator.getDistanceMeasure());
        System.out.println(distance);
        model.addAttribute("companyKMeans", CompanyEvaluator.getDistanceMeasure());
        model.addAttribute("companyKMeansAccuracy", distance);
/////////////////////////  k means for title   //////////////////////////
        KMeans kmeans_title = new KMeans().setK(800).setSeed(1L);
//fit is used on the training data to learn the scaling parameters of that data. Here,
//the model built by us will learn the mean and variance of the features of the training set
        KMeansModel mode_title = kmeans_title.fit(trainingDataTitle);
//Using the transform method we can use the same mean and variance as it is calculated
//from our training data to transform our test data.
//Thus, the parameters learned by our model using the training data will help us to transform our test data.
        Dataset<Row> predictions_title = mode_title.transform(testDataTitle);
        ClusteringEvaluator TitleEvaluator = new ClusteringEvaluator();
//ClusteringEvaluator evaluator_title = new ClusteringEvaluator();
        double distance_title = TitleEvaluator.evaluate(predictions_title);
        System.out.println("Distance Measure: "+kmeans_title.getDistanceMeasure());
        System.out.println(distance_title);
        model.addAttribute("kMeansTitle", kmeans_title.getDistanceMeasure());
        model.addAttribute("kMeansAccuracyTitle", distance_title);
        return "kmeans";
    }

    @RequestMapping("/factorization")
    public String factorization(Model model){
        machineLearning.createOrReplaceTempView ("Factorize_YearsExp");
        Dataset<Row> yearsExp = JobsDAO.ss.sql ("SELECT YearsExp FROM Factorize_YearsExp");
        calculateFactorization(yearsExp);
        model.addAttribute("indexed",averageFactorization);
        model.addAttribute("yearsExp", yearsExp.toJSON().collectAsList());
        return "factorization";
    }

    public void calculateFactorization(Dataset<Row> yearsExp){
        yearsExp.foreach((ForeachFunction<Row>)
                row -> {
                    String x = row.toString().replaceAll("\\[", "").replaceAll("\\]","");
                    if(x.contains("-")){
                        String[] words=x.split("-");
                        Double min = Double.valueOf(words[0]);
                        String temp = String.valueOf(words[1].charAt(0)) + String.valueOf(words[1].charAt(1));
                        temp = temp.trim();
                        Double max = Double.valueOf(temp);
                        Double number = (min+max)/2;
                        averageFactorization.add(number);
                    }else if(x.contains("+")){
                        String[] words=x.split("\\+");
                        Double min = Double.valueOf(words[0]);
                        Double max = min+5;
                        Double number = (min+max)/2;
                        averageFactorization.add(number);
                    }else{
                        averageFactorization.add(0.0);
                    }
                }
        );
    }
}

