package com.andrew.sparkwork.trans;

import com.andrew.sparkwork.trans.AbstractTransformer;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.*;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.feature.*;
import sun.awt.image.ImageWatched;

public class SimpleLogisticRegressionTransformer  extends AbstractTransformer {

    private static final Logger log = LoggerFactory.getLogger(com.andrew.sparkwork.trans.SimpleLogisticRegressionTransformer.class);

    public SimpleLogisticRegressionTransformer(Properties p) {
        super(p);
    }

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> inputs, SparkSession session, String stepName) throws Exception {
        if (inputs != null) {
            log.info("Total inputs: {}", inputs.size());
        }

        Dataset<Row> datasetTrain, datasetTest;

        String trainName = props.getProperty("simpleLogisticRegression.trainDataFrame");
        if ( trainName == null) {
            throw new RuntimeException("You have to define which dataset to use in the train");
        }

        datasetTrain = inputs.get(trainName);

        String testName = props.getProperty("simpleLogisticRegression.testDataFrame");
        if ( testName == null) {
            throw new RuntimeException("You have to define which dataset to use in the train");
        }

        datasetTest = inputs.get(testName);

        if( !props.containsKey("simpleLogisticRegression.labelColumn") ) {
            throw new RuntimeException("We need pipeline defined for MLLIB algorithm");
        }

        String labelColumn = props.getProperty("simpleLogisticRegression.labelColumn");

        List<PipelineStage> list = new LinkedList<>();

        if(  props.containsKey("simpleLogisticRegression.stringIndex" ) ) {

            String[] columns = props.getProperty("simpleLogisticRegression.stringIndex").split(",");

            for( int i=0; i<columns.length; i++ ) {
                String[] cols = columns[i].split(":");
                list.add( new StringIndexer().setInputCol(cols[0]).setOutputCol(cols[1]) );
            }

        }

        VectorAssembler vectorAssembler = new VectorAssembler();
        if( !props.containsKey("simpleLogisticRegression.featureColumns") ) {
            throw new RuntimeException("We need to define features send to GradientBoostedTreeClassifier");
        }

        vectorAssembler.setInputCols(props.getProperty("simpleLogisticRegression.featureColumns").split(","))
                .setOutputCol("features");
        list.add(vectorAssembler);

        int maxIter = Integer.valueOf(props.getProperty("simpleLogisticRegression.params.maxIter", "20"));

        String predictionColumn = "predicted_"+labelColumn;
        LogisticRegression logisticRegression = new LogisticRegression().setLabelCol(labelColumn)
                .setFeaturesCol("features").setPredictionCol(predictionColumn).setMaxIter(maxIter);



        list.add(logisticRegression);
        PipelineStage[] stages = list.toArray(new PipelineStage[]{});

        Pipeline pipeline = new Pipeline().setStages(stages);

        PipelineModel model = pipeline.fit(datasetTrain);

        Dataset<Row> predictions = model.transform(datasetTest);
        Evaluator evaluator = new MulticlassClassificationEvaluator().setLabelCol(labelColumn).setPredictionCol(predictionColumn).setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);

        log.info("GBT accuracy = {}", accuracy);
        System.out.println("Accuracy: " + accuracy);

        Map<String, Dataset<Row>> datasetMap = new HashMap<>();
        datasetMap.put(stepName, predictions);
        return datasetMap;
    }
}