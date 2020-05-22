package com.andrew.sparkwork.trans;

import com.andrew.sparkwork.trans.AbstractTransformer;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
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

public class SimpleRandomForestTransformer  extends AbstractTransformer {

    private static final Logger log = LoggerFactory.getLogger(com.andrew.sparkwork.trans.SimpleRandomForestTransformer.class);

    public SimpleRandomForestTransformer(Properties p) {
        super(p);
    }

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> inputs, SparkSession session, String stepName) throws Exception {
        if (inputs != null) {
            log.info("Total inputs: {}", inputs.size());
        }

        Dataset<Row> datasetTrain, datasetTest;

        String trainName = props.getProperty("simpleRandomForest.trainDataFrame");
        if ( trainName == null) {
            throw new RuntimeException("You have to define which dataset to use in the train");
        }

        datasetTrain = inputs.get(trainName);

        String testName = props.getProperty("simpleRandomForest.testDataFrame");
        if ( testName == null) {
            throw new RuntimeException("You have to define which dataset to use in the train");
        }

        datasetTest = inputs.get(testName);

        if( !props.containsKey("simpleRandomForest.labelColumn") ) {
            throw new RuntimeException("We need pipeline defined for MLLIB algorithm");
        }

        String labelColumn = props.getProperty("simpleRandomForest.labelColumn");

        List<PipelineStage> list = new LinkedList<>();

        if(  props.containsKey("simpleRandomForest.stringIndex" ) ) {

            String[] columns = props.getProperty("simpleRandomForest.stringIndex").split(",");

            for( int i=0; i<columns.length; i++ ) {
                String[] cols = columns[i].split(":");
                list.add( new StringIndexer().setInputCol(cols[0]).setOutputCol(cols[1]) );
            }

        }

        VectorAssembler vectorAssembler = new VectorAssembler();
        if( !props.containsKey("simpleRandomForest.featureColumns") ) {
            throw new RuntimeException("We need to define features send to RandomForestClassifier");
        }

        vectorAssembler.setInputCols(props.getProperty("simpleRandomForest.featureColumns").split(","))
                .setOutputCol("features");
        list.add(vectorAssembler);

        int treeNumbers = Integer.valueOf(props.getProperty("simpleRandomForest.params.numberOfTrees", "5"));

        String predictionColumn = "predicted_"+labelColumn;
        RandomForestClassifier randomForestClassifier = new RandomForestClassifier().setLabelCol(labelColumn)
                .setFeaturesCol("features").setPredictionCol(predictionColumn).setNumTrees(treeNumbers);

        list.add(randomForestClassifier);
        PipelineStage[] stages = list.toArray(new PipelineStage[]{});

        Pipeline pipeline = new Pipeline().setStages(stages);

        PipelineModel model = pipeline.fit(datasetTrain);

        Dataset<Row> predictions = model.transform(datasetTest);
        Evaluator evaluator = new MulticlassClassificationEvaluator().setLabelCol(labelColumn).setPredictionCol(predictionColumn).setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);

        log.info("RandomForest accuracy = {}", accuracy);
        System.out.println("Accuracy: " + accuracy);

        Map<String, Dataset<Row>> datasetMap = new HashMap<>();
        datasetMap.put(stepName, predictions);
        return datasetMap;
    }
}