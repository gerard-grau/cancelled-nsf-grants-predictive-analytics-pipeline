from typing import List, Dict, Optional, Callable, Tuple

import mlflow
import mlflow.spark
from pyspark.sql import DataFrame, SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, IndexToString
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier, LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


class MLExperimentManager:
    """ML experiment manager with Spark + MLflow."""

    def __init__(
        self,
        spark: SparkSession,
        target_col: str,
        feature_cols: Optional[List[str]],
        experiment_name: str,
        tracking_uri: str = "file:./mlruns",
        problem_type: str = "multiclass",
    ):
        self.spark = spark
        self.target_col = target_col
        self.feature_cols = feature_cols
        self.problem_type = problem_type

        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment_name)

        self.train_df: Optional[DataFrame] = None
        self.test_df: Optional[DataFrame] = None

    def load_data(
        self,
        train_loader: Callable[[SparkSession], DataFrame],
        test_loader: Optional[Callable[[SparkSession], DataFrame]] = None,
    ) -> None:
        """Load train and test data."""
        self.train_df = train_loader(self.spark)
        if test_loader is not None:
            self.test_df = test_loader(self.spark)

        if self.feature_cols is None:
            self.feature_cols = [c for c in self.train_df.columns if c != self.target_col]
    def _build_pipeline(self, classifier) -> Pipeline:
        """Build Spark ML pipeline."""
        label_indexer = StringIndexer(inputCol=self.target_col, outputCol="label", handleInvalid="keep")
        assembler = VectorAssembler(inputCols=self.feature_cols, outputCol="features", handleInvalid="keep")
        label_converter = IndexToString(
            inputCol="prediction",
            outputCol="prediction_label",
            labels=label_indexer.fit(self.train_df).labels if self.train_df is not None else None,
        )
        return Pipeline(stages=[label_indexer, assembler, classifier, label_converter])

    def _get_evaluator(self):
        """Get appropriate evaluator."""
        return MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    def _fit_with_mlflow(
        self,
        classifier,
        run_name: str,
        param_grid: Optional[ParamGridBuilder],
        train_ratio: float,
        seed: int,
    ) -> Tuple[Pipeline, Dict[str, float]]:
        """Fit model with MLflow tracking."""
        if self.train_df is None:
            raise ValueError("Must call load_data() before training.")

        from pyspark.sql.functions import when, col as spark_col
        
        # Calculate class weights for imbalanced data
        label_counts = self.train_df.groupBy(self.target_col).count().collect()
        total_samples = self.train_df.count()
        
        class_weights = {
            float(row[self.target_col]): total_samples / (2 * row['count'])
            for row in label_counts
        }
        
        train_with_weights = self.train_df.withColumn(
            "sample_weight",
            when(spark_col(self.target_col) == 0, class_weights[0.0])
            .when(spark_col(self.target_col) == 1, class_weights[1.0])
            .otherwise(1.0)
        )
        
        train_df, valid_df = train_with_weights.randomSplit([train_ratio, 1 - train_ratio], seed=seed)

        pipeline = self._build_pipeline(classifier)
        evaluator = self._get_evaluator()

        with mlflow.start_run(run_name=run_name):
            if param_grid is not None:
                cv = CrossValidator(
                    estimator=pipeline,
                    estimatorParamMaps=param_grid.build(),
                    evaluator=evaluator,
                    numFolds=3,
                    seed=seed,
                )
                best_model = cv.fit(train_df).bestModel
            else:
                best_model = pipeline.fit(train_df)

            valid_predictions = best_model.transform(valid_df)
            accuracy = evaluator.evaluate(valid_predictions)
            metrics = {"valid_accuracy": float(accuracy)}

            mlflow.log_metrics(metrics)
            
            for p in classifier.extractParamMap():
                try:
                    mlflow.log_param(p.name, classifier.getOrDefault(p))
                except Exception:
                    pass

            mlflow.spark.log_model(best_model, artifact_path="model")

        return best_model, metrics
    def train_random_forest(
        self,
        run_name: str = "rf_model",
        train_ratio: float = 0.8,
        seed: int = 42,
        num_trees: int = 100,
        max_depth: int = 10,
        param_grid_dict: Optional[Dict[str, List]] = None,
    ) -> Tuple[Pipeline, Dict[str, float]]:
        """Train Random Forest classifier."""
        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            weightCol="sample_weight",
            numTrees=num_trees,
            maxDepth=max_depth,
            seed=seed,
        )

        param_grid = None
        if param_grid_dict:
            param_grid = ParamGridBuilder()
            for param_name, values in param_grid_dict.items():
                param_grid = param_grid.addGrid(getattr(rf, param_name), values)

        return self._fit_with_mlflow(rf, run_name, param_grid, train_ratio, seed)

    def train_logistic_regression(
        self,
        run_name: str = "logreg_model",
        train_ratio: float = 0.8,
        seed: int = 42,
        reg_param: float = 0.0,
        elastic_net_param: float = 0.0,
        param_grid_dict: Optional[Dict[str, List]] = None,
    ) -> Tuple[Pipeline, Dict[str, float]]:
        """Train Logistic Regression classifier."""
        lr = LogisticRegression(
            featuresCol="features",
            labelCol="label",
            weightCol="sample_weight",
            regParam=reg_param,
            elasticNetParam=elastic_net_param,
            maxIter=100,
        )

        param_grid = None
        if param_grid_dict:
            param_grid = ParamGridBuilder()
            for param_name, values in param_grid_dict.items():
                param_grid = param_grid.addGrid(getattr(lr, param_name), values)

        return self._fit_with_mlflow(lr, run_name, param_grid, train_ratio, seed)

    def train_gbt(
        self,
        run_name: str = "gbt_model",
        train_ratio: float = 0.8,
        seed: int = 42,
        max_iter: int = 50,
        max_depth: int = 5,
        param_grid_dict: Optional[Dict[str, List]] = None,
    ) -> Tuple[Pipeline, Dict[str, float]]:
        """Train Gradient Boosted Trees classifier."""
        gbt = GBTClassifier(
            featuresCol="features",
            labelCol="label",
            weightCol="sample_weight",
            maxIter=max_iter,
            maxDepth=max_depth,
            seed=seed,
        )

        param_grid = None
        if param_grid_dict:
            param_grid = ParamGridBuilder()
            for param_name, values in param_grid_dict.items():
                param_grid = param_grid.addGrid(getattr(gbt, param_name), values)

        return self._fit_with_mlflow(gbt, run_name, param_grid, train_ratio, seed)
    def evaluate_on_test(self, model: Pipeline) -> Dict[str, float]:
        """Evaluate model on test set."""
        if self.test_df is None:
            raise ValueError("No test_df loaded. Pass test_loader to load_data().")

        test_df = self.test_df.drop("sample_weight") if "sample_weight" in self.test_df.columns else self.test_df
        
        predictions = model.transform(test_df)
        accuracy = self._get_evaluator().evaluate(predictions)
        
        return {"test_accuracy": float(accuracy)}
