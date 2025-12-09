# ml_experiment_manager.py

from __future__ import annotations

from typing import List, Dict, Optional, Callable, Tuple

import mlflow
import mlflow.spark

from pyspark.sql import DataFrame, SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, IndexToString
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


class MLExperimentManager:
    """
    Classe per gestionar experiments de ML amb Spark + MLflow.

    ÚS TIPUS:
        from read_delta_files_utils import load_train, load_test
        from ml_experiment_manager import MLExperimentManager
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

        manager = MLExperimentManager(
            spark=spark,
            target_col="cancelled",
            feature_cols=None,   # o llista de columnes
            experiment_name="nsf_cancelled_models",
            tracking_uri="file:./mlruns",
            problem_type="binary"
        )

        manager.load_data(load_train, load_test)
        rf_model, rf_metrics = manager.train_random_forest(
            run_name="rf_baseline",
            train_ratio=0.8,
            num_trees=100,
            max_depth=10
        )
        test_metrics = manager.evaluate_on_test(rf_model)
    """

    def __init__(
        self,
        spark: SparkSession,
        target_col: str,
        feature_cols: Optional[List[str]],
        experiment_name: str,
        tracking_uri: str = "file:./mlruns",
        problem_type: str = "binary",  # "binary" | "multiclass" | "regression"
    ):
        self.spark = spark
        self.target_col = target_col
        self.feature_cols = feature_cols  # si None, s'agafarà automàticament
        self.problem_type = problem_type

        # MLflow config
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment_name)

        self.train_df: Optional[DataFrame] = None
        self.test_df: Optional[DataFrame] = None

    # ---------------------------
    # CÀRREGA DE DADES
    # ---------------------------
    def load_data(
        self,
        train_loader: Callable[[SparkSession], DataFrame],
        test_loader: Optional[Callable[[SparkSession], DataFrame]] = None,
    ) -> None:
        """
        Carrega train i (opcionalment) test a partir de funcions tipus load_train/load_test.

        train_loader: funció que rep spark i retorna un DataFrame de training.
        test_loader: funció que rep spark i retorna un DataFrame de test (opcional).
        """
        self.train_df = train_loader(self.spark)
        if test_loader is not None:
            self.test_df = test_loader(self.spark)

        # Si no s'han especificat feature_cols, agafem totes les columnes menys la target.
        if self.feature_cols is None:
            self.feature_cols = [
                c for c in self.train_df.columns if c != self.target_col
            ]

    # ---------------------------
    # HELPERS PER PIPELINES
    # ---------------------------
    def _build_pipeline(self, classifier) -> Pipeline:
        """
        Construeix un Pipeline Spark:
            StringIndexer(target) -> VectorAssembler(features) -> classifier -> IndexToString(predictions)
        """
        label_indexer = StringIndexer(
            inputCol=self.target_col,
            outputCol="label",
            handleInvalid="keep",
        )

        assembler = VectorAssembler(
            inputCols=self.feature_cols,
            outputCol="features",
            handleInvalid="keep",
        )

        label_converter = IndexToString(
            inputCol="prediction",
            outputCol="prediction_label",
            labels=label_indexer.fit(self.train_df).labels
            if self.train_df is not None
            else None,
        )

        pipeline = Pipeline(stages=[label_indexer, assembler, classifier, label_converter])
        return pipeline

    def _get_evaluator(self):
        """
        Retorna l'evaluator adequat segons el tipus de problema.
        """
        if self.problem_type == "binary":
            return BinaryClassificationEvaluator(
                labelCol="label",
                rawPredictionCol="rawPrediction",
                metricName="areaUnderROC",
            )
        elif self.problem_type == "multiclass":
            return MulticlassClassificationEvaluator(
                labelCol="label",
                predictionCol="prediction",
                metricName="accuracy",
            )
        else:  # per si algun cop el vols per regressió
            from pyspark.ml.evaluation import RegressionEvaluator

            return RegressionEvaluator(
                labelCol="label", predictionCol="prediction", metricName="rmse"
            )

    # ---------------------------
    # ENTRENAR AMB MLflow
    # ---------------------------
    def _fit_with_mlflow(
        self,
        classifier,
        run_name: str,
        param_grid: Optional[ParamGridBuilder],
        train_ratio: float,
        seed: int,
    ) -> Tuple[Pipeline, Dict[str, float]]:
        """
        Fa el split train/valid, construeix el pipeline, entrena (amb o sense cross-validation),
        avalua sobre validació, i ho registra tot a MLflow.
        """
        if self.train_df is None:
            raise ValueError("Cal cridar load_data() abans d'entrenar.")

        train_df, valid_df = self.train_df.randomSplit([train_ratio, 1 - train_ratio], seed=seed)

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
                cv_model = cv.fit(train_df)
                best_model = cv_model.bestModel
                valid_predictions = best_model.transform(valid_df)
            else:
                best_model = pipeline.fit(train_df)
                valid_predictions = best_model.transform(valid_df)

            # Mètrica principal
            main_metric = evaluator.evaluate(valid_predictions)

            metrics = {"valid_metric": float(main_metric)}

            # Algunes mètriques extra (si és classificació)
            if self.problem_type in ("binary", "multiclass"):
                acc_eval = MulticlassClassificationEvaluator(
                    labelCol="label", predictionCol="prediction", metricName="accuracy"
                )
                accuracy = acc_eval.evaluate(valid_predictions)
                metrics["valid_accuracy"] = float(accuracy)

            # Log de mètriques
            mlflow.log_metrics(metrics)

            # Log d'alguns paràmetres principals del classificador
            for p in classifier.extractParamMap():
                try:
                    name = p.name
                    value = classifier.getOrDefault(p)
                    mlflow.log_param(name, value)
                except Exception:
                    # hi ha alguns params interns que poden donar guerra, els ignorem
                    pass

            # Guardar model com a artifact
            mlflow.spark.log_model(best_model, artifact_path="model")

        return best_model, metrics

    # ---------------------------
    # MÈTODES D'ALTS NIVELL
    # ---------------------------
    def train_random_forest(
        self,
        run_name: str = "rf_model",
        train_ratio: float = 0.8,
        seed: int = 42,
        num_trees: int = 100,
        max_depth: int = 10,
        param_grid_dict: Optional[Dict[str, List]] = None,
    ) -> Tuple[Pipeline, Dict[str, float]]:
        """
        Entrena un RandomForestClassifier i el registra a MLflow.
        param_grid_dict: diccionari opcional per fer grid search, ex:
            {
                "numTrees": [50, 100],
                "maxDepth": [5, 10]
            }
        """
        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            numTrees=num_trees,
            maxDepth=max_depth,
            seed=seed,
        )

        param_grid = None
        if param_grid_dict:
            param_grid = ParamGridBuilder()
            for param_name, values in param_grid_dict.items():
                param_grid = param_grid.addGrid(getattr(rf, param_name), values)

        return self._fit_with_mlflow(
            classifier=rf,
            run_name=run_name,
            param_grid=param_grid,
            train_ratio=train_ratio,
            seed=seed,
        )

    def train_logistic_regression(
        self,
        run_name: str = "logreg_model",
        train_ratio: float = 0.8,
        seed: int = 42,
        reg_param: float = 0.0,
        elastic_net_param: float = 0.0,
        param_grid_dict: Optional[Dict[str, List]] = None,
    ) -> Tuple[Pipeline, Dict[str, float]]:
        lr = LogisticRegression(
            featuresCol="features",
            labelCol="label",
            regParam=reg_param,
            elasticNetParam=elastic_net_param,
            maxIter=100,
        )

        param_grid = None
        if param_grid_dict:
            param_grid = ParamGridBuilder()
            for param_name, values in param_grid_dict.items():
                param_grid = param_grid.addGrid(getattr(lr, param_name), values)

        return self._fit_with_mlflow(
            classifier=lr,
            run_name=run_name,
            param_grid=param_grid,
            train_ratio=train_ratio,
            seed=seed,
        )

    def train_gbt(
        self,
        run_name: str = "gbt_model",
        train_ratio: float = 0.8,
        seed: int = 42,
        max_iter: int = 50,
        max_depth: int = 5,
        param_grid_dict: Optional[Dict[str, List]] = None,
    ) -> Tuple[Pipeline, Dict[str, float]]:
        gbt = GBTClassifier(
            featuresCol="features",
            labelCol="label",
            maxIter=max_iter,
            maxDepth=max_depth,
            seed=seed,
        )

        param_grid = None
        if param_grid_dict:
            param_grid = ParamGridBuilder()
            for param_name, values in param_grid_dict.items():
                param_grid = param_grid.addGrid(getattr(gbt, param_name), values)

        return self._fit_with_mlflow(
            classifier=gbt,
            run_name=run_name,
            param_grid=param_grid,
            train_ratio=train_ratio,
            seed=seed,
        )

    # ---------------------------
    # AVALUACIÓ EN TEST
    # ---------------------------
    def evaluate_on_test(self, model: Pipeline) -> Dict[str, float]:
        """
        Avalua un model ja entrenat sobre el test_df i retorna un diccionari de mètriques.
        """
        if self.test_df is None:
            raise ValueError("No hi ha test_df carregat. Passa test_loader a load_data().")

        evaluator = self._get_evaluator()

        predictions = model.transform(self.test_df)
        main_metric = evaluator.evaluate(predictions)

        metrics = {"test_metric": float(main_metric)}

        if self.problem_type in ("binary", "multiclass"):
            acc_eval = MulticlassClassificationEvaluator(
                labelCol="label", predictionCol="prediction", metricName="accuracy"
            )
            accuracy = acc_eval.evaluate(predictions)
            metrics["test_accuracy"] = float(accuracy)

        return metrics
