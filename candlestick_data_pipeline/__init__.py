import json
import glob
from pathlib import Path
import dask.dataframe as dd
from candlestick_data_pipeline import data_io
from candlestick_data_pipeline import transformations
from candlestick_data_pipeline import visualizations
from candlestick_data_pipeline import evaluations
from candlestick_data_pipeline import pipeline_logging
from candlestick_data_pipeline import registration
from typing import List

class PipelineManager:
    """
    A class to manage a data pipeline. A data pipeline is identified by a name, version, and home directory.
    Functionality includes:
    - Applying data transformations (see method process_new_dataset)
    - Running Evaluations on output dataset ( see method evaluate_staging_dataset)
    - Logging
    - Visualizations
    - Data storage
    """

    def __init__(self, home_directory: Path = None, pipeline_name: str = None, pipeline_version: str = None):
        self.home_directory = str(home_directory)
        self.name = str(pipeline_name)
        self.version = str(pipeline_version)
        self.version_path = Path(f"{self.home_directory}/candlestick_data_piplines/"
                                 f"{self.name}/version={self.version}/")
        self.load_config()

    def load_config(self):
        """
        Load pipeline config from storage layer
        """
        self.config_path = Path(self.version_path / f"{self.name}_v{self.version}_config.json")
        if self.config_path.exists():
            with open(self.config_path, 'r') as json_file:
                self.config = json.load(json_file)
                self.transformation_list = self.config['transformations']
                self.visualization_list = self.config['visualizations']
                self.evaluation_list = self.config['evaluations']
                self.output_schema = self.config['output_schema']
                self.input_schema = self.config['input_schema']
        else:
            raise Exception(
                f"Pipeline {self.name} Version {self.version} not found at the following location {self.home_directory}")

    def process_new_dataset_with_logging(self, source_file_path: Path = None, load_control_key: str = None):
        """
        Wrapper for process_new_dataset to handle logging of transformation run
        :param source_file_path: Path to input dataset
        :param load_control_key: Key to be used to identify dataset through ETL process
        :return: None
        """
        print(f'\n\nEvaluating Staging Dataset\nload_control_key={load_control_key}')
        log_name = f'{self.name}_v{self.version}_{load_control_key}_transformation'
        output_log_dir = Path(self.version_path / f"logs/transformation_logs/successful_run_logs/")
        error_log_dir = Path(self.version_path / f"logs/transformation_logs/failed_run_logs/")
        print(f'\n\nLogging to the following locations:\n{output_log_dir}\n{error_log_dir}')
        pipeline_logging.log_function(log_name=log_name, output_log_dir=output_log_dir, error_log_dir=error_log_dir)(
            self.process_new_dataset)(source_file_path=source_file_path, load_control_key=load_control_key)

    def process_new_dataset(self, source_file_path: Path = None, load_control_key: str = None):
        """
        Load new input dataset, apply transformations according to pipeline config, and write data to staging
        :param source_file_path: Path to input dataset
        :param load_control_key: Key to be used to identify dataset through ETL process
        :return: None
        """
        self.data = data_io.read_data_by_file_extension(source_file_path)
        self.save_input_data(load_control_key)
        self.enforce_input_schema()
        self.transform_data()
        self.enforce_output_schema()
        self.save_staging_data(load_control_key)

    def evaluate_staging_dataset_with_logging(self, load_control_key: str = None):
        """
        Wrapper for evaluate_staging_dataset to handle logging of evaluation run
        :param load_control_key: Key to be used to identify dataset through ETL process
        :return: None
        """
        print(f'\n\nEvaluating Staging Dataset\nload_control_key={load_control_key}')
        log_name = f'{self.name}_v{self.version}_{load_control_key}_staging_evaluation'
        output_log_dir = Path(self.version_path / f"logs/evaluation_logs/successful_run_logs/")
        error_log_dir = Path(self.version_path / f"logs/evaluation_logs/failed_run_logs/")
        print(f'\n\nLogging to the following locations:\n{output_log_dir}\n{error_log_dir}')
        pipeline_logging.log_function(log_name=log_name, output_log_dir=output_log_dir, error_log_dir=error_log_dir)(
            self.evaluate_staging_dataset)(load_control_key=load_control_key)

    def evaluate_staging_dataset(self, load_control_key: str = None):
        """
        Load staging dataset, run evaluations on dataset according to pipeline config, and promote/demote dataset based
        on result
        :param load_control_key: Key to be used to identify dataset through ETL process
        :return: None
        """
        print(f'\n\nEvaluating Staging Dataset\nload_control_key={load_control_key}')
        self.data = self.load_dataset_by_key(load_control_key=load_control_key, dataset_type='staging')
        self.enforce_output_schema()
        promote_dataset_bool, failed_evals = self.evaluate_data()
        if promote_dataset_bool:
            print('\n\nDataset passed all evaluations!\nPromoting output file...')
            self.promote_dataset(load_control_key=load_control_key)
        else:
            print('\n\nDataset failed an evaluation\nDemoting output file...')
            self.demote_dataset(load_control_key=load_control_key)
            raise Exception(f'Dataset failed the following evaluations:\n{failed_evals}')
        return promote_dataset_bool

    def promote_dataset(self, load_control_key: str = None):
        """
        Move staging dataset to output/production directory
        :param load_control_key: Key to be used to identify dataset through ETL process
        :return: None
        """
        staging_data_path = Path(
            self.version_path / f"datasets/staging_datasets/{self.name}_v{self.version}_staging_data_{load_control_key}.csv")
        output_data_path = Path(
            self.version_path / f"datasets/output_datasets/{self.name}_v{self.version}_output_data_{load_control_key}.csv")
        print(f'\nMoving Dataset to...\n{output_data_path}')
        staging_data_path.rename(output_data_path)

    def demote_dataset(self, load_control_key: str = None):
        """
        Move staging dataset to failed directory
        :param load_control_key: Key to be used to identify dataset through ETL process
        :return: None
        """
        staging_data_path = Path(
            self.version_path / f"datasets/staging_datasets/{self.name}_v{self.version}_staging_data_{load_control_key}.csv")
        failed_data_path = Path(
            self.version_path / f"datasets/failed_datasets/{self.name}_v{self.version}_failed_data_{load_control_key}.csv")
        print(f'\nMoving Dataset to...\n{failed_data_path}')
        staging_data_path.rename(failed_data_path)

    def evaluate_data(self):
        """
        Run all evaluations on the dataset specified in the pipeline config
        :return:
        """
        promote_dataset = True
        failed_evals = []
        for evaluation in self.evaluation_list:
            evaluation_name = evaluation[0]
            evaluation_arguments = evaluation[1]
            print(f'\nEvaluation: {evaluation_name}\nArguments: {evaluation_arguments}')
            evaluation_function = getattr(evaluations, evaluation_name)
            if evaluation_arguments is not None:
                result = evaluation_function(data=self.data, **evaluation_arguments)
            else:
                result = evaluation_function(data=self.data)
            if result:
                print('Failed')
                promote_dataset = False
                failed_evals.append(evaluation_name)
            else:
                print('Passed')
        return promote_dataset, failed_evals

    def load_dataset_by_key(self, load_control_key: str = None, dataset_type: str = None) -> dd:
        """
        Load dataset in to dask dataframe
        :param load_control_key: Key to be used to identify dataset through ETL process
        :param dataset_type: dataset location input/staging/output/failed
        :return:
        """
        data_path = Path(
            self.version_path / f"datasets/{dataset_type}_datasets/{self.name}_v{self.version}_{dataset_type}_data_{load_control_key}.csv")
        print(f'\nReading Data...\n{data_path}')
        return data_io.read_data_by_file_extension(data_path)

    def list_staging_load_control_keys(self) -> List[str]:
        """
        list all load control keys found in the staging directory
        :return:
        """
        staging_data_path = Path(self.version_path / f"datasets/staging_datasets/*")
        load_control_keys = []
        for file_name in glob.glob(str(staging_data_path)):
            key = file_name.split('_')[-1].split('.')[0]
            load_control_keys.append(key)
        return load_control_keys

    def save_input_data(self, load_control_key: str):
        """
        Save self.data to input data directory
        :param load_control_key: Key to be used to identify dataset through ETL process
        :return:
        """
        input_data_path = Path(
            self.version_path / f"datasets/input_datasets/{self.name}_v{self.version}_input_data_{load_control_key}.csv")
        data_io.write_data_by_file_extension(data=self.data, file_path=input_data_path)

    def transform_data(self):
        """
        Run all transformations on the dataset specified in the pipeline config. Transformations found in
        transformations.py
        :return:
        """
        for transformation in self.transformation_list:
            transformation_name = transformation[0]
            transformation_arguments = transformation[1]
            print(f'\nTransformation: {transformation_name}')
            print(f'Arguments: {transformation_arguments}')
            transformation_function = getattr(transformations, transformation_name)
            if transformation_arguments is not None:
                self.data = transformation_function(data=self.data, **transformation_arguments)
            else:
                self.data = transformation_function(data=self.data)
            print('COMPLETE')

    def save_staging_data(self, load_control_key: str):
        """
        Save self.data to staging data directory
        :param load_control_key: Key to be used to identify dataset through ETL process
        :return:
        """
        staging_data_path = Path(
            self.version_path / f"datasets/staging_datasets/{self.name}_v{self.version}_staging_data_{load_control_key}.csv")
        data_io.write_data_by_file_extension(data=self.data, file_path=staging_data_path)

    def visualize_staging_dataset(self, load_control_key: str, dataset_type: str):
        """
        Load dataset, run all visualizations on dataset according to pipeline config
        :param load_control_key: Key to be used to identify dataset through ETL process
        :return: None
        """
        print(f'\n\nCreating Visualizations for {dataset_type} dataset....\nload_control_key={load_control_key}')
        staging_data_path = Path(
            self.version_path / f"datasets/staging_datasets/{self.name}_v{self.version}_{dataset_type}_data_{load_control_key}.csv")
        vis_data = data_io.read_data_by_file_extension(staging_data_path).compute()
        for visualization in self.visualization_list:
            visualization_name = visualization[0]
            visualization_arguments = visualization[1]
            print(f'\nVisualization: {visualization_name}')
            print(f'Arguments: {visualization_arguments}')
            visualization_function = getattr(visualizations, visualization_name)
            if visualization_arguments is not None:
                self.data = visualization_function(data=vis_data, **visualization_arguments)
            else:
                self.data = visualization_function(data=vis_data)
            print('COMPLETE')

    def enforce_output_schema(self):
        """
        set the data types of all columns in self.data according to output schema specified in pipeling config
        :return:
        """
        for column, data_type in self.output_schema.items():
            self.data[column] = self.data[column].astype(data_type)

    def enforce_input_schema(self):
        """
        set the data types of all columns in self.data according to input schema specified in pipeling config
        :return:
        """
        for column, data_type in self.input_schema.items():
            self.data[column] = self.data[column].astype(data_type)
