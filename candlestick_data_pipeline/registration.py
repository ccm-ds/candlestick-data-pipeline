from pathlib import Path
import shutil
import json


def register_pipeline(registration_dict=None, overwrite=False):
    clean_registration_dict(registration_dict)
    if overwrite:
        delete_pipeline(registration_dict)
    setup_storage_directory(registration_dict)
    save_pipeline_config(registration_dict)

def clean_registration_dict(registration_dict):
    registration_dict['version'] = str(registration_dict['version'])
    registration_dict['home_directory'] = str(registration_dict['home_directory'])
    return registration_dict

def delete_pipeline(registration_dict):
    pipeline_path = Path(f"{registration_dict['home_directory']}/candlestick_data_piplines/"
                         f"{registration_dict['name']}/version={registration_dict['version']}")
    if pipeline_path.exists():
        shutil.rmtree(pipeline_path)


def setup_storage_directory(registration_dict):
    pipeline_path = Path(f"{registration_dict['home_directory']}/candlestick_data_piplines/"
                         f"{registration_dict['name']}/version={registration_dict['version']}")
    if pipeline_path.exists():
        raise Exception(
            f"Pipeline {registration_dict['name']} Version {registration_dict['version']} already exist... Please delete or overwrite")
    else:
        inputs_path = Path(pipeline_path / 'datasets/input_datasets')
        staging_path = Path(pipeline_path / 'datasets/staging_datasets')
        output_path = Path(pipeline_path / 'datasets/output_datasets')
        failed_path = Path(pipeline_path / 'datasets/failed_datasets')
        logging_path1 = Path(pipeline_path / 'logs/transformation_logs')
        logging_path2 = Path(pipeline_path / 'logs/evaluation_logs')
        inputs_path.mkdir(parents=True)
        staging_path.mkdir(parents=True)
        output_path.mkdir(parents=True)
        failed_path.mkdir(parents=True)
        logging_path1.mkdir(parents=True)
        logging_path2.mkdir(parents=True)


def save_pipeline_config(registration_dict):
    config_path = Path(f"{registration_dict['home_directory']}/candlestick_data_piplines/"
                         f"{registration_dict['name']}/version={registration_dict['version']}/"
                       f"{registration_dict['name']}_v{registration_dict['version']}_config.json")
    with open(config_path, 'w') as fp:
        json.dump(registration_dict, fp, indent=4)
