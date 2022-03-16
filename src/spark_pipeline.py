"""
Source file for launching all parts of spark pipeline.
Run `python src/spark_pipeline.py --help` to get usage info
"""

import argparse
import functools
import json
import re

import pyspark
import pyspark.pandas

from typing import Iterable

from argostranslate import package, translate
from pyspark.sql import functions as F, Row
from pyspark.sql.types import StringType, StructType, StructField

package.install_from_path('lib/translate-de_en-1_0.argosmodel')

COMMON_KEYS = ['ID', 'MakeText', 'ModelText', 'ModelTypeText', 'TypeName', 'TypeNameFull']

TARGET_DATA_FNAME = 'data/TargetData.xlsx'
KNOWN_COLORS_FNAME = 'data/KnownColors.json'
KNOWN_MAKES_FNAME = 'data/KnownMakes.json'

INPUT_DATA = 'data/supplier_car.json'
PREPROCESSED_FNAME = 'data/supplier_car_preprocessed.csv'
NORMALIZED_FNAME = 'data/supplier_car_normalized.csv'
EXTRACTED_FNAME = 'data/supplier_car_extracted.csv'
INTEGRATED_FNAME = 'data/supplier_car_integrated.csv'


def get_argos_model(source: str, target: str):
    """
        Get pretrained language transloatoor model
    https://argos-translate.readthedocs.io/en/latest/
    :param source: Source language
    :param target: Target language
    :return language translation model
    """
    lang = f'{source} -> {target}'
    source_lang = [
        model for model in translate.get_installed_languages()
        if lang in map(repr, model.translations_from)
    ]
    target_lang = [
        model for model in translate.get_installed_languages()
        if lang in map(repr, model.translations_to)
    ]
    return source_lang[0].get_translation(target_lang[0])


def preprocess_provider_data(
        rows: Iterable[Row],
        validate_attributes: list[str] = None
) -> Iterable[Row]:
    """
        Mapper UDF function, that transforms ungrouped data from provider.
    Pyspark does not support UDAF functions, but it is possible to run some kind of
    Reduce operation inside of actual Map operation. This requires sorting and data
    partitioning it by key.
    :param rows: Iterator over spark.rdd Rows
    :param validate_attributes: optional list if target columns to parse from `Attribute Names`
        May be helpful to validate if there are corrupted records
    :return: Iterator over Rows
    """
    aggregated_row = None
    for row in rows:
        if aggregated_row is None or row['ID'] != aggregated_row['ID']:
            if aggregated_row is not None:
                if validate_attributes is not None:
                    aggregated_row = {
                        attr: aggregated_row.get(attr)
                        for attr in sorted(COMMON_KEYS + validate_attributes)
                    }
                assert len(aggregated_row) == 25, len(aggregated_row)
                yield Row(**aggregated_row)
            aggregated_row = {key: row[key] for key in COMMON_KEYS}

        for key in COMMON_KEYS:
            if row[key] != aggregated_row[key]:
                print(f'Warn different {key} for ID {row["ID"]} found. Data may be corrupted')

        aggregated_row[row['Attribute Names']] = str(row['Attribute Values'])

    if aggregated_row is not None:
        if validate_attributes:
            aggregated_row = {
                attr: aggregated_row.get(attr)
                for attr in sorted(COMMON_KEYS + validate_attributes)
            }
        assert len(aggregated_row) == 25, len(aggregated_row)
        yield Row(**aggregated_row)


def prepare_mappings(target_data_fname: str, colors_fname: str, makes_fname: str) -> None:
    """
        Given TargetData.xlsx, get known colors and makes, that exist in database.
    This is required to propperly normalize data. Detailed explainatioin in README.md

    Note: pyspark.pandas api is avalilable only in newer versions of pyspark (3.x.x).
        and requere pyarrow

    :param target_data_fname: path to xlsx file with TargetData
    :param colors_fname: path to json file where colors will be saved
    :param makes_fname: path to json file with makes will be saved
    """
    target_data_stream = pyspark.pandas \
        .read_excel(target_data_fname, convert_float=None, squeeze=None).to_spark()

    colors = [row['color'] for row in target_data_stream.groupBy('color').agg({}).fillna('null').collect()]
    with open(colors_fname, 'w') as f:
        json.dump(colors, f, indent=4)

    makes = [row['make'] for row in target_data_stream.groupBy('make').agg({}).fillna('null').collect()]
    with open(makes_fname, 'w') as f:
        json.dump(makes, f, indent=4)


def preprocess_data(spark, input_file: str, output_file: str):
    """
        Preprocessing part of spark pipeline
    :param spark: spark session object
    :param input_file: path to json file with input supplier data
    :param output_file: path, where csv file with preprocessed data should be saved
    """
    input_stream = spark.read.json(input_file)
    attibute_names = input_stream \
        .agg(F.sort_array(F.collect_set('Attribute Names'))) \
        .head()[0]

    output_stream = input_stream \
        .repartition('ID') \
        .sortWithinPartitions('ID', 'Attribute Names') \
        .rdd \
        .mapPartitions(
            functools.partial(preprocess_provider_data, validate_attributes=attibute_names)
        ) \
        .toDF() \
        .fillna('null')

    output_stream.toPandas().to_csv(output_file, index=False)
    return output_stream


def normalize_data(
        spark, input_file: str, output_file: str,
        known_colors_file: str, known_makes_file: str,
):
    """
        Normalizing part of spark pipeline
    :param spark: spark session object
    :param input_file: path to csv file with preprocessed data
    :param output_file: path, where csv file with normalized data should be saved
    :param known_colors_file: path to json file with list of colors from TargetData
    :param known_makes_file: path to json file with list of makes from TargetData
    """

    with open(known_colors_file) as f:
        known_colors = json.load(f)

    with open(known_makes_file) as f:
        known_makes = json.load(f)

    known_colors = {color.lower(): color for color in known_colors}
    known_makes = {make.lower(): make for make in known_makes}

    @F.udf(returnType=StringType())
    def map_known_color(color):
        return known_colors.get(color, 'Other')

    @F.udf(returnType=StringType())
    def map_known_make(make):
        return known_makes.get(make, 'Other')

    translator_model = get_argos_model('German', 'English')

    @F.udf(returnType=StringType())
    def translate_word(word, model=None):
        if model is None:
            model = translator_model
        return model.translate(word)

    @F.udf(returnType=StringType())
    def remove_substring(string, substring):
        return string.replace(substring, '')

    input_stream = spark.read.option('header', True).csv(input_file)
    output_stream = input_stream \
        .select(
            map_known_make(F.lower(F.col('MakeText'))).alias('MakeText'),
            map_known_color(F.lower(translate_word(F.lower(
                remove_substring(F.col('BodyColorText'), F.lit(' mét.'))
            )))).alias('BodyColorText'),
            remove_substring(F.col('ModelTypeText'), F.col('ModelText')).alias('ModelTypeText'),
            *[
                col for col in input_stream.columns
                if col not in {'MakeText', 'BodyColorText', 'ModelTypeText'}
            ],
        )

    output_stream.toPandas().to_csv(output_file, index=False)
    return output_stream


def extract_data(spark, input_file: str, output_file: str):
    """
        Extraction part of spark pipeline
    :param spark: spark session object
    :param input_file: path to csv file with normalized data
    :param output_file: path, where csv file with extracted data should be saved
    """

    @F.udf(returnType=StructType([
        StructField('value', StringType()),
        StructField('unit', StringType())
    ]))
    def parse_consumption_text(consumption):
        consumption_regexp = r'^(\d+\.\d+) l/100(\w+)$'
        if re.match(consumption_regexp, consumption or '') is not None:
            s = re.search(consumption_regexp, consumption or '')
            return {'value': s.group(1), 'unit': s.group(2)}
        return {'value': 'null', 'unit': 'null'}

    input_stream = spark.read.option('header', True).csv(input_file)
    output_stream = input_stream \
        .select(
            '*', parse_consumption_text(F.col('ConsumptionTotalText')).alias('consumption'),
        ) \
        .select(
            '*',
            F.col('consumption').value.alias('extracted-value-ConsumptionTotalText'),
            F.col('consumption').unit.alias('extracted-unit-ConsumptionTotalText'),
        ) \
        .drop(F.col('consumption'))

    output_stream.toPandas().to_csv(output_file, index=False)
    return output_stream


def integrate_data(spark, input_file: str, output_file: str):
    """
        Integration part of spark pipeline.
    :param spark: spark session object
    :param input_file: path to csv file with extracted data
    :param output_file: path, where csv file with ready for integration data should be saved
    """
    input_stream = spark.read.option('header', True).csv(input_file)
    output_stream = input_stream \
        .select(
            F.lit('null').cast(StringType()).alias('carType'),
            F.coalesce(F.col('BodyColorText'), F.lit('Other')).cast(StringType()).alias('color'),
            F.lit('null').cast(StringType()).alias('condition'),
            F.lit('CHF').cast(StringType()).alias('currency'),
            F.lit('null').cast(StringType()).alias('drive'),
            F.col('City').cast(StringType()).alias('city'),
            F.lit('CH').cast(StringType()).alias('country'),
            F.col('MakeText').cast(StringType()).alias('make'),
            F.coalesce(F.col('FirstRegYear').cast(StringType()), F.lit('0')).alias('manufacture_year'),
            F.col('Km').cast(StringType()).alias('mileage'),
            F.lit('kilometer').cast(StringType()).alias('mileage_unit'),
            F.col('ModelText').cast(StringType()).alias('model'),
            F.col('ModelTypeText').cast(StringType()).alias('model_variant'),
            F.lit('true').cast(StringType()).alias('price_on_request'),
            F.lit('car').cast(StringType()).alias('type'),
            F.lit('null').cast(StringType()).alias('zip'),
            F.col('FirstRegMonth').cast(StringType()).alias('manufacture_month'),
            F.lit('null').cast(StringType()).alias('fuel_consumption_unit'),
        )

    output_stream.toPandas().to_csv(output_file, index=False)
    return output_stream


PIPELINE_STAGES_CONFIG = {
    'preprocess': {
        'process': preprocess_data,
        'input_file': INPUT_DATA,
        'output_file': PREPROCESSED_FNAME,
    },
    'normalize': {
        'process': normalize_data,
        'input_file': PREPROCESSED_FNAME,
        'output_file': NORMALIZED_FNAME,
        'extra_params': {
            'known_colors_file': KNOWN_COLORS_FNAME,
            'known_makes_file': KNOWN_MAKES_FNAME,
        }
    },
    'extract': {
        'process': extract_data,
        'input_file': NORMALIZED_FNAME,
        'output_file': EXTRACTED_FNAME,
    },
    'integrate': {
        'process': integrate_data,
        'input_file': EXTRACTED_FNAME,
        'output_file': INTEGRATED_FNAME,
    },
}


def main(run_all=None, prepare=None, stage=None):

    spark = pyspark.sql.SparkSession.builder.getOrCreate()

    if run_all:
        prepare_mappings(TARGET_DATA_FNAME, KNOWN_COLORS_FNAME, KNOWN_MAKES_FNAME)
        for stage in PIPELINE_STAGES_CONFIG:
            stage_cfg = PIPELINE_STAGES_CONFIG[stage]
            method = stage_cfg['process']
            method(spark, stage_cfg['input_file'], stage_cfg['output_file'], **stage_cfg.get('extra_params', {}))
    elif prepare:
        prepare_mappings(TARGET_DATA_FNAME, KNOWN_COLORS_FNAME, KNOWN_MAKES_FNAME)
    elif stage is not None:
        stage_cfg = PIPELINE_STAGES_CONFIG[stage]
        method = stage_cfg['process']
        _ = method(spark, stage_cfg['input_file'], stage_cfg['output_file'], **stage_cfg.get('extra_params', {}))
    else:
        raise ValueError("At least one option must be set. Please run `--help` for more info.")


def parse_args():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-a", "--run-all", action="store_true",
        help="Run whole pipeline"
    )
    group.add_argument(
        "-p", "--prepare-mappings", action="store_true",
        help="Create lists for known colors and makes"
    )
    group.add_argument(
        "-s", "--stage", choices=list(PIPELINE_STAGES_CONFIG.keys()),
        help="Run some particular stage"
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(args.run_all, args.prepare_mappings, args.stage)
