# -*- coding: utf-8 -*-
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Union
import zipfile
import click
from dotenv import find_dotenv, load_dotenv
import polars as pl


def divide_rules_into_parts(rules_text: str, parts: int) -> List[str]:
    # Determine the length of each substring
    part_length = len(rules_text) // parts

    # Divide the string into 'parts' number of substrings
    substrings = [
        rules_text[i : i + part_length] for i in range(0, len(rules_text), part_length)
    ]

    # If there are any leftover characters, add them to the last substring
    if len(substrings) > parts:
        substrings[-2] += substrings[-1]
        substrings.pop()

    # convert the items of substring list to a single string
    for i, text in enumerate(substrings):
        substrings[i] = str(text)

    # remove the \\n characters from the string
    for i, text in enumerate(substrings):
        substrings[i] = text.replace("\\n", "")

    for i, text in enumerate(substrings):
        substrings[i] = text.replace(", ''", "")

    return substrings


def process_cards_file(cards: pl.DataFrame, sets: List[str] = ["BRO"]) -> pl.DataFrame:
    # Filter out cards where sert code is not "BRO"
    cards = cards.filter(pl.col("setCode").is_in(sets))

    cards = cards.unique()

    return cards


@click.command()
@click.argument("input_filepath", type=click.Path(exists=True))
@click.argument("output_filepath", type=click.Path())
def main(input_filepath, output_filepath):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info("making final data set from raw data")

    with open("./data/raw/magic-the-gathering-comprehensive-rules.txt", "r") as f:
        rules_text = f.readlines()

    rules_text_divided = divide_rules_into_parts(rules_text=rules_text, parts=5)

    # for each item in rules_text_divided, write it to a file
    for i, text in enumerate(rules_text_divided):
        with open(
            "./data/processed/magic-the-gathering-comprehensive-rules-part-{}.txt".format(
                i
            ),
            "w",
        ) as f:
            f.write(text)

    cards = process_cards_file(
        cards=pl.read_csv(
            "data/raw/AllPrintingsCSVFiles/cards.csv",
            columns=[
                "name",
                "text",
                "setCode",
                "manaCost",
                "colorIdentity",
                "subtypes",
                "supertypes",
                "types",
                "power",
                "toughness",
            ],
            dtypes={"power": pl.Utf8, "toughness": pl.Utf8},
        )
    )

    # write cards to processed
    cards.write_csv("data/processed/cards.csv")


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
