from tqdm import tqdm
import pandas as pd
import chardet
import os
import csv


def _remove_illegal_chars(x) -> str:
    """
    Remove illegal quote characters

    Parameters
    ----------
    x: str

    Returns
    -------
    str
    """
    return x.replace("\\\\", "").replace('"', "")


def clean_complex_text(path: str):
    """
    Clean files containing complex text. Warning: file is overwritten!

    Parameters
    ----------
    path: str
        File path
    Returns
    -------
    None
    """
    print("Attempting to clean complex text....")
    for filename in tqdm(os.listdir(path)):
        try:
            original_file = open(os.path.join(path, filename), "r")
            contents = list(csv.reader(original_file))
            if "TEXT" not in contents[0]:
                continue
            original_file.close()
            new_file = csv.writer(open(os.path.join(path, filename), "w"),
                                  delimiter=',')
            new_contents = [list(map(_remove_illegal_chars, x)) for x in contents]
            new_file.writerows(new_contents)
        except Exception as e:
            print(f"Failed at {filename}: {str(e)}")


def _read_dataframe(path: str, **kwargs):
    try:
        return pd.read_csv(path, **kwargs)
    except UnicodeError as e:
        raise UnicodeError(f"Error parsing {path}: {str(e)}")
    except pd.errors.ParserError as e:
        raise ValueError(f"Error parsing {path}: {str(e)}")


def safe_read(path: str):
    """
    Attempt to read csv file as a Pandas DataFrame. Catches warnings for improved error handling.

    Parameters
    ----------
    path: str
        File path
    Returns
    -------
    Pandas.DataFrame
    """
    encoding = chardet.detect(open(path, "rb").read()).get("encoding")
    try:
        return _read_dataframe(path, encoding=encoding, low_memory=False)
    except pd.errors.ParserError:
        try:
            return _read_dataframe(path,
                                   engine="python",
                                   escapechar="\\",
                                   quotechar='"',
                                   sep=",",
                                   encoding=encoding)
        except pd.errors.ParserError as e:
            raise ValueError(f"Error parsing {path}: {str(e)}")


def _unique_categories(path: str):
    """
    List all the unique categories of files in a directory containing C&V extracts

    Parameters
    ----------
    path: str
        Data directory
    Returns
    -------
    list
    """
    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    return list(set([x.split("-")[0] for x in files]))


def consolidate(read_path: str,
                write_path: str):
    """
    Given a directory containing C&V extracts, generate consolidated csv files stored in 'write_path'.
    Files consolidated by file category.

    Parameters
    ----------
    read_path: str
        Directory containing original csv files
    write_path: str
        Directory to write new consolidated csv files too
    Returns
    -------
    None
    """
    write_path = os.path.join(write_path, "consolidated")
    if not os.path.isdir(write_path):
        os.mkdir(write_path)
    categories = _unique_categories(read_path)
    files = [f for f in os.listdir(read_path) if os.path.isfile(os.path.join(read_path, f))]
    categories = {k: [f for f in files if f.split("-")[0] == k] for k in categories}
    for k, files in tqdm(categories.items()):
        dataframes = [safe_read(os.path.join(read_path, f)) for f in files]
        dataframes = pd.concat(dataframes)
        dataframes.to_csv(os.path.join(write_path, f"{k}.csv"), index=False)
