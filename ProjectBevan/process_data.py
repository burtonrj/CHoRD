from tqdm import tqdm
import pandas as pd
import os
import csv


def _remove_illegal_chars(x):
    return x.replace("\\\\", "").replace('"', "")


def clean_complex_text(path: str):
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


def _safe_read(path: str):
    try:
        return pd.read_csv(path,
                           engine="python",
                           escapechar="\\",
                           quotechar='"',
                           sep=",")
    except UnicodeError as e:
        raise UnicodeError(f"Error parsing {path}: {str(e)}")
    except pd.errors.ParserError as e:
        raise ValueError(f"Error parsing {path}: {str(e)}")


def _unique_categories(path: str):
    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    return list(set([x.split("-")[0] for x in files]))


def consolidate(read_path: str,
                write_path: str):
    write_path = os.path.join(write_path, "consolidated")
    if not os.path.isdir(write_path):
        os.mkdir(write_path)
    categories = _unique_categories(read_path)
    files = [f for f in os.listdir(read_path) if os.path.isfile(os.path.join(read_path, f))]
    categories = {k: [f for f in files if f.split("-")[0] == k] for k in categories}
    for k, files in tqdm(categories.items()):
        dataframes = [_safe_read(os.path.join(read_path, f)) for f in files]
        dataframes = pd.concat(dataframes)
        dataframes.to_csv(os.path.join(write_path, f"{k}.csv"), index=False)
