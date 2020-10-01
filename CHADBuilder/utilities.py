from IPython import get_ipython
from tqdm import tqdm
from tqdm.notebook import tqdm as tqdm_notebook
import dateparser


def which_environment() -> str:
    """
    Test if module is being executed in the Jupyter environment.
    Returns
    -------
    str
        'jupyter', 'ipython' or 'terminal'
    """
    try:
        ipy_str = str(type(get_ipython()))
        if 'zmqshell' in ipy_str:
            return 'jupyter'
        if 'terminal' in ipy_str:
            return 'ipython'
    except:
        return 'terminal'


def progress_bar(x: iter or None = None,
                 verbose: bool = True,
                 **kwargs) -> callable:
    """
    Generate a progress bar using the tqdm library. If execution environment is Jupyter, return tqdm_notebook
    otherwise used tqdm.
    Parameters
    -----------
    x: iterable
        some iterable to pass to tqdm function
    verbose: bool, (default=True)
        Provide feedback (if False, no progress bar produced)
    kwargs:
        additional keyword arguments for tqdm
    :return: tqdm or tqdm_notebook, depending on environment
    """
    if not verbose:
        return x
    if which_environment() == 'jupyter':
        if x is None:
            return tqdm_notebook(**kwargs)
        return tqdm_notebook(x, **kwargs)
    if x is None:
        return tqdm_notebook(**kwargs)
    return tqdm(x, **kwargs)


def parse_datetime(datetime: str or None) -> dict or None:
    """
    Takes a datetime as string and returns a ISO 8601 standard datetime string. Implements the dateparser
    library for flexible date time parsing (https://dateparser.readthedocs.io/). Assumes GB formatting for
    dates i.e. Day/Month/Year (can handle multiple dividers for date e.g. ".", "/", "-" etc)

    Parameters
    ----------
    datetime: str
        datetime string to parse, can be date, or date and time.
    Returns
    -------
    dict or None
         ISO 8601 formatted string: YYYY-MM-DD hh:mm:ss e.g. 2020-09-01T13:35:37Z
    """
    if type(datetime) is not str:
        return None
    datetime = datetime.strip()
    datetime = dateparser.parse(datetime, locales=["en-GB"])
    if datetime is None:
        return None
    return datetime.strftime("%Y-%m-%dT%H:%M:%SZ")


def verbose_print(verbose: bool):
    """
    Verbose printing

    Parameters
    ----------
    verbose

    Returns
    -------

    """
    return print if verbose else lambda *a, **k: None
