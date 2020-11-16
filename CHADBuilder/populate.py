from CHADBuilder.utilities import parse_datetime, verbose_print, progress_bar
from CHADBuilder.schema import create_database
from CHADBuilder.process_data import safe_read
from multiprocessing import Pool, cpu_count
from functools import partial
from tqdm import tqdm
from typing import List
import sqlite3 as sql
import pandas as pd
import re
import os


def chunker(seq: pd.DataFrame,
            size: int):
    """
    Creates chunks of Pandas DataFrame of given size
    Credit to http://stackoverflow.com/a/434328

    Parameters
    ----------
    seq: Pandas.DataFrame
    size: int

    Returns
    -------
    tuple
        Sequence of Pandas.DataFrame of given size
    """
    # from http://stackoverflow.com/a/434328
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def _re_search_df(pattern: str,
                  x: str,
                  group_idx: int = 0) -> str or None:
    """
    Given a some regex pattern with 1 or more capturing groups and the target string,
    return the group at the given index (if no match, returns None).

    Parameters
    ----------
    pattern: str
        A valid regular exp pattern with 1 or more capturing groups
    x: str
        String to parse
    group_idx: int
        Group index to extract

    Returns
    -------
    str or None
    """
    match = re.search(pattern=pattern,
                      string=x)
    if match is None:
        return None
    if len(match.groups()) == 1:
        return match.groups()[0]
    return match.groups()[group_idx]


def _rename(df: pd.DataFrame,
            additional_mappings: dict or None = None) -> pd.DataFrame:
    """
    Rename columns given a DataFrame from the C&V extracts. By default "PATIENT_ID" is renamed to "patient_id"
    and "REQUEST_LOCATION" is renamed to "request_location". Additional mappings can be given to rename additional
    columns.

    Parameters
    ----------
    df: Pandas.DataFrame
    additional_mappings: dict

    Returns
    -------
    Pandas.DataFrame
    """
    df = df.rename({"PATIENT_ID": "patient_id",
                    "REQUEST_LOCATION": "request_location"},
                   axis=1)
    if additional_mappings is not None:
        df = df.rename(additional_mappings, axis=1)
    return df


def search_covid_results(patient_id: str,
                         covid_df: pd.DataFrame):
    """
    Given a patient ID and a dataframe of COVID-19 PCR results, return whether a patient had
    a positive result at any point and the date of their first positive. If no positives but
    negative results exist, return "N" for negative, otherwise "U" for unknown.

    Parameters
    ----------
    patient_id: str
        Patient ID
    covid_df: Pandas.DataFrame
        COVID-19 PCR results

    Returns
    -------
    str, str or None
    """
    pt_status = covid_df[covid_df.PATIENT_ID == patient_id].sort_values("collection_datetime", ascending=True).copy()
    positives = pt_status[pt_status.TEXT == "Positive"].copy()
    for x in ["collection_datetime", "test_datetime"]:
        positives[x] = positives[x].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if pt_status.shape[0] == 0:
        return "U", None
    if positives.shape[0] != 0:
        first_positive = positives.iloc[0]
        if pd.isnull(first_positive.collection_datetime):
            if pd.isnull(first_positive.test_datetime):
                return "P", None
            return "P", first_positive.test_datetime
        return "P", first_positive.collection_datetime
    negatives = pt_status[pt_status.TEXT == "Negative"]
    if negatives.shape[0] != 0:
        return "N", None
    return "U", None


class Populate:
    """
    Create the CHADBuilder database and populate using C&V data extracts.

    Parameters
    -----------
    database_path: str
        Location of the CHADBuilder database. NOTE: the database is READ ONLY and so a new database
        will always be generated at the path given.
    data_path: str
        Location of the consolidated C&V data extracts (see CHADBuilder.process_data)
    verbose: bool, (default=True)
        If True, print regular feedback
    died_events: list or None,
        List of events that correspond to a patient death
        Default: ['Died - DEATHS INCLUDING STILLBIRTHS',
                'Died In Dept.',
                'Died - USUAL PLACE OF RESIDENCE',
                'Died - NHS HOSP OTHER PROV - GENERAL']
    path_files: list or None,
        List of files expected when generating the Pathology table.
        Default:["LFT",
               "ABG",
               "ACE",
               "AntiXa",
               "BgaPOCT",
               "CoagScr",
               "Covid19Ab",
               "CRP",
               "Ddimer",
               "EPS",
               "FBC",
               "Ferritin",
               "GlucoseRand",
               "HbA1c",
               "HsTrop",
               "ImmGlob",
               "LDH",
               "LFT",
               "Lip",
               "LipF",
               "ParaProt",
               "ProCalc",
               "TCC",
               "TFSat",
               "UandE",
               "VitD"]
    micro_files: list or None
        List of files expected when generating the Microbiology table.
        Default = ["AsperELISA",
                   "AsperPCR",
                   "BCult",
                   "RESPL",
                   "Covid19"]
    comorbid_files: list or None
        List of files expected when generating the Cormobid table.
        Default = ["CoMorbid"]
    patient_files: list or None
        List of files expected when generating the Patient table.
        Default = ["People",
                   "Outcomes",
                   "Covid19"]
    haem_files: list or None
        List of files expected when generating the ComplexHaematology table.
        Default = ["CompAlt",
                    "CompClass"]
    critcare_files: list or None
        List of files expected when generating the CritCare table.
        Default = ["CritCare"]
    radiology_files: list or None
        List of files expected when generating the Radiology table.
        Default = ["XRChest",
                   "CTangio"]
    events_files: list or None
        List of files expected when generating the Events table.
        Default =  ["Outcomes"]
    """
    def __init__(self,
                 database_path: str,
                 data_path: str,
                 verbose: bool = True,
                 died_events: List[str] or None = None,
                 path_files: List[str] or None = None,
                 micro_files: List[str] or None = None,
                 comorbid_files: List[str] or None = None,
                 patient_files: List[str] or None = None,
                 haem_files: List[str] or None = None,
                 critcare_files: List[str] or None = None,
                 radiology_files: List[str] or None = None,
                 events_files: List[str] or None = None,
                 units_files: List[str] or None = None):
        self.verbose = verbose
        self.vprint = verbose_print(verbose)
        create_database(database_path, overwrite=True)
        self._connection = sql.connect(database_path)
        self._curr = self._connection.cursor()
        self.data_path = data_path
        assert os.path.isdir(self.data_path), f"{data_path} is not a valid directory"
        self.path_files = path_files
        self.micro_files = micro_files
        self.comorbid_files = comorbid_files
        self.patient_files = patient_files
        self.haem_files = haem_files
        self.critcare_files = critcare_files
        self.radiology_files = radiology_files
        self.events_files = events_files
        self.units_files = units_files
        if self.path_files is None:
            self.path_files = ["LFT",
                               "ABG",
                               "ACE",
                               "AntiXa",
                               "BgaPOCT",
                               "CoagScr",
                               "Covid19Ab",
                               "CRP",
                               "Ddimer",
                               "EPS",
                               "FBC",
                               "Ferritin",
                               "GlucoseRand",
                               "HbA1c",
                               "HsTrop",
                               "ImmGlob",
                               "LDH",
                               "LFT",
                               "Lip",
                               "LipF",
                               "ParaProt",
                               "ProCalc",
                               "TCC",
                               "TFSat",
                               "UandE",
                               "VitD"]
        if self.micro_files is None:
            self.micro_files = ["AsperELISA",
                                "AsperPCR",
                                "BCult",
                                "RESPL"]
        if self.comorbid_files is None:
            self.comorbid_files = ["CoMorbid"]
        if self.haem_files is None:
            self.haem_files = ["CompAlt",
                               "CompClass"]
        if self.patient_files is None:
            self.patient_files = ["People",
                                  "Outcomes",
                                  "Covid19"]
        if self.critcare_files is None:
            self.critcare_files = ["CritCare"]
        if self.radiology_files is None:
            self.radiology_files = ["XRChest",
                                    "CTangio"]
        if self.events_files is None:
            self.events_files = ["Outcomes"]
        self.died_events = died_events
        if self.died_events is None:
            self.died_events = ['Died - DEATHS INCLUDING STILLBIRTHS',
                                'Died In Dept.',
                                'Died - USUAL PLACE OF RESIDENCE',
                                'Died - NHS HOSP OTHER PROV - GENERAL']
        if self.units_files is None:
            self.units_files = ["TestUnits"]
        self._all_files_present()

    def _all_files_present(self) -> None:
        """
        Assert that all the expected files are present within data_path, if not, AssertionError raised.

        Returns
        -------
        None
        """
        files = [os.path.splitext(f)[0] for f in os.listdir(self.data_path)
                 if os.path.isfile(os.path.join(self.data_path, f))]
        for group in [self.events_files, self.patient_files, self.haem_files,
                      self.radiology_files, self.critcare_files, self.comorbid_files,
                      self.micro_files, self.path_files]:
            for expected_file in group:
                assert expected_file in files, f"{expected_file} missing from data path!"

    def _get_path(self,
                  file_basename: str):
        """
        Produce the file path for a given target file (file_basename expected to be without file extension
        e.g. "Outcomes" not "Outcomes.csv"

        Parameters
        ----------
        file_basename: str

        Returns
        -------
        str
        """
        return os.path.join(self.data_path, f"{file_basename}.csv")

    def _get_date_time(self,
                       df: pd.DataFrame,
                       col_name: str) -> pd.DataFrame:
        """
        Given a DataFrame and a target column (col_name) containing a string with date and/or time content, using
        multiprocessing and the parse_datetime function, generate a new column for dates and a new column for times.
        Original target column will be dropped and modified DataFrame returned.

        Parameters
        ----------
        df: Pandas.DataFrame
        col_name: str
            Target column
        new_date_name: str
            New column name for dates
        new_time_name: str
            New column name for times

        Returns
        -------
        Pandas.DataFrame
        """
        with Pool(cpu_count()) as pool:
            if self.verbose:
                df[col_name] = tqdm(pool.imap(parse_datetime, df[col_name].values), total=len(df[col_name].values))
            else:
                df[col_name] = pool.map(parse_datetime, df[col_name].values)
        return df

    def _insert(self,
                df: pd.DataFrame,
                table_name: str):
        """
        Given a DataFrame and some target table in the CHADBuilder database, append the contents of that
        DataFrame into the target table, whilst also providing a progress bar is verbose set to True.

        Parameters
        ----------
        df: Pandas.DataFrame
        table_name: str

        Returns
        -------
        None
        """
        if not self.verbose:
            df.to_sql(name=table_name, con=self._connection, if_exists="append", index=False)
            return
        chunk_size = int(df.shape[0]/10)
        with tqdm(total=df.shape[0]) as pbar:
            for chunk in chunker(df, chunk_size):
                chunk.to_sql(name=table_name, con=self._connection, if_exists="append", index=False)
                pbar.update(chunk_size)

    def _pathology(self):
        """
        Generate the Pathology table
        Returns
        -------
        None
        """
        self.vprint("---- Populating Pathology Table ----")
        for file in self.path_files:
            self.vprint(f"Processing {file}....")
            df = safe_read(self._get_path(file))
            df.drop(["AGE", "GENDER", "ADMISSION_DATE"], axis=1, inplace=True)
            df = self._get_date_time(df, col_name="TEST_DATE")
            df = self._get_date_time(df, col_name="TAKEN_DATE")
            df = df.melt(id_vars=["PATIENT_ID", "REQUEST_LOCATION", "TEST_DATE", "TAKEN_DATE"],
                         var_name="test_name",
                         value_name="test_result")
            df["valid"] = df.test_result.apply(lambda x: int(x != "Issue with result"))
            df["test_category"] = file
            df = _rename(df, {"TEST_DATE": "test_datetime",
                              "TAKEN_DATE": "collection_datetime"})
            self._insert(df=df, table_name="Pathology")

    def _process_micro_df(self,
                          df: pd.DataFrame,
                          sample_type_pattern: str,
                          result_pattern: str,
                          test_name: str):
        """
        Template method for generalised processing of a Microbiology related DataFrame and subsequent appendage
        to the Microbiology table.

        Parameters
        ----------
        df: Pandas.DataFrame
        sample_type_pattern: str
            Search pattern used for identifying sample type in TEXT column
        result_pattern: str
            Search pattern used for identifying result in TEXT column
        test_name: str
            Test name corresponding to the DataFrame
        Returns
        -------
        None
        """
        df.drop(["AGE", "GENDER", "ADMISSION_DATE"], axis=1, inplace=True)
        df = self._get_date_time(df, col_name="TEST_DATE")
        df = self._get_date_time(df, col_name="TAKEN_DATE")
        # pull out the sample type
        df["sample_type"] = df.TEXT.apply(lambda x: _re_search_df(pattern=sample_type_pattern, x=x, group_idx=0))
        # Pull out result
        df["test_result"] = df.TEXT.apply(lambda x: _re_search_df(pattern=result_pattern, x=x, group_idx=1))
        df["test_name"] = test_name
        df["valid"] = df.TEXT.apply(lambda x: int(x != "Issue with result"))
        df = _rename(df, {"TEXT": "raw_text", "TEST_DATE": "test_datetime", "TAKEN_DATE": "collection_datetime"})
        self._insert(df=df, table_name="Microbiology")

    def _microbiology(self):
        """
        Populate the Microbiology table

        Returns
        -------
        None
        """
        self.vprint("---- Populating Microbiology Table ----")

        # AsperELISA ----------------------------
        self.vprint("...processing Aspergillus ELISA results")
        df = safe_read(self._get_path("AsperELISA"))
        sample_type_pattern = 'Specimen received: ([\w\d\s\(\)\[\]]+) Aspergillus ELISA'
        result_pattern = "Aspergillus Antigen \(Galactomannan\) ([\w\d\s\(\)\[\]]+)"
        self._process_micro_df(df=df,
                               sample_type_pattern=sample_type_pattern,
                               result_pattern=result_pattern,
                               test_name="AsperELISA")
        # AsperPCR ----------------------------
        self.vprint("...processing Aspergillus PCR results")
        df = safe_read(self._get_path("AsperPCR"))
        sample_type_pattern = 'Specimen received: ([\w\d\s\(\)\[\]]+) Aspergillus PCR'
        result_pattern = "PCR\s(DNA\s[Not]*\sDetected)"
        self._process_micro_df(df=df,
                               sample_type_pattern=sample_type_pattern,
                               result_pattern=result_pattern,
                               test_name="AsperELISA")
        # BCult ------------------------------
        self.vprint("...processing Blood Culture results")
        df = safe_read(self._get_path("BCult"))
        sample_type_pattern = 'Specimen received:([\w\s\d\(\)\[\]\-]*)(Culture|Microscopy)'
        result_pattern = "(Culture|Microscopy-)([\w\s\d]*)"
        self._process_micro_df(df=df,
                               sample_type_pattern=sample_type_pattern,
                               result_pattern=result_pattern,
                               test_name="BloodCulture")
        # BGluc ------------------------------
        self.vprint("...processing Beta-Glucan results")
        df = safe_read(self._get_path("BGluc"))
        sample_type_pattern = 'Specimen received:([\w\s\d\(\)\[\]\-]*) Mycology reference unit'
        result_pattern = "Mycology reference unit Cardiff Beta Glucan Antigen Test :([\w\s\d<>/\.\-]*)"
        self._process_micro_df(df=df,
                               sample_type_pattern=sample_type_pattern,
                               result_pattern=result_pattern,
                               test_name="BetaGlucan")
        # RESPL ------------------------------
        self.vprint("...processing Respiratory Virus results")
        df = safe_read(self._get_path("RESPL"))
        sample_type_pattern = 'Specimen received:([\w\s\d<>/\.\-]*) (Microbiological ' \
                              'investigation of respiratory viruses|RESPL)'
        result_pattern = "(Microbiological investigation of respiratory viruses|RESPL)([\w\s\d<>/\.\-\(\):]*)"
        self._process_micro_df(df=df,
                               sample_type_pattern=sample_type_pattern,
                               result_pattern=result_pattern,
                               test_name="RESPL")
        # Covid19 ----------------------------
        self.vprint("...processing Respiratory Virus results")
        df = safe_read(self._get_path("Covid19"))
        df.drop(["AGE", "GENDER", "ADMISSION_DATE"], axis=1, inplace=True)
        df = self._get_date_time(df, col_name="TEST_DATE")
        df = self._get_date_time(df, col_name="TAKEN_DATE")
        df = _rename(df, additional_mappings={"TEXT": "test_result", "TEST_DATE": "test_datetime", "TAKEN_DATE": "collection_datetime"})
        df["valid"] = df.test_result.apply(lambda x: int(x != "Issue with result"))
        df["test_name"] = "Covid19-PCR"
        self._insert(df=df, table_name="Microbiology")

    def _comorbid(self):
        """
        Populate the Comorbid table

        Returns
        -------
        None
        """
        self.vprint("---- Populating Comorbid Table ----")
        for file in self.comorbid_files:
            df = safe_read(self._get_path(file))
            df.drop(["AGE", "GENDER", "ADMISSION_DATE", "TAKEN_DATE"], axis=1, inplace=True)
            df = _rename(df, additional_mappings={"SOLIDORGANTRANSPLANT": "solid_organ_transplant",
                                                  "CANCER": "cancer",
                                                  "SEVERERESPIRATORY": "severe_resp",
                                                  "SEVERESINGLEORGANDISEASE": "severe_single_organ_disease",
                                                  "RAREDISEASES": "rare_disease",
                                                  "IMMUNOSUPPRESSION": "immunosuppressed",
                                                  "PREGNANCYWITHCONGHEARTDIS": "pregnant_with_cong_heart_disease",
                                                  "GPIDENTIFIED_PATIENTS": "gp_identified_patients",
                                                  "RENAL_DIALYSIS": "renal_dialysis",
                                                  "OTHER": "other",
                                                  "TEST_DATE": "datetime"})
            df = self._get_date_time(df=df, col_name="datetime")
            df.drop("request_location", axis=1, inplace=True)
            self._insert(df=df, table_name="Comorbid")

    def _haem(self):
        """
        Populate the ComplexHaematology table

        Returns
        -------
        None
        """
        self.vprint("---- Populating ComplexHaematology Table ----")
        for file in progress_bar(self.haem_files, verbose=self.verbose):
            df = safe_read(self._get_path(file))
            df.drop(["AGE", "GENDER", "ADMISSION_DATE"], axis=1, inplace=True)
            df = self._get_date_time(df, col_name="TEST_DATE")
            df = self._get_date_time(df, col_name="TAKEN_DATE")
            # pull out the sample type
            df["test_name"] = None
            df["test_result"] = None
            df["test_category"] = file
            df = _rename(df, {"TEXT": "raw_text", "TEST_DATE": "test_datetime", "TAKEN_DATE": "collection_datetime"})
            self._insert(df=df, table_name="ComplexHaematology")

    def _covid_status(self, df: pd.DataFrame):
        """
        Given the Patients DataFrame, search the Covid19 file and determine a patients COVID-19 status:
            * Positive = one or more instances of the patient being declared positive
            * Negative = no record of positive COVID-19 and NOT all COVID status are equal to "In Progress"
            * Unknown = Either no value found for the patient or all COVID status values equal to "In Progress"
        Parameters
        ----------
        df: Pandas.DataFrame
        Returns
        -------
        Pandas.DataFrame
            Modified Pandas DataFrame with covid_status column
        """
        covid = safe_read(self._get_path("Covid19"))
        covid = self._get_date_time(covid, col_name="TEST_DATE")
        covid = self._get_date_time(covid, col_name="TAKEN_DATE")
        covid = covid.rename({"TEST_DATE": "test_datetime", "TAKEN_DATE": "collection_datetime"}, axis=1)
        covid["collection_datetime"] = pd.to_datetime(covid["collection_datetime"])
        covid["test_datetime"] = pd.to_datetime(covid["collection_datetime"])
        covid_status = list()
        if self.verbose:
            patient_ids = tqdm(df.patient_id.values)
        else:
            patient_ids = df.patient_id.values
        for pt_id in patient_ids:
            covid_status.append(search_covid_results(patient_id=pt_id, covid_df=covid))
        df["covid_status"] = [x[0] for x in covid_status]
        df["covid_date_first_positive"] = [x[1] for x in covid_status]
        return df

    def _register_death(self, df: pd.DataFrame):
        """
        Given the Patient DataFrame, search the Outcomes file for record of patient death

        Parameters
        ----------
        df: Pandas.DataFrame
        Returns
        -------
        Pandas.DataFrame
            Modified Pandas DataFrame with death column
        """
        events = safe_read(self._get_path("Outcomes"))[["PATIENT_ID", "DESTINATION"]]
        death_status = list()
        for pt_id in progress_bar(df.patient_id.unique(), verbose=self.verbose):
            pt_events = events[events.PATIENT_ID == pt_id]
            pt_events = pt_events[pt_events.DESTINATION.isin(self.died_events)]
            if pt_events.shape[0] == 0:
                death_status.append(0)
            else:
                death_status.append(1)
        df["death"] = death_status
        return df

    def _patients(self):
        """
        Populate Patients table

        Returns
        -------
        None
        """
        self.vprint("---- Populating Patients Table ----")
        self.vprint("...create basic table")
        df = safe_read(self._get_path("People"))
        df = df[df.TEST_PATIENT == "N"]
        df.drop("TEST_PATIENT", axis=1, inplace=True)
        df = self._get_date_time(df, col_name="DATE_FROM")
        df = self._get_date_time(df, col_name="DATE_ENTERED")
        df = df.rename({"PATIENT_ID": "patient_id",
                        "AGE": "age",
                        "GENDER": "gender",
                        "DATE_FROM": "date_from",
                        "DATE_ENTERED": "date_entered"},
                       axis=1)
        self.vprint("...populate with COVID-19 status")
        df = self._covid_status(df)
        self.vprint("...populate with survival status")
        df = self._register_death(df)
        self._insert(df=df, table_name="Patients")

    def _critical_care(self):
        """
        Populate CritCare table

        Returns
        -------
        None
        """
        self.vprint("---- Populating Critical Care Table ----")
        df = safe_read(self._get_path("CritCare"))
        df = self._get_date_time(df, col_name="UNIT_ADMIT_DATE")
        df = self._get_date_time(df, col_name="UNIT_DISCH_DATE")
        df = _rename(df, {"request_location": "location",
                          "UNIT": "unit",
                          "UNIT_OUTCOME": "unit_outcome",
                          "HOSP_OUTCOME": "hospital_outcome",
                          "VENTILATOR": "ventilated",
                          "COVID19_STATUS": "covid_status",
                          "UNIT_ADMIT_DATE": "unit_admit_datetime",
                          "UNIT_DISCH_DATE": "unit_discharge_datetime",
                          "HEIGHT": "height",
                          "WEIGHT": "weight",
                          "AP2": "apache2_score",
                          "ETHNICITY": "ethnicity",
                          "RENALRT": "renal_treatment",
                          "MECHANICALVENTILATION": "ventilated",
                          "DAYSVENTILATED": "ventilated_days",
                          "RADIOTHERAPY": "radiotherapy"})
        self._insert(df=df, table_name="CritCare")

    def _radiology(self):
        """
        Populate Radiology table

        Returns
        -------
        None
        """
        self.vprint("---- Populate Radiology Table ----")
        self.vprint("....processing CT Angiogram pulmonary results")
        df = safe_read(self._get_path("CTangio"))
        df.drop(["AGE", "GENDER", "ADMISSION_DATE"], axis=1, inplace=True)
        df = self._get_date_time(df, col_name="TEST_DATE")
        df = self._get_date_time(df, col_name="TAKEN_DATE")
        df["test_category"] = "CTangio"
        df = _rename(df, additional_mappings={"TEXT": "raw_text", "TEST_DATE": "test_datetime", "TAKEN_DATE": "collection_datetime"})
        self._insert(df=df, table_name="Radiology")
        self.vprint("....processing X-ray results")
        df = safe_read(self._get_path("XRChest"))
        df.drop(["AGE", "GENDER", "ADMISSION_DATE"], axis=1, inplace=True)
        df = self._get_date_time(df, col_name="TEST_DATE")
        df = self._get_date_time(df, col_name="TAKEN_DATE")
        df["test_category"] = "XRChest"
        df = _rename(df, additional_mappings={"TEXT": "raw_text", "TEST_DATE": "test_datetime", "TAKEN_DATE": "collection_datetime"})
        self._insert(df=df, table_name="Radiology")

    def _events(self):
        """
        Populate Events table

        Returns
        -------
        None
        """
        self.vprint("---- Populate Events Table ----")
        df = safe_read(self._get_path(self.events_files[0]))
        df.drop(["WIMD", "GENDER"], axis=1, inplace=True)
        df = self._get_date_time(df, col_name="EVENT_DATE")
        df["death"] = df.DESTINATION.apply(lambda x: int(any([i in str(x) for i in self.died_events])))
        df = df.rename({"PATIENT_ID": "patient_id",
                        "COMPONENT": "component",
                        "EVENT_TYPE": "event_type",
                        "COVID_STATUS": "covid_status",
                        "SOURCE_TYPE": "source_type",
                        "SOURCE": "source",
                        "DESTINATION": "destination",
                        "CRITICAL_CARE": "critical_care",
                        "EVENT_DATE": "event_datetime"}, axis=1)
        self._insert(df=df, table_name="Events")

    def _test_units(self):
        self.vprint("---- Populate Units Table ----")
        df = safe_read(self._get_path("TestUnits"))
        self._insert(df=df, table_name="Units")

    def populate(self):
        """
        Populate all tables.

        Returns
        -------
        None
        """
        self.vprint("=============== Populating database ===============")
        self.vprint("\n")
        self._patients()
        self._comorbid()
        self._events()
        self._pathology()
        self._microbiology()
        self._radiology()
        self._critical_care()
        self._haem()
        self._test_units()
        self.vprint("\n")
        self.vprint("Complete!....")
        self.vprint("====================================================")

    def create_indexes(self):
        """
        Generate some useful indexes (ASSUMES THAT POPULATE METHOD HAS BEEN PREVIOUSLY CALLED)

        Returns
        -------
        None
        """
        self.vprint("=============== Generating Indexes ===============")
        sql_statements = ["CREATE INDEX crit_care_pt_id ON CritCare (patient_id)",
                          "CREATE INDEX events_pt_id ON Events (patient_id)",
                          "CREATE INDEX radiology_pt_id ON Radiology (patient_id)",
                          "CREATE INDEX haem_pt_id ON ComplexHaematology (patient_id)",
                          "CREATE INDEX micro_pt_id ON Microbiology (patient_id)",
                          "CREATE INDEX path_pt_id ON Pathology (patient_id)",
                          "CREATE INDEX crit_care_ventilated ON CritCare (ventilated)",
                          "CREATE INDEX crit_care_covid ON CritCare (covid_status)",
                          "CREATE INDEX type_event ON Events (event_type)",
                          "CREATE INDEX covid_event ON Events (covid_status)",
                          "CREATE INDEX test_haem ON ComplexHaematology (test_category)",
                          "CREATE INDEX name_micro ON Microbiology (test_name)",
                          "CREATE INDEX result_micro ON Microbiology (test_result)",
                          "CREATE INDEX name_path ON Pathology (test_name)",
                          "CREATE INDEX cat_path ON Pathology (test_category)"]
        for x in progress_bar(sql_statements, verbose=self.verbose):
            self._curr.execute(x)
            self._connection.commit()
        self.vprint("\n")
        self.vprint("Complete!....")
        self.vprint("====================================================")

    def close(self):
        """
        Close database connection.

        Returns
        -------
        None
        """
        self._connection.close()