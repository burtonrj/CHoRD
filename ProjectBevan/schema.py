import sqlite3
import os


def _schema():
    """
    Generates list of SQL queries for generating standard tables for sqlite3 database

    Returns
    -------
    list
        List of string values containing SQL queries for each table
    """
    patients = """
        CREATE TABLE Patients(
        patient_id TEXT PRIMARY KEY,
        age INTEGER,
        gender TEXT DEFAULT "U",
        wimd REAL,
        date_from TEXT,
        time_from REAL,
        date_entered TEXT,
        time_entered REAL,
        covid_status TEXT DEFAULT "U",
        admission_date TEXT
        admission_time REAL,
        death INTEGER DEFAULT 0
        );
    """
    comorbid = """
            CREATE TABLE Comorbid(
            patient_id TEXT PRIMARY KEY,
            solid_organ_transplant INTEGER,
            cancer INTEGER,
            severe_resp INTEGER,
            severe_single_organ_disease INTEGER,
            rare_disease INTEGER,
            immunosuppressed INTEGER,   
            pregnant_with_cong_heart_disease INTEGER,
            gp_identified_patients INTEGER,
            renal_dialysis INTEGER,
            other INTEGER
            );
        """
    path = """
            CREATE TABLE Pathology(
            patient_id TEXT PRIMARY KEY,
            request_location TEXT,
            test_date TEXT,
            test_time REAL,
            test_name TEXT,
            test_category TEXT,
            test_result TEXT,
            valid INTEGER DEFAULT 1
            );
        """
    micro = """
            CREATE TABLE Microbiology(
            patient_id TEXT PRIMARY KEY,
            request_location TEXT,
            test_date TEXT,
            test_time REAL,
            test_name TEXT,
            test_category TEXT,
            test_result TEXT,
            raw_text TEXT,
            valid INTEGER DEFAULT 1,
            sample_type TEXT
            );
        """
    haem = """
                CREATE TABLE ComplexHaematology(
                patient_id TEXT PRIMARY KEY,
                request_location TEXT,
                test_date TEXT,
                test_time REAL,
                test_name TEXT,
                test_category TEXT,
                test_result TEXT,
                raw_text TEXT,
                valid INTEGER DEFAULT 1,
                sample_type TEXT
                );
            """
    radiology = """
            CREATE TABLE Radiology(
            patient_id TEXT PRIMARY KEY,
            request_location TEXT,
            test_date TEXT,
            test_time REAL,
            test_category TEXT,
            raw_text TEXT,
            valid INTEGER DEFAULT 1
            );
            """
    event = """
            CREATE TABLE Events(
            patient_id TEXT PRIMARY KEY,
            component TEXT,
            event_type TEXT NOT NULL,
            event_date TEXT NOT NULL,
            event_time REAL,
            covid_status TEXT DEFAULT "U",
            death TEXT DEFAULT "U",
            critical_care_admission INTEGER DEFAULT 0,
            source TEXT,
            source_type TEXT,
            destination TEXT
            );
        """
    critical_care = """
            CREATE TABLE CritCare(
            patient_id TEXT PRIMARY KEY,
            location TEXT,
            unit_admit_date TEXT,
            unit_admit_time REAL,
            unit_discharge_date TEXT,
            unit_discharge_time REAL,
            icu_length_of_stay INTEGER,
            ventilator INTEGER,
            covid_status TEXT DEFAULT "U"
            );
        """
    return [patients,
            event,
            critical_care,
            comorbid,
            radiology,
            path,
            micro,
            haem]


def create_database(db_path: str,
                    overwrite: bool = False,
                    **kwargs):
    """
    Generate a new local unpopulated SQLite database following the standard schema for IDWT project

    Parameters
    ----------
    db_path: str
        Path where new database file is save
    overwrite: bool
        How to handle existing database file. If True and database file exists, database will be deleted and replaced
        witn new unpopulated data
    kwargs
        Additional keyword arguments to pass to sqlite3.conntect() call
    Returns
    -------
    None
    """
    if os.path.exists(db_path):
        if overwrite:
            os.remove(db_path)
        else:
            raise ValueError("Database already exists, set overwrite to True too drop database and generate "
                             "a new database, otherwise specify a new path")
    conn = sqlite3.connect(db_path, **kwargs)
    curr = conn.cursor()
    for x in _schema():
        curr.execute(x)
    conn.commit()
    conn.close()
