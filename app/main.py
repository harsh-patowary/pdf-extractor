import os
from flask import Flask, flash, request, redirect, render_template
from werkzeug.utils import secure_filename
from multiprocessing import Pool
# from extracter import Extract
from dataclasses import field
from email.policy import strict
from lib2to3.pgen2.grammar import opmap_raw
import logging
from pathlib import Path
from multiprocessing import Pool
from ssl import Options
from typing_extensions import Self
import PyPDF2
from typing import Optional
import re
import pandas as pd


app = Flask(__name__)

app.secret_key = "secret key"
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024

# Get current path
path = os.getcwd()
# file Upload
UPLOAD_FOLDER = os.path.join(path, 'uploads')


def _get_logger(name: str, level: str = "INFO"):
    # get named logger and set level
    logger = logging.getLogger(name)
    logger.setLevel(level=level)

    # set output channel and formatting
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger


LOGGER = _get_logger(__name__)

ROOT_DIR = r"/uploads"
ROOT_DIR = Path(ROOT_DIR)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Make directory if uploads is not exists
if not os.path.isdir(UPLOAD_FOLDER):
    os.mkdir(UPLOAD_FOLDER)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Allowed extension you can set your own
ALLOWED_EXTENSIONS = set(['txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'])


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/')
def upload_form():
    return render_template('upload.html')


@app.route('/', methods=['POST'])
def upload_file():
    if request.method == 'POST':

        if 'files[]' not in request.files:
            flash('No file part')
            return redirect(request.url)

        files = request.files.getlist('files[]')

        for file in files:
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

        flash('File(s) successfully uploaded')
        return redirect('/')


@app.route('/extracting', methods=['GET', 'POST'])
def extracting():

    glob_version = "*.pdf"
    pdf_path = ROOT_DIR.rglob(glob_version)
    with Pool() as p:
        extracted_data = p.map(parse_pdf(pdf_path), pdf_path)

    df = pd.DataFrame(extracted_data)
    df.to_csv("datafrompdf.csv")
    print("extracted!!")


def parse_pdf(path: Path) -> dict:
    # LOGGER.info(f"Parsing {path.relative_to(ROOT_DIR)}")

    # prepare data to be returned
    data = {}

    # get email separately due to possible errors
    try:
        data["email"] = extract_email(path)
        data["phone"] = extract_phonenum(path)
    # except PyPDF2.utils.PdfReadError as e:
    #     # typically due to PDF being encrypted/locked
    #     logger.error(f"Failed to open {path.name}: {e}")
    except Exception as e:
        # don't want misc errors crashing the entire script
        # better to have a few blank emails
        logger.error(f"Failed to parse {path}: {e}")

    return data


def validate_email_string(text: str) -> Optional[str]:
    results = set()
    # email_regex = re.compile(r"[a-z]+.w_@gmail.com")
    emails1 = set(re.findall(
        r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.com", text, re.I))
    emails2 = set(re.findall(
        r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.in", text, re.I))
    emails3 = set(re.findall(
        r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.org", text, re.I))
    emails4 = set(re.findall(
        r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.sg", text, re.I))
    emails5 = set(re.findall(
        r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.edu", text, re.I))
    # results = re.findall(email_regex, text)
    results.update(emails1)
    results.update(emails2)
    results.update(emails3)
    results.update(emails4)
    results.update(emails5)
    if results:
        return results


def validate_phonenum(text: str) -> Optional[str]:
    results = set()
    ph_number = set(re.findall(r'[\+\(]?[1-9][0-9 .\-\(\)]{8,}[0-9]', text))
    results.update(ph_number)
    if results:
        return results


def get_email_from_form(reader: PyPDF2.PdfFileReader) -> Optional[str]:
    try:
        fields = reader.getFormTextFields()
        for field_key, field_text in fields.items():
            if 'email' in field_key.lower():
                email = validate_email_string(field_text)
                if email:
                    return email
    except TypeError:
        pass


def get_email_from_pages(reader: PyPDF2.PdfFileReader) -> Optional[str]:
    # some PDFs had the email elsewhere in the document
    # need to iterate through the pages
    for page in reader.pages:
        text = page.extractText()
        email = validate_email_string(text)

        if email:
            return email


def get_phonenum_form(reader: PyPDF2.PdfFileReader) -> Optional[str]:
    try:
        fields = reader.getFormTextFields()
        for field_key, field_text in fields.items():
            if 'phone' in field_key.lower():
                phone = validate_phonenum(field_text)
                if phone:
                    return phone
    except TypeError:
        pass


def get_phonenum_pages(reader: PyPDF2.PdfFileReader) -> Optional[str]:
    for page in reader.pages:
        text = page.extractText()
        phonenum = validate_phonenum(text)

        if phonenum:
            return phonenum


def extract_email(path: Path) -> Optional[str]:
    with open(path, "rb") as pdf:
        reader = PyPDF2.PdfFileReader(pdf, strict=False)
        email = get_email_from_form(reader)
        if not email:
            email = get_email_from_pages(reader)
        if email:
            logger.info(
                f"Emails found in {path.relative_to(ROOT_DIR)}: {email}")
            return email


def extract_phonenum(path: Path) -> Optional[str]:
    with open(path, "rb") as pdf:
        reader = PyPDF2.PdfFileReader(pdf, strict=False)
        phone_number = get_phonenum_form(reader)
        if not phone_number:
            phone_number = get_phonenum_pages(reader)
        if phone_number:
            logger.info(
                f"Phone Numbers found in {path.relative_to(ROOT_DIR)}: {phone_number}")
            return phone_number


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=5000, debug=False, threaded=True)
