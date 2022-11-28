from flask import Flask, render_template, request, send_file
import requests
import os
import pandas as pd
from data_ingestion.data_ingestion import DataIngestion
from utils.mysql_db_connection import MySQLDBConnection
from utils.postgres_db_connection import PostgresDBConnection
from utils.make_upload_dir import make_upload_dir
from utils.get_api_url import get_api_url
from flask_cors import cross_origin, CORS
from logger.logger import MongoLogger

app = Flask(__name__, template_folder='templates', static_folder='static')
CORS(app)
# Columns in the order required for API
INPUT_COLUMNS = ["age", "workclass", "fnlwgt", "education", "education_num", "marital_status", "occupation",
                 "relationship",
                 "race", "sex", "capital_gain", "capital_loss", "hours_per_week", "country"]
API = get_api_url()  # get the URL of API. if no env var is present it defaults to localhost


@app.route("/")
@cross_origin()
def index():
    """
    displays the index.html page
    """
    logger = MongoLogger()
    logger.log_to_db(level="INFO", message="entering index.html")
    return render_template("index.html")


@app.route("/db_input")
@cross_origin()
def db_input():
    """
        displays the db_input.html page
    """
    logger = MongoLogger()
    logger.log_to_db(level="INFO", message="entering db_input.html")
    return render_template("db_input.html")


@app.route("/file_input")
@cross_origin()
def file_input():
    """
        displays the file_input.html page
    """
    logger = MongoLogger()
    logger.log_to_db(level="INFO", message="entering file_input.html")
    return render_template("file_input.html")


@app.route("/help")
@cross_origin()
def help_():
    """
        displays the help.html page
    """
    logger = MongoLogger()
    logger.log_to_db(level="INFO", message="entering help.html")
    return render_template("help.html")


@app.route("/", methods=["POST"])
@cross_origin()
def form_prediction():
    """
    Returns the API response based on the inputs filled in the form.
    """
    logger = MongoLogger()
    try:
        logger.log_to_db(level="INFO", message="clicked predict in index.html (form prediction)")
        logger.log_to_db(level="INFO", message="entering form_prediction")
        # assigning the inputs from the form to respective variables.
        age = request.form["age"]
        age = age if age != "" else -1

        workclass = request.form["workclass"]
        workclass = workclass if request.form["workclass"] != "nan" else "?"

        fnlwgt = request.form["fnlwgt"]
        fnlwgt = fnlwgt if fnlwgt != "" else -1

        education = request.form["education"]
        education = education if request.form["education"] != "nan" else "?"

        nedu = request.form["nedu"]
        nedu = nedu if nedu != "" else -1

        marital = request.form["marital"]
        marital = marital if request.form["marital"] != "nan" else "?"

        occupation = request.form["occupation"]
        occupation = occupation if request.form["occupation"] != "nan" else "?"

        relationship = request.form["relationship"]
        relationship = relationship if request.form["relationship"] != "nan" else "?"

        race = request.form["race"]
        race = race if request.form["race"] != "nan" else "?"

        sex = request.form["sex"]
        sex = sex if request.form["sex"] != "nan" else "?"

        cgain = request.form["cgain"]
        cgain = cgain if cgain != "" else -1

        closs = request.form["closs"]
        closs = closs if closs != "" else -1

        nhours = request.form["nhours"]
        nhours = nhours if nhours != "" else -1

        country = request.form["country"]
        country = country if request.form["country"] != "nan" else "?"

        # input json to the API in the required format
        request_json = {"age": age,
                        "workclass": workclass,
                        "fnlwgt": fnlwgt,
                        "education": education,
                        "education_num": nedu,
                        "marital_status": marital,
                        "occupation": occupation,
                        "relationship": relationship,
                        "race": race,
                        "sex": sex,
                        "capital_gain": cgain,
                        "capital_loss": closs,
                        "hours_per_week": nhours,
                        "country": country
                        }

        error = ""
        response = requests.post(url=API, json=request_json)
        if response.status_code == 200:
            result = f'Prediction: {response.json().get("result")}'
        else:
            result = f'Error: Details at the bottom'
            error = response.json()
            logger.log_to_db(level="CRITICAL", message=error)
    except Exception as e:
        result = f'Error: Details at the bottom'
        error = e
        logger.log_to_db(level="CRITICAL", message=str(error))
    logger.log_to_db(level="INFO", message="exiting form_prediction")
    return render_template("index.html", result=result, error=error)


@app.route("/db_input", methods=["POST"])
@cross_origin()
def db_prediction():
    """
    Predicts the samples with missing target in a database by passing each sample to the API.
    The prediction is updated to each sample based on the specified id (primary key) column.
    """
    result = ""
    error = ""
    db_conn = None
    logger = MongoLogger()
    try:
        logger.log_to_db(level="INFO", message="clicked submit in db_input.html")
        logger.log_to_db(level="INFO", message="entering db_prediction")

        # assigning the input database credentials to the respective variables
        db_name = request.form["db_name"]
        server = request.form["server"]
        database = request.form["database"]
        username = request.form["username"]
        table = request.form["tbl"]
        password = request.form["password"]
        port = request.form["port"]
        id_col = request.form["id_col"]
        target_col = request.form["target_col"]

        if db_name == 'postgres':
            # ingesting the data from database based on the input database credentials.
            query = f"SELECT * FROM {table} WHERE {target_col} IS NULL"
            data_ingestion = DataIngestion(name="postgres", query=query, host=server,
                                           database=database, username=username, password=password,
                                           port=port)
            db_conn = PostgresDBConnection(host=server,
                                           database=database, username=username, password=password,
                                           port=port).connect()
            db_conn.autocommit = False
        else:
            query = f"SELECT * FROM {database}.{table} WHERE {target_col} IS NULL"
            data_ingestion = DataIngestion(name="mysql", query=query, host=server,
                                           database=database, username=username, password=password,
                                           port=port)
            db_conn = MySQLDBConnection(host=server, username=username, password=password,
                                        port=port).connect()

        cursor = db_conn.cursor()
        df = data_ingestion.ingest_data()

        if len(df) > 0:  # checking if there are any samples with missing target variable
            ids = df[id_col].copy()
            if len(ids) != len(ids.unique()):  # checking if the specified id column has unique values.
                raise Exception(f"{id_col} has duplicate values")  # raise an exception in case of duplicate vals in id

            for idx in ids:  # iterating through each id to update the prediction of each sample.
                df_row = df.loc[df[id_col] == idx, INPUT_COLUMNS].copy()
                # replacing None with -1 which will be considered as missing by API
                df_row.replace([None], -1, inplace=True, regex=False)
                # converting the sample / row from dataframe to json
                request_json = df_row.to_dict('records')[0]
                response = requests.post(url=API, json=request_json)
                if response.status_code == 200:
                    if db_name == "postgres":
                        cursor.execute(f"UPDATE {table} SET {target_col}='{response.json().get('result')}'" +
                                       f" WHERE {id_col}={idx}")
                    else:
                        cursor.execute(
                            f"UPDATE {database}.{table} SET {target_col}=\"{response.json().get('result')}\"" +
                            f" WHERE {id_col}={idx}")
                    result = "Success"
                else:
                    # return error and rollback in case the status code of a sample isn't 200
                    result = "Error: Details at the bottom"
                    error = response.json()
                    logger.log_to_db(level="CRITICAL", message=error)
                    db_conn.rollback()
                    break

            db_conn.commit()
            cursor.close()
            db_conn.close()
        else:
            result = f"{target_col} has nothing to predict"
    except Exception as e:
        result = "Error: Details at the bottom"
        error = e
        logger.log_to_db(level="CRITICAL", message=str(error))
        if db_conn is not None:
            db_conn.rollback()
            db_conn.close()
    logger.log_to_db(level="INFO", message="exiting db_prediction")
    return render_template("db_input.html", result=result, error=error)


@app.route("/file_input", methods=["POST"])
@cross_origin()
def file_prediction():
    """
    Inputs a csv / Excel file with input data and appends prediction for each sample to the file.
    """
    logger = MongoLogger()
    try:
        logger.log_to_db(level="INFO", message="clicked submit in file_input.html")
        logger.log_to_db(level="INFO", message="entering file_prediction")
        result = ""
        error = ""
        upload_file_path = ""
        output_file_path = ""
        # setting the UPLOAD_FOLDER app config var to ./uploads
        app.config['UPLOAD_FOLDER'] = os.path.join('.', 'uploads')
        # delete the current ./uploads directory and make a new one
        make_upload_dir(app.config['UPLOAD_FOLDER'])
        upload_file = request.files['fileinput']
        upload_filename = upload_file.filename
        # assigning the target variable name to be given to the prediction column
        target_col = request.form["target_col"]
        upload_file_path = os.path.join(app.config['UPLOAD_FOLDER'], upload_filename)
        # saving the uploaded file to the ./uploads directory for further processing.
        upload_file.save(upload_file_path)
        if upload_filename.endswith(".csv"):
            upload_df = pd.read_csv(os.path.join(upload_file_path))
        else:
            upload_df = pd.read_excel(os.path.join(upload_file_path))

        if len(upload_df) > 0:  # checking if the uploaded file isn't empty
            # upload_df = upload_df.loc[:10].copy()  # delete later
            upload_df = upload_df.reset_index(drop=True).copy()
            # replacing - in column names with _
            upload_df.columns = [col.replace("-", "_") for col in upload_df.columns]
            input_df = upload_df[INPUT_COLUMNS].copy()

            prediction = []
            for row in input_df.index:
                request_json = input_df.loc[row].to_dict()
                response = requests.post(url=API, json=request_json)
                if response.status_code == 200:
                    prediction.append(response.json().get('result'))
                else:
                    result = "Error: Details at the bottom"
                    os.remove(upload_file_path)
                    raise Exception(response.json())

            prediction_df = upload_df.copy()
            prediction_df[target_col] = prediction
            os.remove(upload_file_path)  # deleting the uploaded file since it's of no use anymore.
            output_filename = "prediction_" + upload_filename
            output_file_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)  # output file path
            if output_filename.endswith(".csv"):
                prediction_df.to_csv(output_file_path, index=False)
            else:
                prediction_df.to_excel(output_file_path, index=False)
            return send_file(output_file_path, as_attachment=True)  # auto downloading the prediction file on success
        else:
            result = "Nothing to predict"
            os.remove(upload_file_path)

    except Exception as e:
        result = "Error: Details at the bottom"
        error = e
        logger.log_to_db(level="CRITICAL", message=str(error))
    return render_template("file_input.html", result=result, error=error)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)
