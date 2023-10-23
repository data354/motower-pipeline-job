import traceback
import smtplib
import logging
import requests
from gps import CONFIG



def get_receivers(code: str):
    """
     get receivers from api
    """
    if code in ["trafic", "congestion"]:
        email = ['jean-louis.gbadi@orange.com']
        return email
    objets = requests.get(CONFIG["api_mails"], timeout=15).json()
    objet =  next((obj for obj in objets if obj["typeFichier"] == code), None)
    if objet is None:
        raise ValueError("emails of type %s not available", code)
    email = objet.get("email", ['jean-louis.gbadi@orange.com'])
    if len(email) == 0:
        email = ['jean-louis.gbadi@orange.com']
    return email


def send_email(host, port, user, receivers, subject, content):
    """
     function to send email
    """
    logging.info("Sending mail ... ")
    for receiver in receivers:
        smtp_server = smtplib.SMTP(host, port)
        smtp_server.sendmail(user, receiver, f"Subject: {subject}\n{content}") # sending the mail
        logging.info(f"Email sent succefully :) to {receiver} ")


def alert_failure(**kwargs):
    """
        send mail on failure
    
    """
   
    task_id = kwargs["task_id"]
    dag_id = kwargs["dag_id"]

    exec_date = kwargs["exec_date"]
    exception = kwargs["exception"]
    formatted_exception = ''.join(
                traceback.format_exception(type(exception),
                                            value=exception,
                                            tb=exception.__traceback__)).strip()
    email_content = f"""
            Une erreur est survenue sur la tache {task_id} de OMER.
            *Erreur survenue*: {exception}
            -----------------------------------------------------------------------------

            *Message pour les developpeurs*: 
            *date*: {exec_date.split("T")[0]}
            *heure*: {(exec_date.split("T")[1]).split("+")[0]}
            *Task id*: {task_id}
            *dag id* : {dag_id}
            *Traceback*: 
            {formatted_exception}

            """
    # send email for alerting
    receivers = CONFIG['airflow_receivers']
    logging.info(f"send alert mail to {', '.join(receivers)}")
    subject = "Rapport d'erreur sur GPS"
    send_email(kwargs["host"],kwargs["port"], kwargs["user"], receivers, subject, email_content)
