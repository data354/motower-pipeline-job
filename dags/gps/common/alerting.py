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
        email = CONFIG["default_adress"]
        return email
    print(CONFIG["api_mails"])
    objets = requests.get(CONFIG["api_mails"], timeout=15).json()
    objet =  next((obj for obj in objets if obj["typeFichier"] == code), None)
    if objet is None:
        raise ValueError(f"emails of type {code} not available")
    email = objet.get("email", CONFIG["default_adress"])
    if len(email) == 0:
        email = CONFIG["default_adress"]
    return email

def send_email(host, port, user, receivers, subject, content):
    """
    Function to send email
    """
    logging.info("Sending mail ... ")
    try:
        smtp_server = smtplib.SMTP(host, port)
        for receiver in receivers:
            smtp_server.sendmail(user, receiver, f"Subject: {subject}\n{content}") # sending the mail
            logging.info("Email sent successfully :) to %s", receiver)
        smtp_server.quit()
    except smtplib.SMTPException as e:
        logging.error("Failed to send email: %s", str(e))

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
            Une erreur est survenue sur la tache {task_id} du {dag_id} du projet MOTOWER.
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
    logging.info("send alert mail to %s", ', '.join(receivers))
    subject = "Rapport d'erreur sur MOTOWER"
    send_email(kwargs["host"],kwargs["port"], kwargs["user"], receivers, subject, email_content)
