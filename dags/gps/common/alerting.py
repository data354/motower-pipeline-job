import traceback
import smtplib
import logging
import requests
from gps import CONFIG


def send_email(host, port, user, receivers, subject, content):
    """
     function to send email
    """
    for receiver in receivers:
        smtp_server = smtplib.SMTP(host, port)
        smtp_server.sendmail(user, receiver, f"Subject: {subject}\n{content}") # sending the mail
    logging.info("Email sent succefully :) ")


def alert_failure(**kwargs):
    """
        send mail on failure
    
    """
    # log_url = context.get("task_instance").log_url
    #task_id = context['task'].task_id
    task_id = kwargs["task_id"]
    dag_id = kwargs["dag_id"]
    #dag_id = context['task'].dag_id

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
    reponse = requests.get(CONFIG["api_mails"], timeout=500).content
    #validation_receivers  = [rec["email"] for rec in reponse if rec["typeFichier"] == kwargs["type_fichier"]]
    validation_receivers = CONFIG['airflow_receivers']
    logging.info(f"send alert mail to {', '.join(validation_receivers)}")
    subject = "Rapport d'erreur sur GPS"
    send_email(kwargs["host"],kwargs["port"], kwargs["user"], validation_receivers, subject, email_content)
